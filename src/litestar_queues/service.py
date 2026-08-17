import asyncio
import contextlib
import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from inspect import isawaitable, iscoroutinefunction
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from typing_extensions import Self

from litestar_queues._correlation import (
    bind_correlation_id,
    capture_correlation_id,
    preload_correlation_context,
    reset_correlation_id,
)
from litestar_queues._identity import IDENTITY_VERSION, arguments_identity, task_identity
from litestar_queues.backends.base import interruption_count
from litestar_queues.config import execution_backend_name, queue_backend_name
from litestar_queues.events.context import TaskExecutionContext, bind_task_context
from litestar_queues.events.models import QueueEvent, QueueEventActor
from litestar_queues.events.producer import QueueEventProducer
from litestar_queues.events.sinks import _call_optional_lifecycle, _select_lifecycle_error
from litestar_queues.exceptions import JobCancelledError, NonRetryableError, QueueConfigurationError
from litestar_queues.execution import get_execution_backend
from litestar_queues.task import (
    ScheduleConfig,
    Task,
    TaskResult,
    _ensure_utc,
    _run_sync_callable,
    get_scheduled_tasks,
    get_task_registry,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from types import TracebackType
    from uuid import UUID

    from litestar_queues.backends import BaseQueueBackend
    from litestar_queues.config import QueueConfig
    from litestar_queues.events import QueueEventLog, QueueEventPublisher
    from litestar_queues.execution import BaseExecutionBackend
    from litestar_queues.models import QueuedTaskRecord, QueueStatistics, StaleTaskRecoveryResult, TaskReservation
    from litestar_queues.observability import QueueObservabilityRuntimeProtocol

__all__ = ("QueueService",)

_CLOUD_TASKS_BACKEND = "cloudtasks"
INTERRUPTION_LIMIT_ERROR = "Interrupted during shutdown"


def _utc_now() -> "datetime":
    """Return the current UTC instant.

    One indirection so a single operation reads the clock once and boundary
    checks can be tested without racing it.

    Returns:
        The current time in UTC.
    """
    return datetime.now(timezone.utc)


_LOG_LEVELS = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "warn": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}
_UNVERIFIED_PERSISTENCE = object()
_RESOURCE_BUFFER = "buffer"
_RESOURCE_EVENT_LOG = "event_log"
_RESOURCE_EXECUTION_BACKEND = "execution_backend"
_RESOURCE_QUEUE_BACKEND = "queue_backend"
_RESOURCE_SINK = "sink"
_RESOURCE_SYNC_EXECUTOR = "sync_executor"
_ROLLBACK_ORDER = (
    _RESOURCE_SYNC_EXECUTOR,
    _RESOURCE_BUFFER,
    _RESOURCE_SINK,
    _RESOURCE_EXECUTION_BACKEND,
    _RESOURCE_EVENT_LOG,
    _RESOURCE_QUEUE_BACKEND,
)
_CLOSE_ORDER = (
    _RESOURCE_EXECUTION_BACKEND,
    _RESOURCE_EVENT_LOG,
    _RESOURCE_BUFFER,
    _RESOURCE_QUEUE_BACKEND,
    _RESOURCE_SINK,
    _RESOURCE_SYNC_EXECUTOR,
)


async def _release_failed_forever_reservation(backend: "BaseQueueBackend", key: "str", reserved_id: "UUID") -> "None":
    """Release a confirmed-unpersisted reservation without deleting a successor."""
    persisted: "object | QueuedTaskRecord | None" = _UNVERIFIED_PERSISTENCE
    with contextlib.suppress(Exception):
        persisted = await backend.get_task(reserved_id)
    if persisted is None:
        with contextlib.suppress(Exception):
            await backend.reset_identity(key, expected_task_id=reserved_id)


async def _raise_forever_identity_collision(backend: "BaseQueueBackend", key: "str", reserved_id: "UUID") -> "None":
    """Release this reservation and reject a cross-policy active-key collision."""
    with contextlib.suppress(Exception):
        await backend.reset_identity(key, expected_task_id=reserved_id)
    msg = (
        "A unique_until='forever' enqueue collided with an active task under the same effective key, "
        "but the backend returned a different task ID. The existing task was preserved and this "
        "reservation was released. Do not reuse a deduplication key across uniqueness policies."
    )
    raise QueueConfigurationError(msg)


class QueueService:
    """High-level facade for queue and execution backends."""

    __slots__ = (
        "_config",
        "_event_log",
        "_event_publisher",
        "_execution_backend",
        "_is_open",
        "_logger",
        "_observability_runtime",
        "_opened_resources",
        "_queue_backend",
        "_sync_executor",
    )

    def __init__(
        self,
        config: "QueueConfig",
        *,
        queue_backend: "BaseQueueBackend | None" = None,
        execution_backend: "BaseExecutionBackend | None" = None,
        event_publisher: "QueueEventPublisher | None" = None,
        observability_runtime: "QueueObservabilityRuntimeProtocol | None" = None,
    ) -> "None":
        """Initialize the queue service."""
        self._config = config
        self._logger = logging.getLogger(config.names.logger("service"))
        self._queue_backend = queue_backend
        self._execution_backend = execution_backend
        self._event_log: "QueueEventLog | None" = None
        self._event_publisher = event_publisher
        self._is_open = False
        self._opened_resources: "frozenset[str]" = frozenset()
        if observability_runtime is None and config.observability is not None:
            from litestar_queues.observability import create_observability_runtime

            observability_runtime = create_observability_runtime(config.observability, namespace=config.names)
        self._observability_runtime = observability_runtime
        self._sync_executor: "ThreadPoolExecutor | None" = None
        if queue_backend is not None:
            self._configure_backend_observability(queue_backend)

    @property
    def config(self) -> "QueueConfig":
        """Queue configuration."""
        return self._config

    def get_queue_backend(self) -> "BaseQueueBackend":
        """Return the configured queue backend."""
        if self._queue_backend is None:
            self._queue_backend = self._config.get_queue_backend()
            self._configure_backend_observability(self._queue_backend)
        return self._queue_backend

    def _configure_backend_observability(self, backend: "BaseQueueBackend") -> "None":
        runtime = self._observability_runtime
        backend._set_transport_observability_runtime(  # noqa: SLF001
            runtime if runtime is not None and runtime.enabled else None
        )
        configure = getattr(backend, "_set_package_observability_enabled", None)
        if configure is not None:
            configure(bool(runtime is not None and runtime.enabled))
        configure_sqlcommenter = getattr(backend, "_set_sqlcommenter_enabled", None)
        if configure_sqlcommenter is not None:
            configure_sqlcommenter(bool(runtime is not None and getattr(runtime, "sqlcommenter_enabled", False)))

    def get_execution_backend(self) -> "BaseExecutionBackend":
        """Return the configured execution backend."""
        if self._execution_backend is None:
            self._execution_backend = self._config.get_execution_backend()
        return self._execution_backend

    def get_event_publisher(self) -> "QueueEventPublisher":
        """Return the configured event publisher."""
        if self._event_publisher is None:
            self._event_publisher = self._config.get_event_publisher()
        self._event_publisher.set_observability_runtime(self.observability_runtime)
        return self._event_publisher

    def get_event_producer(self) -> "QueueEventProducer":
        """Return a producer over this service's event publisher."""
        return QueueEventProducer(self.get_event_publisher())

    def get_event_log(self) -> "QueueEventLog | None":
        """Return the backend-owned durable event history, if configured.

        History is wired during :meth:`open` only when ``events.history`` is
        present; otherwise this returns ``None`` and history maintenance is a
        no-op.
        """
        return self._event_log

    @property
    def observability_runtime(self) -> "QueueObservabilityRuntimeProtocol":
        """Return the configured observability runtime."""
        if self._observability_runtime is None:
            from litestar_queues.observability import create_observability_runtime

            self._observability_runtime = create_observability_runtime(
                self._config.observability, namespace=self._config.names
            )
        return self._observability_runtime

    async def open(self) -> "Self":
        """Open queue and execution backends.

        Returns:
            The opened service.
        """
        if self._is_open:
            return self
        opened: "list[str]" = []
        try:
            preload_correlation_context()
            queue_backend = self.get_queue_backend()
            opened.append(_RESOURCE_QUEUE_BACKEND)
            await queue_backend.open()
            self._configure_event_log(queue_backend)
            if self._event_log is not None:
                opened.append(_RESOURCE_EVENT_LOG)
            execution_backend = self.get_execution_backend()
            opened.append(_RESOURCE_EXECUTION_BACKEND)
            await execution_backend.open()
            event_publisher = self.get_event_publisher()
            opened.append(_RESOURCE_SINK)
            await _call_optional_lifecycle(event_publisher.sink, "open")
            opened.append(_RESOURCE_BUFFER)
            event_publisher.start_buffer()
            if self._sync_executor is None:
                self._sync_executor = ThreadPoolExecutor(
                    max_workers=self._config.sync_thread_pool_size,
                    thread_name_prefix=cast("str", self._config.sync_thread_name_prefix),
                )
                opened.append(_RESOURCE_SYNC_EXECUTOR)
        except BaseException:
            await self._teardown_resources(frozenset(opened), rollback=True, raise_errors=False)
            raise
        self._opened_resources = frozenset(opened)
        self._is_open = True
        return self

    async def close(self) -> "None":
        """Close queue and execution backends."""
        opened = self._opened_resources
        if not self._is_open and not opened:
            return
        self._opened_resources = frozenset()
        self._is_open = False
        await self._teardown_resources(opened, rollback=False, raise_errors=True)

    async def _teardown_resources(self, opened: "frozenset[str]", *, rollback: "bool", raise_errors: "bool") -> "None":
        errors: "list[BaseException]" = []
        order = _ROLLBACK_ORDER if rollback else _CLOSE_ORDER
        for resource in order:
            if resource in opened:
                await self._teardown_resource(resource, errors)
        error = _select_lifecycle_error(errors)
        if raise_errors and error is not None:
            raise error

    async def _teardown_resource(self, resource: "str", errors: "list[BaseException]") -> "None":
        try:
            if resource == _RESOURCE_EXECUTION_BACKEND and self._execution_backend is not None:
                await self._execution_backend.close()
            elif resource == _RESOURCE_EVENT_LOG and self._event_log is not None:
                await self._event_log.flush_events()
            elif resource == _RESOURCE_BUFFER and self._event_publisher is not None:
                await self._event_publisher.stop_buffer()
            elif resource == _RESOURCE_QUEUE_BACKEND and self._queue_backend is not None:
                await self._queue_backend.close()
            elif resource == _RESOURCE_SINK and self._event_publisher is not None:
                await _call_optional_lifecycle(self._event_publisher.sink, "close")
            elif resource == _RESOURCE_SYNC_EXECUTOR and self._sync_executor is not None:
                self._sync_executor.shutdown(wait=True, cancel_futures=True)
        except BaseException as exc:
            errors.append(exc)
        finally:
            if resource == _RESOURCE_SYNC_EXECUTOR:
                self._sync_executor = None

    def _configure_event_log(self, queue_backend: "BaseQueueBackend") -> "None":
        event_log_config = self._config.events.history if self._config.events is not None else None
        if event_log_config is None:
            return
        event_log = queue_backend.get_event_log(event_log_config)
        if event_log is None:
            msg = (
                f"{type(queue_backend).__name__} does not support backend-managed queue event history; "
                "disable EventHistoryConfig or use a backend that supports durable event history."
            )
            raise QueueConfigurationError(msg)
        self._event_log = event_log
        self.get_event_publisher().set_event_log(event_log, strict=event_log_config.strict)

    async def __aenter__(self) -> "Self":
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: "type[BaseException] | None",  # noqa: PYI036
        exc_val: "BaseException | None",  # noqa: PYI036
        exc_tb: "TracebackType | None",  # noqa: PYI036
    ) -> "None":
        await self.close()

    async def enqueue(
        self,
        task: "str | Task[Any, Any]",
        *args: "Any",
        scheduled_at: "datetime | None" = None,
        run_after: "float | timedelta | None" = None,
        expires_in: "float | timedelta | None" = None,
        expires_at: "datetime | None" = None,
        key: "str | None" = None,
        queue: "str | None" = None,
        priority: "int | None" = None,
        retries: "int | None" = None,
        timeout: "float | None" = None,
        execution_backend: "str | None" = None,
        execution_profile: "str | None" = None,
        description: "str | None" = None,
        log_level: "str | None" = None,
        log_success: "bool | None" = None,
        requeue_on_stale: "bool | None" = None,
        metadata: "dict[str, Any] | None" = None,
        **kwargs: "Any",
    ) -> "TaskResult":
        """Enqueue a registered task.

        Returns:
            A result handle for the queued record.
        """
        task_obj = self.resolve_task(task)
        effective_key, identity_metadata = self._resolve_identity(task_obj, key, args, kwargs)
        effective_scheduled_at, effective_expires_at = _resolve_schedule_and_expiration(
            task_obj, scheduled_at=scheduled_at, run_after=run_after, expires_in=expires_in, expires_at=expires_at
        )
        effective_execution_backend = (
            execution_backend or task_obj.execution_backend or execution_backend_name(self._config.execution_backend)
        )
        effective_execution_profile = execution_profile if execution_profile is not None else task_obj.execution_profile
        effective_metadata = _task_metadata(
            task_obj,
            metadata=metadata,
            description=description,
            log_level=log_level,
            log_success=log_success,
            requeue_on_stale=requeue_on_stale,
            timeout=timeout,
        )
        self._apply_log_success_default(effective_metadata)
        effective_metadata.update(identity_metadata)
        effective_queue = queue if queue is not None else task_obj.queue

        configured_execution = execution_backend_name(self._config.execution_backend)
        if _CLOUD_TASKS_BACKEND in {configured_execution, effective_execution_backend}:
            effective_metadata["timeout"] = self._validate_cloud_tasks_record(
                configured_execution=configured_execution,
                record_execution=effective_execution_backend,
                task_obj=task_obj,
                timeout=timeout,
                scheduled_at=effective_scheduled_at,
            )

        reserved_id: "UUID | None" = None
        if task_obj.unique_until == "forever" and effective_key is not None:
            reserved_id = uuid4()
            owner = await self.get_queue_backend().reserve_identity(
                effective_key, task_id=reserved_id, task_name=task_obj.name
            )
            if owner is not None:
                return TaskResult(owner.task_id, owner.task_name, service=self)

        runtime = self.observability_runtime
        span_attributes = _base_observability_attributes(
            operation="publish",
            queue=effective_queue,
            task_name=task_obj.name,
            execution_backend=effective_execution_backend,
            execution_profile=effective_execution_profile,
        )
        metric_attributes = _metric_attributes(span_attributes)
        started_at = time.perf_counter()
        span = runtime.start_span("litestar_queues.publish", kind="producer", attributes=span_attributes)
        try:
            _capture_enqueue_context(runtime, effective_metadata)
            record = await self.get_queue_backend().enqueue(
                task_obj.name,
                args=args,
                kwargs=kwargs,
                queue=effective_queue,
                priority=priority if priority is not None else task_obj.priority,
                max_retries=retries if retries is not None else task_obj.retries,
                scheduled_at=effective_scheduled_at,
                key=effective_key,
                execution_backend=effective_execution_backend,
                execution_profile=effective_execution_profile,
                metadata=effective_metadata,
                id=reserved_id,
                **_expiration_enqueue_kwargs(effective_expires_at),
            )
            if reserved_id is not None and record.id != reserved_id and effective_key is not None:
                await _raise_forever_identity_collision(self.get_queue_backend(), effective_key, reserved_id)
        except BaseException as exc:
            runtime.record_exception(span, exc)
            if reserved_id is not None and effective_key is not None:
                # Enqueue may have committed before a later notification failed.
                # Release only when the reserved record is confirmed absent; an
                # inconclusive read retains the reservation fail-closed. The owner
                # predicate prevents recovery from deleting a successor reservation.
                await _release_failed_forever_reservation(self.get_queue_backend(), effective_key, reserved_id)
            raise
        else:
            runtime.set_attribute(span, "messaging.message.id", str(record.id))
            runtime.record_counter("litestar_queues.enqueue", attributes=metric_attributes)
            runtime.record_duration(
                "litestar_queues.enqueue.duration", time.perf_counter() - started_at, attributes=metric_attributes
            )
        finally:
            runtime.end_span(span)

        if record.status in {"pending", "scheduled"} and record.is_expired:
            expired = await self.expire_overdue_tasks()
            record = next((candidate for candidate in expired if candidate.id == record.id), record)

        # After expiry, never before: a record that is already dead should not be
        # handed to a transport that would deliver it days from now.
        record = await self._schedule_persisted(record)

        result = TaskResult(record.id, task_obj.name, service=self, record=record)
        await self._run_immediate(record)
        return result

    async def _run_immediate(self, record: "QueuedTaskRecord") -> "None":
        """Run an immediate-mode record inline, before ``enqueue`` returns."""
        if record.execution_backend != "immediate" or record.status != "pending":
            return
        backend = self._execution_backend_for_name(record.execution_backend)
        if backend.is_external:
            return
        claimed, _ = await self.claim_task(record.id)
        if claimed is not None:
            await backend.execute(self, claimed)

    def _resolve_identity(
        self, task_obj: "Task[Any, Any]", key: "str | None", args: "tuple[Any, ...]", kwargs: "dict[str, Any]"
    ) -> "tuple[str | None, dict[str, Any]]":
        """Select the effective uniqueness key by strict precedence, with diagnostic metadata.

        Precedence: explicit enqueue ``key`` -> configured task ``key`` ->
        ``unique_by="task"`` -> ``unique_by="arguments"`` -> no identity. The
        explicit, configured, and task-only paths never bind, serialize, or hash
        arguments; only ``unique_by="arguments"`` inspects the call.

        Returns:
            The effective key (or ``None``) and JSON-safe diagnostic metadata that
            never contains raw argument material.
        """
        if key is not None:
            return key, self._identity_lifetime_metadata(task_obj, {})
        if task_obj.key is not None:
            return task_obj.key, self._identity_lifetime_metadata(task_obj, {})
        unique_by = task_obj.unique_by
        if unique_by is None:
            return None, {}
        if unique_by == "task":
            derived = task_identity(task_obj.name)
        else:
            derived = arguments_identity(
                task_obj.name,
                task_obj.signature,
                args,
                kwargs,
                max_payload_bytes=self._config.max_argument_identity_bytes,
            ).key
        metadata = {"unique_by": unique_by, "unique_version": IDENTITY_VERSION}
        return derived, self._identity_lifetime_metadata(task_obj, metadata)

    @staticmethod
    def _identity_lifetime_metadata(task_obj: "Task[Any, Any]", metadata: "dict[str, Any]") -> "dict[str, Any]":
        if task_obj.unique_until != "terminal":
            metadata["unique_until"] = task_obj.unique_until
        return metadata

    def _validate_cloud_tasks_record(
        self,
        *,
        configured_execution: "str",
        record_execution: "str",
        task_obj: "Task[Any, Any]",
        timeout: "float | None",
        scheduled_at: "datetime | None",
    ) -> "float":
        """Reject a record a Cloud Tasks queue could never deliver.

        Cloud Tasks owns delivery once it accepts a task, so a budget or
        schedule it cannot honour has to fail here, before any reservation or
        record is written.

        Returns:
            The effective task timeout to persist onto the record.

        Raises:
            QueueConfigurationError: If the record mixes execution backends, or
                its timeout or schedule falls outside what the queue accepts.
        """
        from litestar_queues.execution.cloudtasks import CLOUD_TASKS_MAX_SCHEDULE_HORIZON, CloudTasksExecutionConfig

        cloud_tasks = self._config.execution_backend
        if not isinstance(cloud_tasks, CloudTasksExecutionConfig):
            msg = (
                f"execution_backend={record_execution!r} needs a typed CloudTasksExecutionConfig on "
                f"the queue; this queue is configured for {configured_execution!r}, which carries no "
                f"project, queue, delivery target, or audience."
            )
            raise QueueConfigurationError(msg)
        if record_execution != _CLOUD_TASKS_BACKEND:
            msg = (
                f"execution_backend={record_execution!r} cannot be mixed into a Cloud Tasks queue: "
                f"no worker polls it, so the record would never run. Remove the override."
            )
            raise QueueConfigurationError(msg)

        effective_timeout = timeout if timeout is not None else task_obj.timeout
        if effective_timeout is None:
            effective_timeout = cloud_tasks.default_task_timeout
        if (
            isinstance(effective_timeout, bool)
            or not isinstance(effective_timeout, (int, float))
            or not math.isfinite(effective_timeout)
            or effective_timeout <= 0
        ):
            msg = "Cloud Tasks task timeout must be a finite positive number of seconds."
            raise QueueConfigurationError(msg)
        if effective_timeout + cloud_tasks.response_margin > cloud_tasks.dispatch_deadline:
            msg = (
                f"Cloud Tasks task timeout {effective_timeout} plus the "
                f"{cloud_tasks.response_margin}s response margin exceeds the "
                f"{cloud_tasks.dispatch_deadline}s delivery budget."
            )
            raise QueueConfigurationError(msg)

        if scheduled_at is not None and scheduled_at > _utc_now() + CLOUD_TASKS_MAX_SCHEDULE_HORIZON:
            msg = (
                f"Cloud Tasks schedules at most {CLOUD_TASKS_MAX_SCHEDULE_HORIZON.days} days ahead; "
                f"the create call would be refused."
            )
            raise QueueConfigurationError(msg)
        return effective_timeout

    async def _schedule_persisted(self, record: "QueuedTaskRecord") -> "QueuedTaskRecord":
        """Hand an already-persisted record to a self-scheduling execution backend.

        A no-op for every polled backend, so persistence paths can call it
        unconditionally. The record is reloaded afterwards because the backend
        may have written a delivery reference the caller would otherwise miss.

        Self-scheduling is a property of the queue, not of one record: a backend
        that dispatches without a worker refuses to share a queue with backends
        that need one. Reading the configured backend therefore answers the
        question, and keeps a per-record lookup off the enqueue path.

        Returns:
            The live record.
        """
        backend = self.get_execution_backend()
        if not backend.schedules_on_enqueue:
            return record
        await backend.schedule(self, record)
        return await self.get_queue_backend().get_task(record.id) or record

    def _execution_backend_for_name(self, name: "str") -> "BaseExecutionBackend":
        if name == execution_backend_name(self._config.execution_backend):
            return self.get_execution_backend()
        return get_execution_backend(name, config=self._config)

    def resolve_task(self, task: "str | Task[Any, Any]") -> "Task[Any, Any]":
        """Resolve a task name or wrapper to a registered task.

        Returns:
            The registered task wrapper.

        Raises:
            KeyError: If a task name is not registered.
        """
        if isinstance(task, Task):
            return task
        registry = get_task_registry()
        try:
            return registry[task]
        except KeyError as exc:
            msg = f"Unknown queue task: {task!r}"
            raise KeyError(msg) from exc

    async def get_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        """Return a queued task record by ID."""
        return await self.get_queue_backend().get_task(task_id)

    async def get_statistics(self, *, queue: "str | None" = None) -> "QueueStatistics":
        """Return global or queue-scoped task status counts."""
        return await self.get_queue_backend().get_statistics(queue=queue)

    async def cancel_task(self, task_id: "UUID", *, include_running: "bool" = False) -> "bool":
        """Cancel one queued task and publish its terminal lifecycle event.

        Args:
            task_id: Identifier of the task to cancel.
            include_running: Whether a running task may transition to cancelled.

        Returns:
            ``True`` only for the caller that wins the durable state transition.
        """
        return await self._cancel_task(task_id, include_running=include_running)

    async def _cancel_task(
        self, task_id: "UUID", *, include_running: "bool" = False, message: "str | None" = None
    ) -> "bool":
        backend = self.get_queue_backend()
        if not await backend.cancel_task(task_id, include_running=include_running):
            return False
        record = await backend.get_task(task_id)
        if record is None:
            return True
        payload = {"status": record.status, "retry_count": record.retry_count}
        task_context = _task_execution_context(record, worker_id=None, event_publisher=self.get_event_publisher())
        await task_context.lifecycle("task.cancelled", message=message, payload=payload)
        self._log_task_event("Queue task cancelled", record, level=logging.INFO, payload=payload)
        if include_running:
            # Not gated on a persisted owner: an unclaimed record has no owner
            # to name, and every subscribed worker reconciles the shared
            # control channel against durable status regardless.
            await backend.notify_worker_control(record.worker_id)
        return True

    async def interrupt_task(
        self,
        record: "QueuedTaskRecord",
        *,
        worker_id: "str",
        reason: "str" = "shutdown",
        max_interruptions: "int | None" = None,
    ) -> "QueuedTaskRecord | None":
        """Requeue one owned running attempt after local execution unwinds.

        An attempt that keeps being interrupted would otherwise cycle forever
        without ever consuming its retry budget, so at ``max_interruptions`` the
        interruption is routed through the ordinary retry policy instead.

        Returns:
            The requeued or failed record, or ``None`` when the fence was lost.
        """
        if max_interruptions is not None and interruption_count(record) >= max_interruptions:
            return await self._fail_interrupted_task(record, worker_id=worker_id)
        updated = await self.get_queue_backend().interrupt_task(
            record.id,
            expected_retry_count=record.retry_count,
            worker_id=worker_id,
            queued_at=datetime.now(timezone.utc),
        )
        if updated is None:
            return None
        payload = {"reason": reason, "status": updated.status, "retry_count": updated.retry_count}
        context = _task_execution_context(updated, worker_id=worker_id, event_publisher=self.get_event_publisher())
        await context.lifecycle("task.interrupted", payload=payload)
        self._log_task_event("Queue task interrupted", updated, level=logging.INFO, payload=payload)
        return updated

    async def _fail_interrupted_task(
        self, record: "QueuedTaskRecord", *, worker_id: "str"
    ) -> "QueuedTaskRecord | None":
        updated = await self.get_queue_backend().fail_task(
            record.id, INTERRUPTION_LIMIT_ERROR, retry=True, expected_retry_count=record.retry_count
        )
        if updated is None:
            return None
        payload = {"status": updated.status, "retry_count": updated.retry_count, "will_retry": not updated.is_terminal}
        context = _task_execution_context(updated, worker_id=worker_id, event_publisher=self.get_event_publisher())
        await context.lifecycle("task.failed", message=INTERRUPTION_LIMIT_ERROR, payload=payload)
        self._log_task_event(
            "Queue task exceeded its shutdown interruption budget", updated, level=logging.ERROR, payload=payload
        )
        return updated

    async def reset_task_identity(self, key: "str") -> "bool":
        """Delete a ``unique_until="forever"`` reservation by its exact effective key.

        This is the only supported way to allow a forever identity to be enqueued
        again. It never infers or resets an identity from raw arguments; the caller
        must pass the exact effective key (for example the ``lq:u:v1:...`` value or
        the configured/explicit key).

        Returns:
            ``True`` when a reservation was removed.
        """
        return await self.get_queue_backend().reset_identity(key)

    async def get_task_identity(self, key: "str") -> "TaskReservation | None":
        """Return the forever reservation owning an identity key, if any."""
        return await self.get_queue_backend().has_identity(key)

    async def execute_record(self, record: "QueuedTaskRecord", *, worker_id: "str | None" = None) -> "QueuedTaskRecord":
        """Execute a claimed queue record and persist the lifecycle result.

        Args:
            record: The claimed queue record to execute.
            worker_id: Identity of the worker driving execution, if any. The
                value is forwarded to ``TaskExecutionContext.worker_id`` so
                published events carry stable worker provenance. Service-driven
                executions (no worker) leave this as ``None``.

        Returns:
            The updated queue record.

        Raises:
            asyncio.CancelledError: If task execution is cancelled.
        """
        task_obj = self.resolve_task(record.task_name)
        timeout = record.metadata.get("timeout", task_obj.timeout)
        runtime = self.observability_runtime
        telemetry = _start_execution_observability(runtime, record, worker_id=worker_id)
        task_context = _task_execution_context(record, worker_id=worker_id, event_publisher=self.get_event_publisher())
        task_context.actor = _resolve_task_actor(task_obj)
        execution_scope = _bind_execution_context(task_context, record.metadata)
        final_status = "failed"
        try:
            await task_context.lifecycle("task.started")
            result = await self._execute_task(record, task_obj, task_context, timeout)
        except asyncio.CancelledError as exc:
            final_status = "interrupted"
            telemetry.record_exception(exc)
            telemetry.finish(final_status)
            raise
        except JobCancelledError as exc:
            cancelled = await self._cancel_task(record.id, include_running=True, message=str(exc))
            if not cancelled:
                final_status = "claim_lost"
                current = await self.publish_claim_lost(record, phase="cancel", task_context=task_context)
                telemetry.finish(final_status)
                return current
            cancelled_record = await self._current_or_claimed(record)
            final_status = cancelled_record.status
            telemetry.finish(final_status)
            return cancelled_record
        except NonRetryableError as exc:
            telemetry.record_exception(exc)
            return await self._fail_record_without_retry(record, exc, task_context, telemetry)
        except Exception as exc:
            telemetry.record_exception(exc)
            return await self._fail_record_with_retry(record, exc, task_context, telemetry)
        finally:
            _reset_execution_context(execution_scope)

        completed_record = await self.get_queue_backend().complete_task(
            record.id, result=result, expected_retry_count=record.retry_count
        )
        if completed_record is None:
            final_status = "claim_lost"
            current = await self.publish_claim_lost(record, phase="complete", task_context=task_context)
            telemetry.finish(final_status)
            return current
        completed = completed_record
        final_status = completed.status
        await task_context.lifecycle(
            "task.completed", payload={"status": completed.status, "retry_count": completed.retry_count}
        )
        self._log_task_completed(completed)
        await self._reschedule_if_needed(completed)
        telemetry.finish(final_status)
        return completed

    async def _execute_task(
        self,
        record: "QueuedTaskRecord",
        task_obj: "Task[Any, Any]",
        task_context: "TaskExecutionContext",
        timeout: "object",
    ) -> "object":
        extra_kwargs = await self._resolve_task_dependencies(task_obj, record, task_context)
        coroutine = task_obj.execute_record(
            record, task_context=task_context, extra_kwargs=extra_kwargs, sync_executor=self._sync_executor
        )
        return await asyncio.wait_for(coroutine, timeout=timeout if isinstance(timeout, int | float) else None)

    async def _fail_record_without_retry(
        self,
        record: "QueuedTaskRecord",
        exc: "BaseException",
        task_context: "TaskExecutionContext",
        telemetry: "_ExecutionObservability",
    ) -> "QueuedTaskRecord":
        error_message = self._error_message(exc, record)
        updated = await self.get_queue_backend().fail_task(
            record.id, error_message, retry=False, expected_retry_count=record.retry_count
        )
        if updated is None:
            return await self._finish_claim_lost_observability(record, task_context, telemetry)
        payload = {"status": updated.status, "retry_count": updated.retry_count, "will_retry": False}
        await task_context.lifecycle("task.failed", message=error_message, payload=payload)
        self._log_task_event("Queue task failed", updated, level=logging.ERROR, payload=payload)
        telemetry.finish(updated.status)
        return updated

    async def _fail_record_with_retry(
        self,
        record: "QueuedTaskRecord",
        exc: "BaseException",
        task_context: "TaskExecutionContext",
        telemetry: "_ExecutionObservability",
    ) -> "QueuedTaskRecord":
        error_message = self._error_message(exc, record)
        queued_at = datetime.now(timezone.utc)
        retry_delay = _retry_delay(record)
        retry_at = queued_at + timedelta(seconds=retry_delay) if retry_delay > 0 else None
        updated = await self.get_queue_backend().fail_task(
            record.id, error_message, expected_retry_count=record.retry_count, retry_at=retry_at, queued_at=queued_at
        )
        if updated is None:
            return await self._finish_claim_lost_observability(record, task_context, telemetry)
        payload: "dict[str, Any]" = {
            "status": updated.status,
            "retry_count": updated.retry_count,
            "will_retry": updated.status in {"pending", "scheduled"},
        }
        if updated.status in {"pending", "scheduled"}:
            payload["retry_delay"] = retry_delay
            payload["retry_at"] = retry_at.isoformat() if retry_at is not None else None
        await task_context.lifecycle("task.failed", message=error_message, payload=payload)
        self._log_task_event(
            "Queue task failed",
            updated,
            level=logging.WARNING if updated.status in {"pending", "scheduled"} else logging.ERROR,
            payload=payload,
        )
        if updated.status == "failed":
            await self._reschedule_if_needed(updated)
        elif updated.status in {"pending", "scheduled"}:
            # A retry on a queue with no worker needs its own delivery, and the
            # claim this handler owns is the only thing that knows one is due.
            updated = await self._schedule_persisted(updated)
        telemetry.finish(updated.status)
        return updated

    async def _finish_claim_lost_observability(
        self, record: "QueuedTaskRecord", task_context: "TaskExecutionContext", telemetry: "_ExecutionObservability"
    ) -> "QueuedTaskRecord":
        current = await self.publish_claim_lost(record, phase="fail", task_context=task_context)
        telemetry.finish("claim_lost")
        return current

    async def reconcile_external(self, limit: "int | None" = None) -> "int":
        """Reconcile externally dispatched records against their executor.

        A bounded call first asks the configured backend to repair deliveries
        its transport lost, then spends what is left of the budget on ordinary
        reconciliation, so one maintenance pass stays finite however the two
        divide the work. The worker's unbounded sweep skips repair: it has no
        ceiling to respect, and repair is a maintenance responsibility.

        Args:
            limit: When provided, examine at most this many external records
                across both halves. ``None`` reconciles every outstanding
                external record and repairs nothing.

        Returns:
            Number of records repaired or brought to a terminal queue status.
        """
        if limit is None:
            return await self._reconcile_external_records(limit=None)
        repair = await self.get_execution_backend().repair(self, limit=limit)
        return repair.changed + await self._reconcile_external_records(limit=max(0, limit - repair.examined))

    async def _reconcile_external_records(self, *, limit: "int | None") -> "int":
        """Reconcile outstanding external records against their execution backends.

        Records a metric for every record that reached a terminal queue status.
        Records naming an unknown execution backend are skipped with a warning.

        Returns:
            Number of records that reached a terminal queue status.
        """
        queue_backend = self.get_queue_backend()
        records = await queue_backend.list_running_external(limit=limit)
        reconciled = 0
        current_backend = self.get_execution_backend()
        default_backend_name = execution_backend_name(self._config.execution_backend)
        for record in records:
            if record.execution_ref is None:
                continue
            try:
                execution_backend = (
                    current_backend
                    if record.execution_backend == default_backend_name
                    else get_execution_backend(record.execution_backend, config=self._config)
                )
            except ValueError:
                self._logger.warning(
                    "Skipping external queue record with unknown execution backend",
                    extra={"task_id": str(record.id), "execution_backend": record.execution_backend},
                )
                continue
            # The execution backend owns litestar_queues.execution.reconcile;
            # emitting it here too would double-count with a narrower label set.
            updated = await execution_backend.reconcile(self, record)
            if updated is not None and updated.is_terminal:
                reconciled += 1
        return reconciled

    async def recover_stale_tasks(
        self, *, stale_after: "timedelta", worker_id: "str | None" = None, limit: "int | None" = None
    ) -> "StaleTaskRecoveryResult":
        """Recover stale running tasks and publish a worker summary event.

        Args:
            stale_after: Heartbeat age past which a running task is stale.
            worker_id: Identity attached to published recovery events.
            limit: When provided, recover at most this many records in one
                bounded batch. ``None`` preserves the unbounded worker behavior.

        Returns:
            Summary of recovered, failed, skipped, and handler-needed tasks.
        """
        result = await self.get_queue_backend().requeue_stale_running(stale_after=stale_after, limit=limit)
        if result.requeued or result.failed or result.skipped or result.handler_needed:
            await self._publish_stale_failed_events(result, worker_id=worker_id)
            await self.get_event_publisher().publish(
                QueueEvent(
                    type="worker.stale_recovery",
                    scope="worker",
                    worker_id=worker_id,
                    message="Recovered stale running tasks",
                    payload=result.to_payload(),
                )
            )
        return result

    async def expire_overdue_tasks(
        self, *, limit: "int | None" = None, worker_id: "str | None" = None
    ) -> "list[QueuedTaskRecord]":
        """Expire overdue pending or scheduled records and publish one event each.

        Returns:
            Records transitioned to ``expired``.
        """
        expired = await self.get_queue_backend().expire_overdue(limit=limit)
        for record in expired:
            await self._publish_expired_event(record, worker_id=worker_id)
        return expired

    async def claim_tasks(
        self,
        *,
        limit: "int",
        queues: "tuple[str, ...]" = (),
        execution_backend: "str | None" = None,
        worker_id: "str | None" = None,
        queue_limits: "Mapping[str, int] | None" = None,
    ) -> "list[QueuedTaskRecord]":
        """Claim due tasks and publish events for claim-time expirations.

        Returns:
            Records successfully transitioned to ``running``.
        """
        backend = self.get_queue_backend()
        claimed, expired = await backend.claim_many_with_expired(
            limit=limit, queues=queues, execution_backend=execution_backend, queue_limits=queue_limits
        )
        self.observability_runtime.record_histogram(
            "litestar_queues.claim.batch.size",
            len(claimed),
            unit="records",
            attributes={
                "queue.backend": queue_backend_name(self._config.queue_backend),
                "queue.operation": "claim_many",
            },
        )
        for record in expired:
            await self._publish_expired_event(record, worker_id=worker_id)
        return claimed

    async def claim_task(
        self,
        task_id: "UUID",
        *,
        worker_id: "str | None" = None,
        expected_retry_count: "int | None" = None,
        expected_execution_ref: "str | None" = None,
    ) -> "tuple[QueuedTaskRecord | None, QueuedTaskRecord | None]":
        """Claim one task and publish its claim-time expiration event.

        Returns:
            The claimed record and the expired record, at most one of which is set.
        """
        backend = self.get_queue_backend()
        if expected_retry_count is None and expected_execution_ref is None:
            claimed, expired = await backend.claim_task_with_expired(task_id)
        else:
            claimed, expired = await backend.claim_task_with_expired(
                task_id, expected_retry_count=expected_retry_count, expected_execution_ref=expected_execution_ref
            )
        if expired is not None:
            await self._publish_expired_event(expired, worker_id=worker_id)
        return claimed, expired

    async def initialize_schedules(self) -> "list[QueuedTaskRecord]":
        """Create queue records for registered recurring schedules.

        Returns:
            The created or reused schedule records.
        """
        records: 'list["QueuedTaskRecord"]' = []
        queue_backend = self.get_queue_backend()
        for task_name, schedule in get_scheduled_tasks().items():
            task_obj = self.resolve_task(task_name)
            schedule_metadata = schedule.as_metadata()
            schedule_key = f"scheduled:{task_name}"
            existing = await queue_backend.get_task_by_key(schedule_key)
            active = existing if existing is not None and not existing.is_terminal else None
            if active is not None and active.metadata.get("schedule") == schedule_metadata:
                records.append(active)
                continue
            scheduled_at = schedule.get_next_run(use_initial_delay=True)
            # Both resolved before the cancellation below: refusing a schedule
            # the transport cannot deliver must not first retire the occurrence
            # that is currently in flight.
            self._reject_beyond_scheduling_horizon(task_name, scheduled_at)
            metadata = self._metadata_with_schedule_default(task_obj, schedule_metadata, scheduled_at)
            if active is not None:
                await queue_backend.cancel_task(active.id)
            records.append(
                await self._persist_scheduled_record(
                    task_name,
                    key=schedule_key,
                    max_retries=0,
                    priority=task_obj.priority,
                    scheduled_at=scheduled_at,
                    expires_at=_resolve_expires_at(
                        task_obj, expires_in=None, expires_at=None, scheduled_at=scheduled_at
                    ),
                    execution_backend=task_obj.execution_backend
                    or execution_backend_name(self._config.execution_backend),
                    execution_profile=task_obj.execution_profile,
                    metadata=metadata,
                )
            )
        return records

    async def _persist_scheduled_record(self, task_name: "str", **enqueue_kwargs: "Any") -> "QueuedTaskRecord":
        """Persist one occurrence of a recurring schedule and hand it to the transport.

        The single funnel for both schedule paths, so a queue that dispatches
        without a worker cannot end up with an occurrence nothing delivers.

        Returns:
            The persisted occurrence, refreshed after scheduling.
        """
        record = await self.get_queue_backend().enqueue(task_name, **enqueue_kwargs)
        return await self._schedule_persisted(record)

    def _beyond_scheduling_horizon(self, scheduled_at: "datetime | None") -> "bool":
        """Whether the configured transport would refuse to hold a delivery this far out.

        Returns:
            True when the schedule is past the backend's ceiling.
        """
        horizon = self.get_execution_backend().max_schedule_horizon
        return horizon is not None and scheduled_at is not None and scheduled_at > _utc_now() + horizon

    def _reject_beyond_scheduling_horizon(self, task_name: "str", scheduled_at: "datetime | None") -> "None":
        """Refuse a recurrence the configured transport could never deliver.

        Raises:
            QueueConfigurationError: If the schedule is past the backend's ceiling.
        """
        if not self._beyond_scheduling_horizon(scheduled_at):
            return
        horizon = self.get_execution_backend().max_schedule_horizon
        msg = (
            f"Schedule for {task_name!r} is due beyond the {horizon} that "
            f"{execution_backend_name(self._config.execution_backend)!r} will hold a delivery, "
            f"so the occurrence would never run."
        )
        raise QueueConfigurationError(msg)

    def _metadata_with_schedule_default(
        self, task_obj: "Task[Any, Any]", schedule_metadata: "dict[str, Any]", scheduled_at: "datetime | None"
    ) -> "dict[str, Any]":
        """Build the metadata one occurrence of a recurring schedule runs under.

        Returns:
            The occurrence metadata.
        """
        metadata = task_obj.metadata({"schedule": schedule_metadata})
        self._apply_log_success_default(metadata)
        configured_execution = execution_backend_name(self._config.execution_backend)
        record_execution = task_obj.execution_backend or configured_execution
        if _CLOUD_TASKS_BACKEND in {configured_execution, record_execution}:
            # A recurrence never passes through enqueue, so without this the
            # occurrence reaches the consumer carrying no budget at all and can
            # still be working when the deadline the transport is holding open
            # for it expires -- at which point the delivery is redelivered on
            # top of work already in flight. Later occurrences copy this
            # metadata forward, so settling the first one settles the chain.
            metadata["timeout"] = self._validate_cloud_tasks_record(
                configured_execution=configured_execution,
                record_execution=record_execution,
                task_obj=task_obj,
                timeout=None,
                scheduled_at=scheduled_at,
            )
        return metadata

    def _apply_log_success_default(self, metadata: "dict[str, Any]") -> "None":
        if "log_success" not in metadata:
            metadata["log_success"] = self._config.log_success

    async def _resolve_task_dependencies(
        self, task: "Task[..., object]", record: "QueuedTaskRecord", task_context: "TaskExecutionContext"
    ) -> "Mapping[str, object] | None":
        """Invoke the configured task dependency resolver, if any.

        Returns:
            The resolver's kwargs mapping, or ``None`` when no resolver is configured.
        """
        resolver = self._config.task_dependency_resolver
        if resolver is None:
            return None
        return await resolver(task, record, task_context)

    async def _reschedule_if_needed(self, record: "QueuedTaskRecord") -> "None":
        schedule_data = record.metadata.get("schedule")
        if not isinstance(schedule_data, dict) or record.completed_at is None:
            return
        schedule = ScheduleConfig(
            task_name=str(schedule_data["task_name"]),
            cron=schedule_data.get("cron"),
            initial_delay=schedule_data.get("initial_delay", 0),
            interval=schedule_data.get("interval"),
            jitter=schedule_data.get("jitter", 0),
            timezone=str(schedule_data.get("timezone", "UTC")),
        )
        task_obj = self.resolve_task(record.task_name)
        scheduled_at = schedule.get_next_run(record.completed_at)
        if self._beyond_scheduling_horizon(scheduled_at):
            # The occurrence that just ran stays terminal and this chain ends
            # here. Writing the next one would persist work nothing delivers,
            # and every later occurrence would be further out still.
            await self._publish_schedule_rejected(record)
            return
        expires_at = _resolve_expires_at(task_obj, expires_in=None, expires_at=None, scheduled_at=scheduled_at)
        await self._persist_scheduled_record(
            record.task_name,
            key=record.key,
            queue=record.queue,
            max_retries=record.max_retries,
            priority=record.priority,
            scheduled_at=scheduled_at,
            expires_at=expires_at,
            execution_backend=record.execution_backend,
            execution_profile=record.execution_profile,
            metadata={**record.metadata, "schedule": schedule.as_metadata()},
        )

    async def _publish_schedule_rejected(self, record: "QueuedTaskRecord") -> "None":
        """Report a recurrence that stops because the transport cannot hold it.

        The phase names the backend that refused rather than repeating its
        configuration: the event travels wherever sinks go, and the delivery
        target and calling identity are not part of the news.
        """
        payload = {"phase": f"{record.execution_backend}.schedule_rejected"}
        await self.get_event_publisher().publish(
            QueueEvent(
                type="task.event",
                scope="task",
                task_id=str(record.id),
                task_name=record.task_name,
                queue=record.queue,
                execution_backend=record.execution_backend,
                execution_profile=record.execution_profile,
                attempt=record.retry_count + 1,
                level="warning",
                message="Recurring schedule stopped: the next run is beyond the delivery horizon",
                payload=payload,
            )
        )
        self._log_task_event(
            "Recurring schedule stopped beyond the delivery horizon", record, level=logging.WARNING, payload=payload
        )

    async def _current_or_claimed(self, record: "QueuedTaskRecord") -> "QueuedTaskRecord":
        return await self.get_queue_backend().get_task(record.id) or record

    async def publish_claim_lost(
        self,
        record: "QueuedTaskRecord",
        *,
        phase: "str",
        task_context: "TaskExecutionContext | None" = None,
        worker_id: "str | None" = None,
        expected_retry_count: "int | None" = None,
    ) -> "QueuedTaskRecord":
        """Publish an ownership-loss event and return the current record state.

        Returns:
            Current queue task record state.
        """
        current = await self._current_or_claimed(record)
        expected = record.retry_count if expected_retry_count is None else expected_retry_count
        payload = {
            "phase": phase,
            "expected_retry_count": expected,
            "current_status": current.status,
            "current_retry_count": current.retry_count,
        }
        message = "Queue task ownership lost"
        if task_context is not None:
            await task_context.lifecycle("task.claim_lost", message=message, payload=payload)
        else:
            await self.get_event_publisher().publish(
                QueueEvent(
                    type="task.claim_lost",
                    scope="task",
                    task_id=str(record.id),
                    task_name=record.task_name,
                    queue=record.queue,
                    worker_id=worker_id,
                    execution_backend=record.execution_backend,
                    execution_profile=record.execution_profile,
                    attempt=expected + 1,
                    message=message,
                    payload=payload,
                )
            )
        self._log_task_event(message, current, level=logging.WARNING, payload=payload)
        return current

    async def _publish_stale_failed_events(
        self, result: "StaleTaskRecoveryResult", *, worker_id: "str | None"
    ) -> "None":
        handler_needed_ids = set(result.handler_needed_task_ids)
        for task_id in result.failed_task_ids:
            record = await self.get_queue_backend().get_task(task_id)
            if record is None:
                continue
            requeue_on_stale = record.metadata.get("requeue_on_stale", True) is not False
            payload = {
                "status": record.status,
                "retry_count": record.retry_count,
                "max_retries": record.max_retries,
                "requeue_on_stale": requeue_on_stale,
                "handler_needed": record.id in handler_needed_ids,
            }
            await self.get_event_publisher().publish(
                QueueEvent(
                    type="task.stale_failed",
                    scope="task",
                    task_id=str(record.id),
                    task_name=record.task_name,
                    queue=record.queue,
                    worker_id=worker_id,
                    execution_backend=record.execution_backend,
                    execution_profile=record.execution_profile,
                    attempt=record.retry_count + 1,
                    message=record.error or "Task heartbeat stale",
                    payload=payload,
                )
            )
            self._log_task_event(
                "Queue task failed after stale heartbeat", record, level=logging.ERROR, payload=payload
            )
            await self._invoke_stale_failure_hook(record)

    async def _publish_expired_event(self, record: "QueuedTaskRecord", *, worker_id: "str | None") -> "None":
        payload = {
            "status": record.status,
            "retry_count": record.retry_count,
            "expires_at": record.expires_at.isoformat() if record.expires_at is not None else None,
        }
        await self.get_event_publisher().publish(
            QueueEvent(
                type="task.expired",
                scope="task",
                task_id=str(record.id),
                task_name=record.task_name,
                queue=record.queue,
                worker_id=worker_id,
                execution_backend=record.execution_backend,
                execution_profile=record.execution_profile,
                attempt=record.retry_count + 1,
                message="Task expired before execution",
                payload=payload,
            )
        )
        self._log_task_event("Queue task expired before execution", record, level=logging.WARNING, payload=payload)

    def _log_task_completed(self, record: "QueuedTaskRecord") -> "None":
        if record.metadata.get("log_success") is False:
            return
        self._log_task_event("Queue task completed", record, level=_coerce_log_level(record.metadata.get("log_level")))

    def _log_task_event(
        self, message: "str", record: "QueuedTaskRecord", *, level: "int", payload: "Mapping[str, object] | None" = None
    ) -> "None":
        self._logger.log(
            level,
            message,
            extra={
                "queue_task_id": str(record.id),
                "queue_task_name": record.task_name,
                "queue_task_queue": record.queue,
                "queue_task_status": record.status,
                "queue_task_retry_count": record.retry_count,
                "queue_task_max_retries": record.max_retries,
                "queue_task_execution_backend": record.execution_backend,
                "queue_task_execution_profile": record.execution_profile,
                "queue_task_description": record.metadata.get("description"),
                "queue_task_event_payload": dict(payload or {}),
            },
        )

    def _error_message(self, exc: "BaseException", record: "QueuedTaskRecord") -> "str":
        sanitizer = self._config.error_sanitizer
        if sanitizer is None:
            return str(exc)
        return sanitizer(exc, record)

    async def _invoke_stale_failure_hook(self, record: "QueuedTaskRecord") -> "None":
        try:
            task_obj = self.resolve_task(record.task_name)
        except KeyError:
            self._logger.warning(
                "Queue task stale failure hook skipped for unknown task",
                extra={"queue_task_id": str(record.id), "queue_task_name": record.task_name},
            )
            return
        hook = task_obj.on_stale_failure
        if hook is None:
            return
        if iscoroutinefunction(hook):
            await hook(record)
            return
        result = await _run_sync_callable(hook, (record,), {}, sync_executor=self._sync_executor)
        if isawaitable(result):
            await result


def _coerce_timedelta(value: "float | timedelta | None") -> "timedelta | None":
    if value is None:
        return None
    if isinstance(value, timedelta):
        return value
    return timedelta(seconds=value)


def _resolve_schedule_and_expiration(
    task_obj: "Task[Any, Any]",
    *,
    scheduled_at: "datetime | None",
    run_after: "float | timedelta | None",
    expires_in: "float | timedelta | None",
    expires_at: "datetime | None",
) -> "tuple[datetime | None, datetime | None]":
    effective_run_after = _coerce_timedelta(run_after) if run_after is not None else task_obj.run_after
    effective_scheduled_at = scheduled_at
    if effective_scheduled_at is None and effective_run_after is not None:
        effective_scheduled_at = datetime.now(timezone.utc) + effective_run_after
    if effective_scheduled_at is not None:
        effective_scheduled_at = _ensure_utc(effective_scheduled_at)
    return effective_scheduled_at, _resolve_expires_at(
        task_obj, expires_in=expires_in, expires_at=expires_at, scheduled_at=effective_scheduled_at
    )


def _resolve_expires_at(
    task_obj: "Task[Any, Any]",
    *,
    expires_in: "float | timedelta | None",
    expires_at: "datetime | None",
    scheduled_at: "datetime | None",
) -> "datetime | None":
    if expires_in is not None and expires_at is not None:
        msg = "Cannot specify both expires_in and expires_at."
        raise ValueError(msg)
    if expires_at is not None:
        return _ensure_utc(expires_at)
    effective_expires_in = _coerce_timedelta(expires_in) if expires_in is not None else task_obj.expires_in
    if effective_expires_in is None:
        return None
    if effective_expires_in < timedelta():
        msg = "expires_in must not be negative."
        raise ValueError(msg)
    return (scheduled_at or datetime.now(timezone.utc)) + effective_expires_in


def _expiration_enqueue_kwargs(expires_at: "datetime | None") -> "dict[str, datetime]":
    return {"expires_at": expires_at} if expires_at is not None else {}


def _task_metadata(
    task_obj: "Task[Any, Any]",
    *,
    metadata: "dict[str, Any] | None",
    description: "str | None",
    log_level: "str | None",
    log_success: "bool | None",
    requeue_on_stale: "bool | None",
    timeout: "float | None",
) -> "dict[str, Any]":
    effective_metadata = task_obj.metadata(metadata)
    for key, value in (
        ("description", description),
        ("log_level", log_level),
        ("log_success", log_success),
        ("requeue_on_stale", requeue_on_stale),
        ("timeout", timeout),
    ):
        if value is not None:
            effective_metadata[key] = value
    return effective_metadata


def _task_execution_context(
    record: "QueuedTaskRecord", *, worker_id: "str | None", event_publisher: "QueueEventPublisher"
) -> "TaskExecutionContext":
    return TaskExecutionContext(
        task_id=str(record.id),
        task_name=record.task_name,
        queue=record.queue,
        worker_id=worker_id,
        execution_backend=record.execution_backend,
        execution_profile=record.execution_profile,
        attempt=record.retry_count + 1,
        event_publisher=event_publisher,
    )


def _resolve_task_actor(task_obj: "Task[Any, Any]") -> "QueueEventActor | None":
    """Resolve a task's declared actor for this attempt.

    Returns:
        The resolved actor, or ``None`` when the task declares none.

    Raises:
        QueueConfigurationError: If a declared resolver returns a non-actor.
    """
    declared = task_obj.actor
    if declared is None or isinstance(declared, QueueEventActor):
        return declared
    resolved: Any = declared()
    if not isinstance(resolved, QueueEventActor):
        msg = (
            f"@task(actor=...) resolver for {task_obj.name!r} returned "
            f"{type(resolved).__name__}; QueueEventActor is required."
        )
        raise QueueConfigurationError(msg)
    return resolved


def _capture_enqueue_context(runtime: "QueueObservabilityRuntimeProtocol", metadata: "dict[str, Any]") -> "None":
    """Record the enqueueing caller's trace and correlation context on the queued task."""
    runtime.inject_trace_context(metadata)
    capture_correlation_id(metadata)


def _bind_execution_context(
    task_context: "TaskExecutionContext", metadata: "Mapping[str, Any]"
) -> "tuple[ExitStack, tuple[Any, bool]]":
    """Bind the ambient context a task body runs under.

    Returns:
        Scope state for :func:`_reset_execution_context`.
    """
    stack = ExitStack()
    stack.enter_context(bind_task_context(task_context))
    return stack, bind_correlation_id(metadata)


def _reset_execution_context(scope: "tuple[ExitStack, tuple[Any, bool]]") -> "None":
    """Restore the context that was active before the task body ran."""
    stack, correlation_state = scope
    stack.close()
    reset_correlation_id(correlation_state)


def _retry_delay(record: "QueuedTaskRecord") -> "float":
    """Return the persisted retry delay for the record's current retry count."""
    value = record.metadata.get("retry_backoff")
    if not isinstance(value, dict):
        return 0.0
    initial_delay = float(value.get("initial_delay", 0.0))
    multiplier = float(value.get("multiplier", 1.0))
    delay = initial_delay * multiplier**record.retry_count
    max_delay = value.get("max_delay")
    return min(delay, float(max_delay)) if max_delay is not None else delay


def _base_observability_attributes(
    *,
    operation: "str",
    queue: "str",
    task_name: "str",
    execution_backend: "str",
    execution_profile: "str | None",
    attempt: "int | None" = None,
) -> "dict[str, object]":
    attributes: "dict[str, object]" = {
        "messaging.system": "litestar_queues",
        "messaging.operation.name": operation,
        "messaging.destination.name": queue,
        "queue.task.name": task_name,
        "queue.execution.backend": execution_backend,
    }
    # Spans omit unset attributes rather than carrying an empty value. Metrics
    # cannot: Prometheus binds label names at collector construction, so the key
    # must always be present. An empty label value is the right encoding there --
    # Prometheus treats it as equivalent to the label being absent.
    if execution_profile:
        attributes["queue.execution.profile"] = execution_profile
    if attempt is not None:
        attributes["queue.task.attempt"] = attempt
    return attributes


def _metric_attributes(attributes: "dict[str, object]") -> "dict[str, str]":
    return {
        "messaging.destination.name": str(attributes["messaging.destination.name"]),
        "queue.task.name": str(attributes["queue.task.name"]),
        "queue.execution.backend": str(attributes["queue.execution.backend"]),
        "queue.execution.profile": str(attributes.get("queue.execution.profile", "")),
    }


@dataclass(frozen=True, slots=True)
class _ExecutionObservability:
    """One execution's telemetry handles, kept together as they are always used together."""

    runtime: "QueueObservabilityRuntimeProtocol"
    span: "Any | None"
    started_at: "float"
    metric_attributes: "dict[str, str]"

    def record_exception(self, exc: "BaseException") -> "None":
        """Attach a failure to this execution's span."""
        self.runtime.record_exception(self.span, exc)

    def finish(self, status: "str") -> "None":
        """Close out the span and emit the duration and count for ``status``."""
        self.runtime.set_attribute(self.span, "queue.task.status", status)
        if status == "failed":
            # Trace backends key error rates off span status, not recorded exceptions,
            # and a task can fail without an exception reaching this frame.
            self.runtime.set_status_error(self.span, "task failed")
        attributes = {**self.metric_attributes, "queue.task.status": status}
        self.runtime.record_duration(
            "litestar_queues.task.execution.duration", time.perf_counter() - self.started_at, attributes=attributes
        )
        self.runtime.record_counter("litestar_queues.task.execution", attributes=attributes)
        self.runtime.end_span(self.span)


def _start_execution_observability(
    runtime: "QueueObservabilityRuntimeProtocol", record: "QueuedTaskRecord", *, worker_id: "str | None" = None
) -> "_ExecutionObservability":
    parent_context = runtime.extract_trace_context(record.metadata)
    span_attributes = _base_observability_attributes(
        operation="process",
        queue=record.queue,
        task_name=record.task_name,
        execution_backend=record.execution_backend,
        execution_profile=record.execution_profile,
        attempt=record.retry_count + 1,
    )
    span_attributes["messaging.message.id"] = str(record.id)
    if worker_id is not None:
        span_attributes["queue.worker.id"] = worker_id
    span = runtime.start_span(
        "litestar_queues.process", kind="consumer", attributes=span_attributes, parent=parent_context
    )
    return _ExecutionObservability(runtime, span, time.perf_counter(), _metric_attributes(span_attributes))


def _coerce_log_level(value: "object", default: "int" = logging.INFO) -> "int":
    if not isinstance(value, str):
        return default
    return _LOG_LEVELS.get(value.lower(), default)

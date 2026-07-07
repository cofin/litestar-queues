import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from inspect import isawaitable
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from litestar_queues.config import execution_backend_name
from litestar_queues.events.context import TaskExecutionContext, _bind_task_context, _reset_task_context
from litestar_queues.events.models import QueueEvent
from litestar_queues.exceptions import JobCancelledError, NonRetryableError, QueueConfigurationError
from litestar_queues.execution import get_execution_backend
from litestar_queues.task import ScheduleConfig, Task, TaskResult, _ensure_utc, get_scheduled_tasks, get_task_registry

if TYPE_CHECKING:
    from collections.abc import Mapping
    from types import TracebackType
    from uuid import UUID

    from litestar_queues.backends import BaseQueueBackend
    from litestar_queues.config import QueueConfig
    from litestar_queues.events import QueueEventLog, QueueEventProducer, QueueEventPublisher
    from litestar_queues.execution import BaseExecutionBackend
    from litestar_queues.models import QueuedTaskRecord, StaleTaskRecoveryResult
    from litestar_queues.observability import QueueObservabilityRuntimeProtocol

__all__ = ("QueueService",)

logger = logging.getLogger(__name__)

_LOG_LEVELS = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "warn": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}


class QueueService:
    """High-level facade for queue and execution backends."""

    __slots__ = (
        "_config",
        "_event_log",
        "_event_publisher",
        "_execution_backend",
        "_observability_runtime",
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
        self._queue_backend = queue_backend
        self._execution_backend = execution_backend
        self._event_log: "QueueEventLog | None" = None
        self._event_publisher = event_publisher
        self._observability_runtime = observability_runtime
        self._sync_executor: "ThreadPoolExecutor | None" = None

    @property
    def config(self) -> "QueueConfig":
        """Queue configuration."""
        return self._config

    def get_queue_backend(self) -> "BaseQueueBackend":
        """Return the configured queue backend."""
        if self._queue_backend is None:
            self._queue_backend = self._config.get_queue_backend()
        return self._queue_backend

    def get_execution_backend(self) -> "BaseExecutionBackend":
        """Return the configured execution backend."""
        if self._execution_backend is None:
            self._execution_backend = self._config.get_execution_backend()
        return self._execution_backend

    def get_event_publisher(self) -> "QueueEventPublisher":
        """Return the configured event publisher."""
        if self._event_publisher is None:
            self._event_publisher = self._config.get_event_publisher()
        return self._event_publisher

    def get_event_producer(self) -> "QueueEventProducer":
        """Return a producer over this service's event publisher."""
        from litestar_queues.events import QueueEventProducer

        return QueueEventProducer(self.get_event_publisher())

    @property
    def observability_runtime(self) -> "QueueObservabilityRuntimeProtocol":
        """Return the configured observability runtime."""
        if self._observability_runtime is None:
            from litestar_queues.observability import create_observability_runtime

            self._observability_runtime = create_observability_runtime(self._config.observability)
        return self._observability_runtime

    async def open(self) -> "Self":
        """Open queue and execution backends.

        Returns:
            The opened service.
        """
        queue_backend = self.get_queue_backend()
        await queue_backend.open()
        try:
            self._configure_event_log(queue_backend)
        except Exception:
            await queue_backend.close()
            raise
        await self.get_execution_backend().open()
        await _call_optional_async_method(self.get_event_publisher().sink, "open")
        if self._config.sync_executor_max_workers is not None and self._sync_executor is None:
            self._sync_executor = ThreadPoolExecutor(
                max_workers=self._config.sync_executor_max_workers,
                thread_name_prefix=self._config.sync_executor_thread_name_prefix,
            )
        return self

    async def close(self) -> "None":
        """Close queue and execution backends."""
        if self._execution_backend is not None:
            await self._execution_backend.close()
        if self._event_log is not None:
            await self._event_log.flush_events()
        if self._queue_backend is not None:
            await self._queue_backend.close()
        if self._event_publisher is not None:
            await _call_optional_async_method(self._event_publisher.sink, "close")
        if self._sync_executor is not None:
            self._sync_executor.shutdown(wait=True, cancel_futures=True)
            self._sync_executor = None

    def _configure_event_log(self, queue_backend: "BaseQueueBackend") -> "None":
        event_log_config = self._config.event_log
        if event_log_config is None or not event_log_config.enabled:
            return
        event_log = queue_backend.get_event_log(event_log_config)
        if event_log is None:
            msg = (
                f"{type(queue_backend).__name__} does not support backend-managed queue event history; "
                "disable EventLogConfig or use a backend that supports durable event history."
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
        key: "str | None" = None,
        queue: "str | None" = None,
        priority: "int | None" = None,
        retries: "int | None" = None,
        timeout: "float | None" = None,
        execution_backend: "str | None" = None,
        execution_profile: "str | None" = None,
        description: "str | None" = None,
        log_level: "str | None" = None,
        quiet_success: "bool | None" = None,
        requeue_on_stale: "bool | None" = None,
        metadata: "dict[str, Any] | None" = None,
        **kwargs: "Any",
    ) -> "TaskResult":
        """Enqueue a registered task.

        Returns:
            A result handle for the queued record.
        """
        task_obj = self.resolve_task(task)
        effective_key = key if key is not None else task_obj.key
        coerced_run_after = _coerce_timedelta(run_after)
        effective_run_after = coerced_run_after if run_after is not None else task_obj.run_after
        effective_scheduled_at = scheduled_at
        if effective_scheduled_at is None and effective_run_after is not None:
            effective_scheduled_at = datetime.now(timezone.utc) + effective_run_after
        if effective_scheduled_at is not None:
            effective_scheduled_at = _ensure_utc(effective_scheduled_at)
        effective_execution_backend = (
            execution_backend or task_obj.execution_backend or execution_backend_name(self._config.execution_backend)
        )
        effective_execution_profile = execution_profile if execution_profile is not None else task_obj.execution_profile
        effective_metadata = _task_metadata(
            task_obj,
            metadata=metadata,
            description=description,
            log_level=log_level,
            quiet_success=quiet_success,
            requeue_on_stale=requeue_on_stale,
            timeout=timeout,
        )
        effective_queue = queue if queue is not None else task_obj.queue
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
            runtime.inject_trace_context(effective_metadata)
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
            )
        except BaseException as exc:
            runtime.record_exception(span, exc)
            raise
        else:
            runtime.set_attribute(span, "messaging.message.id", str(record.id))
            runtime.record_counter("litestar_queues.enqueue.count", attributes=metric_attributes)
            runtime.record_duration(
                "litestar_queues.enqueue.duration", time.perf_counter() - started_at, attributes=metric_attributes
            )
        finally:
            runtime.end_span(span)

        result = TaskResult(record.id, task_obj.name, service=self, record=record)

        if record.execution_backend == "immediate" and record.status == "pending":
            execution_backend_impl = self._execution_backend_for_name(record.execution_backend)
            if not execution_backend_impl.is_external:
                claimed = await self.get_queue_backend().claim_task(record.id)
                if claimed is not None:
                    await execution_backend_impl.execute(self, claimed)
        return result

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

    async def claim_next(
        self, *, queue: "str | None" = None, execution_backend: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        """Claim the next due queued task.

        Returns:
            The claimed task record, if one was available.
        """
        return await self.get_queue_backend().claim_next(queue=queue, execution_backend=execution_backend)

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
        span, started_at, metric_base_attributes = _start_execution_observability(runtime, record)
        task_context = _task_execution_context(record, worker_id=worker_id, event_publisher=self.get_event_publisher())
        context_token = _bind_task_context(task_context)
        final_status = "failed"
        try:
            await task_context.lifecycle("task.started")
            result = await self._execute_task(record, task_obj, task_context, timeout)
        except asyncio.CancelledError as exc:
            final_status = "cancelled"
            runtime.record_exception(span, exc)
            await task_context.lifecycle("task.cancelled")
            self._log_task_event("Queue task cancelled", record, level=logging.WARNING)
            _finish_execution_observability(runtime, span, started_at, metric_base_attributes, final_status)
            raise
        except JobCancelledError as exc:
            cancelled = await self.get_queue_backend().cancel_task(record.id, include_running=True)
            if not cancelled:
                final_status = "claim_lost"
                current = await self.publish_claim_lost(record, phase="cancel", task_context=task_context)
                _finish_execution_observability(runtime, span, started_at, metric_base_attributes, final_status)
                return current
            cancelled_record = await self._current_or_claimed(record)
            final_status = cancelled_record.status
            payload = {"status": cancelled_record.status, "retry_count": cancelled_record.retry_count}
            await task_context.lifecycle("task.cancelled", message=str(exc), payload=payload)
            self._log_task_event("Queue task cancelled", cancelled_record, level=logging.INFO, payload=payload)
            _finish_execution_observability(runtime, span, started_at, metric_base_attributes, final_status)
            return cancelled_record
        except NonRetryableError as exc:
            runtime.record_exception(span, exc)
            return await self._fail_record_without_retry(
                record, exc, task_context, runtime, span, started_at, metric_base_attributes
            )
        except Exception as exc:
            runtime.record_exception(span, exc)
            return await self._fail_record_with_retry(
                record, exc, task_context, runtime, span, started_at, metric_base_attributes
            )
        finally:
            _reset_task_context(context_token)

        completed_record = await self.get_queue_backend().complete_task(
            record.id, result=result, expected_retry_count=record.retry_count
        )
        if completed_record is None:
            final_status = "claim_lost"
            current = await self.publish_claim_lost(record, phase="complete", task_context=task_context)
            _finish_execution_observability(runtime, span, started_at, metric_base_attributes, final_status)
            return current
        completed = completed_record
        final_status = completed.status
        await task_context.lifecycle(
            "task.completed", payload={"status": completed.status, "retry_count": completed.retry_count}
        )
        self._log_task_completed(completed)
        await self._reschedule_if_needed(completed)
        _finish_execution_observability(runtime, span, started_at, metric_base_attributes, final_status)
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
        runtime: "QueueObservabilityRuntimeProtocol",
        span: "Any | None",
        started_at: "float",
        metric_base_attributes: "dict[str, str]",
    ) -> "QueuedTaskRecord":
        error_message = self._error_message(exc, record)
        updated = await self.get_queue_backend().fail_task(
            record.id, error_message, retry=False, expected_retry_count=record.retry_count
        )
        if updated is None:
            return await self._finish_claim_lost_observability(
                record, task_context, runtime, span, started_at, metric_base_attributes
            )
        payload = {"status": updated.status, "retry_count": updated.retry_count, "will_retry": False}
        await task_context.lifecycle("task.failed", message=error_message, payload=payload)
        self._log_task_event("Queue task failed", updated, level=logging.ERROR, payload=payload)
        _finish_execution_observability(runtime, span, started_at, metric_base_attributes, updated.status)
        return updated

    async def _fail_record_with_retry(
        self,
        record: "QueuedTaskRecord",
        exc: "BaseException",
        task_context: "TaskExecutionContext",
        runtime: "QueueObservabilityRuntimeProtocol",
        span: "Any | None",
        started_at: "float",
        metric_base_attributes: "dict[str, str]",
    ) -> "QueuedTaskRecord":
        error_message = self._error_message(exc, record)
        updated = await self.get_queue_backend().fail_task(
            record.id, error_message, expected_retry_count=record.retry_count
        )
        if updated is None:
            return await self._finish_claim_lost_observability(
                record, task_context, runtime, span, started_at, metric_base_attributes
            )
        payload = {
            "status": updated.status,
            "retry_count": updated.retry_count,
            "will_retry": updated.status == "pending",
        }
        await task_context.lifecycle("task.failed", message=error_message, payload=payload)
        self._log_task_event(
            "Queue task failed",
            updated,
            level=logging.WARNING if updated.status == "pending" else logging.ERROR,
            payload=payload,
        )
        if updated.status == "failed":
            await self._reschedule_if_needed(updated)
        _finish_execution_observability(runtime, span, started_at, metric_base_attributes, updated.status)
        return updated

    async def _finish_claim_lost_observability(
        self,
        record: "QueuedTaskRecord",
        task_context: "TaskExecutionContext",
        runtime: "QueueObservabilityRuntimeProtocol",
        span: "Any | None",
        started_at: "float",
        metric_base_attributes: "dict[str, str]",
    ) -> "QueuedTaskRecord":
        current = await self.publish_claim_lost(record, phase="fail", task_context=task_context)
        _finish_execution_observability(runtime, span, started_at, metric_base_attributes, "claim_lost")
        return current

    async def recover_stale_tasks(
        self, *, stale_after: "timedelta", worker_id: "str | None" = None
    ) -> "StaleTaskRecoveryResult":
        """Recover stale running tasks and publish a worker summary event.

        Returns:
            Summary of recovered, failed, skipped, and handler-needed tasks.
        """
        result = await self.get_queue_backend().requeue_stale_running(stale_after=stale_after)
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
            if existing is not None and not existing.is_terminal:
                if existing.metadata.get("schedule") == schedule_metadata:
                    records.append(existing)
                    continue
                await queue_backend.cancel_task(existing.id)
            scheduled_at = schedule.get_next_run(use_initial_delay=True)
            records.append(
                await queue_backend.enqueue(
                    task_name,
                    key=schedule_key,
                    max_retries=0,
                    priority=task_obj.priority,
                    scheduled_at=scheduled_at,
                    execution_backend=task_obj.execution_backend
                    or execution_backend_name(self._config.execution_backend),
                    execution_profile=task_obj.execution_profile,
                    metadata=task_obj.metadata({"schedule": schedule_metadata}),
                )
            )
        return records

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
            max_instances=int(schedule_data.get("max_instances", 1)),
            timeout=schedule_data.get("timeout"),
            timezone=str(schedule_data.get("timezone", "UTC")),
        )
        await self.get_queue_backend().enqueue(
            record.task_name,
            key=record.key,
            queue=record.queue,
            max_retries=record.max_retries,
            priority=record.priority,
            scheduled_at=schedule.get_next_run(record.completed_at),
            execution_backend=record.execution_backend,
            execution_profile=record.execution_profile,
            metadata={**record.metadata, "schedule": schedule.as_metadata()},
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

    def _log_task_completed(self, record: "QueuedTaskRecord") -> "None":
        if record.metadata.get("quiet_success") is True:
            return
        self._log_task_event("Queue task completed", record, level=_coerce_log_level(record.metadata.get("log_level")))

    def _log_task_event(
        self, message: "str", record: "QueuedTaskRecord", *, level: "int", payload: "Mapping[str, object] | None" = None
    ) -> "None":
        logger.log(
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
            logger.warning(
                "Queue task stale failure hook skipped for unknown task",
                extra={"queue_task_id": str(record.id), "queue_task_name": record.task_name},
            )
            return
        hook = task_obj.on_stale_failure
        if hook is None:
            return
        result = hook(record)
        if isawaitable(result):
            await result


async def _call_optional_async_method(target: "object", name: "str") -> "None":
    method = getattr(target, name, None)
    if not callable(method):
        return
    result = method()
    if isawaitable(result):
        await result


def _coerce_timedelta(value: "float | timedelta | None") -> "timedelta | None":
    if value is None:
        return None
    if isinstance(value, timedelta):
        return value
    return timedelta(seconds=value)


def _task_metadata(
    task_obj: "Task[Any, Any]",
    *,
    metadata: "dict[str, Any] | None",
    description: "str | None",
    log_level: "str | None",
    quiet_success: "bool | None",
    requeue_on_stale: "bool | None",
    timeout: "float | None",
) -> "dict[str, Any]":
    effective_metadata = task_obj.metadata(metadata)
    for key, value in (
        ("description", description),
        ("log_level", log_level),
        ("quiet_success", quiet_success),
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
        "queue.execution.profile": execution_profile or "",
    }
    if attempt is not None:
        attributes["queue.task.attempt"] = attempt
    return attributes


def _metric_attributes(attributes: "dict[str, object]") -> "dict[str, str]":
    return {
        "messaging.destination.name": str(attributes["messaging.destination.name"]),
        "queue.task.name": str(attributes["queue.task.name"]),
        "queue.execution.backend": str(attributes["queue.execution.backend"]),
        "queue.execution.profile": str(attributes["queue.execution.profile"]),
    }


def _finish_execution_observability(
    runtime: "QueueObservabilityRuntimeProtocol",
    span: "Any | None",
    started_at: "float",
    metric_base_attributes: "dict[str, str]",
    status: "str",
) -> "None":
    runtime.set_attribute(span, "queue.task.status", status)
    attributes = {**metric_base_attributes, "queue.task.status": status}
    runtime.record_duration(
        "litestar_queues.task.execution.duration", time.perf_counter() - started_at, attributes=attributes
    )
    runtime.record_counter("litestar_queues.task.execution.count", attributes=attributes)
    runtime.end_span(span)


def _start_execution_observability(
    runtime: "QueueObservabilityRuntimeProtocol", record: "QueuedTaskRecord"
) -> "tuple[Any | None, float, dict[str, str]]":
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
    span = runtime.start_span(
        "litestar_queues.process", kind="consumer", attributes=span_attributes, parent=parent_context
    )
    return span, time.perf_counter(), _metric_attributes(span_attributes)


def _coerce_log_level(value: "object", default: "int" = logging.INFO) -> "int":
    if not isinstance(value, str):
        return default
    return _LOG_LEVELS.get(value.lower(), default)

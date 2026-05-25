import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from litestar_queues.config import execution_backend_name
from litestar_queues.events.context import TaskExecutionContext, _bind_task_context, _reset_task_context
from litestar_queues.exceptions import NonRetryableError
from litestar_queues.task import ScheduleConfig, Task, TaskResult, get_scheduled_tasks, get_task_registry

if TYPE_CHECKING:
    from collections.abc import Mapping
    from uuid import UUID

    from litestar_queues.backends import BaseQueueBackend
    from litestar_queues.config import QueueConfig
    from litestar_queues.events import QueueEventPublisher
    from litestar_queues.execution import BaseExecutionBackend
    from litestar_queues.models import QueuedTaskRecord

__all__ = ("QueueService",)


class QueueService:
    """High-level facade for queue and execution backends."""

    __slots__ = ("_config", "_event_publisher", "_execution_backend", "_queue_backend")

    def __init__(
        self,
        config: "QueueConfig",
        *,
        queue_backend: "BaseQueueBackend | None" = None,
        execution_backend: "BaseExecutionBackend | None" = None,
        event_publisher: "QueueEventPublisher | None" = None,
    ) -> None:
        """Initialize the queue service."""
        self._config = config
        self._queue_backend = queue_backend
        self._execution_backend = execution_backend
        self._event_publisher = event_publisher

    @property
    def config(self) -> "QueueConfig":
        """Return the queue configuration."""
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

    async def enqueue(
        self,
        task: str | Task[Any, Any],
        *args: Any,
        scheduled_at: "datetime | None" = None,
        run_after: float | timedelta | None = None,
        key: str | None = None,
        queue: str | None = None,
        priority: int | None = None,
        retries: int | None = None,
        timeout: float | None = None,
        execution_backend: str | None = None,
        execution_profile: str | None = None,
        description: str | None = None,
        log_level: str | None = None,
        quiet_success: bool | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> TaskResult:
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
        effective_execution_backend = (
            execution_backend or task_obj.execution_backend or execution_backend_name(self._config.execution_backend)
        )
        effective_execution_profile = execution_profile if execution_profile is not None else task_obj.execution_profile
        effective_metadata = task_obj.metadata(metadata)
        if description is not None:
            effective_metadata["description"] = description
        if log_level is not None:
            effective_metadata["log_level"] = log_level
        if quiet_success is not None:
            effective_metadata["quiet_success"] = quiet_success
        if timeout is not None:
            effective_metadata["timeout"] = timeout
        record = await self.get_queue_backend().enqueue(
            task_obj.name,
            args=args,
            kwargs=kwargs,
            queue=queue if queue is not None else task_obj.queue,
            priority=priority if priority is not None else task_obj.priority,
            max_retries=retries if retries is not None else task_obj.retries,
            scheduled_at=effective_scheduled_at,
            key=effective_key,
            execution_backend=effective_execution_backend,
            execution_profile=effective_execution_profile,
            metadata=effective_metadata,
        )
        result = TaskResult(record.id, task_obj.name, service=self, record=record)

        if record.execution_backend == "immediate" and record.status == "pending":
            claimed = await self.get_queue_backend().claim_task(record.id)
            if claimed is not None:
                await self.get_execution_backend().execute(self, claimed)
        return result

    def resolve_task(self, task: str | Task[Any, Any]) -> Task[Any, Any]:
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
        self,
        *,
        queue: str | None = None,
        execution_backend: str | None = None,
    ) -> "QueuedTaskRecord | None":
        """Claim the next due queued task.

        Returns:
            The claimed task record, if one was available.
        """
        return await self.get_queue_backend().claim_next(queue=queue, execution_backend=execution_backend)

    async def execute_record(
        self,
        record: "QueuedTaskRecord",
        *,
        worker_id: str | None = None,
    ) -> "QueuedTaskRecord":
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
        task_context = TaskExecutionContext(
            task_id=str(record.id),
            task_name=record.task_name,
            queue=record.queue,
            worker_id=worker_id,
            execution_backend=record.execution_backend,
            execution_profile=record.execution_profile,
            attempt=record.retry_count + 1,
            event_publisher=self.get_event_publisher(),
        )
        context_token = _bind_task_context(task_context)
        try:
            await task_context.lifecycle("task.started")
            extra_kwargs = await self._resolve_task_dependencies(task_obj, record, task_context)
            coroutine = task_obj.execute_record(record, task_context=task_context, extra_kwargs=extra_kwargs)
            result = await asyncio.wait_for(coroutine, timeout=timeout if isinstance(timeout, int | float) else None)
        except asyncio.CancelledError:
            await task_context.lifecycle("task.cancelled")
            raise
        except NonRetryableError as exc:
            updated = await self.get_queue_backend().fail_task(record.id, str(exc), retry=False)
            failed = updated or record
            await task_context.lifecycle(
                "task.failed",
                message=str(exc),
                payload={"status": failed.status, "retry_count": failed.retry_count, "will_retry": False},
            )
            return updated or record
        except Exception as exc:  # noqa: BLE001
            updated = await self.get_queue_backend().fail_task(record.id, str(exc))
            failed = updated or record
            await task_context.lifecycle(
                "task.failed",
                message=str(exc),
                payload={
                    "status": failed.status,
                    "retry_count": failed.retry_count,
                    "will_retry": failed.status == "pending",
                },
            )
            if failed.status == "failed":
                await self._reschedule_if_needed(failed)
            return failed
        finally:
            _reset_task_context(context_token)

        updated = await self.get_queue_backend().complete_task(record.id, result=result)
        completed = updated or record
        await task_context.lifecycle(
            "task.completed",
            payload={"status": completed.status, "retry_count": completed.retry_count},
        )
        await self._reschedule_if_needed(completed)
        return completed

    async def initialize_schedules(self) -> "list[QueuedTaskRecord]":
        """Create queue records for registered recurring schedules.

        Returns:
            The created or reused schedule records.
        """
        records: list["QueuedTaskRecord"] = []
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
                    scheduled_at=scheduled_at,
                    execution_backend=task_obj.execution_backend or execution_backend_name(self._config.execution_backend),
                    execution_profile=task_obj.execution_profile,
                    metadata=task_obj.metadata({"schedule": schedule_metadata}),
                )
            )
        return records

    async def _resolve_task_dependencies(
        self,
        task: Task[..., object],
        record: "QueuedTaskRecord",
        task_context: TaskExecutionContext,
    ) -> "Mapping[str, object] | None":
        """Invoke the configured task dependency resolver, if any.

        Returns:
            The resolver's kwargs mapping, or ``None`` when no resolver is configured.
        """
        resolver = self._config.task_dependency_resolver
        if resolver is None:
            return None
        return await resolver(task, record, task_context)

    async def _reschedule_if_needed(self, record: "QueuedTaskRecord") -> None:
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
            scheduled_at=schedule.get_next_run(record.completed_at),
            execution_backend=record.execution_backend,
            execution_profile=record.execution_profile,
            metadata={**record.metadata, "schedule": schedule.as_metadata()},
        )

    async def open(self) -> Self:
        """Open queue and execution backends.

        Returns:
            The opened service.
        """
        await self.get_queue_backend().open()
        await self.get_execution_backend().open()
        return self

    async def close(self) -> None:
        """Close queue and execution backends."""
        if self._execution_backend is not None:
            await self._execution_backend.close()
        if self._queue_backend is not None:
            await self._queue_backend.close()

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        await self.close()


def _coerce_timedelta(value: float | timedelta | None) -> timedelta | None:
    if value is None:
        return None
    if isinstance(value, timedelta):
        return value
    return timedelta(seconds=value)

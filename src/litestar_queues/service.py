import asyncio
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from litestar_queues.task import ScheduleConfig, Task, TaskResult, get_scheduled_tasks, get_task_registry

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID

    from litestar_queues.backends import BaseQueueBackend
    from litestar_queues.config import QueueConfig
    from litestar_queues.execution import BaseExecutionBackend
    from litestar_queues.models import QueuedTaskRecord

__all__ = ("QueueService",)


class QueueService:
    """High-level facade for queue and execution backends."""

    __slots__ = ("_config", "_execution_backend", "_queue_backend")

    def __init__(
        self,
        config: "QueueConfig",
        *,
        queue_backend: "BaseQueueBackend | None" = None,
        execution_backend: "BaseExecutionBackend | None" = None,
    ) -> None:
        """Initialize the queue service."""
        self._config = config
        self._queue_backend = queue_backend
        self._execution_backend = execution_backend

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

    async def enqueue(
        self,
        task: str | Task[Any, Any],
        *args: Any,
        scheduled_at: "datetime | None" = None,
        key: str | None = None,
        **kwargs: Any,
    ) -> TaskResult:
        """Enqueue a registered task.

        Returns:
            A result handle for the queued record.
        """
        task_obj = self.resolve_task(task)
        effective_key = key if key is not None else task_obj.key
        record = await self.get_queue_backend().enqueue(
            task_obj.name,
            args=args,
            kwargs=kwargs,
            queue=task_obj.queue,
            priority=task_obj.priority,
            max_retries=task_obj.retries,
            scheduled_at=scheduled_at,
            key=effective_key,
            metadata={"execution_backend": task_obj.execution_backend or self._config.execution_backend},
        )
        result = TaskResult(record.id, task_obj.name, service=self, record=record)

        execution_backend_name = task_obj.execution_backend or self._config.execution_backend
        if execution_backend_name == "immediate" and record.status == "pending":
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

    async def claim_next(self, *, queue: str | None = None) -> "QueuedTaskRecord | None":
        """Claim the next due queued task.

        Returns:
            The claimed task record, if one was available.
        """
        return await self.get_queue_backend().claim_next(queue=queue)

    async def execute_record(self, record: "QueuedTaskRecord") -> "QueuedTaskRecord":
        """Execute a claimed queue record and persist the lifecycle result.

        Returns:
            The updated queue record.
        """
        task_obj = self.resolve_task(record.task_name)
        try:
            coroutine = task_obj(*record.args, **record.kwargs)
            result = await asyncio.wait_for(coroutine, timeout=task_obj.timeout)
        except Exception as exc:  # noqa: BLE001
            updated = await self.get_queue_backend().fail_task(record.id, str(exc))
            return updated or record

        updated = await self.get_queue_backend().complete_task(record.id, result=result)
        completed = updated or record
        await self._reschedule_if_needed(completed)
        return completed

    async def initialize_schedules(self) -> "list[QueuedTaskRecord]":
        """Create queue records for registered recurring schedules.

        Returns:
            The created or reused schedule records.
        """
        records: list["QueuedTaskRecord"] = []
        for task_name, schedule in get_scheduled_tasks().items():
            scheduled_at = schedule.get_next_run(use_initial_delay=True)
            records.append(
                await self.get_queue_backend().enqueue(
                    task_name,
                    key=f"scheduled:{task_name}",
                    max_retries=0,
                    scheduled_at=scheduled_at,
                    metadata={"schedule": schedule.as_metadata()},
                )
            )
        return records

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
            max_retries=record.max_retries,
            scheduled_at=schedule.get_next_run(record.completed_at),
            metadata={"schedule": schedule.as_metadata()},
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

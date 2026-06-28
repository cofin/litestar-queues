import asyncio
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from litestar_queues.models import QueueBackendCapabilities, QueueStatistics, StaleTaskRecoveryResult

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime, timedelta
    from types import TracebackType
    from uuid import UUID

    from litestar_queues.config import QueueConfig
    from litestar_queues.models import EnqueueSpec, QueuedTaskRecord

__all__ = ("BaseQueueBackend",)


class BaseQueueBackend:
    """Base class for queue persistence backends."""

    __slots__ = ("config",)

    def __init__(self, config: "QueueConfig | None" = None) -> "None":
        """Initialize the queue backend."""
        self.config = config

    @property
    def capabilities(self) -> "QueueBackendCapabilities":
        """Backend behavior capabilities."""
        return QueueBackendCapabilities()

    async def open(self) -> "bool":
        """Open queue resources.

        Returns:
            True when resources are ready.
        """
        return True

    async def close(self) -> "None":
        """Close queue resources."""

    async def enqueue(
        self,
        task_name: "str",
        *,
        args: "tuple[Any, ...]" = (),
        kwargs: "dict[str, Any] | None" = None,
        queue: "str" = "default",
        priority: "int" = 0,
        max_retries: "int" = 0,
        scheduled_at: "datetime | None" = None,
        key: "str | None" = None,
        execution_backend: "str" = "local",
        execution_profile: "str | None" = None,
        metadata: "dict[str, Any] | None" = None,
    ) -> "QueuedTaskRecord":
        """Persist a queued task."""
        raise NotImplementedError

    async def enqueue_many(self, specs: "Sequence[EnqueueSpec]") -> "list[QueuedTaskRecord]":
        """Persist multiple queued tasks, returning records in input order.

        The default implementation issues one :meth:`enqueue` per spec, which
        preserves per-key deduplication and ordering. Backends with a native
        bulk path (e.g. SQLSpec COPY/Arrow/``execute_many``) override this for
        throughput while keeping the same semantics.
        """
        return [
            await self.enqueue(
                spec.task_name,
                args=spec.args,
                kwargs=spec.kwargs,
                queue=spec.queue,
                priority=spec.priority,
                max_retries=spec.max_retries,
                scheduled_at=spec.scheduled_at,
                key=spec.key,
                execution_backend=spec.execution_backend,
                execution_profile=spec.execution_profile,
                metadata=spec.metadata,
            )
            for spec in specs
        ]

    async def get_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        """Return a queued task by ID."""
        raise NotImplementedError

    async def get_task_by_key(self, key: "str") -> "QueuedTaskRecord | None":
        """Return a queued task by deduplication key."""
        raise NotImplementedError

    async def list_pending(
        self, *, limit: "int" = 1, queue: "str | None" = None, execution_backend: "str | None" = None
    ) -> "list[QueuedTaskRecord]":
        """Return due pending or scheduled tasks ordered for execution."""
        raise NotImplementedError

    async def claim_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        """Atomically claim a pending task."""
        raise NotImplementedError

    async def claim_next(
        self, *, queue: "str | None" = None, execution_backend: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        """Claim the next due task.

        Returns:
            The claimed task record, if one was available.
        """
        records = await self.list_pending(limit=1, queue=queue, execution_backend=execution_backend)
        if not records:
            return None
        return await self.claim_task(records[0].id)

    async def complete_task(
        self, task_id: "UUID", *, result: "Any" = None, expected_retry_count: "int | None" = None
    ) -> "QueuedTaskRecord | None":
        """Mark a task as completed.

        Args:
            task_id: Queue record identifier.
            result: Task result payload.
            expected_retry_count: When provided, update only if the record is
                still running with this retry count.
        """
        raise NotImplementedError

    async def fail_task(
        self, task_id: "UUID", error: "str", *, retry: "bool" = True, expected_retry_count: "int | None" = None
    ) -> "QueuedTaskRecord | None":
        """Mark a task as failed or retry it.

        Args:
            task_id: Queue record identifier.
            error: Error message to persist.
            retry: Whether retry policy may requeue the task.
            expected_retry_count: When provided, update only if the record is
                still running with this retry count.
        """
        raise NotImplementedError

    async def cancel_task(self, task_id: "UUID") -> "bool":
        """Cancel a task if it has not started."""
        raise NotImplementedError

    async def touch_heartbeat(self, task_id: "UUID", *, expected_retry_count: "int | None" = None) -> "bool":
        """Update the heartbeat timestamp for a running task.

        Returns:
            True when a running record matched the optional retry-count fence.
        """
        return False

    async def null_heartbeats(self, task_ids: "list[UUID]", *, expected_retry_count: "int | None" = None) -> "None":
        """Clear heartbeat timestamps for task IDs.

        Args:
            task_ids: Queue record identifiers.
            expected_retry_count: When provided, clear only records that still
                match this retry count.
        """

    async def requeue_stale_running(self, *, stale_after: "timedelta") -> "StaleTaskRecoveryResult":
        """Recover running tasks with stale heartbeats.

        Returns:
            Summary of requeued, failed, skipped, and handler-needed records.
        """
        return StaleTaskRecoveryResult()

    async def set_execution_ref(
        self, task_id: "UUID", execution_backend: "str", execution_ref: "str", *, execution_profile: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        """Persist an external execution reference for a running task.

        Returns:
            The updated queued task record, if one exists.
        """
        record = await self.get_task(task_id)
        if record is None:
            return None
        record.execution_backend = execution_backend
        record.execution_ref = execution_ref
        record.execution_profile = execution_profile
        return record

    async def set_execution_backend(
        self, task_id: "UUID", execution_backend: "str", *, execution_profile: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        """Persist an execution backend/profile change for a queued task.

        Returns:
            The updated queued task record, if one exists.
        """
        record = await self.get_task(task_id)
        if record is None:
            return None
        record.execution_backend = execution_backend
        record.execution_profile = execution_profile
        record.execution_ref = None
        return record

    async def list_running_external(self, *, limit: "int | None" = None) -> "list[QueuedTaskRecord]":
        """Return externally dispatched tasks with references to reconcile."""
        return []

    async def get_statistics(self) -> "QueueStatistics":
        """Return queue status counts."""
        return QueueStatistics()

    async def list_completed_by_task(
        self, task_name: "str", *, since: "datetime | None" = None, limit: "int" = 10
    ) -> "list[QueuedTaskRecord]":
        """Return recent completed records for a task name."""
        return []

    async def cleanup_terminal(self, before: "datetime") -> "int":
        """Delete terminal records completed before a cutoff.

        Returns:
            The number of deleted records.
        """
        return 0

    async def notify_new_task(self, record: "QueuedTaskRecord") -> "None":
        """Notify waiters that a new task is available."""

    async def wait_for_notifications(self, timeout: "float | None" = None) -> "bool":
        """Wait until backend notification arrives.

        Returns:
            True when a notification was observed.
        """
        if timeout is not None:
            await asyncio.sleep(timeout)
        return False

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

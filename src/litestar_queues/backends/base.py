import asyncio
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from litestar_queues.models import (
    HeartbeatTouchResult,
    QueueBackendCapabilities,
    QueueStatistics,
    StaleTaskRecoveryResult,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import datetime, timedelta
    from types import TracebackType
    from uuid import UUID

    from litestar_queues.config import QueueConfig
    from litestar_queues.events import EventLogConfig, QueueEventLog
    from litestar_queues.models import EnqueueSpec, HeartbeatTouch, QueuedTaskRecord

__all__ = ("BaseQueueBackend",)

STALE_HEARTBEAT_ERROR = "Task heartbeat stale"
STALE_REQUEUE_PRIORITY = 4


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

    def get_event_log(self, config: "EventLogConfig") -> "QueueEventLog | None":
        """Return a backend-owned queue event history implementation, if supported."""
        return None

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

        Returns:
            Queue task records in the same order as ``specs``.
        """
        records = [
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
        await self.notify_new_tasks(records)
        return records

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
        self, *, queues: "tuple[str, ...]" = (), execution_backend: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        """Claim the next due task across the requested queues.

        An empty ``queues`` tuple claims across all queues.

        Returns:
            The claimed task record, if one was available.
        """
        for queue in queues or (None,):
            records = await self.list_pending(limit=1, queue=queue, execution_backend=execution_backend)
            if not records:
                continue
            claimed = await self.claim_task(records[0].id)
            if claimed is not None:
                return claimed
        return None

    async def claim_many(
        self, *, limit: "int", queues: "tuple[str, ...]" = (), execution_backend: "str | None" = None
    ) -> "list[QueuedTaskRecord]":
        """Claim up to ``limit`` due tasks across the requested queues.

        An empty ``queues`` tuple claims across all queues. Backends that
        advertise ``supports_batch_claim`` override this with a native batch
        path. The fallback preserves :meth:`claim_next` semantics for backends
        with only a single-record primitive.

        Returns:
            Claimed task records.
        """
        records: "list[QueuedTaskRecord]" = []
        for _ in range(max(0, limit)):
            claimed = await self.claim_next(queues=queues, execution_backend=execution_backend)
            if claimed is None:
                break
            records.append(claimed)
        return records

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

    async def cancel_task(self, task_id: "UUID", *, include_running: "bool" = False) -> "bool":
        """Cancel a task.

        Args:
            task_id: Queue record identifier.
            include_running: When true, cancel a running task as part of a
                cooperative cancellation path. Default behavior only cancels
                pending or scheduled records.
        """
        raise NotImplementedError

    async def cancel_tasks(
        self,
        *,
        task_name: "str | None" = None,
        queue: "str | None" = None,
        kwargs: "Mapping[str, Any] | None" = None,
        metadata: "Mapping[str, Any] | None" = None,
        include_running: "bool" = False,
    ) -> "int":
        """Cancel tasks matching a domain predicate.

        Args:
            task_name: Optional task name exact match.
            queue: Optional queue exact match.
            kwargs: Optional top-level kwargs exact-match subset.
            metadata: Optional top-level metadata exact-match subset.
            include_running: When true, running records are included for
                cooperative cancellation.

        Returns:
            Number of records cancelled.
        """
        raise NotImplementedError

    async def touch_heartbeats(self, touches: "Sequence[HeartbeatTouch]") -> "HeartbeatTouchResult":
        """Update heartbeat timestamps for running tasks.

        Returns:
            The task IDs confirmed touched or missed by the backend.
        """
        return HeartbeatTouchResult(missed_task_ids={touch.task_id for touch in touches})

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

    async def acquire_worker_lock(self, name: "str", *, ttl: "timedelta") -> "bool":
        """Acquire a backend-scoped worker coordination lock.

        Backends that can provide fleet-wide locks should override this. The
        default preserves existing behavior for backends without lock support.

        Returns:
            True when the caller should run the coordinated worker action.
        """
        return True

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

    async def notify_new_tasks(self, records: "Sequence[QueuedTaskRecord]") -> "None":
        """Emit one worker-wakeup hint for a batch of newly available tasks."""
        due = tuple(record for record in records if record.status in {"pending", "scheduled"} and record.is_due)
        if due:
            await self.notify_new_task(due[0])

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


def record_matches_filters(
    record: "QueuedTaskRecord",
    *,
    task_name: "str | None" = None,
    queue: "str | None" = None,
    kwargs: "Mapping[str, Any] | None" = None,
    metadata: "Mapping[str, Any] | None" = None,
) -> "bool":
    if task_name is not None and record.task_name != task_name:
        return False
    if queue is not None and record.queue != queue:
        return False
    if kwargs is not None and not _contains_items(record.kwargs, kwargs):
        return False
    return metadata is None or _contains_items(record.metadata, metadata)


def _contains_items(source: "Mapping[str, Any]", expected: "Mapping[str, Any]") -> "bool":
    return all(source.get(key) == value for key, value in expected.items())


def stale_requeue_error(current_error: "str | None") -> "str":
    """Return the error to retain when a stale running task is requeued."""
    return current_error or STALE_HEARTBEAT_ERROR


def stale_requeue_priority(priority: "int") -> "int":
    """Return the priority for a stale requeued task."""
    return min(priority, STALE_REQUEUE_PRIORITY)

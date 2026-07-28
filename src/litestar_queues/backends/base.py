import asyncio
import logging
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from typing_extensions import Self

from litestar_queues.models import (
    HeartbeatTouchResult,
    QueueBackendCapabilities,
    QueueStatistics,
    StaleTaskRecoveryResult,
)
from litestar_queues.namespace import QueueNamespace

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import datetime, timedelta
    from types import TracebackType
    from uuid import UUID

    from litestar_queues.config import QueueConfig
    from litestar_queues.events import EventHistoryConfig, QueueEventLog
    from litestar_queues.models import HeartbeatTouch, QueuedTaskRecord, TaskRequest, TaskReservation

__all__ = ("EXTERNAL_DISPATCH_RESERVATION_PREFIX", "BaseQueueBackend", "is_external_dispatch_reservation")

EXTERNAL_DISPATCH_RESERVATION_PREFIX = "__litestar_queues_dispatching__:"
STALE_HEARTBEAT_ERROR = "Task heartbeat stale"
STALE_REQUEUE_PRIORITY = 4


class BaseQueueBackend:
    """Base class for queue persistence backends."""

    __slots__ = ("_logger", "config")

    def __init__(self, config: "QueueConfig | None" = None) -> "None":
        """Initialize the queue backend."""
        self.config = config
        names = config.names if config is not None else QueueNamespace()
        self._logger = logging.getLogger(names.logger("backends", type(self).__name__))

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

    def get_event_log(self, config: "EventHistoryConfig") -> "QueueEventLog | None":
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
        expires_at: "datetime | None" = None,
        key: "str | None" = None,
        execution_backend: "str" = "local",
        execution_profile: "str | None" = None,
        metadata: "dict[str, Any] | None" = None,
        id: "UUID | None" = None,  # noqa: A002
    ) -> "QueuedTaskRecord":
        """Persist a queued task.

        When ``id`` is provided the persisted record uses it instead of a freshly
        generated identifier; the service pre-generates it for
        ``unique_until="forever"`` enqueues so the identity reservation and the
        executable record share one id.
        """
        raise NotImplementedError

    async def enqueue_many(self, requests: "Sequence[TaskRequest]") -> "list[QueuedTaskRecord]":
        """Persist multiple queued tasks, returning records in input order.

        The default implementation issues one :meth:`enqueue` per request, which
        preserves per-key deduplication and ordering. Backends with a native
        bulk path (e.g. SQLSpec COPY/Arrow/``execute_many``) override this for
        throughput while keeping the same semantics.

        Returns:
            Queue task records in the same order as ``requests``.
        """
        records = [
            await self.enqueue(
                request.task_name,
                args=request.args,
                kwargs=request.kwargs,
                queue=request.queue,
                priority=request.priority,
                max_retries=request.max_retries,
                scheduled_at=request.scheduled_at,
                expires_at=request.expires_at,
                key=request.key,
                execution_backend=request.execution_backend,
                execution_profile=request.execution_profile,
                metadata=request.metadata,
            )
            for request in requests
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

    async def claim_task_with_expired(
        self, task_id: "UUID"
    ) -> "tuple[QueuedTaskRecord | None, QueuedTaskRecord | None]":
        """Claim one task and report when this call expires that task.

        Returns:
            The claimed record and the expired record, at most one of which is set.
        """
        expired = await self.expire_overdue()
        claimed = await self.claim_task(task_id)
        expired.extend(await self.expire_overdue())
        expired_record = next((record for record in expired if record.id == task_id), None)
        return claimed, expired_record

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

        An empty ``queues`` tuple claims across all queues. Backends with a
        native batch-claim primitive override this method; the fallback here
        preserves :meth:`claim_next` semantics for backends with only a
        single-record primitive.

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

    async def claim_many_with_expired(
        self, *, limit: "int", queues: "tuple[str, ...]" = (), execution_backend: "str | None" = None
    ) -> "tuple[list[QueuedTaskRecord], list[QueuedTaskRecord]]":
        """Claim records and report overdue records transitioned while claiming.

        Returns:
            Claimed records and records expired by this call.
        """
        expired = await self.expire_overdue()
        claimed = await self.claim_many(limit=limit, queues=queues, execution_backend=execution_backend)
        expired.extend(await self.expire_overdue())
        unique = {record.id: record for record in expired}
        return claimed, list(unique.values())

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

        Raises:
            NotImplementedError: Always; every backend must answer this.
        """
        raise NotImplementedError

    async def null_heartbeats(self, task_ids: "list[UUID]", *, expected_retry_count: "int | None" = None) -> "None":
        """Clear heartbeat timestamps for task IDs.

        Args:
            task_ids: Queue record identifiers.
            expected_retry_count: When provided, clear only records that still
                match this retry count.

        Raises:
            NotImplementedError: Always; every backend must answer this.
        """
        raise NotImplementedError

    async def requeue_stale_running(
        self, *, stale_after: "timedelta", limit: "int | None" = None
    ) -> "StaleTaskRecoveryResult":
        """Recover running tasks with stale heartbeats.

        Args:
            stale_after: Heartbeat age past which a running task is stale.
            limit: When provided, recover at most this many records ordered
                oldest-first (oldest heartbeat, then record id). ``None``
                preserves the historical unbounded behavior; bounded
                maintenance always supplies a positive limit.

        Returns:
            Summary of requeued, failed, skipped, and handler-needed records.

        Raises:
            NotImplementedError: Always; every backend must answer this.
        """
        raise NotImplementedError

    async def expire_overdue(self, *, limit: "int | None" = None) -> "list[QueuedTaskRecord]":
        """Transition overdue pending or scheduled records to ``expired``.

        Returns:
            Records transitioned to ``expired``.

        Raises:
            NotImplementedError: Always; every backend must answer this.
        """
        raise NotImplementedError

    async def acquire_worker_lock(self, name: "str", *, ttl: "timedelta") -> "bool":
        """Acquire a backend-scoped worker coordination lock.

        Fleet coordination and maintenance ownership are the same primitive, so
        this routes to :meth:`acquire_maintenance` under a fresh token. The lock
        is never released explicitly: ``ttl`` bounds it so a worker that dies
        mid-pass cannot wedge the fleet, and the next holder takes over once the
        stored ownership expires.

        Returns:
            True when the caller should run the coordinated worker action.
        """
        return await self.acquire_maintenance(name, str(uuid4()), ttl=ttl)

    async def acquire_maintenance(self, name: "str", token: "str", *, ttl: "timedelta") -> "bool":
        """Acquire token-fenced distributed maintenance ownership.

        Only backends advertising ``supports_maintenance`` implement a
        real coordination record. The base raises so maintenance fails closed
        rather than silently running unfenced on a backend that cannot prevent
        overlapping runs.

        Raises:
            NotImplementedError: Always, on backends without maintenance support.
        """
        raise NotImplementedError

    async def release_maintenance(self, name: "str", token: "str") -> "bool":
        """Release maintenance ownership held under ``token``.

        Releases only when the persisted token matches ``token``, so a stale
        holder can never delete a successor's ownership record.

        Returns:
            True when ownership held under ``token`` was released.

        Raises:
            NotImplementedError: Always, on backends without maintenance support.
        """
        raise NotImplementedError

    async def set_execution_ref(
        self, task_id: "UUID", execution_backend: "str", execution_ref: "str", *, execution_profile: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        """Persist an external execution reference for a running task.

        Returns:
            The updated queued task record, if one exists.

        Raises:
            NotImplementedError: Always; every backend must answer this.
        """
        raise NotImplementedError

    async def reserve_external_dispatch(
        self,
        task_id: "UUID",
        execution_backend: "str",
        reservation_ref: "str",
        *,
        execution_profile: "str | None" = None,
    ) -> "QueuedTaskRecord | None":
        """Atomically reserve a due, unexpired task for external dispatch.

        The default rejects dispatch because a read-then-write fallback cannot
        protect the external side effect.
        """
        return None

    async def release_external_dispatch(
        self,
        task_id: "UUID",
        reservation_ref: "str",
        execution_backend: "str",
        *,
        execution_profile: "str | None" = None,
    ) -> "QueuedTaskRecord | None":
        """Release a matching external-dispatch reservation."""
        return None

    async def finalize_external_dispatch(
        self,
        task_id: "UUID",
        reservation_ref: "str",
        execution_backend: "str",
        execution_ref: "str",
        *,
        execution_profile: "str | None" = None,
    ) -> "QueuedTaskRecord | None":
        """Replace an owned dispatch reservation with its execution reference."""
        return None

    async def set_execution_backend(
        self, task_id: "UUID", execution_backend: "str", *, execution_profile: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        """Persist an execution backend/profile change for a queued task.

        Returns:
            The updated queued task record, if one exists.

        Raises:
            NotImplementedError: Always; every backend must answer this.
        """
        raise NotImplementedError

    async def list_running_external(self, *, limit: "int | None" = None) -> "list[QueuedTaskRecord]":
        """Return externally dispatched tasks with references to reconcile.

        Raises:
            NotImplementedError: Always; every backend must answer this.
        """
        raise NotImplementedError

    async def get_statistics(self) -> "QueueStatistics":
        """Return queue status counts.

        Raises:
            NotImplementedError: Always; every backend must answer this.
        """
        raise NotImplementedError

    async def list_completed_by_task(
        self, task_name: "str", *, since: "datetime | None" = None, limit: "int" = 10
    ) -> "list[QueuedTaskRecord]":
        """Return recent completed records for a task name.

        Raises:
            NotImplementedError: Always; every backend must answer this.
        """
        raise NotImplementedError

    async def cleanup_terminal(self, before: "datetime", *, limit: "int | None" = None) -> "int":
        """Delete terminal records completed before a cutoff.

        Routine terminal cleanup never touches ``unique_until="forever"``
        reservations; only :meth:`reset_identity` removes them.

        Args:
            before: Delete terminal records completed strictly before this UTC
                cutoff.
            limit: When provided, delete at most this many records ordered
                oldest-first (oldest ``completed_at``, then record id). ``None``
                preserves the historical unbounded behavior; bounded maintenance
                always supplies a positive limit.

        Returns:
            The number of deleted records.

        Raises:
            NotImplementedError: Always; every backend must answer this.
        """
        raise NotImplementedError

    async def reserve_identity(self, key: "str", *, task_id: "UUID", task_name: "str") -> "TaskReservation | None":
        """Atomically reserve a ``unique_until="forever"`` identity.

        Reservation is atomic: exactly one concurrent caller wins a given key.
        The winner receives ``None`` and owns the durable reservation; every other
        caller receives the existing owner reservation. Reservation is the only
        way a reservation is created and must run before the executable record is
        persisted so a committed forever task can never lack its reservation.

        Args:
            key: The effective identity key to reserve.
            task_id: The originating task id (shared with the executable record).
            task_name: The originating registered task name.

        Returns:
            ``None`` when this caller won the reservation; otherwise the existing
            owner reservation.
        """
        raise NotImplementedError

    async def has_identity(self, key: "str") -> "TaskReservation | None":
        """Return the reservation owning a reserved forever identity, if any."""
        raise NotImplementedError

    async def reset_identity(self, key: "str", *, expected_task_id: "UUID | None" = None) -> "bool":
        """Delete a forever identity reservation.

        This is the only reservation deletion path; routine terminal and event
        maintenance never remove reservations. When ``expected_task_id`` is
        provided, delete only when that task still owns the reservation. This
        compare-and-delete form lets enqueue recovery release its own failed
        reservation without deleting a successor created after an explicit
        reset. Omitting it preserves the explicit administrative reset behavior.

        Args:
            key: The exact effective identity key.
            expected_task_id: Optional task owner required for deletion.

        Returns:
            ``True`` when a reservation was removed.
        """
        raise NotImplementedError

    async def notify_new_task(self, record: "QueuedTaskRecord") -> "None":
        """Notify waiters that a new task is available."""

    async def notify_new_tasks(self, records: "Sequence[QueuedTaskRecord]") -> "None":
        """Emit one worker-wakeup hint for a batch of newly available tasks."""
        due = tuple(record for record in records if record.status in {"pending", "scheduled"} and record.is_due)
        if due:
            await self.notify_new_task(due[0])

    async def wait_for_wakeups(self, timeout: "float | None" = None) -> "bool":
        """Wait until backend notification arrives.

        Returns:
            True when a notification was observed.
        """
        if timeout is not None:
            await asyncio.sleep(timeout)
        return False

    async def time_until_next_due(self, *, queues: "tuple[str, ...]" = ()) -> "float | None":
        """Return seconds until the earliest not-yet-due pending/scheduled record.

        Bounds the worker's adaptive polling wait so a scheduled or retried
        task is never discovered later than its own due time: no backend has
        a push notification for "a record's scheduled time arrived," so a
        worker asleep on a long backoff wait would otherwise only notice
        after that wait elapses. The default reports ``None`` (unknown);
        concrete backends that can answer this cheaply override it. An
        unfiltered or slightly-early answer is always safe here (it can only
        wake the worker sooner than strictly necessary, never later).

        Returns:
            Seconds until the next due record across ``queues`` (all queues
            when empty), or ``None`` when there is no upcoming scheduled work
            or the backend does not support this query.
        """
        del queues
        return None

    async def wait_for_completion(self, task_id: "UUID", *, timeout: "float | None" = None) -> "bool":
        """Wait for a terminal-completion signal for one task.

        Backends that advertise ``supports_completion_events`` override this to
        subscribe to a completion channel. The default returns ``False`` so
        callers fall back to polling.

        Returns:
            True when a completion signal for ``task_id`` was observed.
        """
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


def is_external_dispatch_reservation(execution_ref: "str | None") -> "bool":
    """Return whether an execution reference is a temporary dispatch lease."""
    return execution_ref is not None and execution_ref.startswith(EXTERNAL_DISPATCH_RESERVATION_PREFIX)


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

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from litestar_queues.backends._notification_wait import PendingNativeRead
from litestar_queues.backends.base import (
    STALE_HEARTBEAT_ERROR,
    BaseQueueBackend,
    record_matches_filters,
    stale_requeue_error,
    stale_requeue_priority,
)
from litestar_queues.backends.memory.event_log import InMemoryQueueEventLog
from litestar_queues.models import (
    HeartbeatTouchResult,
    QueueBackendCapabilities,
    QueuedTaskRecord,
    QueueStatistics,
    StaleTaskRecoveryResult,
    TaskReservation,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from uuid import UUID

    from litestar_queues.config import QueueConfig
    from litestar_queues.events import EventHistoryConfig, QueueEventLog
    from litestar_queues.models import HeartbeatTouch, TaskRequest

__all__ = ("InMemoryQueueBackend",)


class InMemoryQueueBackend(BaseQueueBackend):
    """In-process queue backend for tests, local development, and examples."""

    __slots__ = (
        "_event_log",
        "_keys",
        "_lock",
        "_maintenances",
        "_notification_event",
        "_pending_read",
        "_records",
        "_reservations",
    )

    def __init__(self, config: "QueueConfig | None" = None) -> "None":
        super().__init__(config=config)
        self._records: "dict[UUID, QueuedTaskRecord]" = {}
        self._keys: "dict[str, UUID]" = {}
        self._reservations: "dict[str, TaskReservation]" = {}
        self._lock = asyncio.Lock()
        self._notification_event = asyncio.Event()
        self._pending_read = PendingNativeRead()
        self._event_log: "QueueEventLog | None" = None
        self._maintenances: "dict[str, tuple[str, datetime]]" = {}

    @property
    def capabilities(self) -> "QueueBackendCapabilities":
        """Backend behavior capabilities."""
        return QueueBackendCapabilities(
            supports_worker_wakeups=True,
            wakeup_backend="asyncio-event",
            wakeups_durable=False,
            supports_maintenance=True,
        )

    def get_event_log(self, config: "EventHistoryConfig") -> "QueueEventLog | None":
        """Return bounded, process-local queue event history when enabled."""
        if self._event_log is None:
            self._event_log = InMemoryQueueEventLog(config)
        return self._event_log

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
        id: "UUID | None" = None,  # noqa: A002
    ) -> "QueuedTaskRecord":
        async with self._lock:
            if key is not None:
                existing_id = self._keys.get(key)
                if existing_id is not None:
                    existing = self._records.get(existing_id)
                    if existing is not None and not existing.is_terminal:
                        return existing

            record = QueuedTaskRecord(
                task_name=task_name,
                args=args,
                kwargs=dict(kwargs or {}),
                queue=queue,
                execution_backend=execution_backend,
                execution_profile=execution_profile,
                status="scheduled" if scheduled_at is not None and scheduled_at > _utc_now() else "pending",
                priority=priority,
                max_retries=max_retries,
                scheduled_at=scheduled_at,
                key=key,
                metadata=dict(metadata or {}),
            )
            if id is not None:
                record.id = id
            self._records[record.id] = record
            if key is not None:
                self._keys[key] = record.id
        await self.notify_new_task(record)
        return record

    async def enqueue_many(self, requests: "Sequence[TaskRequest]") -> "list[QueuedTaskRecord]":
        """Persist multiple in-memory tasks while signaling waiters once.

        Returns:
            Queue task records in the same order as ``requests``.
        """
        if not requests:
            return []

        records: "list[QueuedTaskRecord]" = []
        now = _utc_now()
        async with self._lock:
            for request in requests:
                if request.key is not None:
                    existing_id = self._keys.get(request.key)
                    if existing_id is not None:
                        existing = self._records.get(existing_id)
                        if existing is not None and not existing.is_terminal:
                            records.append(existing)
                            continue

                record = QueuedTaskRecord(
                    task_name=request.task_name,
                    args=request.args,
                    kwargs=dict(request.kwargs or {}),
                    queue=request.queue,
                    execution_backend=request.execution_backend,
                    execution_profile=request.execution_profile,
                    status=(
                        "scheduled" if request.scheduled_at is not None and request.scheduled_at > now else "pending"
                    ),
                    priority=request.priority,
                    max_retries=request.max_retries,
                    scheduled_at=request.scheduled_at,
                    key=request.key,
                    metadata=dict(request.metadata or {}),
                )
                self._records[record.id] = record
                if request.key is not None:
                    self._keys[request.key] = record.id
                records.append(record)

        await self.notify_new_tasks(records)
        return records

    async def get_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        return self._records.get(task_id)

    async def get_task_by_key(self, key: "str") -> "QueuedTaskRecord | None":
        task_id = self._keys.get(key)
        if task_id is None:
            return None
        return self._records.get(task_id)

    async def list_pending(
        self, *, limit: "int" = 1, queue: "str | None" = None, execution_backend: "str | None" = None
    ) -> "list[QueuedTaskRecord]":
        due_records = [
            record
            for record in self._records.values()
            if record.status in {"pending", "scheduled"}
            and record.is_due
            and (queue is None or record.queue == queue)
            and (execution_backend is None or record.execution_backend == execution_backend)
        ]
        due_records.sort(key=lambda record: (-record.priority, record.created_at))
        return due_records[:limit]

    async def claim_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        async with self._lock:
            record = self._records.get(task_id)
            if record is None or record.status not in {"pending", "scheduled"} or not record.is_due:
                return None
            now = _utc_now()
            record.status = "running"
            record.started_at = now
            record.heartbeat_at = now
            return record

    async def claim_many(
        self, *, limit: "int", queues: "tuple[str, ...]" = (), execution_backend: "str | None" = None
    ) -> "list[QueuedTaskRecord]":
        """Claim up to ``limit`` due tasks under a single lock acquisition.

        Selects eligible records with the same queue/execution/due filter and
        priority ordering as :meth:`list_pending`, then transitions them to
        ``running`` inside one critical section using a single ``now`` snapshot.
        The returned records carry the same owner/start/heartbeat fields a
        sequential :meth:`claim_next` loop would produce.

        Returns:
            Claimed task records in claim order.
        """
        if limit <= 0:
            return []
        async with self._lock:
            now = _utc_now()
            eligible = [
                record
                for record in self._records.values()
                if record.status in {"pending", "scheduled"}
                and (record.scheduled_at is None or record.scheduled_at <= now)
                and (not queues or record.queue in queues)
                and (execution_backend is None or record.execution_backend == execution_backend)
            ]
            eligible.sort(key=lambda record: (-record.priority, record.created_at))
            claimed: "list[QueuedTaskRecord]" = []
            for record in eligible[:limit]:
                record.status = "running"
                record.started_at = now
                record.heartbeat_at = now
                claimed.append(record)
            return claimed

    async def complete_task(
        self, task_id: "UUID", *, result: "Any" = None, expected_retry_count: "int | None" = None
    ) -> "QueuedTaskRecord | None":
        async with self._lock:
            record = self._records.get(task_id)
            if record is None:
                return None
            if expected_retry_count is not None and (
                record.status != "running" or record.retry_count != expected_retry_count
            ):
                return None
            now = _utc_now()
            record.status = "completed"
            record.completed_at = now
            record.heartbeat_at = None
            record.result = result
            record.error = None
            return record

    async def fail_task(
        self, task_id: "UUID", error: "str", *, retry: "bool" = True, expected_retry_count: "int | None" = None
    ) -> "QueuedTaskRecord | None":
        async with self._lock:
            record = self._records.get(task_id)
            if record is None:
                return None
            if expected_retry_count is not None and (
                record.status != "running" or record.retry_count != expected_retry_count
            ):
                return None

            record.error = error
            if retry and record.retry_count < record.max_retries:
                record.retry_count += 1
                record.status = "pending"
                record.started_at = None
                record.heartbeat_at = None
                return record

            now = _utc_now()
            record.status = "failed"
            record.completed_at = now
            record.heartbeat_at = None
            return record

    async def cancel_task(self, task_id: "UUID", *, include_running: "bool" = False) -> "bool":
        async with self._lock:
            record = self._records.get(task_id)
            cancellable_statuses = {"pending", "scheduled", "running"} if include_running else {"pending", "scheduled"}
            if record is None or record.status not in cancellable_statuses:
                return False
            record.status = "cancelled"
            record.completed_at = _utc_now()
            record.heartbeat_at = None
            return True

    async def cancel_tasks(
        self,
        *,
        task_name: "str | None" = None,
        queue: "str | None" = None,
        kwargs: "Mapping[str, Any] | None" = None,
        metadata: "Mapping[str, Any] | None" = None,
        include_running: "bool" = False,
    ) -> "int":
        cancellable_statuses = {"pending", "scheduled", "running"} if include_running else {"pending", "scheduled"}
        cancelled = 0
        async with self._lock:
            for record in self._records.values():
                if record.status not in cancellable_statuses:
                    continue
                if not record_matches_filters(
                    record, task_name=task_name, queue=queue, kwargs=kwargs, metadata=metadata
                ):
                    continue
                record.status = "cancelled"
                record.completed_at = _utc_now()
                record.heartbeat_at = None
                cancelled += 1
        return cancelled

    async def touch_heartbeats(self, touches: "Sequence[HeartbeatTouch]") -> "HeartbeatTouchResult":
        result = HeartbeatTouchResult()
        if not touches:
            return result

        now = _utc_now()
        async with self._lock:
            for touch in touches:
                record = self._records.get(touch.task_id)
                if record is None or record.status != "running":
                    result.missed_task_ids.add(touch.task_id)
                    continue
                if touch.expected_retry_count is not None and record.retry_count != touch.expected_retry_count:
                    result.missed_task_ids.add(touch.task_id)
                    continue
                record.heartbeat_at = now
                if touch.metadata_patch:
                    record.metadata.update(touch.metadata_patch)
                result.touched_task_ids.add(touch.task_id)
        return result

    async def null_heartbeats(self, task_ids: "list[UUID]", *, expected_retry_count: "int | None" = None) -> "None":
        task_id_set = set(task_ids)
        async with self._lock:
            for task_id, record in self._records.items():
                if task_id in task_id_set:
                    if expected_retry_count is not None and record.retry_count != expected_retry_count:
                        continue
                    record.heartbeat_at = None

    async def requeue_stale_running(
        self, *, stale_after: "timedelta", limit: "int | None" = None
    ) -> "StaleTaskRecoveryResult":
        cutoff = _utc_now() - stale_after
        result = StaleTaskRecoveryResult()
        async with self._lock:
            candidates = [
                record
                for record in self._records.values()
                if record.status == "running" and (record.heartbeat_at is None or record.heartbeat_at < cutoff)
            ]
            candidates.sort(key=_stale_sort_key)
            if limit is not None:
                candidates = candidates[:limit]
            for record in candidates:
                requeue_on_stale = record.metadata.get("requeue_on_stale", True) is not False
                if requeue_on_stale and record.retry_count < record.max_retries:
                    record.status = "pending"
                    record.priority = stale_requeue_priority(record.priority)
                    record.started_at = None
                    record.heartbeat_at = None
                    record.error = stale_requeue_error(record.error)
                    record.retry_count += 1
                    result.requeued += 1
                    continue
                record.status = "failed"
                record.completed_at = _utc_now()
                record.heartbeat_at = None
                record.error = STALE_HEARTBEAT_ERROR
                result.failed += 1
                result.failed_task_ids.append(record.id)
                if not requeue_on_stale:
                    result.handler_needed += 1
                    result.handler_needed_task_ids.append(record.id)
        return result

    async def set_execution_ref(
        self, task_id: "UUID", execution_backend: "str", execution_ref: "str", *, execution_profile: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        async with self._lock:
            record = self._records.get(task_id)
            if record is None:
                return None
            record.execution_backend = execution_backend
            record.execution_profile = execution_profile
            record.execution_ref = execution_ref
            return record

    async def set_execution_backend(
        self, task_id: "UUID", execution_backend: "str", *, execution_profile: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        async with self._lock:
            record = self._records.get(task_id)
            if record is None:
                return None
            record.execution_backend = execution_backend
            record.execution_profile = execution_profile
            record.execution_ref = None
        await self.notify_new_task(record)
        return record

    async def list_running_external(self, *, limit: "int | None" = None) -> "list[QueuedTaskRecord]":
        records = [
            record for record in self._records.values() if not record.is_terminal and record.execution_ref is not None
        ]
        records.sort(key=lambda record: (record.started_at or record.created_at, str(record.id)))
        return records[:limit] if limit is not None else records

    async def get_statistics(self) -> "QueueStatistics":
        statistics = QueueStatistics()
        for record in self._records.values():
            setattr(statistics, record.status, getattr(statistics, record.status) + 1)
        return statistics

    async def list_completed_by_task(
        self, task_name: "str", *, since: "datetime | None" = None, limit: "int" = 10
    ) -> "list[QueuedTaskRecord]":
        records = [
            record
            for record in self._records.values()
            if record.task_name == task_name
            and record.status == "completed"
            and record.completed_at is not None
            and (since is None or record.completed_at >= since)
        ]
        records.sort(key=lambda record: record.completed_at or record.created_at, reverse=True)
        return records[:limit]

    async def cleanup_terminal(self, before: "datetime", *, limit: "int | None" = None) -> "int":
        removed = 0
        async with self._lock:
            candidates = [
                record
                for record in self._records.values()
                if record.is_terminal and record.completed_at is not None and record.completed_at < before
            ]
            candidates.sort(key=lambda record: (record.completed_at, str(record.id)))
            if limit is not None:
                candidates = candidates[:limit]
            for record in candidates:
                removed += 1
                del self._records[record.id]
                if record.key is not None and self._keys.get(record.key) == record.id:
                    del self._keys[record.key]
        return removed

    async def acquire_maintenance(self, name: "str", token: "str", *, ttl: "timedelta") -> "bool":
        """Acquire expiring, token-fenced maintenance ownership under the async lock.

        Returns:
            True when ownership was granted to ``token``.
        """
        async with self._lock:
            now = _utc_now()
            existing = self._maintenances.get(name)
            if existing is not None and existing[1] > now and existing[0] != token:
                return False
            self._maintenances[name] = (token, now + ttl)
            return True

    async def release_maintenance(self, name: "str", token: "str") -> "bool":
        """Release maintenance ownership only when ``token`` matches the holder.

        Returns:
            True when ownership held under ``token`` was released.
        """
        async with self._lock:
            existing = self._maintenances.get(name)
            if existing is None or existing[0] != token:
                return False
            del self._maintenances[name]
            return True

    async def reserve_identity(self, key: "str", *, task_id: "UUID", task_name: "str") -> "TaskReservation | None":
        """Reserve a forever identity under the shared lock beside key ownership.

        Returns:
            ``None`` when this caller won the reservation; otherwise the existing
            owner reservation.
        """
        async with self._lock:
            existing = self._reservations.get(key)
            if existing is not None:
                return existing
            reservation = TaskReservation(key=key, task_id=task_id, task_name=task_name, created_at=_utc_now())
            self._reservations[key] = reservation
            return None

    async def has_identity(self, key: "str") -> "TaskReservation | None":
        """Return the reservation owning a reserved forever identity, if any."""
        return self._reservations.get(key)

    async def reset_identity(self, key: "str", *, expected_task_id: "UUID | None" = None) -> "bool":
        """Delete a forever identity reservation under the shared lock.

        Args:
            key: The exact effective identity key.
            expected_task_id: Optional task owner required for deletion.

        Returns:
            ``True`` when a reservation was removed.
        """
        async with self._lock:
            owner = self._reservations.get(key)
            if owner is None or (expected_task_id is not None and owner.task_id != expected_task_id):
                return False
            del self._reservations[key]
            return True

    async def notify_new_task(self, record: "QueuedTaskRecord") -> "None":
        if record.status in {"pending", "scheduled"} and record.is_due:
            self._notification_event.set()

    async def wait_for_wakeups(self, timeout: "float | None" = None) -> "bool":
        if not self._pending_read.has_pending and self._notification_event.is_set():
            self._notification_event.clear()
            return True
        task = await self._pending_read.race(self._notification_event.wait, timeout)
        if task is None:
            return False
        task.result()
        self._notification_event.clear()
        return True

    async def time_until_next_due(self, *, queues: "tuple[str, ...]" = ()) -> "float | None":
        """Return seconds until the earliest not-yet-due pending/scheduled record.

        Returns:
            Seconds until the next due record, or ``None`` when there is no
            upcoming scheduled work.
        """
        now = _utc_now()
        async with self._lock:
            upcoming = [
                record.scheduled_at
                for record in self._records.values()
                if record.status in {"pending", "scheduled"}
                and record.scheduled_at is not None
                and record.scheduled_at > now
                and (not queues or record.queue in queues)
            ]
        if not upcoming:
            return None
        return max((min(upcoming) - _utc_now()).total_seconds(), 0.0)

    async def close(self) -> "None":
        """Cancel any retained notification wait."""
        await self._pending_read.aclose()

    async def clear(self) -> "None":
        """Clear all in-memory records."""
        async with self._lock:
            self._records.clear()
            self._keys.clear()
            self._maintenances.clear()
            self._reservations.clear()
            self._notification_event.clear()
        await self._pending_read.aclose()
        if self._event_log is not None:
            clear = getattr(self._event_log, "clear", None)
            if clear is not None:
                await clear()


_MIN_DATETIME = datetime(1, 1, 1, tzinfo=timezone.utc)


def _stale_sort_key(record: "QueuedTaskRecord") -> "tuple[datetime, str]":
    """Order stale candidates oldest-heartbeat-first, then by record id.

    Records that never heartbeated sort first (most stale).

    Returns:
        A sort key of (effective heartbeat, record id).
    """
    return (record.heartbeat_at or _MIN_DATETIME, str(record.id))


def _utc_now() -> "datetime":
    return datetime.now(timezone.utc)

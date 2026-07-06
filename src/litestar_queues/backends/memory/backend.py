import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from litestar_queues.backends.base import (
    STALE_HEARTBEAT_ERROR,
    BaseQueueBackend,
    record_matches_filters,
    stale_requeue_error,
    stale_requeue_priority,
)
from litestar_queues.models import (
    HeartbeatTouchResult,
    QueueBackendCapabilities,
    QueuedTaskRecord,
    QueueStatistics,
    StaleTaskRecoveryResult,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from uuid import UUID

    from litestar_queues.config import QueueConfig
    from litestar_queues.models import HeartbeatTouch

__all__ = ("InMemoryQueueBackend",)


class InMemoryQueueBackend(BaseQueueBackend):
    """In-process queue backend for tests, local development, and examples."""

    __slots__ = ("_keys", "_lock", "_notification_event", "_records")

    def __init__(self, config: "QueueConfig | None" = None) -> "None":
        super().__init__(config=config)
        self._records: "dict[UUID, QueuedTaskRecord]" = {}
        self._keys: "dict[str, UUID]" = {}
        self._lock = asyncio.Lock()
        self._notification_event = asyncio.Event()

    @property
    def capabilities(self) -> "QueueBackendCapabilities":
        """Backend behavior capabilities."""
        return QueueBackendCapabilities(
            supports_notifications=True, notification_backend="asyncio-event", notifications_durable=False
        )

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
            self._records[record.id] = record
            if key is not None:
                self._keys[key] = record.id
        await self.notify_new_task(record)
        return record

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
            record.heartbeat_at = now
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
            record.heartbeat_at = now
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

    async def requeue_stale_running(self, *, stale_after: "timedelta") -> "StaleTaskRecoveryResult":
        cutoff = _utc_now() - stale_after
        result = StaleTaskRecoveryResult()
        async with self._lock:
            for record in self._records.values():
                if record.status != "running":
                    continue
                if record.heartbeat_at is not None and record.heartbeat_at >= cutoff:
                    result.skipped += 1
                    continue
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
        records.sort(key=lambda record: record.started_at or record.created_at)
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

    async def cleanup_terminal(self, before: "datetime") -> "int":
        removed = 0
        async with self._lock:
            for task_id, record in list(self._records.items()):
                if not record.is_terminal or record.completed_at is None or record.completed_at >= before:
                    continue
                removed += 1
                del self._records[task_id]
                if record.key is not None and self._keys.get(record.key) == task_id:
                    del self._keys[record.key]
        return removed

    async def notify_new_task(self, record: "QueuedTaskRecord") -> "None":
        if record.status in {"pending", "scheduled"}:
            self._notification_event.set()

    async def wait_for_notifications(self, timeout: "float | None" = None) -> "bool":
        if self._notification_event.is_set():
            self._notification_event.clear()
            return True
        try:
            await asyncio.wait_for(self._notification_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return False
        self._notification_event.clear()
        return True

    async def clear(self) -> "None":
        """Clear all in-memory records."""
        async with self._lock:
            self._records.clear()
            self._keys.clear()
            self._notification_event.clear()


def _utc_now() -> "datetime":
    return datetime.now(timezone.utc)

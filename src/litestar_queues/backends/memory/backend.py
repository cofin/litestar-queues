import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from litestar_queues.backends.base import BaseQueueBackend
from litestar_queues.models import QueuedTaskRecord

if TYPE_CHECKING:
    from uuid import UUID

    from litestar_queues.config import QueueConfig

__all__ = ("InMemoryQueueBackend",)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class InMemoryQueueBackend(BaseQueueBackend):
    """In-process queue backend for tests, local development, and examples."""

    __slots__ = ("_keys", "_lock", "_records")

    def __init__(self, config: "QueueConfig | None" = None) -> None:
        super().__init__(config=config)
        self._records: dict[UUID, QueuedTaskRecord] = {}
        self._keys: dict[str, UUID] = {}
        self._lock = asyncio.Lock()

    async def enqueue(
        self,
        task_name: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        queue: str = "default",
        priority: int = 0,
        max_retries: int = 0,
        scheduled_at: datetime | None = None,
        key: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> QueuedTaskRecord:
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
            return record

    async def get_task(self, task_id: "UUID") -> QueuedTaskRecord | None:
        return self._records.get(task_id)

    async def get_task_by_key(self, key: str) -> QueuedTaskRecord | None:
        task_id = self._keys.get(key)
        if task_id is None:
            return None
        return self._records.get(task_id)

    async def list_pending(
        self,
        *,
        limit: int = 1,
        queue: str | None = None,
    ) -> list[QueuedTaskRecord]:
        due_records = [
            record
            for record in self._records.values()
            if record.status in {"pending", "scheduled"} and record.is_due and (queue is None or record.queue == queue)
        ]
        due_records.sort(key=lambda record: (-record.priority, record.created_at))
        return due_records[:limit]

    async def claim_task(self, task_id: "UUID") -> QueuedTaskRecord | None:
        async with self._lock:
            record = self._records.get(task_id)
            if record is None or record.status not in {"pending", "scheduled"} or not record.is_due:
                return None
            now = _utc_now()
            record.status = "running"
            record.started_at = now
            record.heartbeat_at = now
            return record

    async def complete_task(self, task_id: "UUID", *, result: Any = None) -> QueuedTaskRecord | None:
        async with self._lock:
            record = self._records.get(task_id)
            if record is None:
                return None
            now = _utc_now()
            record.status = "completed"
            record.completed_at = now
            record.heartbeat_at = now
            record.result = result
            record.error = None
            return record

    async def fail_task(
        self,
        task_id: "UUID",
        error: str,
        *,
        retry: bool = True,
    ) -> QueuedTaskRecord | None:
        async with self._lock:
            record = self._records.get(task_id)
            if record is None:
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

    async def cancel_task(self, task_id: "UUID") -> bool:
        async with self._lock:
            record = self._records.get(task_id)
            if record is None or record.status not in {"pending", "scheduled"}:
                return False
            record.status = "cancelled"
            record.completed_at = _utc_now()
            return True

    async def touch_heartbeat(self, task_id: "UUID") -> None:
        record = self._records.get(task_id)
        if record is not None and record.status == "running":
            record.heartbeat_at = _utc_now()

    async def requeue_stale_running(self, *, stale_after: timedelta) -> int:
        cutoff = _utc_now() - stale_after
        count = 0
        async with self._lock:
            for record in self._records.values():
                if record.status == "running" and (record.heartbeat_at is None or record.heartbeat_at < cutoff):
                    record.status = "pending"
                    record.started_at = None
                    record.heartbeat_at = None
                    record.retry_count += 1
                    count += 1
        return count

    async def clear(self) -> None:
        """Clear all in-memory records."""
        async with self._lock:
            self._records.clear()
            self._keys.clear()

"""Redis queue backend.

Stores queued task records in a Redis-protocol key-value server. The
implementation lives directly on ``RedisQueueBackend``; the Valkey
backend inherits from this class and only swaps the client factory and
``_backend_name`` ClassVar.
"""

import asyncio
import inspect
import json
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, ClassVar, cast
from uuid import UUID, uuid4

from redis import asyncio as redis_asyncio

from litestar_queues.backends.base import BaseQueueBackend
from litestar_queues.backends.redis.config import RedisBackendConfig
from litestar_queues.exceptions import QueueError
from litestar_queues.models import QueueBackendCapabilities, QueuedTaskRecord, QueueStatistics, TaskStatus

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from litestar_queues.config import QueueConfig

__all__ = ("RedisBackendConfig", "RedisQueueBackend")

_DUE_STATUSES = {"pending", "scheduled"}
_STATUS_VALUES = {"cancelled", "completed", "failed", "pending", "running", "scheduled"}
_TERMINAL_STATUSES = {"cancelled", "completed", "failed"}
_RELEASE_LOCK_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


class RedisQueueBackend(BaseQueueBackend):  # noqa: PLR0904
    """Queue backend that stores records in a Redis-protocol key-value server."""

    _backend_name: ClassVar[str] = "redis"

    __slots__ = (
        "_client",
        "_key_prefix",
        "_lock_timeout",
        "_notification_channel",
        "_notifications",
        "_owns_client",
        "_poll_interval",
        "_url",
    )

    def __init__(
        self,
        config: "QueueConfig | None" = None,
        *,
        backend_config: RedisBackendConfig | None = None,
    ) -> None:
        super().__init__(config=config)
        backend_config = backend_config or RedisBackendConfig()
        self._client = backend_config.client
        self._owns_client = self._client is None
        self._url = backend_config.url
        self._key_prefix = backend_config.key_prefix.rstrip(":")
        self._notifications = backend_config.notifications
        self._notification_channel = backend_config.notification_channel
        self._lock_timeout = backend_config.lock_timeout
        self._poll_interval = backend_config.poll_interval

    @property
    def capabilities(self) -> QueueBackendCapabilities:
        """Return backend behavior capabilities."""
        return QueueBackendCapabilities(
            supports_notifications=self._notifications,
            notification_backend=f"{self._backend_name}-pubsub" if self._notifications else None,
            notifications_durable=False,
        )

    async def open(self) -> bool:
        """Open Redis-protocol client resources.

        Returns:
            True when the client is ready.
        """
        if self._client is None:
            self._client = self._create_client(self._url)
            self._owns_client = True
        return True

    async def close(self) -> None:
        """Close owned Redis-protocol client resources."""
        if not self._owns_client or self._client is None:
            return
        close = getattr(self._client, "aclose", None) or getattr(self._client, "close", None)
        if close is None:
            self._client = None
            return
        result = close()
        if inspect.isawaitable(result):
            await result
        self._client = None

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
        execution_backend: str = "local",
        execution_profile: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> QueuedTaskRecord:
        """Persist a queued task.

        Returns:
            The created or deduplicated queued task record.
        """
        if key is not None:
            async with self._lock(f"key:{key}", wait=True):
                existing = await self.get_task_by_key(key)
                if existing is not None and not existing.is_terminal:
                    return existing
                if existing is not None:
                    await self._clear_key(existing)
                record = self._create_record(
                    task_name,
                    args=args,
                    kwargs=kwargs,
                    queue=queue,
                    priority=priority,
                    max_retries=max_retries,
                    scheduled_at=scheduled_at,
                    key=key,
                    execution_backend=execution_backend,
                    execution_profile=execution_profile,
                    metadata=metadata,
                )
                await self._save_record(record)
                await self._client_hset(self._keys_key, key, str(record.id))
        else:
            record = self._create_record(
                task_name,
                args=args,
                kwargs=kwargs,
                queue=queue,
                priority=priority,
                max_retries=max_retries,
                scheduled_at=scheduled_at,
                key=None,
                execution_backend=execution_backend,
                execution_profile=execution_profile,
                metadata=metadata,
            )
            await self._save_record(record)
        await self.notify_new_task(record)
        return record

    async def get_task(self, task_id: UUID) -> QueuedTaskRecord | None:
        """Return a queued task by ID."""
        mapping = await self._client_hgetall(self._task_key(task_id))
        if not mapping:
            return None
        return self._record_from_mapping(mapping)

    async def get_task_by_key(self, key: str) -> QueuedTaskRecord | None:
        """Return a queued task by deduplication key."""
        task_id = await self._client_hget(self._keys_key, key)
        if task_id is None:
            return None
        return await self.get_task(UUID(str(_decode(task_id))))

    async def list_pending(
        self,
        *,
        limit: int = 1,
        queue: str | None = None,
        execution_backend: str | None = None,
    ) -> list[QueuedTaskRecord]:
        """Return due pending or scheduled tasks ordered for execution."""
        client = await self._get_client()
        candidate_ids = await client.zrangebyscore(self._pending_key, "-inf", _utc_now().timestamp())
        due_records = [
            record
            for record in await self._records_from_ids(candidate_ids)
            if record.status in _DUE_STATUSES
            and record.is_due
            and (queue is None or record.queue == queue)
            and (execution_backend is None or record.execution_backend == execution_backend)
        ]
        due_records.sort(key=lambda record: (-record.priority, record.created_at))
        return due_records[:limit]

    async def claim_task(self, task_id: UUID) -> QueuedTaskRecord | None:
        """Atomically claim a pending task.

        Returns:
            The claimed record, if it was still due and claimable.
        """
        async with self._lock(f"task:{task_id}", wait=False) as acquired:
            if not acquired:
                return None
            record = await self.get_task(task_id)
            if record is None or record.status not in _DUE_STATUSES or not record.is_due:
                return None
            now = _utc_now()
            record.status = "running"
            record.started_at = now
            record.heartbeat_at = now
            await self._save_record(record)
            return record

    async def complete_task(self, task_id: UUID, *, result: Any = None) -> QueuedTaskRecord | None:
        """Mark a task as completed.

        Returns:
            The completed record, if it exists.
        """
        async with self._lock(f"task:{task_id}", wait=True):
            record = await self.get_task(task_id)
            if record is None:
                return None
            now = _utc_now()
            record.status = "completed"
            record.completed_at = now
            record.heartbeat_at = now
            record.result = result
            record.error = None
            await self._save_record(record)
            return record

    async def fail_task(
        self,
        task_id: UUID,
        error: str,
        *,
        retry: bool = True,
    ) -> QueuedTaskRecord | None:
        """Mark a task as failed or retry it.

        Returns:
            The updated record, if it exists.
        """
        async with self._lock(f"task:{task_id}", wait=True):
            record = await self.get_task(task_id)
            if record is None:
                return None
            record.error = error
            if retry and record.retry_count < record.max_retries:
                record.status = "pending"
                record.retry_count += 1
                record.started_at = None
                record.heartbeat_at = None
                await self._save_record(record)
                await self.notify_new_task(record)
                return record
            now = _utc_now()
            record.status = "failed"
            record.completed_at = now
            record.heartbeat_at = now
            await self._save_record(record)
            return record

    async def cancel_task(self, task_id: UUID) -> bool:
        """Cancel a task if it has not started.

        Returns:
            True when the task was cancelled.
        """
        async with self._lock(f"task:{task_id}", wait=True):
            record = await self.get_task(task_id)
            if record is None or record.status not in _DUE_STATUSES:
                return False
            record.status = "cancelled"
            record.completed_at = _utc_now()
            await self._save_record(record)
            return True

    async def touch_heartbeat(self, task_id: UUID) -> None:
        """Update the heartbeat timestamp for a running task."""
        async with self._lock(f"task:{task_id}", wait=True):
            record = await self.get_task(task_id)
            if record is None or record.status != "running":
                return
            record.heartbeat_at = _utc_now()
            await self._save_record(record)

    async def null_heartbeats(self, task_ids: list[UUID]) -> None:
        """Clear heartbeat timestamps for task IDs."""
        for task_id in task_ids:
            async with self._lock(f"task:{task_id}", wait=True):
                record = await self.get_task(task_id)
                if record is None:
                    continue
                record.heartbeat_at = None
                await self._save_record(record)

    async def requeue_stale_running(self, *, stale_after: timedelta) -> int:
        """Requeue running tasks with stale heartbeats.

        Returns:
            Number of requeued records.
        """
        cutoff = _utc_now() - stale_after
        count = 0
        for record in await self._list_records():
            if record.status != "running" or (record.heartbeat_at is not None and record.heartbeat_at >= cutoff):
                continue
            async with self._lock(f"task:{record.id}", wait=False) as acquired:
                if not acquired:
                    continue
                latest = await self.get_task(record.id)
                if latest is None or latest.status != "running":
                    continue
                if latest.heartbeat_at is not None and latest.heartbeat_at >= cutoff:
                    continue
                latest.status = "pending"
                latest.started_at = None
                latest.heartbeat_at = None
                latest.retry_count += 1
                await self._save_record(latest)
                await self.notify_new_task(latest)
                count += 1
        return count

    async def set_execution_ref(
        self,
        task_id: UUID,
        execution_backend: str,
        execution_ref: str,
        *,
        execution_profile: str | None = None,
    ) -> QueuedTaskRecord | None:
        """Persist an external execution reference for a running task.

        Returns:
            The updated record, if it exists.
        """
        async with self._lock(f"task:{task_id}", wait=True):
            record = await self.get_task(task_id)
            if record is None:
                return None
            record.execution_backend = execution_backend
            record.execution_profile = execution_profile
            record.execution_ref = execution_ref
            await self._save_record(record)
            return record

    async def set_execution_backend(
        self,
        task_id: UUID,
        execution_backend: str,
        *,
        execution_profile: str | None = None,
    ) -> QueuedTaskRecord | None:
        """Persist an execution backend/profile change for a queued task.

        Returns:
            The updated record, if it exists.
        """
        async with self._lock(f"task:{task_id}", wait=True):
            record = await self.get_task(task_id)
            if record is None:
                return None
            record.execution_backend = execution_backend
            record.execution_profile = execution_profile
            record.execution_ref = None
            await self._save_record(record)
        await self.notify_new_task(record)
        return record

    async def list_running_external(self, *, limit: int | None = None) -> list[QueuedTaskRecord]:
        """Return externally dispatched tasks with references to reconcile."""
        records = [
            record
            for record in await self._list_records()
            if record.status in {"pending", "scheduled", "running"} and record.execution_ref is not None
        ]
        records.sort(key=lambda record: (record.started_at or record.created_at, record.created_at))
        return records[:limit] if limit is not None else records

    async def get_statistics(self) -> QueueStatistics:
        """Return queue status counts."""
        statistics = QueueStatistics()
        for record in await self._list_records():
            setattr(statistics, record.status, getattr(statistics, record.status) + 1)
        return statistics

    async def list_completed_by_task(
        self,
        task_name: str,
        *,
        since: datetime | None = None,
        limit: int = 10,
    ) -> list[QueuedTaskRecord]:
        """Return recent completed records for a task name."""
        records = [
            record
            for record in await self._list_records()
            if record.task_name == task_name
            and record.status == "completed"
            and record.completed_at is not None
            and (since is None or record.completed_at >= since)
        ]
        records.sort(key=lambda record: record.completed_at or record.created_at, reverse=True)
        return records[:limit]

    async def cleanup_terminal(self, before: datetime) -> int:
        """Delete terminal records completed before a cutoff.

        Returns:
            Number of deleted records.
        """
        count = 0
        for record in await self._list_records():
            if record.status not in _TERMINAL_STATUSES or record.completed_at is None or record.completed_at >= before:
                continue
            async with self._lock(f"task:{record.id}", wait=False) as acquired:
                if not acquired:
                    continue
                latest = await self.get_task(record.id)
                if (
                    latest is None
                    or latest.status not in _TERMINAL_STATUSES
                    or latest.completed_at is None
                    or latest.completed_at >= before
                ):
                    continue
                await self._delete_record(latest)
                count += 1
        return count

    async def notify_new_task(self, record: QueuedTaskRecord) -> None:
        """Publish a Redis-protocol pub/sub message when work is available."""
        if not self._notifications or record.status not in _DUE_STATUSES:
            return
        payload = _json_dumps({
            "task_id": str(record.id),
            "task_name": record.task_name,
            "queue": record.queue,
            "execution_backend": record.execution_backend,
        })
        client = await self._get_client()
        await client.publish(self._notification_channel, payload)

    async def wait_for_notifications(self, timeout: float | None = None) -> bool:
        """Wait for a Redis-protocol pub/sub message when notifications are enabled.

        Returns:
            True when a notification was observed.
        """
        if not self._notifications:
            return await super().wait_for_notifications(timeout=timeout)
        client = await self._get_client()
        pubsub = client.pubsub()
        await pubsub.subscribe(self._notification_channel)
        try:
            return await _wait_for_pubsub_message(pubsub, timeout=timeout)
        finally:
            await _close_pubsub(pubsub, self._notification_channel)

    # ------------------------------------------------------------------
    # Subclass hook
    # ------------------------------------------------------------------

    def _create_client(self, url: str) -> Any:
        return redis_asyncio.from_url(url, decode_responses=True)

    # ------------------------------------------------------------------
    # Private machinery
    # ------------------------------------------------------------------

    async def _get_client(self) -> Any:
        if self._client is None:
            await self.open()
        return self._client

    @asynccontextmanager
    async def _lock(self, lock_name: str, *, wait: bool) -> "AsyncIterator[bool]":
        client = await self._get_client()
        lock_key = self._lock_key(lock_name)
        token = uuid4().hex
        timeout_ms = max(1, int(self._lock_timeout * 1000))
        acquired = bool(await client.set(lock_key, token, nx=True, px=timeout_ms))
        if not acquired and wait:
            deadline = asyncio.get_running_loop().time() + self._lock_timeout
            while not acquired and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(min(self._poll_interval, self._lock_timeout))
                acquired = bool(await client.set(lock_key, token, nx=True, px=timeout_ms))
            if not acquired:
                msg = f"Timed out acquiring {self._backend_name} queue lock: {lock_name}"
                raise QueueError(msg)
        try:
            yield acquired
        finally:
            if acquired:
                await self._release_lock(client, lock_key, token)

    async def _release_lock(self, client: Any, lock_key: str, token: str) -> None:
        eval_method = getattr(client, "eval", None)
        if eval_method is not None:
            result = eval_method(_RELEASE_LOCK_SCRIPT, 1, lock_key, token)
            if inspect.isawaitable(result):
                await result
            return
        if _decode(await client.get(lock_key)) == token:
            await client.delete(lock_key)

    def _create_record(
        self,
        task_name: str,
        *,
        args: tuple[Any, ...],
        kwargs: dict[str, Any] | None,
        queue: str,
        priority: int,
        max_retries: int,
        scheduled_at: datetime | None,
        key: str | None,
        execution_backend: str,
        execution_profile: str | None,
        metadata: dict[str, Any] | None,
    ) -> QueuedTaskRecord:
        return QueuedTaskRecord(
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

    async def _save_record(self, record: QueuedTaskRecord) -> None:
        client = await self._get_client()
        await client.hset(self._task_key(record.id), mapping=self._record_to_mapping(record))
        await client.sadd(self._tasks_key, str(record.id))
        if record.status in _DUE_STATUSES:
            await client.zadd(self._pending_key, {str(record.id): _score_datetime(record.scheduled_at)})
        else:
            await client.zrem(self._pending_key, str(record.id))

    async def _delete_record(self, record: QueuedTaskRecord) -> None:
        client = await self._get_client()
        await client.delete(self._task_key(record.id))
        await client.srem(self._tasks_key, str(record.id))
        await client.zrem(self._pending_key, str(record.id))
        if record.key is not None and str(_decode(await client.hget(self._keys_key, record.key))) == str(record.id):
            await client.hdel(self._keys_key, record.key)

    async def _clear_key(self, record: QueuedTaskRecord) -> None:
        if record.key is not None:
            await self._client_hdel(self._keys_key, record.key)

    async def _list_records(self) -> list[QueuedTaskRecord]:
        client = await self._get_client()
        task_ids = await client.smembers(self._tasks_key)
        return await self._records_from_ids(task_ids)

    async def _records_from_ids(self, task_ids: set[Any] | list[Any] | tuple[Any, ...]) -> list[QueuedTaskRecord]:
        records: list[QueuedTaskRecord] = []
        for value in task_ids:
            record = await self.get_task(UUID(str(_decode(value))))
            if record is not None:
                records.append(record)
        return records

    async def _client_hget(self, name: str, key: str) -> Any:
        client = await self._get_client()
        return await client.hget(name, key)

    async def _client_hgetall(self, name: str) -> dict[str, Any]:
        client = await self._get_client()
        return _decode_mapping(await client.hgetall(name))

    async def _client_hset(self, name: str, key: str, value: Any) -> None:
        client = await self._get_client()
        await client.hset(name, key, value)

    async def _client_hdel(self, name: str, key: str) -> None:
        client = await self._get_client()
        await client.hdel(name, key)

    @property
    def _tasks_key(self) -> str:
        return f"{self._key_prefix}:tasks"

    @property
    def _keys_key(self) -> str:
        return f"{self._key_prefix}:keys"

    @property
    def _pending_key(self) -> str:
        return f"{self._key_prefix}:pending"

    def _task_key(self, task_id: UUID) -> str:
        return f"{self._key_prefix}:task:{task_id}"

    def _lock_key(self, lock_name: str) -> str:
        return f"{self._key_prefix}:locks:{lock_name}"

    def _record_to_mapping(self, record: QueuedTaskRecord) -> dict[str, str]:
        return {
            "id": str(record.id),
            "task_name": record.task_name,
            "args": _json_dumps(list(record.args)),
            "kwargs": _json_dumps(record.kwargs),
            "queue": record.queue,
            "execution_backend": record.execution_backend,
            "execution_profile": record.execution_profile or "",
            "execution_ref": record.execution_ref or "",
            "status": record.status,
            "priority": str(record.priority),
            "max_retries": str(record.max_retries),
            "retry_count": str(record.retry_count),
            "scheduled_at": _serialize_datetime(record.scheduled_at),
            "created_at": _serialize_datetime(record.created_at),
            "started_at": _serialize_datetime(record.started_at),
            "completed_at": _serialize_datetime(record.completed_at),
            "heartbeat_at": _serialize_datetime(record.heartbeat_at),
            "result": _json_dumps(record.result),
            "error": record.error or "",
            "key": record.key or "",
            "metadata": _json_dumps(record.metadata),
        }

    def _record_from_mapping(self, mapping: dict[str, Any]) -> QueuedTaskRecord:
        return QueuedTaskRecord(
            id=UUID(str(mapping["id"])),
            task_name=str(mapping["task_name"]),
            args=tuple(_json_loads(mapping.get("args"), [])),
            kwargs=dict(_json_loads(mapping.get("kwargs"), {})),
            queue=str(mapping.get("queue") or "default"),
            execution_backend=str(mapping.get("execution_backend") or "local"),
            execution_profile=str(mapping["execution_profile"]) if mapping.get("execution_profile") else None,
            execution_ref=str(mapping["execution_ref"]) if mapping.get("execution_ref") else None,
            status=_coerce_status(mapping.get("status")),
            priority=int(str(mapping.get("priority") or 0)),
            max_retries=int(str(mapping.get("max_retries") or 0)),
            retry_count=int(str(mapping.get("retry_count") or 0)),
            scheduled_at=_deserialize_datetime(mapping.get("scheduled_at")),
            created_at=_deserialize_datetime(mapping.get("created_at")) or _utc_now(),
            started_at=_deserialize_datetime(mapping.get("started_at")),
            completed_at=_deserialize_datetime(mapping.get("completed_at")),
            heartbeat_at=_deserialize_datetime(mapping.get("heartbeat_at")),
            result=_json_loads(mapping.get("result"), None),
            error=str(mapping["error"]) if mapping.get("error") else None,
            key=str(mapping["key"]) if mapping.get("key") else None,
            metadata=dict(_json_loads(mapping.get("metadata"), {})),
        )


# ---------------------------------------------------------------------------
# Module-level private helpers
# ---------------------------------------------------------------------------


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _serialize_datetime(value: datetime | None) -> str:
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _deserialize_datetime(value: Any) -> datetime | None:
    value = _decode(value)
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _score_datetime(value: datetime | None) -> float:
    if value is None or value <= _utc_now():
        return 0.0
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).timestamp()


def _decode(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode()
    return value


def _decode_mapping(mapping: dict[Any, Any]) -> dict[str, Any]:
    return {str(_decode(key)): _decode(value) for key, value in mapping.items()}


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return _serialize_datetime(value)
    return str(value)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, default=_json_default, separators=(",", ":"), sort_keys=True)


def _json_loads(value: Any, default: Any) -> Any:
    value = _decode(value)
    if value in {None, ""}:
        return default
    return json.loads(str(value))


def _coerce_status(value: Any) -> TaskStatus:
    status = str(_decode(value))
    if status not in _STATUS_VALUES:
        msg = f"Unknown queued task status from Redis-protocol queue backend: {status!r}"
        raise ValueError(msg)
    return cast("TaskStatus", status)


async def _wait_for_pubsub_message(pubsub: Any, *, timeout: float | None) -> bool:
    """Drain pubsub responses until a real ``message`` arrives or timeout.

    ``pubsub.get_message(ignore_subscribe_messages=True)`` returns ``None``
    for both "no message in this read window" AND "subscribe-confirmation
    was filtered". Looping with a deadline distinguishes the two cases.

    Returns:
        True when a real published message was observed before the deadline.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout if timeout is not None else None
    while True:
        remaining = None if deadline is None else max(0.0, deadline - loop.time())
        if remaining is not None and remaining <= 0.0:
            return False
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=remaining)
        if message is not None:
            return True
        if deadline is None:
            return False


async def _close_pubsub(pubsub: Any, channel: str) -> None:
    """Best-effort unsubscribe + close on a pubsub connection."""
    unsubscribe = getattr(pubsub, "unsubscribe", None)
    if unsubscribe is not None:
        result = unsubscribe(channel)
        if inspect.isawaitable(result):
            with suppress(Exception):
                await result
    close = getattr(pubsub, "aclose", None) or getattr(pubsub, "close", None)
    if close is None:
        return
    result = close()
    if inspect.isawaitable(result):
        with suppress(Exception):
            await result

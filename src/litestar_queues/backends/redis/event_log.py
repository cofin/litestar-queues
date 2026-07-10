"""Redis-protocol queue event history."""

# ruff: noqa: SLF001

import asyncio
import hashlib
import inspect
import json
import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from litestar_queues.events._log_records import (
    event_log_record_from_event,
    event_log_record_sort_key,
    optional_float,
    optional_int,
    optional_str,
    parse_datetime,
)
from litestar_queues.events.log import QueueEventLogRecord

if TYPE_CHECKING:
    from litestar_queues.backends.redis._typing import RedisClientLike, RedisPipelineLike
    from litestar_queues.backends.redis.backend import RedisQueueBackend
    from litestar_queues.events import EventLogConfig, QueueEvent, QueueEventStageSummary

__all__ = ("RedisQueueEventLog",)

logger = logging.getLogger(__name__)


class RedisQueueEventLog:
    """Buffered Redis-protocol event-history writer and query interface."""

    __slots__ = ("_backend", "_config", "_flush_lock", "_last_flush", "_pending")

    def __init__(self, *, backend: "RedisQueueBackend", config: "EventLogConfig") -> "None":
        self._backend = backend
        self._config = config
        self._pending: "list[dict[str, str]]" = []
        self._last_flush = time.monotonic()
        self._flush_lock = asyncio.Lock()

    async def publish_event(self, event: "QueueEvent") -> "None":
        """Buffer a queue event and flush when configured thresholds are reached."""
        should_flush = False
        async with self._flush_lock:
            self._pending.append(self._mapping_from_record(event_log_record_from_event(event)))
            should_flush = len(self._pending) >= max(1, self._config.buffer_size) or self._flush_interval_elapsed()
        if should_flush:
            await self.flush_events()

    async def flush_events(self) -> "None":
        """Flush buffered queue events through a Redis pipeline.

        Returns:
            None.
        """
        async with self._flush_lock:
            if not self._pending:
                return
            batch = list(self._pending)
            try:
                client = await self._backend._get_client()
                await self._write_batch(client, batch)
            except Exception:
                if self._config.strict:
                    raise
                logger.warning("Redis queue event history flush failed", exc_info=True)
                return
            del self._pending[: len(batch)]
            self._last_flush = time.monotonic()

    async def list_events(
        self, *, task_id: "str | None" = None, task_name: "str | None" = None, limit: "int | None" = None
    ) -> "list[QueueEventLogRecord]":
        """Return durable event history records."""
        await self.flush_events()
        client = await self._backend._get_client()
        index_key = self._select_index_key(task_id=task_id, task_name=task_name)
        event_ids = await client.zrangebyscore(index_key, "-inf", "+inf")
        records = await self._records_from_ids(client, event_ids)
        records = [
            record
            for record in records
            if (task_id is None or record.task_id == task_id) and (task_name is None or record.task_name == task_name)
        ]
        records.sort(key=event_log_record_sort_key)
        return records[:limit] if limit is not None else records

    async def summarize_stages(self, *, task_name: "str | None" = None) -> "list[QueueEventStageSummary]":
        """Return no aggregate summaries for the Redis-protocol event log."""
        del task_name
        return []

    async def cleanup_before(self, before: "datetime") -> "int":
        """Delete event history older than ``before``.

        Returns:
            Number of removed event-history records.
        """
        await self.flush_events()
        client = await self._backend._get_client()
        event_ids = await client.zrangebyscore(
            self._backend._event_log_global_key(), "-inf", f"({_score_datetime(before)}"
        )
        mappings = await self._mappings_from_ids(client, event_ids)
        removed = 0
        pipeline = _create_pipeline(client)
        for event_id, mapping in zip(event_ids, mappings, strict=True):
            decoded_event_id = str(_decode(event_id))
            if not mapping:
                if pipeline is not None:
                    pipeline.zrem(self._backend._event_log_global_key(), decoded_event_id)
                else:
                    await client.zrem(self._backend._event_log_global_key(), decoded_event_id)
                continue
            record = _record_from_mapping(mapping)
            if record.occurred_at >= before:
                continue
            index_keys = _json_loads(mapping.get("index_keys"), [])
            event_key = self._backend._event_log_event_key(record.event_id)
            if pipeline is not None:
                pipeline.delete(event_key)
                for index_key in index_keys:
                    pipeline.zrem(str(index_key), record.event_id)
            else:
                await client.delete(event_key)
                for index_key in index_keys:
                    await client.zrem(str(index_key), record.event_id)
            removed += 1
        if pipeline is not None:
            await _execute_pipeline(pipeline)
        return removed

    async def _write_batch(self, client: "RedisClientLike", batch: "list[dict[str, str]]") -> "None":
        pipeline = _create_pipeline(client)
        if pipeline is not None:
            for mapping in batch:
                self._queue_write(pipeline, mapping)
            await _execute_pipeline(pipeline)
            return
        for mapping in batch:
            event_id = mapping["event_id"]
            await client.hset(self._backend._event_log_event_key(event_id), mapping=mapping)
            score = _score_datetime(parse_datetime(mapping["occurred_at"]))
            for index_key in _json_loads(mapping["index_keys"], []):
                await client.zadd(str(index_key), {event_id: score})

    def _queue_write(self, pipeline: "RedisPipelineLike", mapping: "dict[str, str]") -> "None":
        event_id = mapping["event_id"]
        pipeline.hset(self._backend._event_log_event_key(event_id), mapping=mapping)
        score = _score_datetime(parse_datetime(mapping["occurred_at"]))
        for index_key in _json_loads(mapping["index_keys"], []):
            pipeline.zadd(str(index_key), {event_id: score})

    def _mapping_from_record(self, record: "QueueEventLogRecord") -> "dict[str, str]":
        index_keys = [self._backend._event_log_global_key(), self._backend._event_log_event_type_key(record.event_type)]
        if record.task_id is not None:
            index_keys.append(self._backend._event_log_task_key(record.task_id))
        if record.task_name is not None:
            index_keys.append(self._backend._event_log_task_name_key(record.task_name))
        return {
            "event_id": record.event_id,
            "event_type": record.event_type,
            "task_id": record.task_id or "",
            "task_name": record.task_name or "",
            "queue": record.queue or "",
            "worker_id": record.worker_id or "",
            "execution_backend": record.execution_backend or "",
            "execution_profile": record.execution_profile or "",
            "level": record.level or "",
            "message": record.message or "",
            "detail": _json_dumps(record.detail),
            "progress_current": _optional_number(record.progress_current),
            "progress_total": _optional_number(record.progress_total),
            "progress_percent": _optional_number(record.progress_percent),
            "sequence": "" if record.sequence is None else str(record.sequence),
            "occurred_at": _serialize_datetime(record.occurred_at),
            "created_at": _serialize_datetime(record.created_at),
            "index_keys": _json_dumps(index_keys),
        }

    async def _records_from_ids(self, client: "RedisClientLike", event_ids: "list[Any]") -> "list[QueueEventLogRecord]":
        return [
            _record_from_mapping(mapping) for mapping in await self._mappings_from_ids(client, event_ids) if mapping
        ]

    async def _mappings_from_ids(self, client: "RedisClientLike", event_ids: "list[Any]") -> "list[dict[str, Any]]":
        event_keys = [self._backend._event_log_event_key(str(_decode(event_id))) for event_id in event_ids]
        if not event_keys:
            return []
        pipeline = _create_pipeline(client)
        if pipeline is None:
            return [_decode_mapping(await client.hgetall(key)) for key in event_keys]
        for key in event_keys:
            pipeline.hgetall(key)
        return [_decode_mapping(cast("dict[Any, Any]", result)) for result in await _execute_pipeline(pipeline)]

    def _select_index_key(self, *, task_id: "str | None", task_name: "str | None") -> "str":
        if task_id is not None:
            return self._backend._event_log_task_key(task_id)
        if task_name is not None:
            return self._backend._event_log_task_name_key(task_name)
        return self._backend._event_log_global_key()

    def _flush_interval_elapsed(self) -> "bool":
        return self._config.flush_interval <= 0 or time.monotonic() - self._last_flush >= self._config.flush_interval


def _record_from_mapping(mapping: "dict[str, Any]") -> "QueueEventLogRecord":
    detail = _json_loads(mapping.get("detail"), {})
    if not isinstance(detail, dict):
        detail = {}
    return QueueEventLogRecord(
        event_id=str(mapping["event_id"]),
        event_type=str(mapping["event_type"]),
        task_id=_optional_mapping_str(mapping.get("task_id")),
        task_name=_optional_mapping_str(mapping.get("task_name")),
        queue=_optional_mapping_str(mapping.get("queue")),
        worker_id=_optional_mapping_str(mapping.get("worker_id")),
        execution_backend=_optional_mapping_str(mapping.get("execution_backend")),
        execution_profile=_optional_mapping_str(mapping.get("execution_profile")),
        stage=optional_str(detail.get("stage")),
        level=_optional_mapping_str(mapping.get("level")),
        message=_optional_mapping_str(mapping.get("message")),
        detail=detail,
        progress_current=optional_float(_json_loads(mapping.get("progress_current"), None)),
        progress_total=optional_float(_json_loads(mapping.get("progress_total"), None)),
        progress_percent=optional_float(_json_loads(mapping.get("progress_percent"), None)),
        duration_ms=optional_float(detail.get("duration_ms")),
        sequence=optional_int(mapping.get("sequence") or None),
        occurred_at=parse_datetime(mapping["occurred_at"]),
        created_at=parse_datetime(mapping["created_at"]),
    )


def _optional_number(value: "float | None") -> "str":
    return "" if value is None else _json_dumps(value)


def _optional_mapping_str(value: "Any") -> "str | None":
    if value in {None, ""}:
        return None
    return optional_str(value)


def _serialize_datetime(value: "datetime") -> "str":
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _score_datetime(value: "datetime") -> "float":
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).timestamp()


def _json_dumps(value: "Any") -> "str":
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _json_loads(value: "Any", default: "Any") -> "Any":
    value = _decode(value)
    if value in {None, ""}:
        return default
    return json.loads(str(value))


def _decode(value: "Any") -> "Any":
    if isinstance(value, bytes):
        return value.decode()
    return value


def _decode_mapping(mapping: "dict[Any, Any]") -> "dict[str, Any]":
    return {str(_decode(key)): _decode(value) for key, value in mapping.items()}


def _create_pipeline(client: "RedisClientLike") -> "RedisPipelineLike | None":
    pipeline_factory = getattr(client, "pipeline", None)
    if pipeline_factory is None:
        return None
    try:
        return cast("RedisPipelineLike", pipeline_factory(transaction=False))
    except TypeError:
        return cast("RedisPipelineLike", pipeline_factory())


async def _execute_pipeline(pipeline: "RedisPipelineLike") -> "list[Any]":
    result = pipeline.execute()
    if inspect.isawaitable(result):
        return list(await result)
    return list(cast("list[Any]", result))


def hashed_index_value(value: "str") -> "str":
    """Return a stable Redis-key-safe index value."""
    return hashlib.sha256(value.encode()).hexdigest()

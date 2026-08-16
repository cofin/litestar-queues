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
from litestar_queues.events.history import QueueEventLogRecord

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from litestar_queues.backends._protocol import ClientLike, PipelineLike
    from litestar_queues.backends.redis.backend import RedisQueueBackend
    from litestar_queues.events import EventHistoryConfig, QueueEvent, QueueEventStageSummary
    from litestar_queues.events.query import QueueEventQuery
    from litestar_queues.events.typing import OffsetPagination

__all__ = ("RedisQueueEventLog",)

logger = logging.getLogger(__name__)


class RedisQueueEventLog:
    """Buffered Redis-protocol event-history writer and query interface."""

    __slots__ = ("_backend", "_config", "_flush_lock", "_last_flush", "_logger", "_pending")

    def __init__(self, *, backend: "RedisQueueBackend", config: "EventHistoryConfig") -> "None":
        self._backend = backend
        self._config = config
        self._pending: "list[dict[str, str]]" = []
        self._last_flush = time.monotonic()
        self._flush_lock = asyncio.Lock()
        self._logger = backend._logger

    async def publish_event(self, event: "QueueEvent") -> "None":
        """Buffer a queue event and flush when configured thresholds are reached."""
        should_flush = False
        async with self._flush_lock:
            self._pending.append(
                self._mapping_from_record(event_log_record_from_event(event, extra_columns=self._config.extra_columns))
            )
            should_flush = len(self._pending) >= max(1, self._config.batch_size) or self._flush_interval_elapsed()
        if should_flush:
            await self.flush_events()

    async def flush_events(self) -> "None":
        """Flush buffered queue events through a Redis pipeline."""
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
                self._logger.warning("Redis queue event history flush failed", exc_info=True)
                return
            del self._pending[: len(batch)]
            self._last_flush = time.monotonic()

    async def query_events(
        self, query: "QueueEventQuery | None" = None, *, extra: "Mapping[str, str] | None" = None
    ) -> "OffsetPagination[QueueEventLogRecord]":
        """Return a filtered, ordered page of event history records.

        Returns:
            The matching page.
        """
        from litestar_queues.events.history import event_extra_filter_matches, validate_event_extra_filter
        from litestar_queues.events.query import match_event_record, paginate_event_records, sort_event_records

        resolved_extra = validate_event_extra_filter(extra, self._config.extra_columns)
        await self.flush_events()
        client = await self._backend._get_client()
        index_key = self._select_index_key(query)
        event_ids = await client.zrangebyscore(index_key, "-inf", "+inf")
        records = [
            record for record in await self._records_from_ids(client, event_ids) if match_event_record(record, query)
        ]
        if resolved_extra:
            records = [record for record in records if event_extra_filter_matches(record, resolved_extra)]
        ordered = sort_event_records(records, order="asc" if query is None else query.order)
        return paginate_event_records(ordered, query)

    async def summarize_stages(self, query: "QueueEventQuery | None" = None) -> "list[QueueEventStageSummary]":
        """Return per-stage event history aggregates."""
        from litestar_queues.events.query import match_event_record, require_unpaginated_query, summarize_event_records

        require_unpaginated_query(query)
        await self.flush_events()
        client = await self._backend._get_client()
        index_key = self._select_index_key(query)
        event_ids = await client.zrangebyscore(index_key, "-inf", "+inf")
        records = [
            record for record in await self._records_from_ids(client, event_ids) if match_event_record(record, query)
        ]
        return summarize_event_records(records)

    async def cleanup_events(  # noqa: C901
        self,
        *,
        before: "datetime",
        match: "QueueEventQuery | None" = None,
        exclude: "Sequence[QueueEventQuery]" = (),
        limit: "int | None" = None,
    ) -> "int":
        """Delete event history older than ``before``.

        Returns:
            Number of removed event-history records.
        """
        from litestar_queues.events.query import match_event_record

        await self.flush_events()
        client = await self._backend._get_client()
        index_key = self._select_index_key(match)
        max_score = f"({_score_datetime(before)}"

        # We read the entire expired window into memory, decode mappings, and filter,
        # then apply the limit. Trade-off: the read window is unbounded but the write is bounded.
        event_ids = await client.zrangebyscore(index_key, "-inf", max_score)

        mappings = await self._mappings_from_ids(client, event_ids)

        # Identify valid records vs orphans
        valid_records = []
        orphans = []
        for event_id, mapping in zip(event_ids, mappings, strict=True):
            if not mapping:
                orphans.append(_decode(event_id))
            else:
                record = _record_from_mapping(mapping)
                if record.occurred_at < before:
                    valid_records.append((record, mapping))

        # Filter valid records
        filtered = []
        for record, mapping in valid_records:
            if match and not match_event_record(record, match):
                continue
            if exclude and any(match_event_record(record, ex) for ex in exclude):
                continue
            filtered.append((record, mapping))

        # Sort ascending by stable key
        filtered.sort(key=lambda item: event_log_record_sort_key(item[0]))

        if limit is not None:
            filtered = filtered[:limit]

        pipeline = _create_pipeline(client)
        removed = 0

        # Cleanup orphans
        for decoded_event_id in orphans:
            if pipeline is not None:
                pipeline.zrem(self._backend._event_log_global_key(), str(decoded_event_id))
            else:
                await client.zrem(self._backend._event_log_global_key(), str(decoded_event_id))

        # Cleanup valid records
        global_key = self._backend._event_log_global_key()
        for record, mapping in filtered:
            index_keys = _json_loads(mapping.get("index_keys"), [])
            event_key = self._backend._event_log_event_key(record.event_id)
            if pipeline is not None:
                pipeline.delete(event_key)
                pipeline.zrem(global_key, record.event_id)
                for i_key in index_keys:
                    if str(i_key) != global_key:
                        pipeline.zrem(str(i_key), record.event_id)
            else:
                await client.delete(event_key)
                await client.zrem(global_key, record.event_id)
                for i_key in index_keys:
                    if str(i_key) != global_key:
                        await client.zrem(str(i_key), record.event_id)
            removed += 1

        if pipeline is not None:
            await _execute_pipeline(pipeline)

        return removed

    async def _write_batch(self, client: "ClientLike", batch: "list[dict[str, str]]") -> "None":
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

    def _queue_write(self, pipeline: "PipelineLike", mapping: "dict[str, str]") -> "None":
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
        if record.scope_key is not None:
            index_keys.append(self._backend._event_log_scope_key_key(record.scope_key))
        if record.entity is not None:
            index_keys.append(self._backend._event_log_entity_key(record.entity))
        result_mapping = {
            "event_id": record.event_id,
            "event_type": record.event_type,
            "task_id": record.task_id or "",
            "task_name": record.task_name or "",
            "queue": record.queue or "",
            "worker_id": record.worker_id or "",
            "execution_backend": record.execution_backend or "",
            "execution_profile": record.execution_profile or "",
            "actor_type": record.actor_type or "",
            "actor_id": record.actor_id or "",
            "level": record.level or "",
            "message": record.message or "",
            "detail": _json_dumps(record.detail),
            "progress_current": _optional_number(record.progress_current),
            "progress_total": _optional_number(record.progress_total),
            "progress_percent": _optional_number(record.progress_percent),
            "sequence": "" if record.sequence is None else str(record.sequence),
            "occurred_at": _serialize_datetime(record.occurred_at),
            "created_at": _serialize_datetime(record.created_at),
            "scope": record.scope or "",
            "scope_key": record.scope_key or "",
            "actor": record.actor or "",
            "entity": record.entity or "",
            "index_keys": _json_dumps(index_keys),
        }
        for extra_key, extra_val in record.extra.items():
            result_mapping[f"extra:{extra_key}"] = str(extra_val)
        return result_mapping

    async def _records_from_ids(self, client: "ClientLike", event_ids: "list[Any]") -> "list[QueueEventLogRecord]":
        return [
            _record_from_mapping(mapping) for mapping in await self._mappings_from_ids(client, event_ids) if mapping
        ]

    async def _mappings_from_ids(self, client: "ClientLike", event_ids: "list[Any]") -> "list[dict[str, Any]]":
        event_keys = [self._backend._event_log_event_key(str(_decode(event_id))) for event_id in event_ids]
        if not event_keys:
            return []
        pipeline = _create_pipeline(client)
        if pipeline is None:
            return [_decode_mapping(await client.hgetall(key)) for key in event_keys]
        for key in event_keys:
            pipeline.hgetall(key)
        return [_decode_mapping(cast("dict[Any, Any]", result)) for result in await _execute_pipeline(pipeline)]

    def _select_index_key(self, query: "QueueEventQuery | None") -> "str":
        if query is None:
            return self._backend._event_log_global_key()
        if query.task_id is not None:
            return self._backend._event_log_task_key(query.task_id)
        if query.entity is not None:
            return self._backend._event_log_entity_key(query.entity)
        if query.scope_key is not None:
            return self._backend._event_log_scope_key_key(query.scope_key)
        if query.task_name is not None:
            return self._backend._event_log_task_name_key(query.task_name)
        if query.event_type is not None:
            return self._backend._event_log_event_type_key(query.event_type)
        return self._backend._event_log_global_key()

    def _flush_interval_elapsed(self) -> "bool":
        return self._config.flush_interval <= 0 or time.monotonic() - self._last_flush >= self._config.flush_interval


def _record_from_mapping(mapping: "dict[str, Any]") -> "QueueEventLogRecord":
    detail = _json_loads(mapping.get("detail"), {})
    if not isinstance(detail, dict):
        detail = {}
    extra = {key[6:]: str(val) for key, val in mapping.items() if key.startswith("extra:")}
    return QueueEventLogRecord(
        event_id=str(mapping["event_id"]),
        event_type=str(mapping["event_type"]),
        task_id=_optional_mapping_str(mapping.get("task_id")),
        task_name=_optional_mapping_str(mapping.get("task_name")),
        queue=_optional_mapping_str(mapping.get("queue")),
        worker_id=_optional_mapping_str(mapping.get("worker_id")),
        execution_backend=_optional_mapping_str(mapping.get("execution_backend")),
        execution_profile=_optional_mapping_str(mapping.get("execution_profile")),
        actor_type=_optional_mapping_str(mapping.get("actor_type")),
        actor_id=_optional_mapping_str(mapping.get("actor_id")),
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
        scope=_optional_mapping_str(mapping.get("scope")),
        scope_key=_optional_mapping_str(mapping.get("scope_key")),
        actor=_optional_mapping_str(mapping.get("actor")),
        entity=_optional_mapping_str(mapping.get("entity")),
        extra=extra,
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


def _create_pipeline(client: "ClientLike") -> "PipelineLike | None":
    pipeline_factory = getattr(client, "pipeline", None)
    if pipeline_factory is None:
        return None
    try:
        return cast("PipelineLike", pipeline_factory(transaction=False))
    except TypeError:
        return cast("PipelineLike", pipeline_factory())


async def _execute_pipeline(pipeline: "PipelineLike") -> "list[Any]":
    result = pipeline.execute()
    if inspect.isawaitable(result):
        return list(await result)
    return list(cast("list[Any]", result))


def hashed_index_value(value: "str") -> "str":
    """Return a stable Redis-key-safe index value."""
    return hashlib.sha256(value.encode()).hexdigest()

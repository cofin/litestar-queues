"""Redis-protocol queue event history."""

# ruff: noqa: SLF001

import hashlib
import inspect
import json
from dataclasses import fields
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from litestar_queues.events._history_buffer import _HistoryBuffer
from litestar_queues.events._log_records import (
    event_log_record_from_event,
    event_log_record_sort_key,
    optional_float,
    optional_int,
    optional_str,
    parse_datetime,
)
from litestar_queues.events.history import QueueEventLogRecord
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence

    from litestar_queues.backends._protocol import ClientLike, PipelineLike
    from litestar_queues.backends.redis.backend import RedisQueueBackend
    from litestar_queues.events import EventHistoryConfig, QueueEvent, QueueEventStageSummary
    from litestar_queues.events.query import QueueEventQuery
    from litestar_queues.events.typing import OffsetPagination

__all__ = ("RedisQueueEventLog",)

_INITIALIZE_EVENT_SCRIPT = """
if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('HSET', KEYS[1], unpack(ARGV))
end
return redis.call('HGETALL', KEYS[1])
"""


class RedisQueueEventLog:
    """Buffered Redis-protocol event-history writer and query interface."""

    __slots__ = ("_backend", "_buffer", "_config")

    def __init__(self, *, backend: "RedisQueueBackend", config: "EventHistoryConfig") -> "None":
        self._backend = backend
        self._config = config
        self._buffer = _HistoryBuffer(config, self._write_history_batch)

    async def publish_event(self, event: "QueueEvent") -> "None":
        """Accept an immutable event for bounded, timed persistence."""
        await self._buffer.enqueue(event_log_record_from_event(event, extra_columns=self._config.extra_columns))

    async def publish_event_after_commit(
        self, event: "QueueEvent", *, release: "Callable[[], Awaitable[None]]", barrier: "bool" = False
    ) -> "None":
        """Release live publication after the hash and all indexes are acknowledged."""
        await self._buffer.enqueue(
            event_log_record_from_event(event, extra_columns=self._config.extra_columns),
            release=release,
            barrier=barrier,
        )

    async def flush_events(self) -> "None":
        """Attempt accepted history and its ordered live releases."""
        await self._buffer.flush()

    async def aclose(self) -> "None":
        """Stop admission and drain history before its client closes."""
        await self._buffer.stop()

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

    async def _write_history_batch(self, records: "Sequence[QueueEventLogRecord]") -> "None":
        client = await self._backend._get_client()
        mappings = [self._mapping_from_record(record) for record in records]
        pipeline = _create_pipeline(client)
        initialized = []
        for mapping in mappings:
            arguments = [item for pair in mapping.items() for item in pair]
            event_key = self._backend._event_log_event_key(mapping["event_id"])
            if pipeline is not None:
                pipeline.eval(_INITIALIZE_EVENT_SCRIPT, 1, event_key, *arguments)
            else:
                result = client.eval(_INITIALIZE_EVENT_SCRIPT, 1, event_key, *arguments)
                initialized.append(await result if inspect.isawaitable(result) else result)
        if pipeline is not None:
            initialized = await _execute_pipeline(pipeline)

        conflict: QueueConfigurationError | None = None
        pipeline = _create_pipeline(client)
        for incoming, raw in zip(mappings, initialized, strict=True):
            stored_mapping = _decode_mapping(dict(zip(raw[::2], raw[1::2], strict=True)))
            stored = _record_from_mapping(stored_mapping)
            candidate = _record_from_mapping(incoming)
            if any(
                getattr(stored, field.name) != getattr(candidate, field.name)
                for field in fields(stored)
                if field.name != "created_at"
            ):
                message = f"Conflicting immutable queue event history record for event ID {candidate.event_id!r}."
                conflict = conflict or QueueConfigurationError(message)
            # A hash may survive an earlier partial index write. Rebuild every
            # membership from its winning record, including on a conflicting replay.
            mapping = self._mapping_from_record(stored)
            score = _score_datetime(stored.occurred_at)
            for index_key in _json_loads(mapping["index_keys"], []):
                if pipeline is not None:
                    pipeline.zadd(str(index_key), {stored.event_id: score})
                else:
                    await client.zadd(str(index_key), {stored.event_id: score})
        if pipeline is not None:
            await _execute_pipeline(pipeline)
        if conflict is not None:
            raise conflict

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
    results = list(await result) if inspect.isawaitable(result) else list(cast("list[Any]", result))
    for item in results:
        if isinstance(item, BaseException):
            raise item
    return results


def hashed_index_value(value: "str") -> "str":
    """Return a stable Redis-key-safe index value."""
    return hashlib.sha256(value.encode()).hexdigest()

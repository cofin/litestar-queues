"""File-backed queue event history for the ephemeral SQLite backend."""

from typing import TYPE_CHECKING, Any

from litestar_queues.backends.ephemeral.codec import event_from_payload, event_to_payload
from litestar_queues.events._log_records import event_log_record_from_event
from litestar_queues.events.history import validate_event_extra_filter
from litestar_queues.events.query import (
    match_event_record,
    paginate_event_records,
    require_unpaginated_query,
    sort_event_records,
    summarize_event_records,
)

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Mapping, Sequence
    from datetime import datetime

    from litestar_queues.backends.ephemeral.backend import EphemeralQueueBackend
    from litestar_queues.events import EventHistoryConfig, QueueEvent, QueueEventLogRecord, QueueEventStageSummary
    from litestar_queues.events.query import QueueEventQuery
    from litestar_queues.events.typing import OffsetPagination

__all__ = ("EphemeralQueueEventLog",)


def _where(query: "QueueEventQuery | None") -> "tuple[str, list[Any]]":
    if query is None:
        return "1 = 1", []
    sql = "1 = 1"
    values: "list[Any]" = []
    for field in ("task_id", "task_name", "event_type", "level", "scope", "scope_key", "entity"):
        val = getattr(query, field)
        if val is not None:
            sql += f" AND {field} = ?"
            values.append(val)
    actor = getattr(query, "actor", None)
    if actor is not None:
        sql += " AND actor = ?"
        values.append(actor)
    return sql, values


def _iso(value: "datetime") -> "str":
    return value.isoformat()


class EphemeralQueueEventLog:
    """Bounded queue event history stored in the server-owned database."""

    __slots__ = ("_backend", "_config")

    def __init__(self, config: "EventHistoryConfig", *, backend: "EphemeralQueueBackend") -> "None":
        self._config = config
        self._backend = backend

    async def publish_event(self, event: "QueueEvent") -> "None":
        """Append an event record and prune the oldest rows beyond capacity."""
        record = event_log_record_from_event(event, extra_columns=self._config.extra_columns)
        capacity = self._config.memory_capacity

        def operation(connection: "sqlite3.Connection") -> "None":
            connection.execute(
                "INSERT OR REPLACE INTO queue_event "
                "(event_id, event_type, task_id, task_name, stage, level, scope, scope_key, actor, entity, "
                "occurred_at, created_at, sequence, payload) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    record.event_id,
                    record.event_type,
                    record.task_id,
                    record.task_name,
                    record.stage,
                    record.level,
                    record.scope,
                    record.scope_key,
                    record.actor_id if record.actor_type is None else f"{record.actor_type}:{record.actor_id}",
                    record.entity,
                    _iso(record.occurred_at),
                    _iso(record.created_at),
                    record.sequence,
                    event_to_payload(record),
                ),
            )
            connection.execute(
                "DELETE FROM queue_event WHERE event_id IN ("
                "SELECT event_id FROM queue_event "
                "ORDER BY occurred_at DESC, COALESCE(sequence, 0) DESC, event_id DESC LIMIT -1 OFFSET ?)",
                (capacity,),
            )

        await self._backend._transaction(operation)  # noqa: SLF001

    async def flush_events(self) -> "None":
        """Flush buffered events.

        Each publish commits immediately, so this is intentionally a no-op.
        """


    async def query_events(
        self, query: "QueueEventQuery | None" = None, *, extra: "Mapping[str, str] | None" = None
    ) -> "OffsetPagination[QueueEventLogRecord]":
        """Return a filtered, ordered page of event history records."""
        where, values = _where(query)
        resolved_extra = validate_event_extra_filter(extra, self._config.extra_columns)

        def operation(connection: "sqlite3.Connection") -> "list[QueueEventLogRecord]":
            rows = connection.execute(f"SELECT payload FROM queue_event WHERE {where}", values).fetchall()  # noqa: S608
            matched = [event_from_payload(row["payload"]) for row in rows]
            if resolved_extra:
                matched = [
                    record for record in matched
                    if all(record.extra.get(k) == v for k, v in resolved_extra.items())
                ]
            return matched

        records = await self._backend._run(operation)  # noqa: SLF001
        ordered = sort_event_records(records, order="asc" if query is None else query.order)
        return paginate_event_records(ordered, query)

    async def summarize_stages(self, query: "QueueEventQuery | None" = None) -> "list[QueueEventStageSummary]":
        """Return per-stage aggregates for the matching records."""
        require_unpaginated_query(query)
        where, values = _where(query)

        def operation(connection: "sqlite3.Connection") -> "list[QueueEventLogRecord]":
            rows = connection.execute(f"SELECT payload FROM queue_event WHERE {where}", values).fetchall()  # noqa: S608
            return [event_from_payload(row["payload"]) for row in rows]

        records = await self._backend._run(operation)  # noqa: SLF001
        return summarize_event_records(records)

    async def cleanup_events(
        self,
        *,
        before: "datetime",
        match: "QueueEventQuery | None" = None,
        exclude: "Sequence[QueueEventQuery]" = (),
        limit: "int | None" = None,
    ) -> "int":
        """Delete the oldest matching records occurring before ``before``."""
        where, values = _where(match)

        def operation(connection: "sqlite3.Connection") -> "int":
            sql = f"SELECT event_id, payload FROM queue_event WHERE occurred_at < ? AND {where} ORDER BY occurred_at ASC, event_id ASC"  # noqa: S608
            rows = connection.execute(sql, [_iso(before), *values]).fetchall()

            doomed_ids: "list[str]" = []
            for row in rows:
                if limit is not None and len(doomed_ids) >= limit:
                    break
                record = event_from_payload(row["payload"])
                if not any(match_event_record(record, other) for other in exclude):
                    doomed_ids.append(row["event_id"])

            if not doomed_ids:
                return 0

            connection.executemany("DELETE FROM queue_event WHERE event_id = ?", [(i,) for i in doomed_ids])
            return len(doomed_ids)

        return await self._backend._transaction(operation)  # noqa: SLF001


    async def clear(self) -> "None":
        """Remove every stored event record."""
        await self._backend._transaction(lambda connection: connection.execute("DELETE FROM queue_event"))  # noqa: SLF001

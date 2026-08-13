"""File-backed queue event history for the ephemeral SQLite backend."""

from typing import TYPE_CHECKING, Any

from litestar_queues.backends.ephemeral.codec import event_from_payload, event_to_payload
from litestar_queues.events._log_records import event_log_record_from_event, event_log_record_sort_key

if TYPE_CHECKING:
    import sqlite3
    from datetime import datetime

    from litestar_queues.backends.ephemeral.backend import EphemeralQueueBackend
    from litestar_queues.events import EventHistoryConfig, QueueEvent, QueueEventLogRecord, QueueEventStageSummary

__all__ = ("EphemeralQueueEventLog",)


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
        record = event_log_record_from_event(event)
        capacity = self._config.memory_capacity

        def operation(connection: "sqlite3.Connection") -> "None":
            connection.execute(
                "INSERT OR REPLACE INTO queue_event "
                "(event_id, task_id, task_name, stage, occurred_at, created_at, sequence, payload) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    record.event_id,
                    record.task_id,
                    record.task_name,
                    record.stage,
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

    async def list_events(
        self,
        *,
        task_id: "str | None" = None,
        task_name: "str | None" = None,
        actor_id: "str | None" = None,
        actor_type: "str | None" = None,
        limit: "int | None" = None,
    ) -> "list[QueueEventLogRecord]":
        """Return matching event records in ascending event order.

        Returns:
            The matching event history records.
        """

        def operation(connection: "sqlite3.Connection") -> "list[QueueEventLogRecord]":
            sql = "SELECT payload FROM queue_event WHERE 1 = 1"
            values: "list[Any]" = []
            if task_id is not None:
                sql += " AND task_id = ?"
                values.append(task_id)
            if task_name is not None:
                sql += " AND task_name = ?"
                values.append(task_name)
            rows = connection.execute(sql, values).fetchall()
            return [event_from_payload(row["payload"]) for row in rows]

        records: "list[QueueEventLogRecord]" = await self._backend._run(operation)  # noqa: SLF001
        # The actor lives in the encoded payload rather than its own indexed
        # column, so it is matched after decoding. The table is capacity-bounded.
        if actor_id is not None:
            records = [record for record in records if record.actor_id == actor_id]
        if actor_type is not None:
            records = [record for record in records if record.actor_type == actor_type]
        records.sort(key=event_log_record_sort_key)
        return records[:limit] if limit is not None else records

    async def summarize_stages(self, *, task_name: "str | None" = None) -> "list[QueueEventStageSummary]":
        """Return no aggregate summaries for the ephemeral event log.

        Returns:
            An empty list, matching the memory event log.
        """
        del task_name
        return []

    async def cleanup_before(self, before: "datetime", *, limit: "int | None" = None) -> "int":
        """Delete the oldest records occurring before ``before``.

        Returns:
            Number of deleted records.
        """

        def operation(connection: "sqlite3.Connection") -> "int":
            sql = "SELECT event_id FROM queue_event WHERE occurred_at < ? ORDER BY occurred_at ASC, event_id ASC"
            values: "list[Any]" = [_iso(before)]
            if limit is not None:
                sql += " LIMIT ?"
                values.append(limit)
            ids = [row["event_id"] for row in connection.execute(sql, values).fetchall()]
            for event_id in ids:
                connection.execute("DELETE FROM queue_event WHERE event_id = ?", (event_id,))
            return len(ids)

        return await self._backend._transaction(operation)  # noqa: SLF001

    async def clear(self) -> "None":
        """Remove every stored event record."""
        await self._backend._transaction(lambda connection: connection.execute("DELETE FROM queue_event"))  # noqa: SLF001

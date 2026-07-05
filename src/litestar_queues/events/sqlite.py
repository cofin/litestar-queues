"""SQLite-backed durable queue event sink."""

import asyncio
import contextlib
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

    from litestar_queues.events.models import QueueEvent

__all__ = ("QueueEventLogRecord", "QueueEventStageSummary", "SQLiteQueueEventSink")

_DEFAULT_TABLE_NAME = "queue_event_log"
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class QueueEventLogRecord:
    """A durable queue event log row."""

    id: "int"
    event_id: "str"
    event_type: "str"
    job_id: "str | None"
    task_name: "str | None"
    queue: "str | None"
    stage: "str | None"
    level: "str | None"
    message: "str | None"
    detail: "dict[str, Any]"
    duration_ms: "float | None"
    sequence: "int | None"
    created_at: "datetime"


@dataclass(frozen=True, slots=True)
class QueueEventStageSummary:
    """Aggregated queue event log data for a single stage."""

    stage: "str | None"
    event_count: "int"
    total_duration_ms: "float"
    first_event_at: "datetime | None"
    last_event_at: "datetime | None"


class SQLiteQueueEventSink:
    """Persist queue events to a local SQLite table with buffered writes."""

    __slots__ = ("_buffer", "_flush_interval", "_flush_task", "_lock", "_path", "_table_name", "buffer_size")

    def __init__(
        self,
        path: "str | Path",
        *,
        buffer_size: "int" = 20,
        flush_interval: "float" = 1.0,
        table_name: "str" = _DEFAULT_TABLE_NAME,
    ) -> "None":
        if buffer_size <= 0:
            msg = "SQLiteQueueEventSink buffer_size must be positive"
            raise ValueError(msg)
        self._path = Path(path)
        self._table_name = _validate_table_name(table_name)
        self.buffer_size = buffer_size
        self._flush_interval = flush_interval
        self._buffer: "list[QueueEvent]" = []
        self._lock = asyncio.Lock()
        self._flush_task: "asyncio.Task[None] | None" = None

    async def open(self) -> "None":
        """Create the durable event table and start interval flushing."""
        await asyncio.to_thread(self._create_schema)
        if self._flush_interval > 0 and self._flush_task is None:
            self._flush_task = asyncio.create_task(self._flush_loop())

    async def close(self) -> "None":
        """Flush pending events and stop interval flushing."""
        flush_task = self._flush_task
        self._flush_task = None
        if flush_task is not None:
            flush_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await flush_task
        await self.flush()

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> "None":
        """Buffer an event for durable insertion."""
        del channels
        should_flush = False
        async with self._lock:
            self._buffer.append(event)
            should_flush = len(self._buffer) >= self.buffer_size
        if should_flush:
            await self.flush()

    async def flush(self) -> "None":
        """Write pending buffered events.

        Returns:
            None.
        """
        async with self._lock:
            events = tuple(self._buffer)
            self._buffer.clear()
        if not events:
            return
        await asyncio.to_thread(self._insert_events, events)

    async def list_events(
        self, *, task_id: "str | None" = None, task_name: "str | None" = None, limit: "int | None" = None
    ) -> "list[QueueEventLogRecord]":
        """Return durable event log rows ordered by insertion."""
        await self.flush()
        return await asyncio.to_thread(self._list_events_sync, task_id, task_name, limit)

    async def summarize_stages(self, *, task_name: "str | None" = None) -> "list[QueueEventStageSummary]":
        """Return per-stage event counts and duration totals."""
        await self.flush()
        return await asyncio.to_thread(self._summarize_stages_sync, task_name)

    async def cleanup_before(self, before: "datetime") -> "int":
        """Delete event log rows older than ``before``.

        Returns:
            The number of deleted rows.
        """
        await self.flush()
        return await asyncio.to_thread(self._cleanup_before_sync, _serialize_datetime(before))

    async def _flush_loop(self) -> "None":
        while True:
            await asyncio.sleep(self._flush_interval)
            await self.flush()

    def _connect(self) -> "sqlite3.Connection":
        self._path.parent.mkdir(parents=True, exist_ok=True)
        return sqlite3.connect(self._path)

    def _create_schema(self) -> "None":
        table = _quote_identifier(self._table_name)
        with self._connect() as connection:
            connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    event_type TEXT NOT NULL,
                    job_id TEXT,
                    task_name TEXT,
                    queue TEXT,
                    stage TEXT,
                    level TEXT,
                    message TEXT,
                    detail_json TEXT NOT NULL,
                    duration_ms REAL,
                    sequence INTEGER,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                f"CREATE INDEX IF NOT EXISTS {_quote_identifier(f'{self._table_name}_job_idx')} ON {table}(job_id)"
            )
            connection.execute(
                f"CREATE INDEX IF NOT EXISTS {_quote_identifier(f'{self._table_name}_task_stage_idx')} "
                f"ON {table}(task_name, stage)"
            )

    def _insert_events(self, events: "tuple[QueueEvent, ...]") -> "None":
        rows = [_event_row(event) for event in events]
        table = _quote_identifier(self._table_name)
        insert_sql = f"""
            INSERT OR IGNORE INTO {table} (
                event_id,
                event_type,
                job_id,
                task_name,
                queue,
                stage,
                level,
                message,
                detail_json,
                duration_ms,
                sequence,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """  # noqa: S608 - table name is validated and quoted.
        with self._connect() as connection:
            connection.executemany(insert_sql, rows)

    def _list_events_sync(
        self, task_id: "str | None", task_name: "str | None", limit: "int | None"
    ) -> "list[QueueEventLogRecord]":
        table = _quote_identifier(self._table_name)
        criteria: "list[str]" = []
        params: "list[object]" = []
        if task_id is not None:
            criteria.append("job_id = ?")
            params.append(task_id)
        if task_name is not None:
            criteria.append("task_name = ?")
            params.append(task_name)
        where_sql = f"WHERE {' AND '.join(criteria)}" if criteria else ""
        limit_sql = "LIMIT ?" if limit is not None else ""
        if limit is not None:
            params.append(limit)
        select_sql = f"""
            SELECT
                id,
                event_id,
                event_type,
                job_id,
                task_name,
                queue,
                stage,
                level,
                message,
                detail_json,
                duration_ms,
                sequence,
                created_at
            FROM {table}
            {where_sql}
            ORDER BY id ASC
            {limit_sql}
            """  # noqa: S608 - table name is validated and quoted.
        with self._connect() as connection:
            rows = connection.execute(select_sql, params).fetchall()
        return [_record_from_row(row) for row in rows]

    def _summarize_stages_sync(self, task_name: "str | None") -> "list[QueueEventStageSummary]":
        table = _quote_identifier(self._table_name)
        criteria = "WHERE task_name = ?" if task_name is not None else ""
        params = [task_name] if task_name is not None else []
        summary_sql = f"""
            SELECT
                stage,
                COUNT(*),
                COALESCE(SUM(duration_ms), 0),
                MIN(created_at),
                MAX(created_at),
                MIN(id)
            FROM {table}
            {criteria}
            GROUP BY stage
            ORDER BY MIN(id) ASC
            """  # noqa: S608 - table name is validated and quoted.
        with self._connect() as connection:
            rows = connection.execute(summary_sql, params).fetchall()
        return [
            QueueEventStageSummary(
                stage=row[0],
                event_count=int(row[1]),
                total_duration_ms=float(row[2]),
                first_event_at=_parse_datetime(row[3]),
                last_event_at=_parse_datetime(row[4]),
            )
            for row in rows
        ]

    def _cleanup_before_sync(self, before: "str") -> "int":
        table = _quote_identifier(self._table_name)
        with self._connect() as connection:
            cursor = connection.execute(f"DELETE FROM {table} WHERE created_at < ?", (before,))  # noqa: S608
            return int(cursor.rowcount if cursor.rowcount is not None else 0)


def _event_row(event: "QueueEvent") -> "tuple[object, ...]":
    detail = dict(event.payload)
    stage_value = detail.get("stage")
    duration_value = detail.get("duration_ms")
    return (
        event.id,
        event.type,
        event.task_id,
        event.task_name,
        event.queue,
        stage_value if isinstance(stage_value, str) else None,
        event.level,
        event.message,
        json.dumps(detail, default=str, separators=(",", ":")),
        float(duration_value) if isinstance(duration_value, int | float) else None,
        event.sequence,
        _serialize_datetime(event.occurred_at),
    )


def _record_from_row(row: "tuple[Any, ...]") -> "QueueEventLogRecord":
    return QueueEventLogRecord(
        id=int(row[0]),
        event_id=str(row[1]),
        event_type=str(row[2]),
        job_id=row[3],
        task_name=row[4],
        queue=row[5],
        stage=row[6],
        level=row[7],
        message=row[8],
        detail=json.loads(row[9]),
        duration_ms=float(row[10]) if row[10] is not None else None,
        sequence=int(row[11]) if row[11] is not None else None,
        created_at=_parse_datetime(row[12]) or datetime.now(timezone.utc),
    )


def _serialize_datetime(value: "datetime") -> "str":
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _parse_datetime(value: "str | None") -> "datetime | None":
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _validate_table_name(value: "str") -> "str":
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        msg = f"Invalid SQLite queue event table name: {value!r}"
        raise ValueError(msg)
    return value


def _quote_identifier(value: "str") -> "str":
    return f'"{value}"'

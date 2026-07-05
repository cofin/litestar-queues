"""SQLSpec-backed queue event history."""

import asyncio
import logging
import time
from contextlib import suppress
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from sqlspec import sql

from litestar_queues.backends.sqlspec.schema import event_log_table_name_for, validate_table_name
from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore
from litestar_queues.events.log import QueueEventLogConfig, QueueEventLogRecord, QueueEventStageSummary

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractAsyncContextManager

    from sqlspec.builder import CreateIndex, CreateTable, Delete, DropIndex, DropTable, Select

    from litestar_queues.backends.sqlspec._typing import DatetimeParam, SQLSpecDriver, SQLSpecStoreConfig
    from litestar_queues.events.models import QueueEvent

__all__ = (
    "SQLSpecQueueEventLog",
    "SQLSpecQueueEventLogStore",
    "create_event_log_store",
    "resolve_event_log_table_name",
)

logger = logging.getLogger(__name__)

_EVENT_COLUMNS = (
    "event_id",
    "event_type",
    "task_id",
    "task_name",
    "queue",
    "worker_id",
    "execution_backend",
    "execution_profile",
    "stage",
    "level",
    "message",
    "detail_json",
    "progress_current",
    "progress_total",
    "progress_percent",
    "duration_ms",
    "sequence",
    "occurred_at",
    "created_at",
)


class SQLSpecQueueEventLogStore(SQLSpecQueueStore):
    """SQLSpec statement store for backend-managed queue event history."""

    __slots__ = ()

    def create_statements(self) -> "list[str]":
        """Return statements that create the event-log table and indexes."""
        if not self._manage_schema:
            return []
        return [self._create_event_table_sql(), *self._create_event_index_statements()]

    def drop_statements(self) -> "list[str]":
        """Return statements that drop event-log artifacts."""
        if not self._manage_schema:
            return []
        return [
            self._to_sql(sql.drop_index(self._index_name("occurred_at")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("task_name")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("task_id")).if_exists()),
            self._to_sql(sql.drop_table(self.table_name).if_exists()),
        ]

    def insert_events_template(self) -> "str":
        """Return a parametrized batch INSERT template for event rows."""
        columns = ", ".join(self._quote_identifier(column) for column in _EVENT_COLUMNS)
        placeholders = ", ".join(f":{column}" for column in _EVENT_COLUMNS)
        return f"INSERT INTO {self._quoted_table_name()} ({columns}) VALUES ({placeholders})"  # noqa: S608

    def select_events(
        self, *, task_id: "str | None" = None, task_name: "str | None" = None, limit: "int | None" = None
    ) -> "Select":
        """Return a SELECT for event-log records."""
        statement = sql.select(*_EVENT_COLUMNS).from_(self.table_name)
        if task_id is not None:
            statement = statement.where_eq("task_id", task_id)
        if task_name is not None:
            statement = statement.where_eq("task_name", task_name)
        statement = statement.order_by(
            _raw_order("occurred_at ASC"), _raw_order("sequence ASC"), _raw_order("event_id ASC")
        )
        return statement.limit(limit) if limit is not None else statement

    def summarize_stages(self, *, task_name: "str | None" = None) -> "tuple[str, dict[str, Any]]":
        """Return SQL and parameters for per-stage event summaries."""
        params: "dict[str, Any]" = {}
        where = ""
        if task_name is not None:
            where = f" WHERE {self._quoted_col('task_name')} = :task_name"
            params["task_name"] = task_name
        statement = (
            f"SELECT {self._quoted_col('stage')} AS stage, "  # noqa: S608
            "COUNT(*) AS event_count, "
            f"COALESCE(SUM({self._quoted_col('duration_ms')}), 0) AS total_duration_ms, "
            f"MIN({self._quoted_col('occurred_at')}) AS first_event_at, "
            f"MAX({self._quoted_col('occurred_at')}) AS last_event_at "
            f"FROM {self._quoted_table_name()}"
            f"{where} "
            f"GROUP BY {self._quoted_col('stage')} "
            f"ORDER BY {self._quoted_col('stage')} ASC"
        )
        return statement, params

    def count_events_before(self, *, before: "DatetimeParam") -> "Select":
        """Return a COUNT statement for event-log cleanup."""
        return (
            sql
            .select(sql.raw("COUNT(*) AS event_count"))
            .from_(self.table_name)
            .where("occurred_at < :event_log_before", event_log_before=before)
        )

    def cleanup_events_before(self, *, before: "DatetimeParam") -> "Delete":
        """Return a DELETE statement for event-log cleanup."""
        return sql.delete(self.table_name).where("occurred_at < :event_log_before", event_log_before=before)

    def serialize_detail(self, detail: "dict[str, Any]") -> "Any":
        """Serialize event detail payloads with the SQLSpec JSON serializer.

        Returns:
            The adapter-shaped serialized detail payload.
        """
        return self._serialize_json(detail)

    def deserialize_detail(self, value: "Any") -> "dict[str, Any]":
        """Deserialize event detail payloads returned by a SQLSpec driver.

        Returns:
            The decoded detail mapping, or an empty mapping for non-object JSON.
        """
        detail = self.deserialize_json("detail_json", value)
        return detail if isinstance(detail, dict) else {}

    def _create_event_table_statement(self) -> "CreateTable":
        return (
            sql
            .create_table(self.table_name)
            .if_not_exists()
            .column("event_id", self._id_type(), primary_key=True)
            .column("event_type", self._indexed_text_type(), not_null=True)
            .column("task_id", self._id_type())
            .column("task_name", self._indexed_text_type())
            .column("queue", self._indexed_text_type())
            .column("worker_id", self._indexed_text_type())
            .column("execution_backend", self._indexed_text_type())
            .column("execution_profile", self._indexed_text_type())
            .column("stage", self._indexed_text_type())
            .column("level", self._indexed_text_type())
            .column("message", self._text_type())
            .column("detail_json", self._json_type(), not_null=True)
            .column("progress_current", self._float_type())
            .column("progress_total", self._float_type())
            .column("progress_percent", self._float_type())
            .column("duration_ms", self._float_type())
            .column("sequence", self._integer_type())
            .column("occurred_at", self._timestamp_type(), not_null=True)
            .column("created_at", self._timestamp_type(), not_null=True)
        )

    def _create_event_table_sql(self) -> "str":
        rendered = self._to_sql(self._create_event_table_statement())
        unsplit_target = self._quote_unsplit_identifier(self.table_name)
        split_target = self._quoted_table_name()
        if unsplit_target != split_target:
            rendered = rendered.replace(unsplit_target, split_target, 1)
        return rendered

    def _create_event_index_statements(self) -> "list[str]":
        return [
            self._to_sql(
                sql
                .create_index(self._index_name("task_id"))
                .if_not_exists()
                .on_table(self.table_name)
                .columns("task_id", "sequence", "occurred_at")
            ),
            self._to_sql(
                sql
                .create_index(self._index_name("task_name"))
                .if_not_exists()
                .on_table(self.table_name)
                .columns("task_name", "stage", "occurred_at")
            ),
            self._to_sql(
                sql
                .create_index(self._index_name("occurred_at"))
                .if_not_exists()
                .on_table(self.table_name)
                .columns("occurred_at")
            ),
        ]

    def _float_type(self) -> "str":
        return self._dialect_type("float", fallback="REAL")

    def _to_sql(self, statement: "CreateIndex | CreateTable | DropIndex | DropTable") -> "str":
        built = statement.build(dialect=self.dialect_name)
        return built.sql


class SQLSpecQueueEventLog:
    """Buffered SQLSpec event-history writer and query interface."""

    __slots__ = (
        "_config",
        "_datetime_serializer",
        "_flush_lock",
        "_last_flush",
        "_pending",
        "_session_factory",
        "_store",
    )

    def __init__(
        self,
        *,
        session_factory: "Callable[[], AbstractAsyncContextManager[SQLSpecDriver]]",
        datetime_serializer: "Callable[[datetime], datetime | str]",
        config: "QueueEventLogConfig",
        store: "SQLSpecQueueEventLogStore",
    ) -> "None":
        self._session_factory = session_factory
        self._datetime_serializer = datetime_serializer
        self._config = config
        self._store = store
        self._pending: "list[dict[str, Any]]" = []
        self._last_flush = time.monotonic()
        self._flush_lock = asyncio.Lock()

    async def publish_event(self, event: "QueueEvent") -> "None":
        """Buffer a queue event and flush when configured thresholds are reached."""
        should_flush = False
        async with self._flush_lock:
            self._pending.append(self._params_from_event(event))
            should_flush = len(self._pending) >= max(1, self._config.buffer_size) or self._flush_interval_elapsed()
        if should_flush:
            await self.flush_events()

    async def flush_events(self) -> "None":
        """Flush buffered queue events through a SQLSpec session.

        Returns:
            None.
        """
        async with self._flush_lock:
            if not self._pending:
                return
            batch = list(self._pending)
            try:
                async with self._session_factory() as driver:
                    await driver.begin()
                    try:
                        await driver.execute_many(self._store.insert_events_template(), batch)
                        await driver.commit()
                    except Exception:
                        with suppress(Exception):
                            await driver.rollback()
                        raise
            except Exception:
                if self._config.strict:
                    raise
                logger.warning("SQLSpec queue event history flush failed", exc_info=True)
                return
            del self._pending[: len(batch)]
            self._last_flush = time.monotonic()

    async def list_events(
        self, *, task_id: "str | None" = None, task_name: "str | None" = None, limit: "int | None" = None
    ) -> "list[QueueEventLogRecord]":
        """Return durable event history records."""
        await self.flush_events()
        async with self._session_factory() as driver:
            rows = await driver.select(self._store.select_events(task_id=task_id, task_name=task_name, limit=limit))
        return [self._record_from_row(cast("dict[str, Any]", row)) for row in rows]

    async def summarize_stages(self, *, task_name: "str | None" = None) -> "list[QueueEventStageSummary]":
        """Return per-stage event history aggregates."""
        await self.flush_events()
        statement, params = self._store.summarize_stages(task_name=task_name)
        async with self._session_factory() as driver:
            rows = await driver.select(statement, params)
        return [self._summary_from_row(cast("dict[str, Any]", row)) for row in rows]

    async def cleanup_before(self, before: "datetime") -> "int":
        """Delete event history older than ``before``.

        Returns:
            Number of deleted event-history rows.
        """
        await self.flush_events()
        before_value = self._datetime_serializer(before)
        async with self._session_factory() as driver:
            await driver.begin()
            try:
                count_row = await driver.select_one_or_none(self._store.count_events_before(before=before_value))
                deleted = int(count_row["event_count"]) if count_row is not None else 0
                if deleted > 0:
                    await driver.execute(self._store.cleanup_events_before(before=before_value))
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        return deleted

    def _flush_interval_elapsed(self) -> "bool":
        return self._config.flush_interval <= 0 or time.monotonic() - self._last_flush >= self._config.flush_interval

    def _params_from_event(self, event: "QueueEvent") -> "dict[str, Any]":
        detail = dict(event.payload)
        return {
            "event_id": event.id,
            "event_type": event.type,
            "task_id": event.task_id,
            "task_name": event.task_name,
            "queue": event.queue,
            "worker_id": event.worker_id,
            "execution_backend": event.execution_backend,
            "execution_profile": event.execution_profile,
            "stage": _optional_str(detail.get("stage")),
            "level": event.level,
            "message": event.message,
            "detail_json": self._store.serialize_detail(detail),
            "progress_current": _optional_float(event.progress_current),
            "progress_total": _optional_float(event.progress_total),
            "progress_percent": _optional_float(event.progress_percent),
            "duration_ms": _optional_float(detail.get("duration_ms")),
            "sequence": event.sequence,
            "occurred_at": self._datetime_serializer(event.occurred_at),
            "created_at": self._datetime_serializer(datetime.now(timezone.utc)),
        }

    def _record_from_row(self, row: "dict[str, Any]") -> "QueueEventLogRecord":
        return QueueEventLogRecord(
            event_id=str(row["event_id"]),
            event_type=str(row["event_type"]),
            task_id=cast("str | None", row["task_id"]),
            task_name=cast("str | None", row["task_name"]),
            queue=cast("str | None", row["queue"]),
            worker_id=cast("str | None", row["worker_id"]),
            execution_backend=cast("str | None", row["execution_backend"]),
            execution_profile=cast("str | None", row["execution_profile"]),
            stage=cast("str | None", row["stage"]),
            level=cast("str | None", row["level"]),
            message=cast("str | None", row["message"]),
            detail=self._store.deserialize_detail(row["detail_json"]),
            progress_current=_optional_float(row["progress_current"]),
            progress_total=_optional_float(row["progress_total"]),
            progress_percent=_optional_float(row["progress_percent"]),
            duration_ms=_optional_float(row["duration_ms"]),
            sequence=_optional_int(row["sequence"]),
            occurred_at=_deserialize_datetime(row["occurred_at"]),
            created_at=_deserialize_datetime(row["created_at"]),
        )

    def _summary_from_row(self, row: "dict[str, Any]") -> "QueueEventStageSummary":
        return QueueEventStageSummary(
            stage=cast("str | None", row["stage"]),
            event_count=int(row["event_count"]),
            total_duration_ms=float(row["total_duration_ms"] or 0),
            first_event_at=_deserialize_optional_datetime(row["first_event_at"]),
            last_event_at=_deserialize_optional_datetime(row["last_event_at"]),
        )


def create_event_log_store(
    config: "SQLSpecStoreConfig",
    *,
    queue_table_name: "str",
    event_log_table_name: "str | None" = None,
    manage_schema: "bool" = True,
) -> "SQLSpecQueueEventLogStore":
    """Create an event-log store for a SQLSpec adapter configuration.

    Returns:
        SQLSpec event-log store configured for the resolved event-log table.
    """
    return SQLSpecQueueEventLogStore(
        config,
        table_name=resolve_event_log_table_name(queue_table_name, event_log_table_name=event_log_table_name),
        manage_schema=manage_schema,
    )


def resolve_event_log_table_name(queue_table_name: "str", *, event_log_table_name: "str | None" = None) -> "str":
    """Resolve the SQLSpec event-log table name for a queue table.

    Returns:
        The explicit event-log table name, or the derived queue-table event log name.
    """
    if event_log_table_name is not None:
        return validate_table_name(event_log_table_name)
    return event_log_table_name_for(queue_table_name)


def _deserialize_datetime(value: "Any") -> "datetime":
    parsed = _deserialize_optional_datetime(value)
    if parsed is None:
        msg = "SQLSpec queue event log expected a non-null datetime value"
        raise ValueError(msg)
    return parsed


def _deserialize_optional_datetime(value: "Any") -> "datetime | None":
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        if isinstance(value, bytes):
            value = value.decode()
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _optional_str(value: "Any") -> "str | None":
    return value if isinstance(value, str) else None


def _optional_float(value: "Any") -> "float | None":
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    return None


def _optional_int(value: "Any") -> "int | None":
    if value is None:
        return None
    return int(value)


def _raw_order(expression: "str") -> "Any":
    return sql.raw(expression)

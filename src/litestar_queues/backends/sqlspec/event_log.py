"""SQLSpec-backed queue event history."""

import asyncio
import logging
from contextlib import suppress
from dataclasses import fields, replace
from datetime import datetime, timezone
from hashlib import sha1
from sqlite3 import IntegrityError as SQLiteIntegrityError
from typing import TYPE_CHECKING, Any, cast

from sqlspec import sql
from sqlspec.exceptions import SQLSpecError, UniqueViolationError
from sqlspec.utils.text import quote_backtick_identifier, quote_identifier, split_qualified_identifier

from litestar_queues.backends.sqlspec.schema import (
    EVENT_HISTORY_COLUMNS,
    event_history_table_name_for,
    validate_table_name,
)
from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore, _adapter_name
from litestar_queues.backends.sqlspec.stores.spanner import SpannerQueueStore
from litestar_queues.events import (
    EventHistoryExtraColumn,
    event_actor_key,
    validate_event_extra_filter,
    validate_event_history_extra_columns,
)
from litestar_queues.events._history_buffer import _HistoryBuffer
from litestar_queues.events._log_records import event_log_record_from_event
from litestar_queues.events.history import EventHistoryConfig, QueueEventLogRecord, QueueEventStageSummary
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence
    from contextlib import AbstractAsyncContextManager

    from sqlspec.builder import CreateIndex, CreateTable, Delete, DropIndex, DropTable, Select

    from litestar_queues.backends.sqlspec._typing import DatetimeParam, SQLSpecDriver, SQLSpecStoreConfig
    from litestar_queues.events.models import QueueEvent
    from litestar_queues.events.query import QueueEventQuery
    from litestar_queues.events.typing import OffsetPagination

__all__ = (
    "SQLSpecQueueEventLog",
    "SQLSpecQueueEventLogStore",
    "SpannerQueueEventLogStore",
    "create_event_log_store",
    "resolve_event_history_table_name",
)

_PORTABLE_INDEX_NAME_LENGTH = 63
_SQLITE_CONSTRAINT_PRIMARYKEY = 1555

logger = logging.getLogger(__name__)


class SQLSpecQueueEventLogStore(SQLSpecQueueStore):
    """SQLSpec statement store for backend-managed queue event history."""

    __slots__ = ("_extra_columns",)

    def __init__(
        self, *args: "Any", extra_columns: "Sequence[EventHistoryExtraColumn] | None" = None, **kwargs: "Any"
    ) -> "None":
        super().__init__(*args, **kwargs)
        self._column_map = {"level": "event_level"} if self._event_dialect_name() == "oracle" else {}
        self._extra_columns = validate_event_history_extra_columns(extra_columns or ())

    @property
    def extra_columns(self) -> "tuple[EventHistoryExtraColumn, ...]":
        """Adopter-declared extra scoping columns on this event-history table."""
        return self._extra_columns

    def _all_columns(self) -> "tuple[str, ...]":
        return (*EVENT_HISTORY_COLUMNS, *(column.name for column in self._extra_columns))

    def _event_dialect_name(self) -> "str | None":
        dialect = self._data_dictionary_dialect_name()
        return "mssql" if dialect == "tsql" else dialect

    def _quote_identifier(self, identifier: "str") -> "str":
        if self._event_dialect_name() == "oracle":
            return ".".join(part.upper() for part in split_qualified_identifier(identifier) or (identifier,))
        quote = quote_backtick_identifier if self._event_dialect_name() in {"mysql", "spanner"} else quote_identifier
        parts = split_qualified_identifier(identifier)
        if not parts:
            return quote(identifier)
        return ".".join(quote(part) for part in parts)

    def _quote_unsplit_identifier(self, identifier: "str") -> "str":
        if self._event_dialect_name() == "oracle":
            return identifier.upper()
        quote = quote_backtick_identifier if self._event_dialect_name() in {"mysql", "spanner"} else quote_identifier
        return quote(identifier)

    def _index_name(self, suffix: "str") -> "str":
        name = super()._index_name(suffix)
        if len(name) <= _PORTABLE_INDEX_NAME_LENGTH:
            return name
        digest = sha1(name.encode()).hexdigest()[:8]  # noqa: S324 - stable identifier shortening.
        prefix_length = _PORTABLE_INDEX_NAME_LENGTH - len(digest) - 1
        return f"{name[:prefix_length]}_{digest}"

    def _validated_extra_filter(self, extra: "Mapping[str, str] | None") -> "tuple[tuple[str, str], ...]":
        resolved = validate_event_extra_filter(extra, self._extra_columns)
        return tuple(resolved.items())

    def create_statements(self) -> "list[str]":
        """Return statements that create the event-log table and indexes."""
        if not self._manage_schema:
            return []
        return [self._create_event_table_sql(), *self._create_event_index_statements()]

    def drop_statements(self) -> "list[str]":
        """Return statements that drop event-log artifacts."""
        if not self._manage_schema:
            return []
        if self._event_dialect_name() == "oracle":

            def _oracle_drop(name: "str") -> "str":
                return f"""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP INDEX {self._index_name(name)}';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -1418 AND SQLCODE != -942 THEN
                            RAISE;
                        END IF;
                END;
                """

            def _oracle_drop_tbl() -> "str":
                return f"""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE {self._quoted_table_name()} CASCADE CONSTRAINTS';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN
                            RAISE;
                        END IF;
                END;
                """

            return [
                *(_oracle_drop(column.name) for column in reversed(self._extra_columns) if column.indexed),
                _oracle_drop("occurred_at"),
                _oracle_drop("actor_id"),
                _oracle_drop("task_name"),
                _oracle_drop("task_id"),
                _oracle_drop_tbl(),
            ]
        if self._event_dialect_name() == "mssql":

            def _mssql_drop(name: "str") -> "str":
                return (
                    "IF EXISTS (SELECT 1 FROM sys.indexes "  # noqa: S608
                    f"WHERE name = N'{self._index_name(name)}' AND object_id = OBJECT_ID(N'{self.table_name}')) "
                    f"DROP INDEX [{self._index_name(name)}] ON {self._quoted_table_name()};"
                )

            return [
                *(_mssql_drop(column.name) for column in reversed(self._extra_columns) if column.indexed),
                _mssql_drop("occurred_at"),
                _mssql_drop("actor_id"),
                _mssql_drop("task_name"),
                _mssql_drop("task_id"),
                f"IF OBJECT_ID(N'{self.table_name}', N'U') IS NOT NULL DROP TABLE {self._quoted_table_name()};",
            ]
        if self._event_dialect_name() == "mysql":
            return [self._to_sql(sql.drop_table(self.table_name).if_exists())]
        return [
            *(
                self._to_sql(sql.drop_index(self._index_name(column.name)).if_exists())
                for column in reversed(self._extra_columns)
                if column.indexed
            ),
            self._to_sql(sql.drop_index(self._index_name("entity")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("scope_key")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("occurred_at")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("actor_id")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("task_name")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("task_id")).if_exists()),
            self._to_sql(sql.drop_table(self.table_name).if_exists()),
        ]

    def insert_events_template(self) -> "str":
        """Return a parametrized batch INSERT template for event rows."""
        names = self._all_columns()
        columns = ", ".join(self._quoted_col(column) for column in names)
        placeholders = ", ".join(f":{self.parameter_name(column)}" for column in names)
        return f"INSERT INTO {self._quoted_table_name()} ({columns}) VALUES ({placeholders})"  # noqa: S608

    def select_existing_event_ids(self, event_ids: "Sequence[str]") -> "Select":
        """Fetch immutable content for one bounded replay lookup."""
        return (
            sql.select(*self._select_columns()).from_(self.table_name).where_in(self._col("event_id"), list(event_ids))
        )

    def parameter_name(self, column: "str") -> "str":
        """Return the bind parameter name for a public event column."""
        return self._col(column)

    def _select_columns(self) -> "tuple[Any, ...]":
        return tuple(self._col(column) if self._col(column) != column else column for column in self._all_columns())

    def select_events(self, query: "QueueEventQuery", extra: "Mapping[str, str] | None" = None) -> "Select":
        """Return a SELECT for event-log records.

        Raises:
            ValueError: If ``extra`` names a column that was not declared.
        """
        statement = self._filter_events(sql.select(*self._select_columns()).from_(self.table_name), query, extra)

        if query.order == "asc":
            statement = statement.order_by(
                _raw_order("occurred_at ASC"), _raw_order("sequence ASC"), _raw_order("event_id ASC")
            )
        else:
            statement = statement.order_by(
                _raw_order("occurred_at DESC"), _raw_order("sequence DESC"), _raw_order("event_id DESC")
            )

        if query.offset:
            statement = statement.offset(query.offset)
        return statement.limit(query.limit) if query.limit is not None else statement

    def count_events(self, query: "QueueEventQuery", extra: "Mapping[str, str] | None" = None) -> "Select":
        """Count matching event-log records before ordering and pagination."""
        return self._filter_events(sql.select(sql.count("*").as_("total")).from_(self.table_name), query, extra)

    def _filter_events(
        self, statement: "Select", query: "QueueEventQuery", extra: "Mapping[str, str] | None"
    ) -> "Select":
        filters = dict(query.filters())
        if extra:
            extra = dict(extra)
            for name in ("actor_id", "actor_type"):
                if name in extra:
                    filters[name] = extra.pop(name)
            filters.update(self._validated_extra_filter(extra))
        for name, value in filters.items():
            statement = statement.where_eq(self._col(name), value)
        return statement

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

    def select_event_ids_before(self, *, before: "DatetimeParam", limit: "int | None") -> "Select":
        """Return a SELECT of the oldest bounded event ids before a cutoff.

        Raises:
            ValueError: If ``limit`` is less than 1.
        """
        statement = (
            sql
            .select("event_id")
            .from_(self.table_name)
            .where("occurred_at < :event_log_before", event_log_before=before)
            .order_by(_raw_order("occurred_at ASC"), _raw_order("sequence ASC"), _raw_order("event_id ASC"))
        )
        return statement.limit(limit) if limit is not None else statement

    def delete_events_by_ids(self, *, event_ids: "Sequence[str]") -> "Delete":
        """Return a DELETE statement scoped to the given event ids."""
        return sql.delete(self.table_name).where_in("event_id", list(event_ids))

    def serialize_detail(self, detail: "dict[str, Any]") -> "Any":
        """Serialize event detail payloads with the SQLSpec JSON serializer.

        Returns:
            The adapter-shaped serialized detail payload.
        """
        if _adapter_name(self._config) == "psqlpy":
            return detail
        return self._serialize_json(detail)

    def deserialize_detail(self, value: "Any") -> "dict[str, Any]":
        """Deserialize event detail payloads returned by a SQLSpec driver.

        Returns:
            The decoded detail mapping, or an empty mapping for non-object JSON.
        """
        if isinstance(value, dict):
            return value
        detail = self.deserialize_json("detail", value)
        return detail if isinstance(detail, dict) else {}

    def _create_event_table_statement(self) -> "CreateTable":
        statement = (
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
            .column("actor_type", self._indexed_text_type())
            .column("actor_id", self._indexed_text_type())
            .column("stage", self._indexed_text_type())
            .column(self._col("level"), self._indexed_text_type())
            .column("message", self._text_type())
            .column("detail", self._json_type(), not_null=True)
            .column("progress_current", self._float_type())
            .column("progress_total", self._float_type())
            .column("progress_percent", self._float_type())
            .column("duration_ms", self._float_type())
            .column("sequence", self._integer_type())
            .column("occurred_at", self._timestamp_type(), not_null=True)
            .column("created_at", self._timestamp_type(), not_null=True)
            .column("scope", self._indexed_text_type())
            .column("scope_key", self._indexed_text_type())
            .column("actor", self._indexed_text_type())
            .column("entity", self._indexed_text_type())
        )
        for column in self._extra_columns:
            statement = statement.column(column.name, self._indexed_text_type())
        return statement

    def _create_event_table_sql(self) -> "str":
        if self._event_dialect_name() == "mssql":
            cols = [
                f"{self._quoted_col('event_id')} {self._id_type()} PRIMARY KEY",
                f"{self._quoted_col('event_type')} {self._indexed_text_type()} NOT NULL",
                f"{self._quoted_col('task_id')} {self._id_type()}",
                f"{self._quoted_col('task_name')} {self._indexed_text_type()}",
                f"{self._quoted_col('queue')} {self._indexed_text_type()}",
                f"{self._quoted_col('worker_id')} {self._indexed_text_type()}",
                f"{self._quoted_col('execution_backend')} {self._indexed_text_type()}",
                f"{self._quoted_col('execution_profile')} {self._indexed_text_type()}",
                f"{self._quoted_col('actor_type')} {self._indexed_text_type()}",
                f"{self._quoted_col('actor_id')} {self._indexed_text_type()}",
                f"{self._quoted_col('stage')} {self._indexed_text_type()}",
                f"{self._quoted_col('level')} {self._indexed_text_type()}",
                f"{self._quoted_col('message')} {self._text_type()}",
                f"{self._quoted_col('detail')} {self._json_type()} NOT NULL",
                f"{self._quoted_col('progress_current')} {self._float_type()}",
                f"{self._quoted_col('progress_total')} {self._float_type()}",
                f"{self._quoted_col('progress_percent')} {self._float_type()}",
                f"{self._quoted_col('duration_ms')} {self._float_type()}",
                f"{self._quoted_col('sequence')} {self._integer_type()}",
                f"{self._quoted_col('occurred_at')} {self._timestamp_type()} NOT NULL",
                f"{self._quoted_col('created_at')} {self._timestamp_type()} NOT NULL",
                f"{self._quoted_col('scope')} {self._indexed_text_type()}",
                f"{self._quoted_col('scope_key')} {self._indexed_text_type()}",
                f"{self._quoted_col('actor')} {self._indexed_text_type()}",
                f"{self._quoted_col('entity')} {self._indexed_text_type()}",
                *(f"{self._quoted_col(column.name)} {self._indexed_text_type()}" for column in self._extra_columns),
            ]
            column_sql = ",\n  ".join(cols)
            return f"""
            IF OBJECT_ID(N'{self.table_name}', N'U') IS NULL
            BEGIN
                CREATE TABLE {self._quoted_table_name()} (
                    {column_sql}
                )
            END
            """
        if self._event_dialect_name() == "oracle":
            cols = [
                f"{self._quoted_col('event_id')} {self._id_type()} PRIMARY KEY",
                f"{self._quoted_col('event_type')} {self._indexed_text_type()} NOT NULL",
                f"{self._quoted_col('task_id')} {self._id_type()}",
                f"{self._quoted_col('task_name')} {self._indexed_text_type()}",
                f"{self._quoted_col('queue')} {self._indexed_text_type()}",
                f"{self._quoted_col('worker_id')} {self._indexed_text_type()}",
                f"{self._quoted_col('execution_backend')} {self._indexed_text_type()}",
                f"{self._quoted_col('execution_profile')} {self._indexed_text_type()}",
                f"{self._quoted_col('actor_type')} {self._indexed_text_type()}",
                f"{self._quoted_col('actor_id')} {self._indexed_text_type()}",
                f"{self._quoted_col('stage')} {self._indexed_text_type()}",
                f"{self._quoted_col('level')} {self._indexed_text_type()}",
                f"{self._quoted_col('message')} {self._text_type()}",
                f"{self._quoted_col('detail')} {self._json_type()} NOT NULL",
                f"{self._quoted_col('progress_current')} {self._float_type()}",
                f"{self._quoted_col('progress_total')} {self._float_type()}",
                f"{self._quoted_col('progress_percent')} {self._float_type()}",
                f"{self._quoted_col('duration_ms')} {self._float_type()}",
                f"{self._quoted_col('sequence')} {self._integer_type()}",
                f"{self._quoted_col('occurred_at')} {self._timestamp_type()} NOT NULL",
                f"{self._quoted_col('created_at')} {self._timestamp_type()} NOT NULL",
                f"{self._quoted_col('scope')} {self._indexed_text_type()}",
                f"{self._quoted_col('scope_key')} {self._indexed_text_type()}",
                f"{self._quoted_col('actor')} {self._indexed_text_type()}",
                f"{self._quoted_col('entity')} {self._indexed_text_type()}",
                *(f"{self._quoted_col(column.name)} {self._indexed_text_type()}" for column in self._extra_columns),
            ]
            column_sql = ",\n  ".join(cols)
            return f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE {self._quoted_table_name()} (
                    {column_sql}
                )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN
                        RAISE;
                    END IF;
            END;
            """
        if self._event_dialect_name() == "mysql":
            cols = [
                f"{self._quoted_col('event_id')} {self._id_type()} PRIMARY KEY",
                f"{self._quoted_col('event_type')} {self._indexed_text_type()} NOT NULL",
                f"{self._quoted_col('task_id')} {self._id_type()}",
                f"{self._quoted_col('task_name')} {self._indexed_text_type()}",
                f"{self._quoted_col('queue')} {self._indexed_text_type()}",
                f"{self._quoted_col('worker_id')} {self._indexed_text_type()}",
                f"{self._quoted_col('execution_backend')} {self._indexed_text_type()}",
                f"{self._quoted_col('execution_profile')} {self._indexed_text_type()}",
                f"{self._quoted_col('actor_type')} {self._indexed_text_type()}",
                f"{self._quoted_col('actor_id')} {self._indexed_text_type()}",
                f"{self._quoted_col('stage')} {self._indexed_text_type()}",
                f"{self._quoted_col('level')} {self._indexed_text_type()}",
                f"{self._quoted_col('message')} {self._text_type()}",
                f"{self._quoted_col('detail')} {self._json_type()} NOT NULL",
                f"{self._quoted_col('progress_current')} {self._float_type()}",
                f"{self._quoted_col('progress_total')} {self._float_type()}",
                f"{self._quoted_col('progress_percent')} {self._float_type()}",
                f"{self._quoted_col('duration_ms')} {self._float_type()}",
                f"{self._quoted_col('sequence')} {self._integer_type()}",
                f"{self._quoted_col('occurred_at')} {self._timestamp_type()} NOT NULL",
                f"{self._quoted_col('created_at')} {self._timestamp_type()} NOT NULL",
                f"{self._quoted_col('scope')} {self._indexed_text_type()}",
                f"{self._quoted_col('scope_key')} {self._indexed_text_type()}",
                f"{self._quoted_col('actor')} {self._indexed_text_type()}",
                f"{self._quoted_col('entity')} {self._indexed_text_type()}",
                *(f"{self._quoted_col(column.name)} {self._indexed_text_type()}" for column in self._extra_columns),
                f"INDEX {self._quoted_index_name('task_id')} ({self._quoted_col('task_id')}, {self._quoted_col('sequence')}, {self._quoted_col('occurred_at')})",
                f"INDEX {self._quoted_index_name('task_name')} ({self._quoted_col('task_name')}, {self._quoted_col('stage')}, {self._quoted_col('occurred_at')})",
                f"INDEX {self._quoted_index_name('actor_id')} ({self._quoted_col('actor_id')}, {self._quoted_col('occurred_at')})",
                f"INDEX {self._quoted_index_name('occurred_at')} ({self._quoted_col('occurred_at')})",
                f"INDEX {self._quoted_index_name('scope_key')} ({self._quoted_col('scope_key')}, {self._quoted_col('occurred_at')})",
                f"INDEX {self._quoted_index_name('entity')} ({self._quoted_col('entity')}, {self._quoted_col('occurred_at')})",
                *(
                    f"INDEX {self._quoted_index_name(column.name)} ({self._quoted_col(column.name)}, {self._quoted_col('occurred_at')})"
                    for column in self._extra_columns
                    if column.indexed
                ),
            ]
            column_sql = ",\n  ".join(cols)
            return f"CREATE TABLE IF NOT EXISTS {self._quoted_table_name()} (\n  {column_sql}\n)"

        rendered = self._to_sql(self._create_event_table_statement())
        unsplit_target = self._quote_unsplit_identifier(self.table_name)
        split_target = self._quoted_table_name()
        if unsplit_target != split_target:
            rendered = rendered.replace(unsplit_target, split_target, 1)
        return rendered

    def _create_event_index_statements(self) -> "list[str]":
        if self._event_dialect_name() == "mysql":
            return []
        if self._event_dialect_name() == "mssql":

            def _mssql_idx(name: "str", cols: "str") -> "str":
                return (
                    "IF NOT EXISTS (SELECT 1 FROM sys.indexes "  # noqa: S608
                    f"WHERE name = N'{self._index_name(name)}' AND object_id = OBJECT_ID(N'{self.table_name}')) "
                    f"CREATE INDEX {self._quoted_index_name(name)} ON {self._quoted_table_name()} ({cols});"
                )

            return [
                _mssql_idx(
                    "task_id",
                    f"{self._quoted_col('task_id')}, {self._quoted_col('sequence')}, {self._quoted_col('occurred_at')}",
                ),
                _mssql_idx(
                    "task_name",
                    f"{self._quoted_col('task_name')}, {self._quoted_col('stage')}, {self._quoted_col('occurred_at')}",
                ),
                _mssql_idx("actor_id", f"{self._quoted_col('actor_id')}, {self._quoted_col('occurred_at')}"),
                _mssql_idx("occurred_at", f"{self._quoted_col('occurred_at')}"),
                _mssql_idx("scope_key", f"{self._quoted_col('scope_key')}, {self._quoted_col('occurred_at')}"),
                _mssql_idx("entity", f"{self._quoted_col('entity')}, {self._quoted_col('occurred_at')}"),
                *(
                    _mssql_idx(column.name, f"{self._quoted_col(column.name)}, {self._quoted_col('occurred_at')}")
                    for column in self._extra_columns
                    if column.indexed
                ),
            ]
        if self._event_dialect_name() == "oracle":

            def _oracle_idx(name: "str", cols: "str") -> "str":
                return f"""
                BEGIN
                    EXECUTE IMMEDIATE 'CREATE INDEX {self._index_name(name)} ON {self._quoted_table_name()} ({cols})';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -955 AND SQLCODE != -1408 THEN
                            RAISE;
                        END IF;
                END;
                """

            return [
                _oracle_idx(
                    "task_id",
                    f"{self._quoted_col('task_id')}, {self._quoted_col('sequence')}, {self._quoted_col('occurred_at')}",
                ),
                _oracle_idx(
                    "task_name",
                    f"{self._quoted_col('task_name')}, {self._quoted_col('stage')}, {self._quoted_col('occurred_at')}",
                ),
                _oracle_idx("actor_id", f"{self._quoted_col('actor_id')}, {self._quoted_col('occurred_at')}"),
                _oracle_idx("occurred_at", self._quoted_col("occurred_at")),
                _oracle_idx("scope_key", f"{self._quoted_col('scope_key')}, {self._quoted_col('occurred_at')}"),
                _oracle_idx("entity", f"{self._quoted_col('entity')}, {self._quoted_col('occurred_at')}"),
                *(
                    _oracle_idx(column.name, f"{self._quoted_col(column.name)}, {self._quoted_col('occurred_at')}")
                    for column in self._extra_columns
                    if column.indexed
                ),
            ]
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
                .create_index(self._index_name("actor_id"))
                .if_not_exists()
                .on_table(self.table_name)
                .columns("actor_id", "occurred_at")
            ),
            self._to_sql(
                sql
                .create_index(self._index_name("occurred_at"))
                .if_not_exists()
                .on_table(self.table_name)
                .columns("occurred_at")
            ),
            self._to_sql(
                sql
                .create_index(self._index_name("scope_key"))
                .if_not_exists()
                .on_table(self.table_name)
                .columns("scope_key", "occurred_at")
            ),
            self._to_sql(
                sql
                .create_index(self._index_name("entity"))
                .if_not_exists()
                .on_table(self.table_name)
                .columns("entity", "occurred_at")
            ),
            *(
                self._to_sql(
                    sql
                    .create_index(self._index_name(column.name))
                    .if_not_exists()
                    .on_table(self.table_name)
                    .columns(column.name, "occurred_at")
                )
                for column in self._extra_columns
                if column.indexed
            ),
        ]

    def _float_type(self) -> "str":
        return self._dialect_type("float", fallback="REAL")

    def _to_sql(self, statement: "CreateIndex | CreateTable | DropIndex | DropTable") -> "str":
        built = statement.build(dialect=self.dialect_name)
        return built.sql


class SpannerQueueEventLogStore(SQLSpecQueueEventLogStore, SpannerQueueStore):
    """Spanner event-log store using native DDL operations."""

    __slots__ = ()

    auto_native_json_columns = frozenset({"detail"})

    def create_statements(self) -> "list[str]":
        """Return Spanner-compatible event-log table and index statements."""
        if not self._manage_schema:
            return []
        columns = (
            f"{self._quote_identifier('event_id')} {self._id_type()} NOT NULL",
            f"{self._quote_identifier('event_type')} {self._indexed_text_type()} NOT NULL",
            f"{self._quote_identifier('task_id')} {self._id_type()}",
            f"{self._quote_identifier('task_name')} {self._indexed_text_type()}",
            f"{self._quote_identifier('queue')} {self._indexed_text_type()}",
            f"{self._quote_identifier('worker_id')} {self._indexed_text_type()}",
            f"{self._quote_identifier('execution_backend')} {self._indexed_text_type()}",
            f"{self._quote_identifier('execution_profile')} {self._indexed_text_type()}",
            f"{self._quote_identifier('actor_type')} {self._indexed_text_type()}",
            f"{self._quote_identifier('actor_id')} {self._indexed_text_type()}",
            f"{self._quote_identifier('stage')} {self._indexed_text_type()}",
            f"{self._quote_identifier('level')} {self._indexed_text_type()}",
            f"{self._quote_identifier('message')} {self._text_type()}",
            f"{self._quote_identifier('detail')} {self._json_type()} NOT NULL",
            f"{self._quote_identifier('progress_current')} {self._float_type()}",
            f"{self._quote_identifier('progress_total')} {self._float_type()}",
            f"{self._quote_identifier('progress_percent')} {self._float_type()}",
            f"{self._quote_identifier('duration_ms')} {self._float_type()}",
            f"{self._quote_identifier('sequence')} {self._integer_type()}",
            f"{self._quote_identifier('occurred_at')} {self._timestamp_type()} NOT NULL",
            f"{self._quote_identifier('created_at')} {self._timestamp_type()} NOT NULL",
            f"{self._quote_identifier('scope')} {self._indexed_text_type()}",
            f"{self._quote_identifier('scope_key')} {self._indexed_text_type()}",
            f"{self._quote_identifier('actor')} {self._indexed_text_type()}",
            f"{self._quote_identifier('entity')} {self._indexed_text_type()}",
            *(f"{self._quote_identifier(column.name)} {self._indexed_text_type()}" for column in self._extra_columns),
        )
        column_sql = ",\n  ".join(columns)
        return [
            (
                f"CREATE TABLE {self._quoted_table_name()} (\n  {column_sql}\n) "
                f"PRIMARY KEY ({self._quote_identifier('event_id')})"
            ),
            (
                f"CREATE INDEX {self._quoted_index_name('task_id')} ON {self._quoted_table_name()} "
                f"({self._quote_identifier('task_id')}, {self._quote_identifier('sequence')}, "
                f"{self._quote_identifier('occurred_at')})"
            ),
            (
                f"CREATE INDEX {self._quoted_index_name('task_name')} ON {self._quoted_table_name()} "
                f"({self._quote_identifier('task_name')}, {self._quote_identifier('stage')}, "
                f"{self._quote_identifier('occurred_at')})"
            ),
            (
                f"CREATE INDEX {self._quoted_index_name('actor_id')} ON {self._quoted_table_name()} "
                f"({self._quote_identifier('actor_id')}, {self._quote_identifier('occurred_at')})"
            ),
            (
                f"CREATE INDEX {self._quoted_index_name('occurred_at')} ON {self._quoted_table_name()} "
                f"({self._quote_identifier('occurred_at')})"
            ),
            (
                f"CREATE INDEX {self._quoted_index_name('scope_key')} ON {self._quoted_table_name()} "
                f"({self._quote_identifier('scope_key')}, {self._quote_identifier('occurred_at')})"
            ),
            (
                f"CREATE INDEX {self._quoted_index_name('entity')} ON {self._quoted_table_name()} "
                f"({self._quote_identifier('entity')}, {self._quote_identifier('occurred_at')})"
            ),
            *(
                f"CREATE INDEX {self._quoted_index_name(column.name)} ON {self._quoted_table_name()} "
                f"({self._quote_identifier(column.name)}, {self._quote_identifier('occurred_at')})"
                for column in self._extra_columns
                if column.indexed
            ),
        ]

    def drop_statements(self) -> "list[str]":
        """Return Spanner-compatible event-log DROP statements."""
        if not self._manage_schema:
            return []
        return [
            *(
                f"DROP INDEX {self._quoted_index_name(column.name)}"
                for column in reversed(self._extra_columns)
                if column.indexed
            ),
            f"DROP INDEX {self._quoted_index_name('entity')}",
            f"DROP INDEX {self._quoted_index_name('scope_key')}",
            f"DROP INDEX {self._quoted_index_name('occurred_at')}",
            f"DROP INDEX {self._quoted_index_name('actor_id')}",
            f"DROP INDEX {self._quoted_index_name('task_name')}",
            f"DROP INDEX {self._quoted_index_name('task_id')}",
            f"DROP TABLE {self._quoted_table_name()}",
        ]


class SQLSpecQueueEventLog:
    """Buffered SQLSpec event-history writer and query interface."""

    __slots__ = ("_buffer", "_datetime_serializer", "_logger", "_session_factory", "_store")

    def __init__(
        self,
        *,
        session_factory: "Callable[[], AbstractAsyncContextManager[SQLSpecDriver]]",
        datetime_serializer: "Callable[[datetime], datetime | str]",
        config: "EventHistoryConfig",
        store: "SQLSpecQueueEventLogStore",
        runtime_logger: "logging.Logger | None" = None,
    ) -> "None":
        self._session_factory = session_factory
        self._datetime_serializer = datetime_serializer
        self._store = store
        self._buffer = _HistoryBuffer(config, self._write_history_batch)
        self._logger = runtime_logger or logger

    @property
    def extra_columns(self) -> "tuple[EventHistoryExtraColumn, ...]":
        """Declared extra scoping columns for this event log."""
        return self._store.extra_columns

    async def publish_event(self, event: "QueueEvent") -> "None":
        """Accept an immutable event for bounded, timed persistence."""
        await self._buffer.enqueue(self._record_from_event(event))

    async def publish_event_after_commit(
        self, event: "QueueEvent", *, release: "Callable[[], Awaitable[None]]", barrier: "bool" = False
    ) -> "None":
        """Release the live callback only after history commits."""
        await self._buffer.enqueue(self._record_from_event(event), release=release, barrier=barrier)

    async def flush_events(self) -> "None":
        """Attempt accepted history and its ordered live releases."""
        await self._buffer.flush()

    async def aclose(self) -> "None":
        """Stop admission and finish the history coordinator before pool cleanup."""
        await self._buffer.stop()

    def _record_from_event(self, event: "QueueEvent") -> "QueueEventLogRecord":
        record = event_log_record_from_event(event, extra_columns=self._store.extra_columns)
        return replace(record, actor=event_actor_key(event.actor))

    async def _write_history_batch(self, records: "Sequence[QueueEventLogRecord]") -> "None":
        for attempt in range(2):
            try:
                await self._write_transaction(records)
            except Exception as exc:  # noqa: PERF203 - one bounded fresh-transaction retry for duplicate races.
                if attempt == 0 and _is_duplicate_event_error(exc):
                    continue
                raise
            else:
                return

    async def _write_transaction(self, records: "Sequence[QueueEventLogRecord]") -> "None":
        primary: BaseException | None = None
        try:
            async with self._session_factory() as driver:
                try:
                    await driver.begin()
                    await self._insert_missing_records(driver, records)
                    await driver.commit()
                except (Exception, asyncio.CancelledError) as exc:  # noqa: BLE001
                    primary = exc
                    try:
                        await driver.rollback()
                    except asyncio.CancelledError as cancelled:
                        primary = cancelled
                    except Exception:
                        with suppress(Exception):
                            self._logger.warning("SQLSpec event history rollback failed", exc_info=True)
                    raise primary from primary.__cause__
        except BaseException as cleanup:
            if isinstance(cleanup, asyncio.CancelledError):
                raise
            if primary is not None:
                if cleanup is not primary:
                    with suppress(Exception):
                        self._logger.warning("SQLSpec event history session cleanup also failed", exc_info=True)
                raise primary from primary.__cause__
            raise
        if primary is not None:
            raise primary

    async def _insert_missing_records(
        self, driver: "SQLSpecDriver", records: "Sequence[QueueEventLogRecord]"
    ) -> "None":
        incoming: dict[str, tuple[QueueEventLogRecord, dict[str, Any]]] = {}
        for record in records:
            params = self._params_from_record(record)
            canonical = self._record_from_row(params)
            previous = incoming.get(record.event_id)
            if previous is not None:
                normalized = await self._comparison_records(driver, (previous[0], canonical))
                _require_identical_event(*normalized)
            else:
                incoming[record.event_id] = (canonical, params)
        missing = dict(incoming)
        event_ids = tuple(incoming)
        # SQLite's traditional 999-bind limit and Oracle's 1000-item IN limit
        # both exceed this fixed lookup bound, even for a large configured batch.
        for start in range(0, len(event_ids), 500):
            rows = await driver.select(self._store.select_existing_event_ids(event_ids[start : start + 500]))
            existing = [self._record_from_row(cast("dict[str, Any]", row)) for row in rows]
            compared = await self._comparison_records(driver, [incoming[record.event_id][0] for record in existing])
            for stored, candidate in zip(existing, compared, strict=True):
                # Reapply the bind codec (e.g. millisecond text on Arrow ODBC)
                # to both sides while keeping stored created_at untouched.
                stored = self._record_from_row(self._params_from_record(stored))
                _require_identical_event(stored, candidate)
                missing.pop(stored.event_id, None)
        if missing:
            statement = self._store.insert_events_template()
            values = [item[1] for item in missing.values()]
            if _adapter_name(self._store._config) == "arrow_odbc":  # noqa: SLF001
                # Arrow ODBC has no row-oriented executemany. Keep these bound
                # inserts inside the caller's single explicit transaction.
                for params in values:
                    await driver.execute(statement, params)
            else:
                await driver.execute_many(statement, values)

    async def _comparison_records(
        self, driver: "SQLSpecDriver", records: "Sequence[QueueEventLogRecord]"
    ) -> "list[QueueEventLogRecord]":
        if not records:
            return list(records)
        dialect = self._store._event_dialect_name()  # noqa: SLF001
        float_type = self._store._float_type()  # noqa: SLF001
        normalized: list[QueueEventLogRecord] = []
        # Match both timestamp and numeric storage precision. Restrict casts to
        # compared records, with at most 500 binds per projection.
        for start in range(0, len(records), 100):
            batch = records[start : start + 100]
            parameters: dict[str, Any] = {}
            projections = []
            changes: list[dict[str, Any]] = [{} for _ in batch]
            bindings: list[tuple[int, str, str]] = []
            for index, record in enumerate(batch):
                columns = [
                    (column, getattr(record, column), float_type)
                    for column in ("progress_current", "progress_total", "progress_percent", "duration_ms")
                    if getattr(record, column) is not None
                ]
                if dialect == "mysql":
                    columns.append(("occurred_at", self._datetime_serializer(record.occurred_at), "DATETIME"))
                for column, value, data_type in columns:
                    name = f"value_{len(parameters)}"
                    parameters[name] = value
                    projections.append(f"CAST(:{name} AS {data_type}) AS {name}")
                    bindings.append((index, column, name))
            if parameters:
                statement = "SELECT " + ", ".join(projections)
                if dialect == "oracle":
                    statement += " FROM DUAL"
                rows = await driver.select(statement, parameters)
                for index, column, name in bindings:
                    value = rows[0][name]
                    changes[index][column] = (
                        _deserialize_datetime(value) if column == "occurred_at" else _optional_float(value)
                    )
            normalized.extend(replace(record, **change) for record, change in zip(batch, changes, strict=True))
        return normalized

    async def query_events(
        self, query: "QueueEventQuery | None" = None, *, extra: "Mapping[str, str] | None" = None
    ) -> "OffsetPagination[QueueEventLogRecord]":
        """Query durable event history with the total before pagination.

        Page and count statements share a session; their consistency under
        concurrent writes follows the configured transaction isolation.
        """
        from litestar_queues.events.query import QueueEventQuery
        from litestar_queues.events.typing import OffsetPagination

        query = query or QueueEventQuery()
        await self.flush_events()
        async with self._session_factory() as driver:
            rows = await driver.select(self._store.select_events(query, extra=extra))
            count_rows = await driver.select(self._store.count_events(query, extra=extra))
        records = [self._record_from_row(cast("dict[str, Any]", row)) for row in rows]
        return OffsetPagination(
            items=records,
            total=int(count_rows[0]["total"]),
            offset=query.offset,
            limit=query.limit or len(records) or 1,
        )

    async def summarize_stages(self, query: "QueueEventQuery | None" = None) -> "list[QueueEventStageSummary]":
        """Return per-stage event history aggregates."""
        from litestar_queues.events.query import QueueEventQuery, require_unpaginated_query, summarize_event_records

        require_unpaginated_query(query)
        unpaginated = replace(query or QueueEventQuery(), order="asc", limit=None, offset=0)
        await self.flush_events()
        async with self._session_factory() as driver:
            rows = await driver.select(self._store.select_events(unpaginated))
        records = [self._record_from_row(cast("dict[str, Any]", row)) for row in rows]
        return summarize_event_records(records)

    async def cleanup_events(
        self,
        *,
        before: "datetime",
        match: "QueueEventQuery | None" = None,
        exclude: "Sequence[QueueEventQuery]" = (),
        limit: "int | None" = None,
    ) -> "int":
        """Delete event history older than ``before``.

        ``limit`` bounds one batch, deleting the oldest matching rows first
        (oldest ``occurred_at``, then ``event_id``).

        Returns:
            Number of deleted event-history rows.
        """
        from litestar_queues.events.query import QueueEventQuery, match_event_record, sort_event_records

        await self.flush_events()
        unpaginated = replace(match or QueueEventQuery(), order="asc", limit=None, offset=0)
        async with self._session_factory() as driver:
            await driver.begin()
            try:
                rows = await driver.select(self._store.select_events(unpaginated))
                records = [self._record_from_row(cast("dict[str, Any]", row)) for row in rows]
                doomed = [
                    record
                    for record in records
                    if record.occurred_at < before and not any(match_event_record(record, other) for other in exclude)
                ]
                doomed = sort_event_records(doomed)
                if limit is not None:
                    doomed = doomed[:limit]
                event_ids = [record.event_id for record in doomed]
                deleted = len(event_ids)
                if event_ids:
                    await driver.execute(self._store.delete_events_by_ids(event_ids=event_ids))
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        return deleted

    def _params_from_record(self, record: "QueueEventLogRecord") -> "dict[str, Any]":
        params = {column: getattr(record, column) for column in EVENT_HISTORY_COLUMNS}
        params["detail"] = self._store.serialize_detail(record.detail)
        params["occurred_at"] = self._datetime_serializer(record.occurred_at)
        params["created_at"] = self._datetime_serializer(record.created_at)
        params.update({column.name: record.extra.get(column.name) for column in self._store.extra_columns})
        level_parameter = self._store.parameter_name("level")
        if level_parameter != "level":
            params[level_parameter] = params.pop("level")
        if self._store._event_dialect_name() == "oracle":  # noqa: SLF001
            params = {key: None if value == "" else value for key, value in params.items()}
        return params

    def _record_from_row(self, row: "dict[str, Any]") -> "QueueEventLogRecord":
        extra = {
            column.name: str(row[column.name])
            for column in self._store.extra_columns
            if column.name in row and row[column.name] is not None
        }
        return QueueEventLogRecord(
            event_id=str(row["event_id"]),
            event_type=str(row["event_type"]),
            task_id=cast("str | None", row["task_id"]),
            task_name=cast("str | None", row["task_name"]),
            queue=cast("str | None", row["queue"]),
            worker_id=cast("str | None", row["worker_id"]),
            execution_backend=cast("str | None", row["execution_backend"]),
            execution_profile=cast("str | None", row["execution_profile"]),
            actor_type=cast("str | None", row["actor_type"]),
            actor_id=cast("str | None", row["actor_id"]),
            stage=cast("str | None", row["stage"]),
            level=cast("str | None", row.get("level", row.get("event_level"))),
            message=cast("str | None", row["message"]),
            detail=self._store.deserialize_detail(row["detail"]),
            progress_current=_optional_float(row["progress_current"]),
            progress_total=_optional_float(row["progress_total"]),
            progress_percent=_optional_float(row["progress_percent"]),
            duration_ms=_optional_float(row["duration_ms"]),
            sequence=_optional_int(row["sequence"]),
            extra=extra,
            occurred_at=_deserialize_datetime(row["occurred_at"]),
            created_at=_deserialize_datetime(row["created_at"]),
            scope=cast("str | None", row.get("scope")),
            scope_key=cast("str | None", row.get("scope_key")),
            actor=cast("str | None", row.get("actor")),
            entity=cast("str | None", row.get("entity")),
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
    event_history_table_name: "str | None" = None,
    manage_schema: "bool" = True,
    extra_columns: "Sequence[EventHistoryExtraColumn]" = (),
) -> "SQLSpecQueueEventLogStore":
    """Create an event-log store for a SQLSpec adapter configuration.

    Returns:
        SQLSpec event-log store configured for the resolved event-log table.
    """
    store_type = SpannerQueueEventLogStore if _adapter_name(config) == "spanner" else SQLSpecQueueEventLogStore
    return store_type(
        config,
        table_name=resolve_event_history_table_name(
            queue_table_name, event_history_table_name=event_history_table_name
        ),
        manage_schema=manage_schema,
        extra_columns=extra_columns,
    )


def resolve_event_history_table_name(
    queue_table_name: "str", *, event_history_table_name: "str | None" = None
) -> "str":
    """Resolve the SQLSpec event-log table name for a queue table.

    Returns:
        The explicit event-log table name, or the derived queue-table event log name.
    """
    if event_history_table_name is not None:
        return validate_table_name(event_history_table_name)
    return event_history_table_name_for(queue_table_name)


def _require_identical_event(stored: "QueueEventLogRecord", incoming: "QueueEventLogRecord") -> "None":
    if any(
        getattr(stored, field.name) != getattr(incoming, field.name)
        for field in fields(stored)
        if field.name != "created_at"
    ):
        message = f"Conflicting immutable queue event history record for event ID {incoming.event_id!r}."
        raise QueueConfigurationError(message)


def _is_duplicate_event_error(error: "BaseException") -> "bool":
    """Return True if the error represents a duplicate primary-key or unique violation.

    SQLSpec 0.62 maps SQLite's UNIQUE code (2067) but not its distinct PRIMARYKEY
    code (1555). We inspect the native cause, context, and the mapped SQLSpecError message.
    """
    seen: set[int] = set()
    while id(error) not in seen:
        seen.add(id(error))
        if isinstance(error, UniqueViolationError):
            return True
        if (
            isinstance(error, SQLiteIntegrityError)
            and getattr(error, "sqlite_errorcode", None) == _SQLITE_CONSTRAINT_PRIMARYKEY
        ):
            return True
        if isinstance(error, SQLSpecError) and f"[code {_SQLITE_CONSTRAINT_PRIMARYKEY}]" in str(error):
            return True
        cause = error.__cause__ or error.__context__
        if cause is None:
            return False
        error = cause
    return False


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

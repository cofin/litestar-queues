"""Shared SQLSpec queue store primitives."""

from functools import cache
from importlib.util import find_spec
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

from sqlspec import sql
from sqlspec.data_dictionary import get_dialect_config
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.text import quote_backtick_identifier, quote_identifier, split_qualified_identifier

from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.schema import (
    DEFAULT_TABLE_NAME,
    validate_column_map,
    validate_native_json_columns,
    validate_table_name,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlspec.data_dictionary import DialectConfig

__all__ = ("SQLSpecQueueStore",)

_TASK_COLUMNS = (
    "id",
    "task_name",
    "args_json",
    "kwargs_json",
    "queue",
    "execution_backend",
    "execution_profile",
    "execution_ref",
    "status",
    "priority",
    "max_retries",
    "retry_count",
    "scheduled_at",
    "created_at",
    "started_at",
    "completed_at",
    "heartbeat_at",
    "result_json",
    "error",
    "task_key",
    "metadata_json",
)
_DUE_STATUSES = ("pending", "scheduled")


class SQLSpecQueueStore:
    """Base SQLSpec queue statement store."""

    __slots__ = ("_column_map", "_config", "_manage_schema", "_native_json_columns", "_table_name")

    data_dictionary_dialect: "ClassVar[str | None]" = None
    identifier_quote_style: 'ClassVar[Literal["double", "backtick", "none"]]' = "double"
    claim_select_stream_chunk_size: "ClassVar[int | None]" = None
    # Per-store opt-in: canonical JSON columns whose driver round-trips
    # native Python values rather than JSON-encoded strings. Subclasses
    # whose drivers register a JSON codec (asyncpg JSONB, psycopg JSONB,
    # psqlpy PyJSON, MySQL JSON, Oracle JSON, etc.) override this. Stores
    # whose driver returns JSON columns as plain strings (DuckDB, SQLite)
    # keep the default empty frozenset. Unioned with adopter-supplied
    # ``native_json_columns`` at ``__init__``.
    auto_native_json_columns: "ClassVar[frozenset[str]]" = frozenset()
    bind_datetime_as_text: "ClassVar[bool]" = False

    def __init__(
        self,
        config: "Any",
        *,
        table_name: "str | None" = None,
        column_map: "Mapping[str, str] | None" = None,
        native_json_columns: "frozenset[str] | None" = None,
        manage_schema: "bool" = True,
    ) -> "None":
        self._config = config
        self._table_name = _configured_table_name(config, table_name)
        self._column_map = validate_column_map(column_map or {})
        configured = validate_native_json_columns(native_json_columns or frozenset())
        self._native_json_columns = configured | type(self).auto_native_json_columns
        self._manage_schema = manage_schema

    @property
    def table_name(self) -> "str":
        """Configured queue table name."""
        return self._table_name

    @property
    def dialect_name(self) -> "str | None":
        """SQLSpec dialect configured for this store."""
        statement_config = getattr(self._config, "statement_config", None)
        dialect = getattr(statement_config, "dialect", None)
        return str(dialect) if dialect is not None else None

    @property
    def supports_skip_locked(self) -> "bool":
        """Whether the adapter supports ``SELECT ... FOR UPDATE SKIP LOCKED``.

        Resolved from SQLSpec's data dictionary. Some dialects expose
        ``supports_skip_locked`` as a static flag; version-gated dialects
        expose the minimum supported version instead, which is treated as an
        adapter capability until live server-version checks are introduced.
        """
        dialect_config = self._dialect_config()
        if dialect_config is None or dialect_config.get_feature_flag("supports_for_update") is not True:
            return False
        return dialect_config.get_feature_flag("supports_skip_locked") is True or (
            dialect_config.get_feature_version("supports_skip_locked") is not None
        )

    @property
    def supports_native_bulk_ingest(self) -> "bool":
        """Whether the adapter can ingest records via the native Arrow import path.

        Gated on the SQLSpec config's ``supports_native_arrow_import`` ClassVar
        (asyncpg COPY, MySQL/Oracle executemany-Arrow, or DuckDB zero-copy)
        *and* ``pyarrow`` being importable,
        since :meth:`load_from_records` normalizes rows through an Arrow table.
        Adapters without the capability fall back to the universal
        ``execute_many`` bulk tier, so this only ever upgrades throughput.
        """
        if not getattr(type(self._config), "supports_native_arrow_import", False):
            return False
        return _pyarrow_available()

    def create_statements(self) -> "list[str]":
        """Return statements that create the queue table and indexes."""
        if not self._manage_schema:
            return []
        return [self._to_sql(self._create_table_statement()), *self._create_index_statements()]

    def drop_statements(self) -> "list[str]":
        """Return statements that drop queue artifacts."""
        if not self._manage_schema:
            return []
        return [
            self._to_sql(sql.drop_index(self._index_name("heartbeat")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("pending")).if_exists()),
            self._to_sql(sql.drop_table(self.table_name).if_exists()),
        ]

    def insert_task(self, values: "dict[str, Any]") -> "Any":
        """Return an INSERT statement for a queued task."""
        mapped_values = self._mapped_values(values)
        return sql.insert(self.table_name).columns(*mapped_values.keys()).values(**mapped_values)

    def insert_tasks_template(self) -> "str":
        """Return a parametrized multi-row INSERT for ``driver.execute_many``.

        The template carries one named placeholder per physical column so a
        sequence of :meth:`bulk_values` rows can be batched in a single
        ``execute_many`` call. Identifiers are quoted per the store dialect.
        """
        columns = [self._col(canonical) for canonical in _TASK_COLUMNS]
        quoted_columns = ", ".join(self._quote_identifier(column) for column in columns)
        placeholders = ", ".join(f":{column}" for column in columns)
        # Identifiers come from the fixed _TASK_COLUMNS tuple and are dialect-quoted;
        # every value is a bound :name placeholder, so there is no injection surface.
        return f"INSERT INTO {self._quoted_table_name()} ({quoted_columns}) VALUES ({placeholders})"  # noqa: S608

    def bulk_values(self, rows: "Sequence[dict[str, Any]]") -> "list[dict[str, Any]]":
        """Return column-mapped parameter rows for bulk insert.

        Shared by the ``execute_many`` template path and the native
        ``load_from_records`` Arrow path; both consume physical-column keys.
        JSON serialization is already applied upstream by the backend.

        Columns are emitted in :data:`_TASK_COLUMNS` (CREATE TABLE) order.
        This ordering is mandatory: the native Arrow ingest path inserts
        positionally on some adapters (DuckDB runs
        ``INSERT INTO t SELECT * FROM arrow``), so an Arrow column order that
        differs from the table DDL silently lands each value in the wrong
        column. Name-binding adapters (asyncpg COPY, SQLite) are order-immune,
        which is exactly why a column scramble can pass on one adapter and
        fail on another.
        """
        return [{self._col(column): row[column] for column in _TASK_COLUMNS} for row in rows]

    def select_task(self, task_id: "str") -> "Any":
        """Return a SELECT statement for one task id."""
        return self._select_all().where_eq(self._col("id"), task_id)

    def select_task_by_key(self, key: "str") -> "Any":
        """Return a SELECT statement for one task key."""
        return self._select_all().where_eq(self._col("task_key"), key)

    def select_tasks_by_keys(self, keys: "Sequence[str]") -> "Any":
        """Return a SELECT statement for all tasks matching any of ``keys``.

        Used by bulk enqueue to resolve existing deduplication keys in a single
        round trip before inserting the remaining rows.
        """
        return self._select_all().where_in(self._col("task_key"), list(keys))

    def list_pending(
        self, *, now: "Any", limit: "int", queue: "str | None" = None, execution_backend: "str | None" = None
    ) -> "Any":
        """Return a SELECT statement for due pending tasks."""
        statement = (
            self
            ._select_all()
            .where_in(self._col("status"), _DUE_STATUSES)
            .where(f"{self._col('scheduled_at')} IS NULL OR {self._col('scheduled_at')} <= :now", now=now)
        )
        if queue is not None:
            statement = statement.where_eq(self._col("queue"), queue)
        if execution_backend is not None:
            statement = statement.where_eq(self._col("execution_backend"), execution_backend)
        return statement.order_by(
            sql.raw(f"{self._col('priority')} DESC"), sql.raw(f"{self._col('created_at')} ASC")
        ).limit(limit)

    def select_claimable(
        self, *, now: "Any", limit: "int", queue: "str | None" = None, execution_backend: "str | None" = None
    ) -> "Any":
        """Return a due-task SELECT that locks rows with ``FOR UPDATE SKIP LOCKED``.

        Mirrors :meth:`list_pending` but adds row-level locking so competing
        workers each claim a distinct row instead of colliding on the optimistic
        CAS claim. Callers must only use this on adapters that report
        :attr:`supports_skip_locked`; on dialects without locking support
        sqlglot drops the clause, so it is never relied upon as a guarantee.
        """
        return self.list_pending(now=now, limit=limit, queue=queue, execution_backend=execution_backend).for_update(
            skip_locked=True
        )

    def claim_task(self, *, task_id: "str", due_at: "Any", started_at: "Any", heartbeat_at: "Any") -> "Any":
        """Return an UPDATE statement that claims a due task."""
        return (
            sql
            .update(self.table_name)
            .set(**self._mapped_values({"status": "running", "started_at": started_at, "heartbeat_at": heartbeat_at}))
            .where_eq(self._col("id"), task_id)
            .where_in(self._col("status"), _DUE_STATUSES)
            .where(f"{self._col('scheduled_at')} IS NULL OR {self._col('scheduled_at')} <= :due_at", due_at=due_at)
        )

    def complete_task(
        self,
        *,
        task_id: "str",
        completed_at: "Any",
        heartbeat_at: "Any",
        result_json: "Any",
        expected_retry_count: "int | None" = None,
    ) -> "Any":
        """Return an UPDATE statement that completes a task."""
        statement = (
            sql
            .update(self.table_name)
            .set(
                **self._mapped_values({
                    "status": "completed",
                    "completed_at": completed_at,
                    "heartbeat_at": heartbeat_at,
                    "result_json": result_json,
                    "error": None,
                })
            )
            .where_eq(self._col("id"), task_id)
        )
        if expected_retry_count is not None:
            statement = statement.where_eq(self._col("status"), "running").where_eq(
                self._col("retry_count"), expected_retry_count
            )
        return statement

    def retry_task(
        self,
        *,
        task_id: "str",
        error: "str",
        retry_count: "int",
        expected_retry_count: "int | None" = None,
        heartbeat_cutoff: "Any | None" = None,
    ) -> "Any":
        """Return an UPDATE statement that schedules a retry."""
        statement = (
            sql
            .update(self.table_name)
            .set(
                **self._mapped_values({
                    "status": "pending",
                    "retry_count": retry_count,
                    "started_at": None,
                    "heartbeat_at": None,
                    "error": error,
                })
            )
            .where_eq(self._col("id"), task_id)
            .where_eq(self._col("status"), "running")
        )
        if expected_retry_count is not None:
            statement = statement.where_eq(self._col("retry_count"), expected_retry_count)
        if heartbeat_cutoff is not None:
            statement = statement.where(
                f"{self._col('heartbeat_at')} IS NULL OR {self._col('heartbeat_at')} < :heartbeat_cutoff",
                heartbeat_cutoff=heartbeat_cutoff,
            )
        return statement

    def fail_task(
        self,
        *,
        task_id: "str",
        completed_at: "Any",
        heartbeat_at: "Any",
        error: "str",
        expected_retry_count: "int | None" = None,
        heartbeat_cutoff: "Any | None" = None,
    ) -> "Any":
        """Return an UPDATE statement that permanently fails a task."""
        statement = (
            sql
            .update(self.table_name)
            .set(
                **self._mapped_values({
                    "status": "failed",
                    "completed_at": completed_at,
                    "heartbeat_at": heartbeat_at,
                    "error": error,
                })
            )
            .where_eq(self._col("id"), task_id)
            .where_eq(self._col("status"), "running")
        )
        if expected_retry_count is not None:
            statement = statement.where_eq(self._col("retry_count"), expected_retry_count)
        if heartbeat_cutoff is not None:
            statement = statement.where(
                f"{self._col('heartbeat_at')} IS NULL OR {self._col('heartbeat_at')} < :heartbeat_cutoff",
                heartbeat_cutoff=heartbeat_cutoff,
            )
        return statement

    def cancel_task(self, *, task_id: "str", completed_at: "Any") -> "Any":
        """Return an UPDATE statement that cancels a due task."""
        return (
            sql
            .update(self.table_name)
            .set(**self._mapped_values({"status": "cancelled", "completed_at": completed_at}))
            .where_eq(self._col("id"), task_id)
            .where_in(self._col("status"), _DUE_STATUSES)
        )

    def touch_heartbeat(self, *, task_id: "str", heartbeat_at: "Any") -> "Any":
        """Return an UPDATE statement that touches a running task heartbeat."""
        return (
            sql
            .update(self.table_name)
            .set(**self._mapped_values({"heartbeat_at": heartbeat_at}))
            .where_eq(self._col("id"), task_id)
            .where_eq(self._col("status"), "running")
        )

    def null_heartbeats(self, *, task_ids: "list[str]") -> "Any":
        """Return an UPDATE statement that clears task heartbeats."""
        return (
            sql
            .update(self.table_name)
            .set(**self._mapped_values({"heartbeat_at": None}))
            .where_in(self._col("id"), task_ids)
        )

    def requeue_stale(self, *, cutoff: "Any") -> "Any":
        """Return an UPDATE statement that requeues stale running tasks."""
        return (
            sql
            .update(self.table_name)
            .set(
                **self._mapped_values({
                    "status": "pending",
                    "started_at": None,
                    "heartbeat_at": None,
                    "retry_count": sql.raw(f"{self._col('retry_count')} + 1"),
                })
            )
            .where_eq(self._col("status"), "running")
            .where(f"{self._col('heartbeat_at')} IS NULL OR {self._col('heartbeat_at')} < :cutoff", cutoff=cutoff)
        )

    def list_stale_running(self, *, cutoff: "Any") -> "Any":
        """Return a SELECT statement for stale running tasks."""
        return (
            self
            ._select_all()
            .where_eq(self._col("status"), "running")
            .where(f"{self._col('heartbeat_at')} IS NULL OR {self._col('heartbeat_at')} < :cutoff", cutoff=cutoff)
        )

    def clear_key(self, *, task_id: "str") -> "Any":
        """Return an UPDATE statement that releases a terminal task key."""
        return (
            sql
            .update(self.table_name)
            .set(**self._mapped_values({"task_key": None}))
            .where_eq(self._col("id"), task_id)
        )

    def set_execution_ref(
        self, *, task_id: "str", execution_backend: "str", execution_ref: "str", execution_profile: "str | None"
    ) -> "Any":
        """Return an UPDATE statement that stores an external execution reference."""
        return (
            sql
            .update(self.table_name)
            .set(
                **self._mapped_values({
                    "execution_backend": execution_backend,
                    "execution_profile": execution_profile,
                    "execution_ref": execution_ref,
                })
            )
            .where_eq(self._col("id"), task_id)
        )

    def set_execution_backend(
        self, *, task_id: "str", execution_backend: "str", execution_profile: "str | None"
    ) -> "Any":
        """Return an UPDATE statement that changes execution routing."""
        return (
            sql
            .update(self.table_name)
            .set(
                **self._mapped_values({
                    "execution_backend": execution_backend,
                    "execution_profile": execution_profile,
                    "execution_ref": None,
                })
            )
            .where_eq(self._col("id"), task_id)
        )

    def list_running_external(self, *, limit: "int | None" = None) -> "Any":
        """Return a SELECT statement for externally dispatched records."""
        statement = (
            self
            ._select_all()
            .where(f"{self._col('status')} IN ('pending', 'scheduled', 'running')")
            .where(f"{self._col('execution_ref')} IS NOT NULL")
            .order_by(sql.raw(f"{self._col('started_at')} ASC"), sql.raw(f"{self._col('created_at')} ASC"))
        )
        return statement.limit(limit) if limit is not None else statement

    def list_all(self) -> "Any":
        """Return a SELECT statement for all queue records."""
        return self._select_all()

    def list_completed_by_task(self, *, task_name: "str", since: "Any | None" = None, limit: "int" = 10) -> "Any":
        """Return a SELECT statement for completed records by task name."""
        statement = (
            self._select_all().where_eq(self._col("task_name"), task_name).where_eq(self._col("status"), "completed")
        )
        if since is not None:
            statement = statement.where(f"{self._col('completed_at')} >= :completed_since", completed_since=since)
        return statement.order_by(sql.raw(f"{self._col('completed_at')} DESC")).limit(limit)

    def count_terminal(self, *, before: "Any") -> "Any":
        """Return a COUNT statement matching the same predicate as cleanup_terminal.

        Used by the backend to return a deterministic row count even when the
        underlying driver cannot report ``rows_affected`` for DELETE.
        """
        return (
            sql
            .select(sql.raw("COUNT(*) AS terminal_count"))
            .from_(self.table_name)
            .where_in(self._col("status"), ("completed", "failed", "cancelled"))
            .where(
                f"{self._col('completed_at')} IS NOT NULL AND {self._col('completed_at')} < :terminal_before",
                terminal_before=before,
            )
        )

    def cleanup_terminal(self, *, before: "Any") -> "Any":
        """Return a DELETE statement for terminal records before a cutoff."""
        return (
            sql
            .delete(self.table_name)
            .where_in(self._col("status"), ("completed", "failed", "cancelled"))
            .where(
                f"{self._col('completed_at')} IS NOT NULL AND {self._col('completed_at')} < :terminal_before",
                terminal_before=before,
            )
        )

    def serialize_json(self, canonical: "str", value: "Any") -> "Any":
        """Serialize a JSON value for a canonical queue column.

        Native JSON columns (driver registers a JSON codec — e.g.
        SQLSpec's asyncpg/psycopg JSONB codec, psqlpy's PyJSON type,
        mysql JSON, oracle JSON) accept structured Python values
        directly. Primitives (``str``, ``int``, ``float``, ``bool``,
        ``None``) must be pre-encoded because most codecs treat a raw
        ``str`` parameter as already-JSON-encoded text — sending
        ``"ok"`` would be parsed as the bare JSON token ``ok`` (invalid).
        TEXT columns receive the JSON-encoded string from
        ``_serialize_json``.

        Returns:
            The value shaped for the configured adapter.
        """
        if canonical in self._native_json_columns:
            if isinstance(value, (dict, list, tuple)):
                return value
            return self._serialize_json(value)
        return self._serialize_json(value)

    def deserialize_json(self, canonical: "str", value: "Any") -> "Any":
        """Deserialize a task JSON value returned by the database driver.

        When ``canonical`` is in ``_native_json_columns`` the driver has
        already decoded the JSON value (e.g. psycopg JSONB → Python value);
        pass the value through. Otherwise the value is a JSON-encoded
        ``str``/``bytes`` and must be decoded with ``from_json``.

        Returns:
            The decoded Python JSON value.
        """
        if value is None:
            return None
        read = getattr(value, "read", None)
        if callable(read):
            value = read()
        if canonical in self._native_json_columns:
            return value
        if isinstance(value, (list, dict)):
            return value

        return from_json(value)

    def _col(self, canonical: "str") -> "str":
        """Return the configured database column name for ``canonical``."""
        return self._column_map.get(canonical, canonical)

    def _select_all(self) -> "Any":
        columns = tuple(self._select_column(canonical) for canonical in _TASK_COLUMNS)
        return sql.select(*columns).from_(self.table_name)

    def _create_table_statement(self) -> "Any":
        return (
            sql
            .create_table(self.table_name)
            .if_not_exists()
            .column(self._col("id"), self._id_type(), primary_key=True)
            .column(self._col("task_name"), self._indexed_text_type(), not_null=True)
            .column(self._col("args_json"), self._payload_json_type("args_json"), not_null=True)
            .column(self._col("kwargs_json"), self._payload_json_type("kwargs_json"), not_null=True)
            .column(self._col("queue"), self._indexed_text_type(), not_null=True)
            .column(self._col("execution_backend"), self._indexed_text_type(), not_null=True)
            .column(self._col("execution_profile"), self._indexed_text_type())
            .column(self._col("execution_ref"), self._indexed_text_type())
            .column(self._col("status"), self._indexed_text_type(), not_null=True)
            .column(self._col("priority"), self._integer_type(), not_null=True)
            .column(self._col("max_retries"), self._integer_type(), not_null=True)
            .column(self._col("retry_count"), self._integer_type(), not_null=True)
            .column(self._col("scheduled_at"), self._timestamp_type())
            .column(self._col("created_at"), self._timestamp_type(), not_null=True)
            .column(self._col("started_at"), self._timestamp_type())
            .column(self._col("completed_at"), self._timestamp_type())
            .column(self._col("heartbeat_at"), self._timestamp_type())
            .column(self._col("result_json"), self._result_json_type("result_json"), not_null=True)
            .column(self._col("error"), self._error_type())
            .column(self._col("task_key"), self._indexed_text_type(), unique=True)
            .column(self._col("metadata_json"), self._metadata_json_type("metadata_json"), not_null=True)
        )

    def _create_index_statements(self) -> "list[str]":
        return [
            self._to_sql(
                sql
                .create_index(self._index_name("pending"))
                .if_not_exists()
                .on_table(self.table_name)
                .columns(
                    *(
                        self._col(canonical)
                        for canonical in (
                            "status",
                            "queue",
                            "execution_backend",
                            "scheduled_at",
                            "priority",
                            "created_at",
                        )
                    )
                )
            ),
            self._to_sql(
                sql
                .create_index(self._index_name("heartbeat"))
                .if_not_exists()
                .on_table(self.table_name)
                .columns(*(self._col(canonical) for canonical in ("status", "heartbeat_at")))
            ),
        ]

    def _select_column(self, canonical: "str") -> "str":
        column = self._col(canonical)
        if column == canonical:
            return canonical
        return f"{column} AS {canonical}"

    def _mapped_values(self, values: "dict[str, Any]") -> "dict[str, Any]":
        return {self._col(column): value for column, value in values.items()}

    def _index_name(self, suffix: "str") -> "str":
        return f"ix_{self.table_name.replace('.', '_')}_{suffix}"

    def _id_type(self) -> "str":
        return self._string_type(64)

    def _text_type(self) -> "str":
        return self._dialect_type("text", fallback="TEXT")

    def _string_type(self, length: "int | None" = None) -> "str":
        return self._text_type() if length is None else f"VARCHAR({length})"

    def _indexed_text_type(self) -> "str":
        return self._string_type(255)

    def _integer_type(self) -> "str":
        return self._dialect_type("integer", fallback="INTEGER")

    def _json_type(self) -> "str":
        return self._dialect_type("json", fallback=self._text_type())

    def _payload_json_type(self, column_name: "str") -> "str":
        return self._json_type()

    def _result_json_type(self, column_name: "str") -> "str":
        return self._json_type()

    def _metadata_json_type(self, column_name: "str") -> "str":
        return self._json_type()

    def _timestamp_type(self) -> "str":
        return self._dialect_type("timestamp", fallback=self._text_type())

    def _error_type(self) -> "str":
        return self._text_type()

    def _serialize_json(self, value: "Any") -> "str":
        return to_json(value)

    def _to_sql(self, statement: "Any") -> "str":
        built = statement.build(dialect=self.dialect_name)
        return cast("str", built.sql)

    def _data_dictionary_dialect_name(self) -> "str | None":
        return type(self).data_dictionary_dialect or self.dialect_name

    def _dialect_config(self) -> "DialectConfig | None":
        dialect_name = self._data_dictionary_dialect_name()
        if dialect_name is None:
            return None
        try:
            return get_dialect_config(dialect_name)
        except ValueError:
            return None

    def _dialect_type(self, logical_type: "str", *, fallback: "str") -> "str":
        dialect_config = self._dialect_config()
        if dialect_config is not None and logical_type in dialect_config.type_mappings:
            return dialect_config.get_optimal_type(logical_type)
        return fallback

    def _quoted_table_name(self) -> "str":
        return self._quote_identifier(self.table_name)

    def _quoted_index_name(self, suffix: "str") -> "str":
        return self._quote_identifier(self._index_name(suffix))

    def _quoted_col(self, canonical: "str") -> "str":
        return self._quote_identifier(self._col(canonical))

    def _quote_identifier(self, identifier: "str") -> "str":
        if type(self).identifier_quote_style == "none":
            return identifier
        quote = quote_backtick_identifier if type(self).identifier_quote_style == "backtick" else quote_identifier
        parts = split_qualified_identifier(identifier)
        if not parts:
            return quote(identifier)
        return ".".join(quote(part) for part in parts)


def _configured_table_name(config: "Any", table_name: "str | None") -> "str":
    if table_name is not None:
        return validate_table_name(table_name)
    extension_config = cast("dict[str, Any]", getattr(config, "extension_config", {}) or {})
    queue_settings = cast("dict[str, Any]", extension_config.get(QUEUE_EXTENSION_NAME, {}) or {})
    return validate_table_name(str(queue_settings.get("table_name", DEFAULT_TABLE_NAME)))


def _adapter_name(config: "Any") -> "str":
    module_name = type(config).__module__
    if module_name.startswith("sqlspec.adapters."):
        return module_name.split(".")[2]
    return ""


@cache
def _pyarrow_available() -> "bool":
    """Return whether ``pyarrow`` is importable, caching the lookup."""
    return find_spec("pyarrow") is not None

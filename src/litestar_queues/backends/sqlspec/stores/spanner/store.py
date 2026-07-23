"""spanner SQLSpec queue store."""

from typing import Any, ClassVar, Literal

from sqlspec.utils.serializers import from_json

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("SpannerQueueStore",)


class SpannerQueueStore(SQLSpecQueueStore):
    """spanner-specific SQLSpec queue statement store."""

    __slots__ = ()

    data_dictionary_dialect: "ClassVar[str | None]" = "spanner"
    identifier_quote_style: 'ClassVar[Literal["double", "backtick", "none"]]' = "backtick"
    skip_cleanup_rollback: "ClassVar[bool]" = True
    auto_native_json_columns: "ClassVar[frozenset[str]]" = frozenset({
        "args_json",
        "kwargs_json",
        "metadata_json",
        "result_json",
    })

    def create_statements(self) -> "list[str]":
        """Return statements that create the Spanner queue table and indexes."""
        if not self._manage_schema:
            return []
        return [self._build_create_table_sql(), *self._create_index_statements()]

    def drop_statements(self) -> "list[str]":
        """Return statements that drop Spanner queue artifacts."""
        if not self._manage_schema:
            return []
        return [
            f"DROP INDEX {self._quoted_index_name('task_key')}",
            f"DROP INDEX {self._quoted_index_name('heartbeat')}",
            f"DROP INDEX {self._quoted_index_name('pending')}",
            f"DROP TABLE {self._quoted_table_name()}",
        ]

    def create_schema_for_config(self, config: "Any") -> "None":
        """Create Spanner schema objects through the native DDL operation API."""
        if not self._manage_schema:
            return
        get_database = getattr(config, "get_database", None)
        if not callable(get_database):
            msg = "Spanner queue schema creation requires a SQLSpec SpannerSyncConfig."
            raise TypeError(msg)
        database = get_database()
        for statement in self.create_statements():
            _execute_spanner_ddl(database, statement)

    def serialize_json(self, canonical: "str", value: "Any") -> "Any":
        """Serialize JSON values as native Spanner JSON parameters.

        Returns:
            A Spanner JSON parameter value for JSON columns, otherwise a
            serialized JSON value.
        """
        if canonical in self._native_json_columns:
            from sqlspec.adapters.spanner import spanner_json

            return spanner_json(value)
        return super().serialize_json(canonical, value)

    def deserialize_json(self, canonical: "str", value: "Any") -> "Any":
        """Deserialize native Spanner JSON wrappers into Python values.

        Returns:
            The decoded Python JSON value.
        """
        if canonical in self._native_json_columns:
            return _deserialize_spanner_json(value)
        return super().deserialize_json(canonical, value)

    def _build_create_table_sql(self) -> "str":
        columns = (
            f"{self._quoted_col('id')} {self._id_type()} NOT NULL",
            f"{self._quoted_col('task_name')} {self._indexed_text_type()} NOT NULL",
            f"{self._quoted_col('args_json')} {self._payload_json_type('args_json')} NOT NULL",
            f"{self._quoted_col('kwargs_json')} {self._payload_json_type('kwargs_json')} NOT NULL",
            f"{self._quoted_col('queue')} {self._indexed_text_type()} NOT NULL",
            f"{self._quoted_col('execution_backend')} {self._indexed_text_type()} NOT NULL",
            f"{self._quoted_col('execution_profile')} {self._indexed_text_type()}",
            f"{self._quoted_col('execution_ref')} {self._indexed_text_type()}",
            f"{self._quoted_col('status')} {self._indexed_text_type()} NOT NULL",
            f"{self._quoted_col('priority')} {self._integer_type()} NOT NULL",
            f"{self._quoted_col('max_retries')} {self._integer_type()} NOT NULL",
            f"{self._quoted_col('retry_count')} {self._integer_type()} NOT NULL",
            f"{self._quoted_col('scheduled_at')} {self._timestamp_type()}",
            f"{self._quoted_col('created_at')} {self._timestamp_type()} NOT NULL",
            f"{self._quoted_col('started_at')} {self._timestamp_type()}",
            f"{self._quoted_col('completed_at')} {self._timestamp_type()}",
            f"{self._quoted_col('heartbeat_at')} {self._timestamp_type()}",
            f"{self._quoted_col('result_json')} {self._result_json_type('result_json')}",
            f"{self._quoted_col('error')} {self._error_type()}",
            f"{self._quoted_col('task_key')} {self._indexed_text_type()}",
            f"{self._quoted_col('metadata_json')} {self._metadata_json_type('metadata_json')} NOT NULL",
        )
        column_sql = ",\n  ".join(columns)
        return f"CREATE TABLE {self._quoted_table_name()} (\n  {column_sql}\n) PRIMARY KEY ({self._quoted_col('id')})"

    def _create_index_statements(self) -> "list[str]":
        return [
            (
                f"CREATE INDEX {self._quoted_index_name('pending')} ON {self._quoted_table_name()} "
                f"({self._quoted_col('status')}, {self._quoted_col('queue')}, "
                f"{self._quoted_col('execution_backend')}, {self._quoted_col('scheduled_at')}, "
                f"{self._quoted_col('priority')}, {self._quoted_col('created_at')})"
            ),
            (
                f"CREATE INDEX {self._quoted_index_name('heartbeat')} ON {self._quoted_table_name()} "
                f"({self._quoted_col('status')}, {self._quoted_col('heartbeat_at')})"
            ),
            (
                f"CREATE UNIQUE NULL_FILTERED INDEX {self._quoted_index_name('task_key')} "
                f"ON {self._quoted_table_name()} ({self._quoted_col('task_key')})"
            ),
        ]

    def _string_type(self, length: "int | None" = None) -> "str":
        return "STRING(MAX)" if length is None else f"STRING({length})"

    def _integer_type(self) -> "str":
        return "INT64"

    def _timestamp_type(self) -> "str":
        return "TIMESTAMP"


def _is_spanner_already_exists_error(exc: "Exception") -> "bool":
    try:
        from google.api_core.exceptions import AlreadyExists
    except ImportError:
        return "already exists" in str(exc).lower()
    return isinstance(exc, AlreadyExists) or "already exists" in str(exc).lower()


def _deserialize_spanner_json(value: "Any") -> "Any":
    if value is None:
        return None
    if not _is_spanner_json_object(value):
        return value
    serialized = value.serialize()
    if serialized is None:
        return None
    return from_json(serialized)


def _is_spanner_json_object(value: "Any") -> "bool":
    serialize = getattr(value, "serialize", None)
    if not callable(serialize):
        return False
    return type(value).__module__.startswith("google.cloud.spanner")


def _execute_spanner_ddl(database: "Any", statement: "str") -> "None":
    try:
        database.update_ddl([statement]).result()
    except Exception as exc:
        if _is_spanner_already_exists_error(exc):
            return
        raise

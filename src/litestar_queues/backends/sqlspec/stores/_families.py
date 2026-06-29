"""Shared SQLSpec queue store implementations for dialect families."""

from typing import ClassVar, Literal

from sqlspec import sql

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("MySQLQueueStore", "PostgresQueueStore")


class PostgresQueueStore(SQLSpecQueueStore):
    """Postgres-family queue store with partial indexes."""

    __slots__ = ()

    data_dictionary_dialect: "ClassVar[str | None]" = "postgres"
    table_storage_parameters: "ClassVar[bool]" = False
    auto_native_json_columns: "ClassVar[frozenset[str]]" = frozenset({
        "args_json",
        "kwargs_json",
        "metadata_json",
        "result_json",
    })

    def create_statements(self) -> "list[str]":
        """Return statements that create Postgres-family queue artifacts."""
        if not self._manage_schema:
            return []
        create_table = self._to_sql(self._create_table_statement())
        if type(self).table_storage_parameters:
            create_table = f"{create_table} WITH (fillfactor = 80)"
        statements = [create_table, *self._create_index_statements()]
        if type(self).table_storage_parameters:
            statements.append(
                f"ALTER TABLE {self._quoted_table_name()} SET ("
                "autovacuum_vacuum_scale_factor = 0.05, "
                "autovacuum_analyze_scale_factor = 0.02)"
            )
        return statements

    def drop_statements(self) -> "list[str]":
        """Return statements that drop Postgres-family queue artifacts."""
        if not self._manage_schema:
            return []
        return [
            self._to_sql(sql.drop_index(self._index_name("heartbeat")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("scheduled")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("pending")).if_exists()),
            self._to_sql(sql.drop_table(self.table_name).if_exists()),
        ]

    def _create_index_statements(self) -> "list[str]":
        table_name = self._quoted_table_name()
        return [
            (
                f"CREATE INDEX IF NOT EXISTS {self._quoted_index_name('pending')} "
                f"ON {table_name} ({self._quoted_col('queue')}, {self._quoted_col('execution_backend')}, "
                f"{self._quoted_col('priority')} DESC, {self._quoted_col('created_at')}) "
                f"WHERE {self._quoted_col('status')} IN ('pending', 'scheduled')"
            ),
            (
                f"CREATE INDEX IF NOT EXISTS {self._quoted_index_name('scheduled')} "
                f"ON {table_name} ({self._quoted_col('scheduled_at')}) "
                f"WHERE {self._quoted_col('status')} = 'scheduled'"
            ),
            (
                f"CREATE INDEX IF NOT EXISTS {self._quoted_index_name('heartbeat')} "
                f"ON {table_name} ({self._quoted_col('heartbeat_at')}) "
                f"WHERE {self._quoted_col('status')} = 'running'"
            ),
        ]

    def _json_type(self) -> "str":
        return "JSONB"

    def _timestamp_type(self) -> "str":
        return "TIMESTAMPTZ"


class MySQLQueueStore(SQLSpecQueueStore):
    """MySQL-family queue store with shared InnoDB DDL."""

    __slots__ = ()

    data_dictionary_dialect: "ClassVar[str | None]" = "mysql"
    identifier_quote_style: 'ClassVar[Literal["double", "backtick", "none"]]' = "backtick"
    auto_native_json_columns: "ClassVar[frozenset[str]]" = frozenset({
        "args_json",
        "kwargs_json",
        "metadata_json",
        "result_json",
    })

    def create_statements(self) -> "list[str]":
        """Return statements that create MySQL-family queue artifacts."""
        if not self._manage_schema:
            return []
        return [self._create_mysql_table_statement()]

    def drop_statements(self) -> "list[str]":
        """Return statements that drop MySQL-family queue artifacts."""
        if not self._manage_schema:
            return []
        return [self._to_sql(sql.drop_table(self.table_name).if_exists())]

    def _create_mysql_table_statement(self) -> "str":
        return f"""
        CREATE TABLE IF NOT EXISTS {self._quoted_table_name()} (
            {self._quoted_col("id")} {self._id_type()} PRIMARY KEY,
            {self._quoted_col("task_name")} {self._indexed_text_type()} NOT NULL,
            {self._quoted_col("args_json")} {self._payload_json_type("args_json")} NOT NULL,
            {self._quoted_col("kwargs_json")} {self._payload_json_type("kwargs_json")} NOT NULL,
            {self._quoted_col("queue")} {self._indexed_text_type()} NOT NULL,
            {self._quoted_col("execution_backend")} {self._indexed_text_type()} NOT NULL,
            {self._quoted_col("execution_profile")} {self._indexed_text_type()},
            {self._quoted_col("execution_ref")} {self._indexed_text_type()},
            {self._quoted_col("status")} {self._indexed_text_type()} NOT NULL,
            {self._quoted_col("priority")} {self._integer_type()} NOT NULL,
            {self._quoted_col("max_retries")} {self._integer_type()} NOT NULL,
            {self._quoted_col("retry_count")} {self._integer_type()} NOT NULL,
            {self._quoted_col("scheduled_at")} {self._timestamp_type()},
            {self._quoted_col("created_at")} {self._timestamp_type()} NOT NULL,
            {self._quoted_col("started_at")} {self._timestamp_type()},
            {self._quoted_col("completed_at")} {self._timestamp_type()},
            {self._quoted_col("heartbeat_at")} {self._timestamp_type()},
            {self._quoted_col("result_json")} {self._result_json_type("result_json")} NOT NULL,
            {self._quoted_col("error")} {self._error_type()},
            {self._quoted_col("task_key")} {self._indexed_text_type()} UNIQUE,
            {self._quoted_col("metadata_json")} {self._metadata_json_type("metadata_json")} NOT NULL,
            INDEX {self._quoted_index_name("pending")} (
                {self._prefixed_col("status", 32)}, {self._prefixed_col("queue", 191)},
                {self._prefixed_col("execution_backend", 191)}, {self._quoted_col("scheduled_at")},
                {self._quoted_col("priority")}, {self._quoted_col("created_at")}
            ),
            INDEX {self._quoted_index_name("heartbeat")} (
                {self._prefixed_col("status", 32)}, {self._quoted_col("heartbeat_at")}
            )
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

    def _prefixed_col(self, canonical: "str", length: "int") -> "str":
        return f"{self._quoted_col(canonical)}({length})"

    def _timestamp_type(self) -> "str":
        return "DATETIME(6)"

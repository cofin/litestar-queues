"""Shared SQLSpec queue store implementations for dialect families."""

from sqlspec import sql
from sqlspec.utils.text import split_qualified_identifier

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("MySQLQueueStore", "PostgresQueueStore", "SQLServerQueueStore")


class PostgresQueueStore(SQLSpecQueueStore):
    """Postgres-family queue store with partial indexes."""

    __slots__ = ()

    data_dictionary_dialect = "postgres"
    table_storage_parameters = False
    auto_native_json_columns = frozenset({"args_json", "kwargs_json", "metadata_json", "result_json"})

    def create_statements(self) -> list[str]:
        """Return statements that create Postgres-family queue artifacts."""
        if not self._manage_schema:
            return []
        create_table = self._to_sql(self._create_table_statement())
        if type(self).table_storage_parameters:
            create_table = f"{create_table} WITH (fillfactor = 80)"
        statements = [create_table, *self._create_index_statements()]
        if type(self).table_storage_parameters:
            statements.append(
                (
                    f"ALTER TABLE {self._quoted_table_name()} SET ("
                    "autovacuum_vacuum_scale_factor = 0.05, "
                    "autovacuum_analyze_scale_factor = 0.02)"
                )
            )
        return statements

    def drop_statements(self) -> list[str]:
        """Return statements that drop Postgres-family queue artifacts."""
        if not self._manage_schema:
            return []
        return [
            self._to_sql(sql.drop_index(self._index_name("heartbeat")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("scheduled")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("pending")).if_exists()),
            self._to_sql(sql.drop_table(self.table_name).if_exists()),
        ]

    def _create_index_statements(self) -> list[str]:
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


class MySQLQueueStore(SQLSpecQueueStore):
    """MySQL-family queue store with shared InnoDB DDL."""

    __slots__ = ()

    data_dictionary_dialect = "mysql"
    identifier_quote_style = "backtick"
    id_type = "VARCHAR(64)"
    indexed_text_type = "VARCHAR(255)"
    integer_type = "INTEGER"
    timestamp_type = "VARCHAR(64)"
    error_type = "LONGTEXT"
    auto_native_json_columns = frozenset({"args_json", "kwargs_json", "metadata_json", "result_json"})

    def create_statements(self) -> list[str]:
        """Return statements that create MySQL-family queue artifacts."""
        if not self._manage_schema:
            return []
        return [self._create_mysql_table_statement()]

    def drop_statements(self) -> list[str]:
        """Return statements that drop MySQL-family queue artifacts."""
        if not self._manage_schema:
            return []
        return [self._to_sql(sql.drop_table(self.table_name).if_exists())]

    def _create_mysql_table_statement(self) -> str:
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

    def _prefixed_col(self, canonical: str, length: int) -> str:
        return f"{self._quoted_col(canonical)}({length})"


class SQLServerQueueStore(SQLSpecQueueStore):
    """SQL Server queue store with T-SQL existence guards."""

    __slots__ = ()

    data_dictionary_dialect = "mssql"
    identifier_quote_style = "none"
    id_type = "NVARCHAR(64)"
    indexed_text_type = "NVARCHAR(255)"
    integer_type = "INT"

    def create_statements(self) -> list[str]:
        """Return statements that create SQL Server queue artifacts."""
        if not self._manage_schema:
            return []
        return [
            self._wrap_create_table(self._create_sqlserver_table_statement()),
            self._wrap_create_index(
                "pending",
                (
                    f"{self._quoted_col('status')}, {self._quoted_col('queue')}, "
                    f"{self._quoted_col('execution_backend')}, {self._quoted_col('scheduled_at')}, "
                    f"{self._quoted_col('priority')}, {self._quoted_col('created_at')}"
                ),
            ),
            self._wrap_create_index(
                "heartbeat", f"{self._quoted_col('status')}, {self._quoted_col('heartbeat_at')}"
            ),
        ]

    def drop_statements(self) -> list[str]:
        """Return statements that drop SQL Server queue artifacts."""
        if not self._manage_schema:
            return []
        return [
            self._wrap_drop_index("heartbeat"),
            self._wrap_drop_index("pending"),
            self._wrap_drop_table(),
        ]

    def _create_sqlserver_table_statement(self) -> str:
        return f"""
        CREATE TABLE {self._quoted_table_name()} (
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
            {self._quoted_col("metadata_json")} {self._metadata_json_type("metadata_json")} NOT NULL
        )
        """

    def _wrap_create_table(self, statement: str) -> str:
        return f"IF OBJECT_ID(N'{_object_name(self.table_name)}', N'U') IS NULL BEGIN {statement}; END"

    def _wrap_create_index(self, suffix: str, columns: str) -> str:
        index_name = self._index_name(suffix)
        return (
            "IF NOT EXISTS (SELECT 1 FROM sys.indexes "
            f"WHERE name = N'{index_name}' AND object_id = OBJECT_ID(N'{_object_name(self.table_name)}')) "
            f"BEGIN CREATE INDEX {self._quoted_index_name(suffix)} ON {self._quoted_table_name()}({columns}); END"
        )

    def _wrap_drop_index(self, suffix: str) -> str:
        index_name = self._index_name(suffix)
        return (
            "IF EXISTS (SELECT 1 FROM sys.indexes "
            f"WHERE name = N'{index_name}' AND object_id = OBJECT_ID(N'{_object_name(self.table_name)}')) "
            f"DROP INDEX {self._quoted_index_name(suffix)} ON {self._quoted_table_name()};"
        )

    def _wrap_drop_table(self) -> str:
        return f"IF OBJECT_ID(N'{_object_name(self.table_name)}', N'U') IS NOT NULL DROP TABLE {self._quoted_table_name()};"


def _object_name(table_name: str) -> str:
    parts = split_qualified_identifier(table_name, quote_chars='"')
    if len(parts) < 2:
        schema_name = "dbo"
        bare_table_name = parts[0] if parts else table_name
    else:
        schema_name = ".".join(parts[:-1])
        bare_table_name = parts[-1]
    return f"{_quote_bracket_identifier(schema_name)}.{_quote_bracket_identifier(bare_table_name)}"


def _quote_bracket_identifier(identifier: str) -> str:
    return f"[{identifier.replace(']', ']]')}]"

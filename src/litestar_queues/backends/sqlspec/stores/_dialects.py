"""Shared SQLSpec queue store implementations, one per SQL dialect."""

from typing import ClassVar, Literal

from sqlspec import sql
from sqlspec.utils.text import split_qualified_identifier

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("CockroachQueueStore", "MssqlQueueStore", "MySQLQueueStore", "PostgresQueueStore")

_NVARCHAR_MAX_THRESHOLD = 4000


def _quote_tsql_identifier(identifier: "str") -> "str":
    return f"[{identifier.replace(']', ']]')}]"


class MssqlQueueStore(SQLSpecQueueStore):
    """SQL Server queue store with shared T-SQL DDL."""

    __slots__ = ()

    data_dictionary_dialect: "ClassVar[str | None]" = "mssql"
    identifier_quote_style: 'ClassVar[Literal["double", "backtick", "none"]]' = "none"

    def create_statements(self) -> "list[str]":
        """Return statements that create SQL Server queue artifacts."""
        return self._create_statements(include_expiration=True)

    def _create_statements(self, *, include_expiration: "bool") -> "list[str]":
        if not self._manage_schema:
            return []
        return [
            self._create_mssql_table_statement(include_expiration=include_expiration),
            self._create_mssql_unique_task_key_index_statement(),
            self._create_mssql_index_statement("pending"),
            self._create_mssql_index_statement("heartbeat"),
        ]

    def drop_statements(self) -> "list[str]":
        """Return statements that drop SQL Server queue artifacts."""
        if not self._manage_schema:
            return []
        return [
            self._drop_mssql_index_statement("task_key"),
            self._drop_mssql_index_statement("heartbeat"),
            self._drop_mssql_index_statement("pending"),
            self._drop_mssql_table_statement(),
        ]

    def _string_type(self, length: "int | None" = None) -> "str":
        if length is None:
            return self._text_type()
        if length >= _NVARCHAR_MAX_THRESHOLD:
            return "NVARCHAR(MAX)"
        return f"NVARCHAR({length})"

    def _integer_type(self) -> "str":
        return "INT"

    def _create_mssql_table_statement(self, *, include_expiration: "bool" = True) -> "str":
        expiration_column = f"{self._quoted_col('expires_at')} {self._timestamp_type()}," if include_expiration else ""
        return f"""
        IF OBJECT_ID(N'{self.table_name}', N'U') IS NULL
        BEGIN
            CREATE TABLE {self._quoted_table_name()} (
                {self._quoted_col("id")} {self._id_type()} PRIMARY KEY,
                {self._quoted_col("task_name")} {self._indexed_text_type()} NOT NULL,
                {self._quoted_col("args_json")} {self._payload_json_type("args_json")} NOT NULL,
                {self._quoted_col("kwargs_json")} {self._payload_json_type("kwargs_json")} NOT NULL,
                {self._quoted_col("queue")} {self._indexed_text_type()} NOT NULL,
                {self._quoted_col("execution_backend")} {self._indexed_text_type()} NOT NULL,
                {self._quoted_col("execution_profile")} {self._indexed_text_type()},
                {self._quoted_col("execution_ref")} {self._indexed_text_type()},
                {self._quoted_col("worker_id")} {self._indexed_text_type()},
                {self._quoted_col("status")} {self._indexed_text_type()} NOT NULL,
                {self._quoted_col("priority")} {self._integer_type()} NOT NULL,
                {self._quoted_col("max_retries")} {self._integer_type()} NOT NULL,
                {self._quoted_col("retry_count")} {self._integer_type()} NOT NULL,
                {self._quoted_col("scheduled_at")} {self._timestamp_type()},
                {expiration_column}
                {self._quoted_col("created_at")} {self._timestamp_type()} NOT NULL,
                {self._quoted_col("queued_at")} {self._timestamp_type()} NOT NULL,
                {self._quoted_col("started_at")} {self._timestamp_type()},
                {self._quoted_col("completed_at")} {self._timestamp_type()},
                {self._quoted_col("heartbeat_at")} {self._timestamp_type()},
                {self._quoted_col("result_json")} {self._result_json_type("result_json")} NOT NULL,
                {self._quoted_col("error")} {self._error_type()},
                {self._quoted_col("task_key")} {self._indexed_text_type()},
                {self._quoted_col("metadata_json")} {self._metadata_json_type("metadata_json")} NOT NULL
            );
        END;
        """

    def _create_mssql_unique_task_key_index_statement(self) -> str:
        index_name = self._index_name("task_key")
        return (
            "IF NOT EXISTS (SELECT 1 FROM sys.indexes "  # noqa: S608 - filtered SQL Server index requires raw T-SQL
            f"WHERE name = N'{index_name}' AND object_id = OBJECT_ID(N'{self.table_name}')) "
            f"CREATE UNIQUE INDEX {self._quoted_index_name('task_key')} "
            f"ON {self._quoted_table_name()} ({self._quoted_col('task_key')}) "
            f"WHERE {self._quoted_col('task_key')} IS NOT NULL;"
        )

    def _create_mssql_index_statement(self, suffix: str) -> str:
        if suffix == "pending":
            columns = ", ".join(
                self._quoted_col(canonical)
                for canonical in (
                    "status",
                    "queue",
                    "execution_backend",
                    "scheduled_at",
                    "priority",
                    "queued_at",
                    "created_at",
                )
            )
            return self._create_mssql_index_statement_sql("pending", columns)
        if suffix == "heartbeat":
            columns = ", ".join(self._quoted_col(canonical) for canonical in ("status", "heartbeat_at"))
            return self._create_mssql_index_statement_sql("heartbeat", columns)
        msg = f"Unsupported SQL Server index suffix: {suffix}"
        raise ValueError(msg)

    def _create_mssql_index_statement_sql(self, suffix: str, columns: str) -> str:
        index_name = self._index_name(suffix)
        return (
            "IF NOT EXISTS (SELECT 1 FROM sys.indexes "  # noqa: S608 - SQL Server index existence guard.
            f"WHERE name = N'{index_name}' AND object_id = OBJECT_ID(N'{self.table_name}')) "
            f"CREATE INDEX {self._quoted_index_name(suffix)} ON {self._quoted_table_name()} ({columns});"
        )

    def _drop_mssql_table_statement(self) -> str:
        return f"IF OBJECT_ID(N'{self.table_name}', N'U') IS NOT NULL DROP TABLE {self._quoted_table_name()};"

    def _drop_mssql_index_statement(self, suffix: str) -> str:
        index_name = self._index_name(suffix)
        return (
            "IF EXISTS (SELECT 1 FROM sys.indexes "  # noqa: S608 - SQL Server index existence guard.
            f"WHERE name = N'{index_name}' AND object_id = OBJECT_ID(N'{self.table_name}')) "
            f"DROP INDEX {self._quoted_index_name(suffix)} ON {self._quoted_table_name()};"
        )

    def _quote_identifier(self, identifier: "str") -> "str":
        parts = split_qualified_identifier(identifier)
        if not parts:
            return _quote_tsql_identifier(identifier)
        return ".".join(_quote_tsql_identifier(part) for part in parts)


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
    supports_bulk_touch_heartbeats: "ClassVar[bool]" = True
    supports_dml_returning: "ClassVar[bool]" = True
    supports_returning_claim: "ClassVar[bool]" = True
    supports_combined_expiry_claim: "ClassVar[bool]" = True

    def claim_batch_with_expired_returning_sql(self, *, queue_count: "int", filter_execution_backend: "bool") -> "str":
        """Return one PostgreSQL statement that expires and claims due records."""
        cache_key = f"claim_batch_with_expired:{queue_count}:{int(filter_execution_backend)}"

        def build() -> "str":
            table = self._quoted_table_name()
            id_col = self._quoted_col("id")
            status_col = self._quoted_col("status")
            scheduled_col = self._quoted_col("scheduled_at")
            expires_col = self._quoted_col("expires_at")
            execution_ref_col = self._quoted_col("execution_ref")
            queue_col = self._quoted_col("queue")
            eb_col = self._quoted_col("execution_backend")
            priority_col = self._quoted_col("priority")
            queued_col = self._quoted_col("queued_at")
            created_col = self._quoted_col("created_at")
            claim_conditions = [
                f"{status_col} IN ('pending', 'scheduled')",
                f"({scheduled_col} IS NULL OR {scheduled_col} <= :now)",
                f"({expires_col} IS NULL OR {expires_col} > :expires_now)",
                f"({execution_ref_col} IS NULL OR {execution_ref_col} NOT LIKE :reservation_prefix)",
                f"{id_col} NOT IN (SELECT {id_col} FROM expired_candidates)",  # noqa: S608
            ]
            if queue_count:
                placeholders = ", ".join(f":queue_{index}" for index in range(queue_count))
                claim_conditions.append(f"{queue_col} IN ({placeholders})")
            if filter_execution_backend:
                claim_conditions.append(f"{eb_col} = :execution_backend")
            claim_where = " AND ".join(claim_conditions)
            returned = self._prefixed_returning_columns_sql("t")
            return (
                f"WITH expired_candidates AS (SELECT {id_col} FROM {table} "  # noqa: S608
                f"WHERE {status_col} IN ('pending', 'scheduled') AND {execution_ref_col} IS NULL "
                f"AND {expires_col} IS NOT NULL AND {expires_col} <= :expires_now "
                f"ORDER BY {expires_col} ASC, {created_col} ASC, {id_col} ASC FOR UPDATE SKIP LOCKED), "
                f"expired AS (UPDATE {table} AS t SET {status_col} = 'expired', "
                f"{self._quoted_col('completed_at')} = :completed_at, {self._quoted_col('heartbeat_at')} = NULL "
                f"FROM expired_candidates AS e WHERE t.{id_col} = e.{id_col} RETURNING {returned}), "
                f"claim_candidates AS (SELECT {id_col} FROM {table} WHERE {claim_where} "
                f"ORDER BY {priority_col} DESC, {queued_col} ASC, {created_col} ASC, {id_col} ASC "
                f"FOR UPDATE SKIP LOCKED LIMIT :limit), "
                f"claimed AS (UPDATE {table} AS t SET {status_col} = 'running', "
                f"{self._quoted_col('started_at')} = :started_at, {self._quoted_col('heartbeat_at')} = :heartbeat_at "
                f"FROM claim_candidates AS c WHERE t.{id_col} = c.{id_col} RETURNING {returned}) "
                f"SELECT 'expired' AS \"_claim_outcome\", expired.* FROM expired "
                f"UNION ALL SELECT 'claimed' AS \"_claim_outcome\", claimed.* FROM claimed"
            )

        return self._cached(cache_key, build)

    def create_statements(self) -> "list[str]":
        """Return statements that create Postgres-family queue artifacts."""
        return self._create_statements(include_expiration=True)

    def _create_statements(self, *, include_expiration: "bool") -> "list[str]":
        if not self._manage_schema:
            return []
        create_table = self._create_table_sql() if include_expiration else self._create_initial_table_sql()
        if type(self).table_storage_parameters:
            create_table = f"{create_table} WITH (fillfactor = 80)"
        statements = [create_table, *self._create_index_statements(include_expiration=include_expiration)]
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

    def _create_index_statements(self, *, include_expiration: "bool" = True) -> "list[str]":
        table_name = self._quoted_table_name()
        return [
            (
                f"CREATE INDEX IF NOT EXISTS {self._quoted_index_name('pending')} "
                f"ON {table_name} ({self._quoted_col('queue')}, {self._quoted_col('execution_backend')}, "
                f"{self._quoted_col('priority')} DESC, {self._quoted_col('queued_at')}, "
                f"{self._quoted_col('created_at')}) "
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

    def interruptions_expression(self) -> "str":
        """Return the JSONB accessor for the record's shutdown-interruption count."""
        metadata = self._quoted_col("metadata_json")
        if "metadata_json" not in self._native_json_columns:
            metadata = f"({metadata})::JSONB"
        return f"COALESCE(({metadata} ->> 'interruptions')::INT, 0)"

    def _json_type(self) -> "str":
        return "JSONB"

    def _timestamp_type(self) -> "str":
        return "TIMESTAMPTZ"

    def _bulk_metadata_merge_expression(self, *, target_metadata: "str", source_metadata: "str") -> "str":
        return f"CASE WHEN {source_metadata} IS NULL THEN {target_metadata} ELSE {target_metadata} || {source_metadata} END"


class CockroachQueueStore(PostgresQueueStore):
    """Cockroach-family queue store with Postgres-compatible DDL."""

    __slots__ = ()

    data_dictionary_dialect: "ClassVar[str | None]" = "cockroachdb"
    table_storage_parameters: "ClassVar[bool]" = False
    supports_returning_claim: "ClassVar[bool]" = False
    supports_combined_expiry_claim: "ClassVar[bool]" = False

    @property
    def supports_skip_locked(self) -> "bool":
        """Cockroach claim transactions stay on the portable CAS path."""
        return False


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
        return self._create_statements(include_expiration=True)

    def _create_statements(self, *, include_expiration: "bool") -> "list[str]":
        if not self._manage_schema:
            return []
        return [self._create_mysql_table_statement(include_expiration=include_expiration)]

    def drop_statements(self) -> "list[str]":
        """Return statements that drop MySQL-family queue artifacts."""
        if not self._manage_schema:
            return []
        return [self._to_sql(sql.drop_table(self.table_name).if_exists())]

    def _create_mysql_table_statement(self, *, include_expiration: "bool" = True) -> "str":
        expiration_column = f"{self._quoted_col('expires_at')} {self._timestamp_type()}," if include_expiration else ""
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
            {self._quoted_col("worker_id")} {self._indexed_text_type()},
            {self._quoted_col("status")} {self._indexed_text_type()} NOT NULL,
            {self._quoted_col("priority")} {self._integer_type()} NOT NULL,
            {self._quoted_col("max_retries")} {self._integer_type()} NOT NULL,
            {self._quoted_col("retry_count")} {self._integer_type()} NOT NULL,
            {self._quoted_col("scheduled_at")} {self._timestamp_type()},
            {expiration_column}
            {self._quoted_col("created_at")} {self._timestamp_type()} NOT NULL,
            {self._quoted_col("queued_at")} {self._timestamp_type()} NOT NULL,
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
                {self._quoted_col("priority")}, {self._quoted_col("queued_at")}, {self._quoted_col("created_at")}
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

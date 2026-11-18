"""Shared SQLSpec queue store implementations, one per SQL dialect."""

import math
import re
from collections.abc import Mapping
from hashlib import sha256
from typing import Any, ClassVar, Literal

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError
from sqlspec import sql
from sqlspec.utils.text import split_qualified_identifier

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("CockroachQueueStore", "MssqlQueueStore", "MySQLQueueStore", "PostgresQueueStore")

_NVARCHAR_MAX_THRESHOLD = 4000
_POSTGRES_IDENTIFIER_BYTES = 63
_COCKROACH_TTL_TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled", "expired"})
_COCKROACH_TTL_ENABLED_RE = re.compile(r"\bttl\s*=\s*'on'", re.IGNORECASE)
_COCKROACH_TTL_EXPR_RE = re.compile(r"ttl_expiration_expression\s*=\s*'((?:[^']|'')*)'", re.IGNORECASE)
_COCKROACH_INTERVAL_RE = re.compile(
    r"(?:INTERVAL\s*'([^']+)'|'([^']+)'\s*::\s*INTERVAL\b|CAST\s*\(\s*'([^']+)'\s+AS\s+INTERVAL\s*\))", re.IGNORECASE
)
_COCKROACH_CLOCK_INTERVAL_RE = re.compile(r"^(?:(\d+)\s*days?,?\s+)?(\d+):(\d{2}):(\d{2})(?:\.0+)?$", re.IGNORECASE)
_COCKROACH_UNIT_TOKEN_RE = re.compile(
    r"(\d+)\s*(days?|d|hours?|hrs?|hr|h|minutes?|mins?|min|m|seconds?|secs?|sec|s)\b", re.IGNORECASE
)
_COCKROACH_UNIT_SECONDS = {
    "d": 86400,
    "day": 86400,
    "days": 86400,
    "h": 3600,
    "hr": 3600,
    "hrs": 3600,
    "hour": 3600,
    "hours": 3600,
    "m": 60,
    "min": 60,
    "mins": 60,
    "minute": 60,
    "minutes": 60,
    "s": 1,
    "sec": 1,
    "secs": 1,
    "second": 1,
    "seconds": 1,
}
_MSSQL_INDEX_COLUMNS: "dict[str, tuple[tuple[str, str], ...]]" = {
    "pending": (
        ("status", ""),
        ("queue", ""),
        ("execution_backend", ""),
        ("scheduled_at", ""),
        ("priority", ""),
        ("queued_at", ""),
        ("created_at", ""),
    ),
    "heartbeat": (("status", ""), ("heartbeat_at", "")),
    "created": (("created_at", " DESC"), ("id", " DESC")),
    "list": (("status", ""), ("queue", ""), ("created_at", " DESC"), ("id", " DESC")),
}


class MssqlQueueStore(SQLSpecQueueStore):
    """SQL Server queue store with shared T-SQL DDL."""

    __slots__ = ()

    data_dictionary_dialect: "ClassVar[str | None]" = "mssql"
    identifier_quote_style: 'ClassVar[Literal["double", "backtick", "none"]]' = "none"

    def create_table_statement(self) -> "str":
        """Return the SQL Server table-only ``CREATE TABLE`` statement."""
        if not self._manage_schema:
            return ""
        return self._create_mssql_table_statement(include_expiration=True)

    def companion_indexes(self) -> "tuple[tuple[str, str], ...]":
        """Return SQL Server companion index ``(index_name, create_sql)`` pairs."""
        if not self._manage_schema:
            return ()
        return self._mssql_index_specs()

    def create_statements(self) -> "list[str]":
        """Return statements that create SQL Server queue artifacts."""
        return self._create_statements(include_expiration=True)

    def drop_statements(self) -> "list[str]":
        """Return statements that drop SQL Server queue artifacts."""
        if not self._manage_schema:
            return []
        return [
            self._drop_mssql_index_statement("task_key"),
            self._drop_mssql_index_statement("list"),
            self._drop_mssql_index_statement("created"),
            self._drop_mssql_index_statement("heartbeat"),
            self._drop_mssql_index_statement("pending"),
            self._drop_mssql_table_statement(),
        ]

    def _create_statements(self, *, include_expiration: "bool") -> "list[str]":
        if not self._manage_schema:
            return []
        return [
            self._create_mssql_table_statement(include_expiration=include_expiration),
            *(stmt for _, stmt in self._mssql_index_specs()),
        ]

    def _mssql_index_specs(self) -> "tuple[tuple[str, str], ...]":
        dispatch_columns = ", ".join(
            self._quoted_col(c) for c in ("execution_backend", "status", "dispatch_checked_at", "created_at", "id")
        )
        return (
            (self._index_name("task_key"), self._create_mssql_unique_task_key_index_statement()),
            (self._index_name("pending"), self._create_mssql_index_statement("pending")),
            (self._index_name("heartbeat"), self._create_mssql_index_statement("heartbeat")),
            (
                self._index_name("dispatch_repair"),
                self._create_mssql_index_statement_sql("dispatch_repair", dispatch_columns),
            ),
            (self._index_name("created"), self._create_mssql_index_statement("created")),
            (self._index_name("list"), self._create_mssql_index_statement("list")),
        )

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
                {self._quoted_col("dispatch_checked_at")} {self._dispatch_checked_type()},
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
        spec = _MSSQL_INDEX_COLUMNS.get(suffix)
        if spec is None:
            msg = f"Unsupported SQL Server index suffix: {suffix}"
            raise ValueError(msg)
        columns = ", ".join(f"{self._quoted_col(canonical)}{order}" for canonical, order in spec)
        return self._create_mssql_index_statement_sql(suffix, columns)

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

    def claim_batch_with_expired_returning_sql(
        self, *, queue_count: "int", filter_execution_backend: "bool", cap_count: "int" = 0
    ) -> "str":
        """Return one PostgreSQL statement that expires and claims due records."""
        cache_key = f"claim_batch_with_expired:{queue_count}:{int(filter_execution_backend)}:{cap_count}"

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
            if cap_count:
                candidates_sql = (
                    f"{self._capped_candidate_ctes_sql(cap_count=cap_count, where=claim_where)}"  # noqa: S608
                    f"claim_candidates AS (SELECT {id_col} FROM {table} "
                    f"WHERE {id_col} IN (SELECT {id_col} FROM picked) AND {claim_where} "
                    f"FOR UPDATE SKIP LOCKED), "
                )
            else:
                candidates_sql = (
                    f"claim_candidates AS (SELECT {id_col} FROM {table} WHERE {claim_where} "  # noqa: S608
                    f"ORDER BY {priority_col} DESC, {queued_col} ASC, {created_col} ASC, {id_col} ASC "
                    f"FOR UPDATE SKIP LOCKED LIMIT :limit), "
                )
            return (
                f"WITH expired_candidates AS (SELECT {id_col} FROM {table} "  # noqa: S608
                f"WHERE {status_col} IN ('pending', 'scheduled') AND {execution_ref_col} IS NULL "
                f"AND {expires_col} IS NOT NULL AND {expires_col} <= :expires_now "
                f"ORDER BY {expires_col} ASC, {created_col} ASC, {id_col} ASC FOR UPDATE SKIP LOCKED), "
                f"expired AS (UPDATE {table} AS t SET {status_col} = 'expired', "
                f"{self._quoted_col('completed_at')} = :completed_at, {self._quoted_col('heartbeat_at')} = NULL "
                f"FROM expired_candidates AS e WHERE t.{id_col} = e.{id_col} RETURNING {returned}), "
                f"{candidates_sql}"
                f"claimed AS (UPDATE {table} AS t SET {status_col} = 'running', "
                f"{self._quoted_col('started_at')} = :started_at, {self._quoted_col('heartbeat_at')} = :heartbeat_at "
                f"FROM claim_candidates AS c WHERE t.{id_col} = c.{id_col} RETURNING {returned}) "
                f"SELECT 'expired' AS \"_claim_outcome\", expired.* FROM expired "
                f"UNION ALL SELECT 'claimed' AS \"_claim_outcome\", claimed.* FROM claimed"
            )

        return self._cached(cache_key, build)

    def create_table_statement(self) -> "str":
        """Return the Postgres-family table-only ``CREATE TABLE`` statement."""
        if not self._manage_schema:
            return ""
        return f"{self._create_table_sql()}{self._table_storage_clause()}"

    def companion_indexes(self) -> "tuple[tuple[str, str], ...]":
        """Return Postgres-family companion index ``(index_name, create_sql)`` pairs."""
        if not self._manage_schema:
            return ()
        return self._postgres_index_specs()

    def reconcile_storage_options_sync(self, driver: "Any") -> "None":
        """Apply Postgres autovacuum table storage parameters on a synchronous driver."""
        if not self._manage_schema:
            return
        for statement in self._storage_option_statements():
            driver.execute(statement)

    async def reconcile_storage_options_async(self, driver: "Any") -> "None":
        """Apply Postgres autovacuum table storage parameters on an asynchronous driver."""
        if not self._manage_schema:
            return
        for statement in self._storage_option_statements():
            await driver.execute(statement)

    def create_statements(self) -> "list[str]":
        """Return statements that create Postgres-family queue artifacts."""
        return self._create_statements(include_expiration=True)

    def drop_statements(self) -> "list[str]":
        """Return statements that drop Postgres-family queue artifacts."""
        if not self._manage_schema:
            return []
        return [
            self._to_sql(sql.drop_index(self._index_name("list")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("created")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("heartbeat")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("scheduled")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("pending")).if_exists()),
            self._to_sql(sql.drop_table(self.table_name).if_exists()),
        ]

    def interruptions_expression(self) -> "str":
        """Return the JSONB accessor for the record's shutdown-interruption count."""
        metadata = self._quoted_col("metadata_json")
        if "metadata_json" not in self._native_json_columns:
            metadata = f"({metadata})::JSONB"
        return f"COALESCE(({metadata} ->> 'interruptions')::INT, 0)"

    def _create_statements(self, *, include_expiration: "bool") -> "list[str]":
        if not self._manage_schema:
            return []
        base_table = self._create_table_sql() if include_expiration else self._create_initial_table_sql()
        create_table = f"{base_table}{self._table_storage_clause()}"
        return [create_table, *(stmt for _, stmt in self._postgres_index_specs()), *self._storage_option_statements()]

    def _storage_option_statements(self) -> "list[str]":
        if not type(self).table_storage_parameters:
            return []
        return [
            (
                f"ALTER TABLE {self._quoted_table_name()} SET ("
                "autovacuum_vacuum_scale_factor = 0.05, "
                "autovacuum_analyze_scale_factor = 0.02)"
            )
        ]

    def _table_storage_clause(self) -> "str":
        if type(self).table_storage_parameters:
            return " WITH (fillfactor = 80)"
        return ""

    def _index_name(self, suffix: "str") -> "str":
        name = f"ix_{self.table_name.replace('.', '_')}_{suffix}"
        encoded = name.encode("utf-8")
        if len(encoded) <= _POSTGRES_IDENTIFIER_BYTES:
            return name
        prefix = encoded[: _POSTGRES_IDENTIFIER_BYTES - 9].decode("utf-8", errors="ignore")
        return f"{prefix}_{sha256(encoded).hexdigest()[:8]}"

    def _postgres_index_specs(self) -> "tuple[tuple[str, str], ...]":
        table_name = self._quoted_table_name()
        return (
            (
                self.dispatch_repair_index_name,
                self.dispatch_repair_index_sql().replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ", 1),
            ),
            (
                self._index_name("pending"),
                (
                    f"CREATE INDEX IF NOT EXISTS {self._quoted_index_name('pending')} "
                    f"ON {table_name} ({self._quoted_col('queue')}, {self._quoted_col('execution_backend')}, "
                    f"{self._quoted_col('priority')} DESC, {self._quoted_col('queued_at')}, "
                    f"{self._quoted_col('created_at')}) "
                    f"WHERE {self._quoted_col('status')} IN ('pending', 'scheduled')"
                ),
            ),
            (
                self._index_name("scheduled"),
                (
                    f"CREATE INDEX IF NOT EXISTS {self._quoted_index_name('scheduled')} "
                    f"ON {table_name} ({self._quoted_col('scheduled_at')}) "
                    f"WHERE {self._quoted_col('status')} = 'scheduled'"
                ),
            ),
            (
                self._index_name("heartbeat"),
                (
                    f"CREATE INDEX IF NOT EXISTS {self._quoted_index_name('heartbeat')} "
                    f"ON {table_name} ({self._quoted_col('heartbeat_at')}) "
                    f"WHERE {self._quoted_col('status')} = 'running'"
                ),
            ),
            (
                self._index_name("created"),
                (
                    f"CREATE INDEX IF NOT EXISTS {self._quoted_index_name('created')} "
                    f"ON {table_name} ({self._quoted_col('created_at')} DESC, {self._quoted_col('id')} DESC)"
                ),
            ),
            (
                self._index_name("list"),
                (
                    f"CREATE INDEX IF NOT EXISTS {self._quoted_index_name('list')} "
                    f"ON {table_name} ({self._quoted_col('status')}, {self._quoted_col('queue')}, "
                    f"{self._quoted_col('created_at')} DESC, {self._quoted_col('id')} DESC)"
                ),
            ),
        )

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

    def reconcile_storage_options_sync(self, driver: "Any") -> "None":
        """Reconcile CockroachDB row-level TTL storage options on a synchronous driver."""
        if not self._manage_schema:
            return
        row = driver.select_one_or_none(f"SHOW CREATE TABLE {self._quoted_table_name()}")
        alter_sql = self._cockroach_ttl_alter_statement(self._extract_create_statement(row))
        if alter_sql is not None:
            driver.execute(alter_sql)

    async def reconcile_storage_options_async(self, driver: "Any") -> "None":
        """Reconcile CockroachDB row-level TTL storage options on an asynchronous driver."""
        if not self._manage_schema:
            return
        row = await driver.select_one_or_none(f"SHOW CREATE TABLE {self._quoted_table_name()}")
        alter_sql = self._cockroach_ttl_alter_statement(self._extract_create_statement(row))
        if alter_sql is not None:
            await driver.execute(alter_sql)

    def _table_storage_clause(self) -> "str":
        options = self._ttl_storage_options()
        if options is None:
            return ""
        return f" WITH ({options})"

    def _ttl_expiration_expression(self) -> "tuple[int, str]":
        if self._terminal_ttl is None:
            msg = "CockroachDB TTL expiration expression requires terminal_ttl."
            raise ValueError(msg)
        ttl_seconds = max(1, math.ceil(self._terminal_ttl))
        status_col = self._quoted_col("status")
        completed_col = self._quoted_col("completed_at")
        expr = (
            f"CASE WHEN {status_col} IN ('completed', 'failed', 'cancelled', 'expired') "
            f"AND {completed_col} IS NOT NULL "
            f"THEN {completed_col} + INTERVAL '{ttl_seconds} seconds' "
            "ELSE NULL END"
        )
        return ttl_seconds, expr

    def _ttl_storage_options(self) -> "str | None":
        if self._terminal_ttl is None:
            return None
        _, expr = self._ttl_expiration_expression()
        escaped = expr.replace("'", "''")
        return f"ttl = 'on', ttl_expiration_expression = '{escaped}'"

    def _cockroach_ttl_alter_statement(self, create_statement: "str | None") -> "str | None":
        ttl_enabled, live_expr = self._parse_cockroach_ttl_state(create_statement)
        if self._terminal_ttl is None:
            if ttl_enabled or live_expr is not None:
                return f"ALTER TABLE {self._quoted_table_name()} RESET (ttl)"
            return None
        desired_seconds, _ = self._ttl_expiration_expression()
        if ttl_enabled and self._cockroach_ttl_matches(
            live_expr,
            desired_seconds=desired_seconds,
            raw_status_col=self._col("status"),
            raw_completed_col=self._col("completed_at"),
        ):
            return None
        options = self._ttl_storage_options()
        return f"ALTER TABLE {self._quoted_table_name()} SET ({options})"

    def _index_name(self, suffix: "str") -> "str":
        if suffix in {"created", "list"}:
            return PostgresQueueStore._index_name(self, suffix)
        return SQLSpecQueueStore._index_name(self, suffix)

    @staticmethod
    def _extract_create_statement(row: "Any") -> "str | None":
        if row is None:
            return None
        value: "Any" = None
        if isinstance(row, Mapping):
            for key, item in row.items():
                if isinstance(key, str) and key.lower() == "create_statement":
                    value = item
                    break
        else:
            value = getattr(row, "create_statement", None)
            if value is None and hasattr(row, "__dict__"):
                for key, item in vars(row).items():
                    if key.lower() == "create_statement":
                        value = item
                        break
            if value is None:
                value = getattr(row, "CREATE_STATEMENT", None)
        return str(value) if value is not None else None

    @staticmethod
    def _parse_cockroach_ttl_state(create_statement: "str | None") -> "tuple[bool, str | None]":
        if not create_statement:
            return False, None
        ttl_enabled = bool(_COCKROACH_TTL_ENABLED_RE.search(create_statement))
        expr_match = _COCKROACH_TTL_EXPR_RE.search(create_statement)
        live_expr = expr_match.group(1).replace("''", "'") if expr_match is not None else None
        return ttl_enabled, live_expr

    @staticmethod
    def _parse_cockroach_interval_seconds(expr: "str") -> "int | None":
        interval_match = _COCKROACH_INTERVAL_RE.search(expr)
        if interval_match is None:
            return None
        body = next((group for group in interval_match.groups() if group is not None), "").strip()
        if not body:
            return None
        clock_match = _COCKROACH_CLOCK_INTERVAL_RE.match(body)
        if clock_match is not None:
            days = int(clock_match.group(1) or 0)
            hours = int(clock_match.group(2))
            minutes = int(clock_match.group(3))
            seconds = int(clock_match.group(4))
            return days * 86400 + hours * 3600 + minutes * 60 + seconds
        unit_matches = list(_COCKROACH_UNIT_TOKEN_RE.finditer(body))
        if not unit_matches:
            return None
        leftover = _COCKROACH_UNIT_TOKEN_RE.sub("", body).replace(",", "").strip()
        if leftover:
            return None
        return sum(int(match.group(1)) * _COCKROACH_UNIT_SECONDS[match.group(2).lower()] for match in unit_matches)

    @staticmethod
    def _unwrap_parens(node: "exp.Expr | None") -> "exp.Expr | None":
        while isinstance(node, exp.Paren):
            node = node.this
        return node

    @staticmethod
    def _cockroach_column_matches(node: "exp.Expr | None", expected: "str") -> "bool":
        unwrapped = CockroachQueueStore._unwrap_parens(node)
        if not isinstance(unwrapped, exp.Column) or unwrapped.table:
            return False
        identifier = unwrapped.this
        if not isinstance(identifier, exp.Identifier):
            return False
        name = identifier.name
        if identifier.quoted:
            return name == expected
        return name.lower() == expected if expected.islower() else name == expected

    @staticmethod
    def _is_not_null_check(node: "exp.Expr | None", expected_col: "str") -> "bool":
        unwrapped = CockroachQueueStore._unwrap_parens(node)
        if (
            isinstance(unwrapped, exp.Is)
            and isinstance(unwrapped.expression, exp.Null)
            and unwrapped.args.get("negate")
        ):
            return CockroachQueueStore._cockroach_column_matches(unwrapped.this, expected_col)
        if isinstance(unwrapped, exp.Not):
            inner = CockroachQueueStore._unwrap_parens(unwrapped.this)
            if isinstance(inner, exp.Is) and isinstance(inner.expression, exp.Null) and not inner.args.get("negate"):
                return CockroachQueueStore._cockroach_column_matches(inner.this, expected_col)
        return False

    @staticmethod
    def _is_terminal_status_check(node: "exp.Expr | None", expected_col: "str") -> "bool":
        unwrapped = CockroachQueueStore._unwrap_parens(node)
        if not isinstance(unwrapped, exp.In):
            return False
        if not CockroachQueueStore._cockroach_column_matches(unwrapped.this, expected_col):
            return False
        expressions = unwrapped.expressions
        if len(expressions) != len(_COCKROACH_TTL_TERMINAL_STATUSES):
            return False
        statuses = {item.name for item in expressions if isinstance(item, exp.Literal) and item.is_string}
        return statuses == _COCKROACH_TTL_TERMINAL_STATUSES

    @staticmethod
    def _is_interval_expression(node: "exp.Expr | None") -> "bool":
        unwrapped = CockroachQueueStore._unwrap_parens(node)
        if isinstance(unwrapped, exp.Interval):
            return True
        if isinstance(unwrapped, exp.Cast):
            to_type = unwrapped.to
            return isinstance(to_type, exp.DataType) and bool(to_type.this == exp.DataType.Type.INTERVAL)
        return False

    @staticmethod
    def _cockroach_ttl_matches(
        live_expr: "str | None", *, desired_seconds: "int", raw_status_col: "str", raw_completed_col: "str"
    ) -> "bool":
        if not live_expr:
            return False
        if CockroachQueueStore._parse_cockroach_interval_seconds(live_expr) != desired_seconds:
            return False
        try:
            parsed = CockroachQueueStore._unwrap_parens(sqlglot.parse_one(live_expr, read="postgres"))
        except ParseError:
            return False
        if not isinstance(parsed, exp.Case) or not isinstance(parsed.args.get("default"), exp.Null):
            return False
        ifs = parsed.args.get("ifs")
        if not isinstance(ifs, list) or len(ifs) != 1:
            return False
        branch = ifs[0]
        if not isinstance(branch, exp.If):
            return False
        condition = CockroachQueueStore._unwrap_parens(branch.this)
        if not isinstance(condition, exp.And):
            return False
        left, right = condition.this, condition.expression
        has_predicate = (
            CockroachQueueStore._is_terminal_status_check(left, raw_status_col)
            and CockroachQueueStore._is_not_null_check(right, raw_completed_col)
        ) or (
            CockroachQueueStore._is_terminal_status_check(right, raw_status_col)
            and CockroachQueueStore._is_not_null_check(left, raw_completed_col)
        )
        if not has_predicate:
            return False
        then_expr = CockroachQueueStore._unwrap_parens(branch.args.get("true"))
        if not isinstance(then_expr, exp.Add):
            return False
        return (
            CockroachQueueStore._cockroach_column_matches(then_expr.this, raw_completed_col)
            and CockroachQueueStore._is_interval_expression(then_expr.expression)
        ) or (
            CockroachQueueStore._cockroach_column_matches(then_expr.expression, raw_completed_col)
            and CockroachQueueStore._is_interval_expression(then_expr.this)
        )


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

    def create_table_statement(self) -> "str":
        """Return the MySQL table-only ``CREATE TABLE`` statement without inline indexes."""
        if not self._manage_schema:
            return ""
        return self._create_mysql_table_statement(include_expiration=True, include_indexes=False)

    def companion_indexes(self) -> "tuple[tuple[str, str], ...]":
        """Return MySQL companion ``ALTER TABLE ... ADD INDEX`` pairs."""
        if not self._manage_schema:
            return ()
        table_name = self._quoted_table_name()
        return tuple(
            (
                self._index_name(suffix),
                f"ALTER TABLE {table_name} ADD INDEX {self._quoted_index_name(suffix)} ({columns})",
            )
            for suffix, columns in self._mysql_index_definitions()
        )

    def create_statements(self) -> "list[str]":
        """Return statements that create MySQL-family queue artifacts."""
        return self._create_statements(include_expiration=True)

    def drop_statements(self) -> "list[str]":
        """Return statements that drop MySQL-family queue artifacts."""
        if not self._manage_schema:
            return []
        return [self._to_sql(sql.drop_table(self.table_name).if_exists())]

    def _create_statements(self, *, include_expiration: "bool") -> "list[str]":
        if not self._manage_schema:
            return []
        return [self._create_mysql_table_statement(include_expiration=include_expiration, include_indexes=True)]

    def _mysql_index_definitions(self) -> "tuple[tuple[str, str], ...]":
        return (
            (
                "pending",
                (
                    f"{self._prefixed_col('status', 32)}, {self._prefixed_col('queue', 191)}, "
                    f"{self._prefixed_col('execution_backend', 191)}, {self._quoted_col('scheduled_at')}, "
                    f"{self._quoted_col('priority')}, {self._quoted_col('queued_at')}, "
                    f"{self._quoted_col('created_at')}"
                ),
            ),
            (
                "dispatch_repair",
                (
                    f"{self._quoted_col('execution_backend')}, {self._quoted_col('status')}, "
                    f"{self._quoted_col('dispatch_checked_at')}, {self._quoted_col('created_at')}, "
                    f"{self._quoted_col('id')}"
                ),
            ),
            ("heartbeat", f"{self._prefixed_col('status', 32)}, {self._quoted_col('heartbeat_at')}"),
            ("created", f"{self._quoted_col('created_at')}, {self._quoted_col('id')}"),
            (
                "list",
                (
                    f"{self._prefixed_col('status', 32)}, {self._prefixed_col('queue', 191)}, "
                    f"{self._quoted_col('created_at')}, {self._quoted_col('id')}"
                ),
            ),
        )

    def _create_mysql_table_statement(
        self, *, include_expiration: "bool" = True, include_indexes: "bool" = True
    ) -> "str":
        expiration_column = f"{self._quoted_col('expires_at')} {self._timestamp_type()}," if include_expiration else ""
        inline_indexes = ""
        if include_indexes:
            index_clauses = ",\n            ".join(
                f"INDEX {self._quoted_index_name(suffix)} ({columns})"
                for suffix, columns in self._mysql_index_definitions()
            )
            inline_indexes = f",\n            {index_clauses}"
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
            {self._quoted_col("dispatch_checked_at")} {self._dispatch_checked_type()},
            {self._quoted_col("result_json")} {self._result_json_type("result_json")} NOT NULL,
            {self._quoted_col("error")} {self._error_type()},
            {self._quoted_col("task_key")} {self._indexed_text_type()} UNIQUE,
            {self._quoted_col("metadata_json")} {self._metadata_json_type("metadata_json")} NOT NULL{inline_indexes}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

    def _prefixed_col(self, canonical: "str", length: "int") -> "str":
        return f"{self._quoted_col(canonical)}({length})"

    def _timestamp_type(self) -> "str":
        return "DATETIME(6)"


def _quote_tsql_identifier(identifier: "str") -> "str":
    return f"[{identifier.replace(']', ']]')}]"

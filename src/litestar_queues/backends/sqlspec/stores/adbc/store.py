"""adbc SQLSpec queue store."""

from typing import Any, cast

from sqlspec import sql

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("AdbcQueueStore",)

_ADBC_DIALECT_BIGQUERY = "bigquery"
_ADBC_DIALECT_DUCKDB = "duckdb"
_ADBC_DIALECT_FLIGHTSQL = "flightsql"
_ADBC_DIALECT_POSTGRES = "postgres"
_ADBC_DIALECT_SNOWFLAKE = "snowflake"
_ADBC_DIALECT_SQLITE = "sqlite"


class AdbcQueueStore(SQLSpecQueueStore):
    """adbc SQLSpec queue statement store with dialect-sensitive DDL."""

    __slots__ = ("_dialect",)

    def __init__(self, config: Any, *, table_name: str | None = None) -> None:
        super().__init__(config, table_name=table_name)
        self._dialect: str | None = None

    @property
    def adbc_dialect(self) -> str:
        """Return the detected ADBC database dialect."""
        if self._dialect is None:
            self._dialect = self._detect_dialect()
        return self._dialect

    def create_statements(self) -> list[str]:
        """Return statements that create adbc queue artifacts."""
        if self.adbc_dialect == _ADBC_DIALECT_BIGQUERY:
            return [self._bigquery_create_table_statement()]
        if self.adbc_dialect == _ADBC_DIALECT_SNOWFLAKE:
            return [self._snowflake_create_table_statement()]
        if self.adbc_dialect == _ADBC_DIALECT_POSTGRES:
            return [
                self._to_sql(self._create_table_statement()),
                *self._postgres_index_statements(),
            ]
        return super().create_statements()

    def drop_statements(self) -> list[str]:
        """Return statements that drop adbc queue artifacts."""
        if self.adbc_dialect in {_ADBC_DIALECT_BIGQUERY, _ADBC_DIALECT_SNOWFLAKE}:
            return [f"DROP TABLE IF EXISTS {self.table_name}"]
        if self.adbc_dialect == _ADBC_DIALECT_POSTGRES:
            return [
                self._to_sql(sql.drop_index(self._index_name("heartbeat")).if_exists()),
                self._to_sql(sql.drop_index(self._index_name("scheduled")).if_exists()),
                self._to_sql(sql.drop_index(self._index_name("pending")).if_exists()),
                self._to_sql(sql.drop_table(self.table_name).if_exists()),
            ]
        return super().drop_statements()

    def _create_index_statements(self) -> list[str]:
        if self.adbc_dialect == _ADBC_DIALECT_POSTGRES:
            return self._postgres_index_statements()
        if self.adbc_dialect in {_ADBC_DIALECT_BIGQUERY, _ADBC_DIALECT_SNOWFLAKE}:
            return []
        return super()._create_index_statements()

    def _id_type(self) -> str:
        if self.adbc_dialect == _ADBC_DIALECT_BIGQUERY:
            return "STRING"
        if self.adbc_dialect == _ADBC_DIALECT_DUCKDB:
            return "VARCHAR"
        return super()._id_type()

    def _indexed_text_type(self) -> str:
        if self.adbc_dialect == _ADBC_DIALECT_BIGQUERY:
            return "STRING"
        if self.adbc_dialect == _ADBC_DIALECT_DUCKDB:
            return "VARCHAR"
        return super()._indexed_text_type()

    def _integer_type(self) -> str:
        if self.adbc_dialect == _ADBC_DIALECT_BIGQUERY:
            return "INT64"
        return super()._integer_type()

    def _json_type(self) -> str:
        if self.adbc_dialect == _ADBC_DIALECT_POSTGRES:
            return "JSONB"
        if self.adbc_dialect in {_ADBC_DIALECT_BIGQUERY, _ADBC_DIALECT_DUCKDB}:
            return "JSON"
        return super()._json_type()

    def _timestamp_type(self) -> str:
        if self.adbc_dialect == _ADBC_DIALECT_POSTGRES:
            return "TIMESTAMPTZ"
        if self.adbc_dialect in {_ADBC_DIALECT_BIGQUERY, _ADBC_DIALECT_DUCKDB}:
            return "TIMESTAMP"
        return super()._timestamp_type()

    def _postgres_index_statements(self) -> list[str]:
        table_name = self.table_name
        return [
            (
                f"CREATE INDEX IF NOT EXISTS {self._index_name('pending')} "
                f"ON {table_name} (queue, execution_backend, priority DESC, created_at) "
                "WHERE status IN ('pending', 'scheduled')"
            ),
            (
                f"CREATE INDEX IF NOT EXISTS {self._index_name('scheduled')} "
                f"ON {table_name} (scheduled_at) WHERE status = 'scheduled'"
            ),
            (
                f"CREATE INDEX IF NOT EXISTS {self._index_name('heartbeat')} "
                f"ON {table_name} (heartbeat_at) WHERE status = 'running'"
            ),
        ]

    def _bigquery_create_table_statement(self) -> str:
        table_name = self.table_name
        return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id STRING NOT NULL,
            task_name STRING NOT NULL,
            args_json {self._payload_json_type("args_json")} NOT NULL,
            kwargs_json {self._payload_json_type("kwargs_json")} NOT NULL,
            queue STRING NOT NULL,
            execution_backend STRING NOT NULL,
            execution_profile STRING,
            execution_ref STRING,
            status STRING NOT NULL,
            priority INT64 NOT NULL,
            max_retries INT64 NOT NULL,
            retry_count INT64 NOT NULL,
            scheduled_at TIMESTAMP,
            created_at TIMESTAMP NOT NULL,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            heartbeat_at TIMESTAMP,
            result_json {self._result_json_type("result_json")} NOT NULL,
            error STRING,
            task_key STRING,
            metadata_json {self._metadata_json_type("metadata_json")} NOT NULL
        )
        CLUSTER BY status, queue, execution_backend, scheduled_at
        """

    def _snowflake_create_table_statement(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id VARCHAR(64) PRIMARY KEY,
            task_name VARCHAR(255) NOT NULL,
            args_json VARIANT NOT NULL,
            kwargs_json VARIANT NOT NULL,
            queue VARCHAR(255) NOT NULL,
            execution_backend VARCHAR(255) NOT NULL,
            execution_profile VARCHAR(255),
            execution_ref VARCHAR(255),
            status VARCHAR(255) NOT NULL,
            priority INTEGER NOT NULL,
            max_retries INTEGER NOT NULL,
            retry_count INTEGER NOT NULL,
            scheduled_at TIMESTAMP_TZ,
            created_at TIMESTAMP_TZ NOT NULL,
            started_at TIMESTAMP_TZ,
            completed_at TIMESTAMP_TZ,
            heartbeat_at TIMESTAMP_TZ,
            result_json VARIANT NOT NULL,
            error VARCHAR,
            task_key VARCHAR(255) UNIQUE,
            metadata_json VARIANT NOT NULL
        )
        """

    def _detect_dialect(self) -> str:
        connection_config = cast("dict[str, Any]", getattr(self._config, "connection_config", {}) or {})
        driver_name = str(connection_config.get("driver_name", "")).lower()
        uri = str(connection_config.get("uri", "")).lower()

        if "postgres" in driver_name or uri.startswith(("postgres://", "postgresql://")):
            return _ADBC_DIALECT_POSTGRES
        if "duckdb" in driver_name or uri.startswith("duckdb://"):
            return _ADBC_DIALECT_DUCKDB
        if (
            "gizmosql" in driver_name
            or "gizmo" in driver_name
            or uri.startswith(("gizmosql://", "gizmo://", "grpc+tls://"))
        ):
            return _ADBC_DIALECT_DUCKDB
        if "bigquery" in driver_name or uri.startswith("bigquery://"):
            return _ADBC_DIALECT_BIGQUERY
        if "snowflake" in driver_name or uri.startswith("snowflake://"):
            return _ADBC_DIALECT_SNOWFLAKE
        if "flightsql" in driver_name or "grpc" in driver_name or uri.startswith("grpc://"):
            return _ADBC_DIALECT_FLIGHTSQL
        if "sqlite" in driver_name or uri.startswith("sqlite://"):
            return _ADBC_DIALECT_SQLITE

        statement_config = getattr(self._config, "statement_config", None)
        dialect = getattr(statement_config, "dialect", None)
        if dialect is not None:
            dialect_name = str(dialect).lower()
            if dialect_name in {
                _ADBC_DIALECT_BIGQUERY,
                _ADBC_DIALECT_DUCKDB,
                _ADBC_DIALECT_POSTGRES,
                _ADBC_DIALECT_SNOWFLAKE,
                _ADBC_DIALECT_SQLITE,
            }:
                return dialect_name

        return _ADBC_DIALECT_SQLITE

"""arrow-odbc SQLSpec queue store."""

from typing import Any, cast

from sqlspec.utils.text import quote_backtick_identifier, split_qualified_identifier

from litestar_queues.backends.sqlspec.stores._families import MySQLQueueStore, PostgresQueueStore, SQLServerQueueStore
from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("ArrowOdbcQueueStore",)

_ARROW_ODBC_MSSQL = "mssql"
_ARROW_ODBC_MYSQL = "mysql"
_ARROW_ODBC_POSTGRES = "postgres"
_ARROW_ODBC_SQLITE = "sqlite"
_ARROW_ODBC_DUCKDB = "duckdb"
_ARROW_ODBC_BIGQUERY = "bigquery"
_ARROW_ODBC_SNOWFLAKE = "snowflake"
_ARROW_ODBC_ORACLE = "oracle"

_DIALECT_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (_ARROW_ODBC_MSSQL, ("sql server", "sqlserver", "microsoft sql", "msodbcsql")),
    (_ARROW_ODBC_ORACLE, ("oracle",)),
    (_ARROW_ODBC_MYSQL, ("mysql", "mariadb")),
    (_ARROW_ODBC_POSTGRES, ("postgres", "postgresql")),
    (_ARROW_ODBC_SQLITE, ("sqlite",)),
    (_ARROW_ODBC_DUCKDB, ("duckdb",)),
    (_ARROW_ODBC_BIGQUERY, ("bigquery",)),
    (_ARROW_ODBC_SNOWFLAKE, ("snowflake",)),
)


class ArrowOdbcQueueStore(SQLSpecQueueStore):
    """arrow-odbc SQLSpec queue statement store with runtime-dialect DDL."""

    __slots__ = ("_runtime_dialect",)

    def __init__(self, config: Any, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self._runtime_dialect: str | None = None

    @property
    def runtime_dialect(self) -> str:
        """Return the detected ODBC target dialect."""
        if self._runtime_dialect is None:
            self._runtime_dialect = self._detect_runtime_dialect()
        return self._runtime_dialect

    def create_statements(self) -> list[str]:
        """Return statements that create arrow-odbc queue artifacts."""
        if self.runtime_dialect == _ARROW_ODBC_POSTGRES:
            return PostgresQueueStore.create_statements(self)
        if self.runtime_dialect == _ARROW_ODBC_MYSQL:
            return MySQLQueueStore.create_statements(self)
        if self.runtime_dialect == _ARROW_ODBC_MSSQL:
            return SQLServerQueueStore.create_statements(self)
        return super().create_statements()

    def drop_statements(self) -> list[str]:
        """Return statements that drop arrow-odbc queue artifacts."""
        if self.runtime_dialect == _ARROW_ODBC_POSTGRES:
            return PostgresQueueStore.drop_statements(self)
        if self.runtime_dialect == _ARROW_ODBC_MYSQL:
            return MySQLQueueStore.drop_statements(self)
        if self.runtime_dialect == _ARROW_ODBC_MSSQL:
            return SQLServerQueueStore.drop_statements(self)
        return super().drop_statements()

    def _id_type(self) -> str:
        if self.runtime_dialect == _ARROW_ODBC_MYSQL:
            return "VARCHAR(64)"
        if self.runtime_dialect == _ARROW_ODBC_MSSQL:
            return "NVARCHAR(64)"
        if self.runtime_dialect in {_ARROW_ODBC_BIGQUERY, _ARROW_ODBC_DUCKDB}:
            return self._column_type(None, logical_type="text", fallback="VARCHAR")
        return super()._id_type()

    def _indexed_text_type(self) -> str:
        if self.runtime_dialect == _ARROW_ODBC_MYSQL:
            return "VARCHAR(255)"
        if self.runtime_dialect == _ARROW_ODBC_MSSQL:
            return "NVARCHAR(255)"
        if self.runtime_dialect == _ARROW_ODBC_BIGQUERY:
            return self._column_type(None, logical_type="text", fallback="STRING")
        if self.runtime_dialect == _ARROW_ODBC_DUCKDB:
            return "VARCHAR"
        return super()._indexed_text_type()

    def _integer_type(self) -> str:
        if self.runtime_dialect == _ARROW_ODBC_BIGQUERY:
            return "INT64"
        if self.runtime_dialect == _ARROW_ODBC_MSSQL:
            return "INT"
        return super()._integer_type()

    def _timestamp_type(self) -> str:
        if self.runtime_dialect in {_ARROW_ODBC_BIGQUERY, _ARROW_ODBC_DUCKDB, _ARROW_ODBC_POSTGRES}:
            return self._column_type(None, logical_type="timestamp", fallback="TIMESTAMP")
        return super()._timestamp_type()

    def _error_type(self) -> str:
        if self.runtime_dialect == _ARROW_ODBC_MYSQL:
            return "LONGTEXT"
        return super()._error_type()

    def _create_index_statements(self) -> list[str]:
        if self.runtime_dialect == _ARROW_ODBC_POSTGRES:
            return PostgresQueueStore._create_index_statements(self)
        return super()._create_index_statements()

    def _create_mysql_table_statement(self) -> str:
        return MySQLQueueStore._create_mysql_table_statement(self)

    def _prefixed_col(self, canonical: str, length: int) -> str:
        return MySQLQueueStore._prefixed_col(self, canonical, length)

    def _create_sqlserver_table_statement(self) -> str:
        return SQLServerQueueStore._create_sqlserver_table_statement(self)

    def _wrap_create_table(self, statement: str) -> str:
        return SQLServerQueueStore._wrap_create_table(self, statement)

    def _wrap_create_index(self, suffix: str, columns: str) -> str:
        return SQLServerQueueStore._wrap_create_index(self, suffix, columns)

    def _wrap_drop_index(self, suffix: str) -> str:
        return SQLServerQueueStore._wrap_drop_index(self, suffix)

    def _wrap_drop_table(self) -> str:
        return SQLServerQueueStore._wrap_drop_table(self)

    def _data_dictionary_dialect_name(self) -> str | None:
        if self.runtime_dialect in {
            _ARROW_ODBC_BIGQUERY,
            _ARROW_ODBC_DUCKDB,
            _ARROW_ODBC_MSSQL,
            _ARROW_ODBC_MYSQL,
            _ARROW_ODBC_ORACLE,
            _ARROW_ODBC_POSTGRES,
            _ARROW_ODBC_SQLITE,
        }:
            return self.runtime_dialect
        return super()._data_dictionary_dialect_name()

    def _quote_identifier(self, identifier: str) -> str:
        if self.runtime_dialect == _ARROW_ODBC_MYSQL:
            parts = split_qualified_identifier(identifier)
            if not parts:
                return quote_backtick_identifier(identifier)
            return ".".join(quote_backtick_identifier(part) for part in parts)
        if self.runtime_dialect == _ARROW_ODBC_MSSQL:
            return identifier
        return super()._quote_identifier(identifier)

    def _detect_runtime_dialect(self) -> str:
        connection_config = cast("dict[str, Any]", getattr(self._config, "connection_config", {}) or {})
        driver_features = cast("dict[str, Any]", getattr(self._config, "driver_features", {}) or {})
        for value in (
            driver_features.get("dbms_name"),
            connection_config.get("driver"),
            connection_config.get("dsn"),
            connection_config.get("connection_string"),
        ):
            dialect = _resolve_dialect_from_dbms_name(str(value) if value is not None else None)
            if dialect != _ARROW_ODBC_SQLITE:
                return dialect

        statement_config = getattr(self._config, "statement_config", None)
        dialect = getattr(statement_config, "dialect", None)
        if dialect is not None:
            return _normalize_dialect_name(str(dialect))
        return _ARROW_ODBC_SQLITE


def _resolve_dialect_from_dbms_name(dbms_name: str | None) -> str:
    if not dbms_name:
        return _ARROW_ODBC_SQLITE
    lowered = dbms_name.lower()
    for dialect, patterns in _DIALECT_PATTERNS:
        if any(pattern in lowered for pattern in patterns):
            return dialect
    return _ARROW_ODBC_SQLITE


def _normalize_dialect_name(dialect: str) -> str:
    normalized = dialect.lower().replace("-", "_")
    if normalized in {"postgresql"}:
        return _ARROW_ODBC_POSTGRES
    if normalized in {"mariadb"}:
        return _ARROW_ODBC_MYSQL
    if normalized in {"tsql", "sqlserver"}:
        return _ARROW_ODBC_MSSQL
    return normalized

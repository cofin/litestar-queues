"""arrow_odbc SQLSpec queue store."""

from typing import TYPE_CHECKING, Any, ClassVar

from sqlspec.utils.text import split_qualified_identifier

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from datetime import datetime

__all__ = ("ArrowOdbcQueueStore",)

_SUPPORTED_TARGET_DIALECT = "mssql"
_UNRESOLVED_DIALECTS = frozenset({"", "sqlite"})
_TARGET_DIALECT_PATTERNS = (
    ("mssql", ("sql server", "sqlserver", "microsoft sql", "msodbcsql")),
    ("oracle", ("oracle",)),
    ("mysql", ("mysql", "mariadb")),
    ("postgres", ("postgres", "postgresql")),
    ("duckdb", ("duckdb",)),
)


class ArrowOdbcQueueStore(SQLSpecQueueStore):
    """SQLSpec queue store for validated Arrow ODBC SQL Server targets."""

    __slots__ = ()

    bind_datetime_as_text: "ClassVar[bool]" = True
    data_dictionary_dialect: "ClassVar[str | None]" = "mssql"
    skip_explicit_begin: "ClassVar[bool]" = True

    def __init__(
        self,
        config: "Any",
        *,
        table_name: "str | None" = None,
        column_map: "Mapping[str, str] | None" = None,
        native_json_columns: "frozenset[str] | None" = None,
        manage_schema: "bool" = True,
    ) -> "None":
        _resolve_target_dialect(config)
        super().__init__(
            config,
            table_name=table_name,
            column_map=column_map,
            native_json_columns=native_json_columns,
            manage_schema=manage_schema,
        )

    def _timestamp_type(self) -> "str":
        """Return a timestamp type that SQLSpec's DDL builder can parse."""
        return "DATETIME"

    def serialize_datetime_text(self, value: "datetime") -> "str":
        """Serialize datetimes as SQL Server DATETIME-compatible text.

        Returns:
            A SQL Server ``DATETIME`` compatible text value.
        """
        return value.replace(tzinfo=None).isoformat(sep=" ", timespec="milliseconds")

    def create_statements(self) -> "list[str]":
        """Return SQL Server queue artifacts for Arrow ODBC."""
        statements = super().create_statements()
        if not statements:
            return []
        task_key_column = _quote_tsql_identifier(self._col("task_key"))
        table_name = self._table_object_name()
        task_key_index = self._index_name("task_key")
        quoted_table_name = _quote_tsql_identifier(self.table_name)
        quoted_task_key_index = _quote_tsql_identifier(task_key_index)
        statements.append(
            f"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = object_id('{table_name}') "  # noqa: S608
            f"AND name = '{task_key_index}') "
            f"EXEC('CREATE UNIQUE INDEX {quoted_task_key_index} ON {quoted_table_name}({task_key_column}) "
            f"WHERE {task_key_column} IS NOT NULL')"
        )
        return statements

    def _inline_unique_task_key(self) -> "bool":
        return False

    def drop_statements(self) -> "list[str]":
        """Return SQL Server statements that drop Arrow ODBC queue artifacts."""
        statements = super().drop_statements()
        if not statements:
            return []
        task_key_index = self._index_name("task_key")
        table_name = self._table_object_name()
        quoted_table_name = _quote_tsql_identifier(self.table_name)
        quoted_task_key_index = _quote_tsql_identifier(task_key_index)
        return [
            f"IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = object_id('{table_name}') "  # noqa: S608
            f"AND name = '{task_key_index}') DROP INDEX {quoted_task_key_index} ON {quoted_table_name}",
            *statements,
        ]

    def _table_object_name(self) -> "str":
        return self.table_name.replace("'", "''")


def _resolve_target_dialect(config: "Any") -> "str":
    """Resolve and validate the Arrow ODBC target dialect from SQLSpec metadata.

    Returns:
        The resolved SQLSpec target dialect.
    """
    statement_config = getattr(config, "statement_config", None)
    statement_dialect = _normalize_target_dialect(getattr(statement_config, "dialect", None))
    if statement_dialect == _SUPPORTED_TARGET_DIALECT:
        return statement_dialect
    if statement_dialect is not None and statement_dialect not in _UNRESOLVED_DIALECTS:
        raise _unsupported_target_error(statement_dialect)

    for candidate in _iter_metadata_targets(config):
        if candidate == _SUPPORTED_TARGET_DIALECT:
            return candidate
        raise _unsupported_target_error(candidate)

    raise _unknown_target_error()


def _iter_metadata_targets(config: "Any") -> "Iterator[str]":
    driver_features = getattr(config, "driver_features", {}) or {}
    connection_config = getattr(config, "connection_config", {}) or {}

    if isinstance(driver_features, dict):
        for value in (driver_features.get("dbms_name"), driver_features.get("connection_string")):
            if (target := _normalize_target_dialect(value)) is not None:
                yield target

    if isinstance(connection_config, dict):
        for value in (
            connection_config.get("dbms_name"),
            connection_config.get("driver"),
            connection_config.get("connection_string"),
        ):
            if (target := _normalize_target_dialect(value)) is not None:
                yield target


def _normalize_target_dialect(value: "Any") -> "str | None":
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    if normalized in {"mssql", "tsql"}:
        return _SUPPORTED_TARGET_DIALECT
    for dialect, patterns in _TARGET_DIALECT_PATTERNS:
        if any(pattern in normalized for pattern in patterns):
            return dialect
    return None


def _unsupported_target_error(target_dialect: "str") -> "QueueConfigurationError":
    msg = (
        "Arrow ODBC queue support is only available for the SQL Server target dialect. "
        f"Resolved SQLSpec target dialect: {target_dialect!r}."
    )
    return QueueConfigurationError(msg)


def _unknown_target_error() -> "QueueConfigurationError":
    msg = (
        "Arrow ODBC queue support requires SQLSpec target-dialect metadata. "
        "Supported target dialect: SQL Server (mssql)."
    )
    return QueueConfigurationError(msg)


def _quote_tsql_identifier(identifier: "str") -> "str":
    return ".".join(f"[{part.replace(']', ']]')}]" for part in split_qualified_identifier(identifier))

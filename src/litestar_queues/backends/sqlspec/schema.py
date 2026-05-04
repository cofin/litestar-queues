"""Packaged SQL and migration helpers for the SQLSpec queue backend."""

import re
from importlib.resources import files
from pathlib import Path
from typing import Any

from litestar_queues.exceptions import QueueConfigurationError

__all__ = (
    "DEFAULT_TABLE_NAME",
    "load_packaged_sql",
    "load_queue_queries",
    "migration_paths",
    "schema_sql",
    "sql_file_path",
    "validate_table_name",
)

DEFAULT_TABLE_NAME = "litestar_queue_tasks"
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_QUERY_NAMES = (
    "insert_task",
    "get_task",
    "get_task_by_key",
    "list_pending",
    "claim_task",
    "complete_task",
    "retry_task",
    "fail_task",
    "cancel_task",
    "touch_heartbeat",
    "requeue_stale",
    "clear_key",
)


def validate_table_name(table_name: str) -> str:
    """Validate a SQL identifier used for the queue table name."""
    if not _IDENTIFIER_RE.match(table_name):
        msg = f"Invalid SQLSpec queue table name: {table_name!r}"
        raise QueueConfigurationError(msg)
    return table_name


def sql_file_path() -> Path:
    """Return the packaged queue SQL file path."""
    return Path(str(files("litestar_queues.backends.sqlspec").joinpath("sql").joinpath("queue.sql")))


def migration_paths() -> tuple[str, ...]:
    """Return packaged SQLSpec migration file paths."""
    migration_root = files("litestar_queues.backends.sqlspec").joinpath("migrations")
    return (str(migration_root.joinpath("0001_litestar_queue_tasks.sql")),)


def load_packaged_sql(loader: Any) -> Any:
    """Load packaged queue SQL into a SQLSpec loader."""
    loader.load_sql(sql_file_path())
    return loader


def _format_sql(sql: str, table_name: str) -> str:
    return sql.format(table_name=validate_table_name(table_name))


def schema_sql(loader: Any, table_name: str = DEFAULT_TABLE_NAME) -> str:
    """Return the schema bootstrap SQL for a queue table."""
    if not loader.has_query("create_schema"):
        load_packaged_sql(loader)
    return _format_sql(str(loader.get_query_text("create_schema")), table_name)


def load_queue_queries(loader: Any, table_name: str = DEFAULT_TABLE_NAME) -> dict[str, str]:
    """Return formatted queue DML queries keyed by query name."""
    if not all(loader.has_query(name) for name in _QUERY_NAMES):
        load_packaged_sql(loader)
    return {name: _format_sql(str(loader.get_query_text(name)), table_name) for name in _QUERY_NAMES}

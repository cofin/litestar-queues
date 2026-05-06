"""Schema and migration helpers for the SQLSpec queue backend."""

import re
from importlib.resources import files
from pathlib import Path

from litestar_queues.exceptions import QueueConfigurationError

__all__ = (
    "DEFAULT_TABLE_NAME",
    "migration_directory",
    "migration_paths",
    "validate_table_name",
)

DEFAULT_TABLE_NAME = "litestar_queue_tasks"
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_table_name(table_name: str) -> str:
    """Validate a SQL identifier used for the queue table name.

    Returns:
        The validated table name.

    Raises:
        QueueConfigurationError: If the table name is not a valid SQL identifier.
    """
    if not _IDENTIFIER_RE.match(table_name):
        msg = f"Invalid SQLSpec queue table name: {table_name!r}"
        raise QueueConfigurationError(msg)
    return table_name


def migration_paths() -> tuple[str, ...]:
    """Return packaged SQLSpec migration file paths."""
    return (str(migration_directory().joinpath("0001_create_queue_tasks.py")),)


def migration_directory() -> Path:
    """Return the packaged SQLSpec queue extension migration directory."""
    return Path(str(files("litestar_queues.backends.sqlspec").joinpath("migrations")))

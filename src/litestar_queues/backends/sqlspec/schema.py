"""Schema and migration helpers for the SQLSpec queue backend."""

from collections.abc import Mapping
from importlib.resources import files
from pathlib import Path

from sqlspec.utils.text import quote_identifier, split_qualified_identifier

from litestar_queues.exceptions import QueueConfigurationError

__all__ = (
    "DEFAULT_TABLE_NAME",
    "migration_directory",
    "migration_paths",
    "validate_column_map",
    "validate_native_json_columns",
    "validate_table_name",
)

DEFAULT_TABLE_NAME = "litestar_queue_tasks"
_CANONICAL_COLUMNS = frozenset({
    "id",
    "task_name",
    "args_json",
    "kwargs_json",
    "queue",
    "execution_backend",
    "execution_profile",
    "execution_ref",
    "status",
    "priority",
    "max_retries",
    "retry_count",
    "scheduled_at",
    "created_at",
    "started_at",
    "completed_at",
    "heartbeat_at",
    "result_json",
    "error",
    "task_key",
    "metadata_json",
})
_JSON_COLUMNS = frozenset({"args_json", "kwargs_json", "result_json", "metadata_json"})


def validate_table_name(table_name: str) -> str:
    """Validate a SQL identifier used for the queue table name.

    Returns:
        The validated table name, normalized to unquoted SQLSpec identifier
        parts.

    Raises:
        QueueConfigurationError: If the table name is not a valid SQL identifier.
    """
    cleaned = table_name.strip()
    parts = split_qualified_identifier(cleaned)
    if (
        not parts
        or cleaned.count(".") != len(parts) - 1
        or any(not _is_unquoted_identifier_part(part) for part in parts)
        or split_qualified_identifier(".".join(quote_identifier(part) for part in parts)) != parts
    ):
        msg = f"Invalid SQLSpec queue table name: {table_name!r}"
        raise QueueConfigurationError(msg)
    return ".".join(parts)


def validate_column_map(column_map: Mapping[str, str]) -> dict[str, str]:
    """Validate a canonical-to-adopter column map.

    Returns:
        A defensive copy of the validated map.

    Raises:
        QueueConfigurationError: If a canonical name is unknown or a mapped
            name is not a valid SQL identifier.
    """
    resolved: dict[str, str] = {}
    for canonical, mapped in column_map.items():
        if canonical not in _CANONICAL_COLUMNS:
            msg = f"Unknown canonical column in column_map: {canonical!r}"
            raise QueueConfigurationError(msg)
        if not _is_unquoted_identifier_part(mapped):
            msg = f"Invalid SQL identifier in column_map: {mapped!r}"
            raise QueueConfigurationError(msg)
        resolved[canonical] = mapped
    return resolved


def validate_native_json_columns(columns: frozenset[str]) -> frozenset[str]:
    """Validate native JSON passthrough columns.

    Returns:
        The validated column set.

    Raises:
        QueueConfigurationError: If any column is not a canonical JSON column.
    """
    unknown = columns - _JSON_COLUMNS
    if unknown:
        msg = f"native_json_columns contains non-JSON canonical names: {sorted(unknown)!r}"
        raise QueueConfigurationError(msg)
    return columns


def migration_paths() -> tuple[str, ...]:
    """Return packaged SQLSpec migration file paths."""
    return (str(migration_directory().joinpath("0001_create_queue_tasks.py")),)


def migration_directory() -> Path:
    """Return the packaged SQLSpec queue extension migration directory."""
    return Path(str(files("litestar_queues.backends.sqlspec").joinpath("migrations")))


def _is_unquoted_identifier_part(identifier: str) -> bool:
    """Return whether a SQLSpec-split identifier part is safe unquoted text."""
    return (
        identifier.isascii()
        and bool(identifier)
        and (identifier[0].isalpha() or identifier[0] == "_")
        and all(character.isalnum() or character == "_" for character in identifier)
    )

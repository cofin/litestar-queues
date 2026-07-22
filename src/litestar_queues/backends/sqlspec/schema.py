"""Schema and migration helpers for the SQLSpec queue backend."""

from importlib.resources import files
from pathlib import Path
from typing import TYPE_CHECKING

from sqlspec.utils.text import quote_identifier, split_qualified_identifier

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = (
    "DEFAULT_COLUMN_MAP",
    "DEFAULT_EVENT_LOG_TABLE_SUFFIX",
    "DEFAULT_TABLE_NAME",
    "DEFAULT_UNIQUENESS_TABLE_SUFFIX",
    "event_log_table_name_for",
    "migration_directory",
    "migration_paths",
    "resolve_column_map",
    "uniqueness_table_name_for",
    "validate_column_map",
    "validate_native_json_columns",
    "validate_table_name",
)

DEFAULT_TABLE_NAME = "litestar_queue_task"
DEFAULT_EVENT_LOG_TABLE_SUFFIX = "_event_log"
DEFAULT_UNIQUENESS_TABLE_SUFFIX = "_uniqueness"
DEFAULT_COLUMN_MAP = {
    "args_json": "task_args",
    "kwargs_json": "task_kwargs",
    "result_json": "result",
    "metadata_json": "metadata",
}
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


def validate_table_name(table_name: "str") -> "str":
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


def validate_column_map(column_map: "Mapping[str, str]") -> "dict[str, str]":
    """Validate a canonical-to-adopter column map.

    Returns:
        A defensive copy of the validated map.

    Raises:
        QueueConfigurationError: If a canonical name is unknown or a mapped
            name is not a valid SQL identifier.
    """
    resolved: "dict[str, str]" = {}
    for canonical, mapped in column_map.items():
        if canonical not in _CANONICAL_COLUMNS:
            msg = f"Unknown canonical column in column_map: {canonical!r}"
            raise QueueConfigurationError(msg)
        if not _is_unquoted_identifier_part(mapped):
            msg = f"Invalid SQL identifier in column_map: {mapped!r}"
            raise QueueConfigurationError(msg)
        resolved[canonical] = mapped

    physical_to_canonical: "dict[str, str]" = {}
    for canonical in sorted(_CANONICAL_COLUMNS):
        mapped = resolved.get(canonical, canonical)
        previous = physical_to_canonical.get(mapped)
        if previous is not None:
            msg = f"Duplicate physical column in column_map: {mapped!r} is used for {previous!r} and {canonical!r}."
            raise QueueConfigurationError(msg)
        physical_to_canonical[mapped] = canonical
    return resolved


def resolve_column_map(column_map: "Mapping[str, str] | None" = None) -> "dict[str, str]":
    """Return the default physical column map with adopter overrides applied."""
    return validate_column_map({**DEFAULT_COLUMN_MAP, **dict(column_map or {})})


def validate_native_json_columns(columns: "frozenset[str]") -> "frozenset[str]":
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


def event_log_table_name_for(table_name: "str") -> "str":
    """Return the default event-log table for a queue table name.

    Schema-qualified names keep their schema and append
    :data:`DEFAULT_EVENT_LOG_TABLE_SUFFIX` to the table part.
    """
    validated = validate_table_name(table_name)
    parts = validated.rsplit(".", maxsplit=1)
    if len(parts) == 1:
        return validate_table_name(f"{validated}{DEFAULT_EVENT_LOG_TABLE_SUFFIX}")
    schema, table = parts
    return validate_table_name(f"{schema}.{table}{DEFAULT_EVENT_LOG_TABLE_SUFFIX}")


def uniqueness_table_name_for(table_name: "str") -> "str":
    """Return the default forever-uniqueness tombstone table for a queue table.

    Schema-qualified names keep their schema and append
    :data:`DEFAULT_UNIQUENESS_TABLE_SUFFIX` to the table part.
    """
    validated = validate_table_name(table_name)
    parts = validated.rsplit(".", maxsplit=1)
    if len(parts) == 1:
        return validate_table_name(f"{validated}{DEFAULT_UNIQUENESS_TABLE_SUFFIX}")
    schema, table = parts
    return validate_table_name(f"{schema}.{table}{DEFAULT_UNIQUENESS_TABLE_SUFFIX}")


def migration_paths() -> "tuple[str, ...]":
    """Return packaged SQLSpec migration file paths."""
    directory = migration_directory()
    return (
        str(directory.joinpath("0001_create_queue_tasks.py")),
        str(directory.joinpath("0002_create_uniqueness_tombstones.py")),
    )


def migration_directory() -> "Path":
    """Return the packaged SQLSpec queue extension migration directory."""
    return Path(str(files("litestar_queues.backends.sqlspec").joinpath("migrations")))


def _is_unquoted_identifier_part(identifier: "str") -> "bool":
    """Return whether a SQLSpec-split identifier part is safe unquoted text."""
    return (
        identifier.isascii()
        and bool(identifier)
        and (identifier[0].isalpha() or identifier[0] == "_")
        and all(character.isalnum() or character == "_" for character in identifier)
    )

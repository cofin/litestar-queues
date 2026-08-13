"""Schema and migration helpers for the SQLSpec queue backend."""

from dataclasses import dataclass
from hashlib import sha1
from importlib.resources import files
from pathlib import Path
from typing import TYPE_CHECKING

from sqlspec.utils.text import quote_identifier, split_qualified_identifier

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = (
    "DEFAULT_COLUMN_MAP",
    "DEFAULT_EVENT_HISTORY_TABLE_SUFFIX",
    "DEFAULT_MAINTENANCE_TABLE_SUFFIX",
    "DEFAULT_TABLE_NAME",
    "DEFAULT_TASK_RESERVATION_TABLE_SUFFIX",
    "EVENT_HISTORY_COLUMNS",
    "RESERVED_EVENT_HISTORY_COLUMNS",
    "EventHistoryExtraColumn",
    "event_history_table_name_for",
    "maintenance_table_name_for",
    "migration_directory",
    "migration_paths",
    "resolve_column_map",
    "task_reservation_table_name_for",
    "validate_column_map",
    "validate_event_history_extra_columns",
    "validate_native_json_columns",
    "validate_table_name",
)

DEFAULT_TABLE_NAME = "queue_task"
DEFAULT_EVENT_HISTORY_TABLE_SUFFIX = "_event_history"
DEFAULT_MAINTENANCE_TABLE_SUFFIX = "_maintenance"
DEFAULT_TASK_RESERVATION_TABLE_SUFFIX = "_reservation"
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
    "worker_id",
    "status",
    "priority",
    "max_retries",
    "retry_count",
    "scheduled_at",
    "expires_at",
    "created_at",
    "queued_at",
    "started_at",
    "completed_at",
    "heartbeat_at",
    "result_json",
    "error",
    "task_key",
    "metadata_json",
})
_JSON_COLUMNS = frozenset({"args_json", "kwargs_json", "result_json", "metadata_json"})

EVENT_HISTORY_COLUMNS = (
    "event_id",
    "event_type",
    "task_id",
    "task_name",
    "queue",
    "worker_id",
    "execution_backend",
    "execution_profile",
    "actor_type",
    "actor_id",
    "stage",
    "level",
    "message",
    "detail",
    "progress_current",
    "progress_total",
    "progress_percent",
    "duration_ms",
    "sequence",
    "occurred_at",
    "created_at",
)
"""Physical columns the package owns on the SQLSpec event-history table."""

_EVENT_HISTORY_COLUMN_NAMES = frozenset(EVENT_HISTORY_COLUMNS)

RESERVED_EVENT_HISTORY_COLUMNS = frozenset({"entity", "scope", "scope_key"})
"""Names held for built-in event-history scoping dimensions.

These are not columns on the table yet. They are reserved so an adopter-declared
extra column cannot claim a name the package intends to own.
"""


@dataclass(frozen=True, slots=True)
class EventHistoryExtraColumn:
    """Adopter-declared scoping column on the SQLSpec event-history table."""

    name: "str"
    """Physical column name; must be a valid unquoted SQL identifier."""

    source: "str"
    """Key looked up in the event payload (``QueueEvent.payload``)."""

    indexed: "bool" = False
    """Whether a ``(name, occurred_at)`` index is created."""


def validate_event_history_extra_columns(
    columns: "Sequence[EventHistoryExtraColumn]",
) -> "tuple[EventHistoryExtraColumn, ...]":
    """Validate adopter-declared extra event-history columns.

    Returns:
        The validated declarations as a tuple.

    Raises:
        QueueConfigurationError: If a name is not a valid unquoted SQL
            identifier, collides with a package-owned column, uses a reserved
            scoping-dimension name, repeats another declaration, or the payload
            source key is empty.
    """
    seen: "set[str]" = set()
    validated: "list[EventHistoryExtraColumn]" = []
    for column in columns:
        if not _is_unquoted_identifier_part(column.name):
            msg = f"Invalid SQL identifier in event_history_extra_columns: {column.name!r}"
            raise QueueConfigurationError(msg)
        # Unquoted SQL identifiers fold case, so every name comparison below does
        # too: ``TASK_ID`` and ``task_id`` are one column to the database.
        folded = column.name.lower()
        if folded in _EVENT_HISTORY_COLUMN_NAMES:
            msg = f"event_history_extra_columns may not redeclare package-owned column {column.name!r}"
            raise QueueConfigurationError(msg)
        if folded in RESERVED_EVENT_HISTORY_COLUMNS:
            msg = (
                f"event_history_extra_columns may not use {column.name!r}: "
                f"{sorted(RESERVED_EVENT_HISTORY_COLUMNS)!r} are reserved for built-in scoping dimensions"
            )
            raise QueueConfigurationError(msg)
        if folded in seen:
            msg = f"Duplicate column in event_history_extra_columns: {column.name!r}"
            raise QueueConfigurationError(msg)
        if not column.source:
            msg = f"event_history_extra_columns entry {column.name!r} requires a non-empty payload source key"
            raise QueueConfigurationError(msg)
        seen.add(folded)
        validated.append(column)
    return tuple(validated)


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


def event_history_table_name_for(table_name: "str") -> "str":
    """Return the default event-history table for a queue table name.

    Schema-qualified names keep their schema and append
    :data:`DEFAULT_EVENT_HISTORY_TABLE_SUFFIX` to the table part.
    """
    validated = validate_table_name(table_name)
    parts = validated.rsplit(".", maxsplit=1)
    if len(parts) == 1:
        return validate_table_name(f"{validated}{DEFAULT_EVENT_HISTORY_TABLE_SUFFIX}")
    schema, table = parts
    return validate_table_name(f"{schema}.{table}{DEFAULT_EVENT_HISTORY_TABLE_SUFFIX}")


# Smallest common identifier limit across supported dialects (PostgreSQL is 63,
# MySQL is 64). Derived coordination tables are deterministically shortened so
# the same name is produced by the packaged migration and the runtime backend.
_MAX_IDENTIFIER_LENGTH = 63


def _bounded_table_part(table: "str", suffix: "str") -> "str":
    candidate = f"{table}{suffix}"
    if len(candidate) <= _MAX_IDENTIFIER_LENGTH:
        return candidate
    digest = sha1(table.encode()).hexdigest()[:8]  # noqa: S324 - non-cryptographic name shortening.
    keep = _MAX_IDENTIFIER_LENGTH - len(suffix) - len(digest) - 1
    return f"{table[:keep]}_{digest}{suffix}"


def maintenance_table_name_for(table_name: "str") -> "str":
    """Return the default maintenance table for a queue table name.

    Schema-qualified names keep their schema and append
    :data:`DEFAULT_MAINTENANCE_TABLE_SUFFIX` to the table part. When the
    derived name would exceed the portable identifier limit it is
    deterministically shortened (truncated table part plus a stable hash) so the
    packaged migration and the runtime backend agree on one name.
    """
    validated = validate_table_name(table_name)
    parts = validated.rsplit(".", maxsplit=1)
    if len(parts) == 1:
        if validated == DEFAULT_TABLE_NAME:
            return "queue_maintenance"
        return validate_table_name(_bounded_table_part(validated, DEFAULT_MAINTENANCE_TABLE_SUFFIX))
    schema, table = parts
    if table == DEFAULT_TABLE_NAME:
        return validate_table_name(f"{schema}.queue_maintenance")
    return validate_table_name(f"{schema}.{_bounded_table_part(table, DEFAULT_MAINTENANCE_TABLE_SUFFIX)}")


def task_reservation_table_name_for(table_name: "str") -> "str":
    """Return the default forever-uniqueness reservation table for a queue table.

    Schema-qualified names keep their schema and append
    :data:`DEFAULT_TASK_RESERVATION_TABLE_SUFFIX` to the table part. Long derived
    names use the same portable deterministic shortening as maintenance tables.
    """
    validated = validate_table_name(table_name)
    parts = validated.rsplit(".", maxsplit=1)
    if len(parts) == 1:
        return validate_table_name(_bounded_table_part(validated, DEFAULT_TASK_RESERVATION_TABLE_SUFFIX))
    schema, table = parts
    return validate_table_name(f"{schema}.{_bounded_table_part(table, DEFAULT_TASK_RESERVATION_TABLE_SUFFIX)}")


def migration_paths() -> "tuple[str, ...]":
    """Return packaged SQLSpec migration file paths."""
    directory = migration_directory()
    return (str(directory.joinpath("0001_create_queue_tasks.py")),)


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

"""Backend-owned queue event history contracts."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import datetime

    from litestar_queues.events.models import QueueEvent

__all__ = (
    "RESERVED_EVENT_HISTORY_COLUMNS",
    "EventHistoryConfig",
    "EventHistoryExtraColumn",
    "QueueEventLog",
    "QueueEventLogRecord",
    "QueueEventStageSummary",
    "extract_event_extras",
    "validate_event_extra_filter",
    "validate_event_history_extra_columns",
)


def _is_unquoted_identifier_part(identifier: "str") -> "bool":
    """Return whether an identifier part is safe unquoted text."""
    return (
        identifier.isascii()
        and bool(identifier)
        and (identifier[0].isalpha() or identifier[0] == "_")
        and all(character.isalnum() or character == "_" for character in identifier)
    )


_EVENT_HISTORY_COLUMN_NAMES = frozenset({
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
})


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


def validate_event_extra_filter(
    filter_map: "Mapping[str, str] | None", declared_columns: "Sequence[EventHistoryExtraColumn]"
) -> "dict[str, str]":
    """Validate and resolve extra column filter key-value pairs against declared columns.

    Returns:
        Mapping of resolved physical column names to expected filter values.

    Raises:
        QueueConfigurationError: If any filter key is not declared in declared_columns.
    """
    if not filter_map:
        return {}
    declared_map = {col.name.lower(): col.name for col in declared_columns}
    resolved: "dict[str, str]" = {}
    for key, value in filter_map.items():
        folded = key.lower()
        if folded not in declared_map:
            msg = f"Undeclared event_history extra column in filter: {key!r}"
            raise QueueConfigurationError(msg)
        resolved[declared_map[folded]] = str(value)
    return resolved


def extract_event_extras(
    payload: "Mapping[str, Any] | None", declared_columns: "Sequence[EventHistoryExtraColumn]"
) -> "dict[str, str]":
    """Extract declared extra columns from an event payload dict.

    Returns:
        Mapping of physical column names to extracted string values.
    """
    if not payload or not declared_columns:
        return {}
    extracted: "dict[str, str]" = {}
    for col in declared_columns:
        if col.source in payload and payload[col.source] is not None:
            extracted[col.name] = str(payload[col.source])
    return extracted


@dataclass(slots=True)
class EventHistoryConfig:
    """Configuration for backend-managed queue event history."""

    batch_size: "int" = 20
    """Maximum history records written in one batch."""

    flush_interval: "float" = 1.0
    """Maximum delay between history batch writes in seconds."""

    strict: "bool" = False
    """Whether event-history write failures propagate to the publisher."""

    memory_capacity: "int" = 1000
    """Maximum retained records for the memory backend."""

    extra_columns: "tuple[EventHistoryExtraColumn, ...]" = field(default_factory=tuple)
    """Adopter-declared scoping columns on the event-history table."""

    def __post_init__(self) -> "None":
        """Validate event-history configuration."""
        if self.batch_size <= 0:
            msg = "EventHistoryConfig.batch_size must be greater than 0."
            raise QueueConfigurationError(msg)
        if self.flush_interval <= 0:
            msg = "EventHistoryConfig.flush_interval must be greater than 0."
            raise QueueConfigurationError(msg)
        if self.memory_capacity <= 0:
            msg = "EventHistoryConfig.memory_capacity must be greater than 0."
            raise QueueConfigurationError(msg)
        self.extra_columns = validate_event_history_extra_columns(self.extra_columns)


@dataclass(frozen=True, slots=True)
class QueueEventLogRecord:
    """A durable queue event history record."""

    event_id: "str"
    event_type: "str"
    task_id: "str | None"
    task_name: "str | None"
    queue: "str | None"
    worker_id: "str | None"
    execution_backend: "str | None"
    execution_profile: "str | None"
    actor_type: "str | None"
    actor_id: "str | None"
    stage: "str | None"
    level: "str | None"
    message: "str | None"
    detail: "dict[str, Any]"
    progress_current: "float | None"
    progress_total: "float | None"
    progress_percent: "float | None"
    duration_ms: "float | None"
    sequence: "int | None"
    occurred_at: "datetime"
    created_at: "datetime"
    scope: "str | None" = None
    """Envelope scope of the event (``task``/``queue``/``worker``/...)."""
    scope_key: "str | None" = None
    """Adopter scoping key carried on the envelope (tenant, project, account)."""
    entity: "str | None" = None
    """Canonical entity key from :func:`event_entity_key`."""
    extra: "dict[str, str]" = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class QueueEventStageSummary:
    """Aggregated queue event history data for a single stage."""

    stage: "str | None"
    event_count: "int"
    total_duration_ms: "float"
    first_event_at: "datetime | None"
    last_event_at: "datetime | None"


class QueueEventLog(Protocol):
    """Backend-owned queue event history writer and query interface."""

    async def publish_event(self, event: "QueueEvent") -> "None":
        """Record a queue event for durable history."""
        ...

    async def flush_events(self) -> "None":
        """Flush any buffered queue event history writes."""
        ...

    async def list_events(
        self,
        *,
        task_id: "str | None" = None,
        task_name: "str | None" = None,
        actor_id: "str | None" = None,
        actor_type: "str | None" = None,
        extra: "Mapping[str, str] | None" = None,
        limit: "int | None" = None,
    ) -> "list[QueueEventLogRecord]":
        """Return durable event history records.

        Every filter uses equality and is ANDed with the others.
        """
        ...

    async def summarize_stages(self, *, task_name: "str | None" = None) -> "list[QueueEventStageSummary]":
        """Return per-stage event history aggregates."""
        ...

    async def cleanup_before(self, before: "datetime", *, limit: "int | None" = None) -> "int":
        """Delete event history older than ``before``.

        ``limit`` bounds one bounded maintenance batch (oldest ``occurred_at``,
        then record id); ``None`` preserves the historical unbounded behavior.
        """
        ...

"""Backend-owned queue event history contracts."""

from dataclasses import dataclass, field
from math import isfinite
from typing import TYPE_CHECKING, Any, Protocol

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence
    from datetime import datetime

    from litestar_queues.events.models import QueueEvent
    from litestar_queues.events.query import QueueEventQuery
    from litestar_queues.events.typing import OffsetPagination

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


RESERVED_EVENT_HISTORY_COLUMNS = frozenset({"actor", "entity", "scope", "scope_key"})
"""Names held for built-in event-history scoping dimensions.

These are not columns on the table yet. They are reserved so an adopter-declared
extra column cannot claim a name the package intends to own.
"""

BUILTIN_EVENT_EXTRA_FILTERS = frozenset({"actor_id", "actor_type"})
"""Built-in dimensions retained on the compatibility ``extra=`` surface."""


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
    declared_map.update({name: name for name in BUILTIN_EVENT_EXTRA_FILTERS})
    resolved: "dict[str, str]" = {}
    for key, value in filter_map.items():
        folded = key.lower()
        if folded not in declared_map:
            msg = f"Undeclared event_history extra column in filter: {key!r}"
            raise QueueConfigurationError(msg)
        resolved[declared_map[folded]] = str(value)
    return resolved


def event_extra_filter_matches(record: "QueueEventLogRecord", filters: "Mapping[str, str]") -> "bool":
    """Return whether a record matches validated built-in and adopter filters."""
    return all(
        (getattr(record, key) if key in BUILTIN_EVENT_EXTRA_FILTERS else record.extra.get(key)) == value
        for key, value in filters.items()
    )


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
    """Delay before scheduling the oldest pending history batch, in seconds."""

    strict: "bool" = False
    """Whether event-history write failures propagate to the publisher."""

    memory_capacity: "int" = 1000
    """Maximum retained records for the memory backend."""

    extra_columns: "tuple[EventHistoryExtraColumn, ...]" = field(default_factory=tuple)
    """Adopter-declared scoping columns on the event-history table."""

    max_pending: "int" = 2000
    """Maximum accepted records awaiting persistence or live release."""

    def __post_init__(self) -> "None":
        """Validate event-history configuration."""
        if self.batch_size <= 0:
            msg = "EventHistoryConfig.batch_size must be greater than 0."
            raise QueueConfigurationError(msg)
        if not isfinite(self.flush_interval) or self.flush_interval <= 0:
            msg = "EventHistoryConfig.flush_interval must be finite and greater than 0."
            raise QueueConfigurationError(msg)
        if self.max_pending < self.batch_size:
            msg = "EventHistoryConfig.max_pending must be at least batch_size."
            raise QueueConfigurationError(msg)
        if self.memory_capacity <= 0:
            msg = "EventHistoryConfig.memory_capacity must be greater than 0."
            raise QueueConfigurationError(msg)
        self.extra_columns = validate_event_history_extra_columns(self.extra_columns)


@dataclass(frozen=True, slots=True)
class QueueEventLogRecord:
    """A durable queue event history record."""

    event_id: "str"
    """Unique identifier of the persisted event."""

    event_type: "str"
    """Event type string (for example `"task.started"` or `"task.progress"`)."""

    task_id: "str | None"
    """Associated task record identifier, if task-scoped."""

    task_name: "str | None"
    """Associated registered task name, if task-scoped."""

    queue: "str | None"
    """Logical queue name associated with the event."""

    worker_id: "str | None"
    """Worker identifier that emitted the event."""

    execution_backend: "str | None"
    """Execution transport name active when the event was emitted."""

    execution_profile: "str | None"
    """Optional named execution profile active when the event was emitted."""

    actor_type: "str | None"
    """Logical actor category (`"user"`, `"system"`, `"service"`)."""

    actor_id: "str | None"
    """Identifier of the actor within `actor_type`."""

    stage: "str | None"
    """Optional logical pipeline stage reported in the event payload."""

    level: "str | None"
    """Severity level (`"debug"`, `"info"`, `"warning"`, `"error"`, `"critical"`)."""

    message: "str | None"
    """Human-readable summary message carried on the event."""

    detail: "dict[str, Any]"
    """Structured JSON payload dictionary persisted with the event."""

    progress_current: "float | None"
    """Current numeric progress counter reported by the event."""

    progress_total: "float | None"
    """Total numeric progress target reported by the event."""

    progress_percent: "float | None"
    """Computed or explicit completion percentage (`0.0`-`100.0`)."""

    duration_ms: "float | None"
    """Optional elapsed duration in milliseconds reported in the event payload."""

    sequence: "int | None"
    """Monotonic per-attempt sequence number assigned by the execution context."""

    occurred_at: "datetime"
    """UTC timestamp when the event was emitted."""

    created_at: "datetime"
    """UTC timestamp when the history record was persisted."""

    scope: "str | None" = None
    """Envelope scope of the event (`task`/`queue`/`worker`/...)."""

    scope_key: "str | None" = None
    """Adopter scoping key carried on the envelope (tenant, project, account)."""

    actor: "str | None" = None
    """The logical actor."""

    entity: "str | None" = None
    """Canonical entity key from `event_entity_key()`."""

    extra: "dict[str, str]" = field(default_factory=dict)
    """Values extracted for configured `EventHistoryConfig.extra_columns`."""


@dataclass(frozen=True, slots=True)
class QueueEventStageSummary:
    """Aggregated queue event history data for a single stage."""

    stage: "str | None"
    """Stage identifier being summarized, or `None` for unstaged events."""

    event_count: "int"
    """Total number of persisted events matching this stage."""

    total_duration_ms: "float"
    """Sum of `duration_ms` across all events in this stage."""

    first_event_at: "datetime | None"
    """Earliest `occurred_at` timestamp observed in this stage."""

    last_event_at: "datetime | None"
    """Latest `occurred_at` timestamp observed in this stage."""

    latest_sequence: "int | None" = None
    """Sequence of the newest record in the stage by the stable order key."""

    latest_message: "str | None" = None
    """Message of the newest record in the stage by the stable order key."""

    worst_level: "str | None" = None
    """Highest-ranked level present in the stage; `None` when no record has one."""


class QueueEventLog(Protocol):
    """Backend-owned history with explicit commit acknowledgement and closure.

    Custom providers must implement commit-aware publication and closure;
    accepting an event through `publish_event` alone is not a commit signal.
    """

    async def publish_event(self, event: "QueueEvent") -> "None":
        """Persist or enqueue a single event into the durable event history."""
        ...

    async def publish_event_after_commit(
        self, event: "QueueEvent", *, release: "Callable[[], Awaitable[None]]", barrier: "bool" = False
    ) -> "None":
        """Release live delivery after commitment, waiting through a barrier when requested."""
        ...

    async def flush_events(self) -> "None":
        """Flush any buffered event history writes to durable storage."""
        ...

    async def aclose(self) -> "None":
        """Finish owned history work and release its lifecycle resources."""
        ...

    async def query_events(
        self, query: "QueueEventQuery | None" = None, *, extra: "Mapping[str, str] | None" = None
    ) -> "OffsetPagination[QueueEventLogRecord]":
        """Query persisted event history records with filtering, ordering, and pagination."""
        ...

    async def summarize_stages(self, query: "QueueEventQuery | None" = None) -> "list[QueueEventStageSummary]":
        """Aggregate matching event history records into per-stage summaries."""
        ...

    async def cleanup_events(
        self,
        *,
        before: "datetime",
        match: "QueueEventQuery | None" = None,
        exclude: "Sequence[QueueEventQuery]" = (),
        limit: "int | None" = None,
    ) -> "int":
        """Delete persisted event history records older than `before` matching the retention filter."""
        ...

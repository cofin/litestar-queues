"""Backend-owned queue event history contracts."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from datetime import datetime

    from litestar_queues.events.models import QueueEvent

__all__ = ("EventHistoryConfig", "QueueEventLog", "QueueEventLogRecord", "QueueEventStageSummary")


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
        self, *, task_id: "str | None" = None, task_name: "str | None" = None, limit: "int | None" = None
    ) -> "list[QueueEventLogRecord]":
        """Return durable event history records."""
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

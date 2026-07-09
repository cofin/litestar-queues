"""Backend-owned queue event history contracts."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from datetime import datetime

    from litestar_queues.events.models import QueueEvent

__all__ = ("EventLogConfig", "QueueEventLog", "QueueEventLogRecord", "QueueEventStageSummary")


@dataclass(slots=True)
class EventLogConfig:
    """Configuration for backend-managed queue event history."""

    enabled: "bool" = True
    buffer_size: "int" = 20
    flush_interval: "float" = 1.0
    strict: "bool" = False
    max_records: "int" = 1000

    def __post_init__(self) -> "None":
        """Validate event-history configuration."""
        if self.max_records <= 0:
            msg = "EventLogConfig.max_records must be greater than 0."
            raise ValueError(msg)


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

    async def cleanup_before(self, before: "datetime") -> "int":
        """Delete event history older than ``before``."""
        ...

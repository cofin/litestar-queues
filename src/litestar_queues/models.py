from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID, uuid4

__all__ = ("TERMINAL_STATUSES", "QueueBackendCapabilities", "QueueStatistics", "QueuedTaskRecord", "TaskStatus")

TaskStatus = Literal["pending", "scheduled", "running", "completed", "failed", "cancelled"]
"""Queue task lifecycle states."""

TERMINAL_STATUSES: frozenset[TaskStatus] = frozenset({"completed", "failed", "cancelled"})
"""Statuses that represent finished queue records."""


@dataclass(slots=True)
class QueueBackendCapabilities:
    """Behavior advertised by a queue backend."""

    supports_notifications: bool = False
    notification_backend: str | None = None
    notifications_durable: bool = False
    supports_heartbeats: bool = True
    supports_atomic_claim: bool = True
    supports_atomic_delayed_promotion: bool = True
    supports_external_refs: bool = True
    supports_terminal_cleanup: bool = True


@dataclass(slots=True)
class QueueStatistics:
    """Operational status counts for a queue backend."""

    pending: int = 0
    scheduled: int = 0
    running: int = 0
    completed: int = 0
    failed: int = 0
    cancelled: int = 0

    @property
    def total(self) -> int:
        """Return the total number of known queue records."""
        return self.pending + self.scheduled + self.running + self.completed + self.failed + self.cancelled


@dataclass(slots=True)
class QueuedTaskRecord:
    """Backend-neutral representation of a queued task."""

    task_name: str
    id: UUID = field(default_factory=uuid4)
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    queue: str = "default"
    execution_backend: str = "local"
    execution_profile: str | None = None
    execution_ref: str | None = None
    status: TaskStatus = "pending"
    priority: int = 0
    max_retries: int = 0
    retry_count: int = 0
    scheduled_at: datetime | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: datetime | None = None
    completed_at: datetime | None = None
    heartbeat_at: datetime | None = None
    result: Any = None
    error: str | None = None
    key: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_terminal(self) -> bool:
        """Return whether the record is in a terminal state."""
        return self.status in TERMINAL_STATUSES

    @property
    def is_due(self) -> bool:
        """Return whether the record is eligible to be claimed now."""
        return self.scheduled_at is None or self.scheduled_at <= datetime.now(timezone.utc)

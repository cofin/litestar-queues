from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID, uuid4

__all__ = (
    "TERMINAL_STATUSES",
    "QueuedTaskRecord",
    "TaskStatus",
)

TaskStatus = Literal["pending", "scheduled", "running", "completed", "failed", "cancelled"]
"""Queue task lifecycle states."""

TERMINAL_STATUSES: frozenset[TaskStatus] = frozenset({"completed", "failed", "cancelled"})
"""Statuses that represent finished queue records."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class QueuedTaskRecord:
    """Backend-neutral representation of a queued task."""

    task_name: str
    id: UUID = field(default_factory=uuid4)
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    queue: str = "default"
    status: TaskStatus = "pending"
    priority: int = 0
    max_retries: int = 0
    retry_count: int = 0
    scheduled_at: datetime | None = None
    created_at: datetime = field(default_factory=_utc_now)
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
        return self.scheduled_at is None or self.scheduled_at <= _utc_now()

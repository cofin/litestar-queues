from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID, uuid4

__all__ = (
    "TERMINAL_STATUSES",
    "EnqueueSpec",
    "HeartbeatTouch",
    "HeartbeatTouchResult",
    "QueueBackendCapabilities",
    "QueueStatistics",
    "QueuedTaskRecord",
    "StaleTaskRecoveryResult",
    "TaskStatus",
)

TaskStatus = Literal["pending", "scheduled", "running", "completed", "failed", "cancelled"]
"""Queue task lifecycle states."""

TERMINAL_STATUSES: "frozenset[TaskStatus]" = frozenset({"completed", "failed", "cancelled"})
"""Statuses that represent finished queue records."""


@dataclass(frozen=True, slots=True)
class HeartbeatTouch:
    """A fenced heartbeat update request for one running task."""

    task_id: "UUID"
    expected_retry_count: "int | None"
    metadata_patch: "dict[str, Any] | None" = None


@dataclass(slots=True)
class HeartbeatTouchResult:
    """Backend-neutral result for a bulk heartbeat update."""

    touched_task_ids: "set[UUID]" = field(default_factory=set)
    missed_task_ids: "set[UUID]" = field(default_factory=set)
    failed_task_ids: "set[UUID]" = field(default_factory=set)


@dataclass(slots=True)
class QueueBackendCapabilities:
    """Behavior advertised by a queue backend."""

    supports_notifications: "bool" = False
    notification_backend: "str | None" = None
    notifications_durable: "bool" = False
    supports_batch_claim: "bool" = False
    supports_heartbeats: "bool" = True
    supports_atomic_claim: "bool" = True
    supports_atomic_delayed_promotion: "bool" = True
    supports_external_refs: "bool" = True
    supports_terminal_cleanup: "bool" = True
    supports_completion_events: "bool" = False


@dataclass(slots=True)
class QueueStatistics:
    """Operational status counts for a queue backend."""

    pending: "int" = 0
    scheduled: "int" = 0
    running: "int" = 0
    completed: "int" = 0
    failed: "int" = 0
    cancelled: "int" = 0

    @property
    def total(self) -> "int":
        """Total number of known queue records."""
        return self.pending + self.scheduled + self.running + self.completed + self.failed + self.cancelled


@dataclass(slots=True)
class StaleTaskRecoveryResult:
    """Summary of stale running task recovery."""

    requeued: "int" = 0
    failed: "int" = 0
    skipped: "int" = 0
    handler_needed: "int" = 0
    failed_task_ids: "list[UUID]" = field(default_factory=list)
    handler_needed_task_ids: "list[UUID]" = field(default_factory=list)

    def to_payload(self) -> "dict[str, int]":
        """Return a JSON-compatible event payload."""
        return {
            "requeued": self.requeued,
            "failed": self.failed,
            "skipped": self.skipped,
            "handler_needed": self.handler_needed,
        }


@dataclass(slots=True)
class EnqueueSpec:
    """A single task specification for bulk enqueue via ``enqueue_many``.

    Mirrors the keyword arguments of :meth:`BaseQueueBackend.enqueue` so a batch
    of tasks can be described declaratively and submitted in one call.
    """

    task_name: "str"
    args: "tuple[Any, ...]" = ()
    kwargs: "dict[str, Any] | None" = None
    queue: "str" = "default"
    priority: "int" = 0
    max_retries: "int" = 0
    scheduled_at: "datetime | None" = None
    key: "str | None" = None
    execution_backend: "str" = "local"
    execution_profile: "str | None" = None
    metadata: "dict[str, Any] | None" = None

    def __post_init__(self) -> "None":
        if self.scheduled_at is not None:
            self.scheduled_at = _ensure_utc_datetime(self.scheduled_at)


@dataclass(slots=True)
class QueuedTaskRecord:
    """Backend-neutral representation of a queued task."""

    task_name: "str"
    id: "UUID" = field(default_factory=uuid4)
    args: "tuple[Any, ...]" = ()
    kwargs: "dict[str, Any]" = field(default_factory=dict)
    queue: "str" = "default"
    execution_backend: "str" = "local"
    execution_profile: "str | None" = None
    execution_ref: "str | None" = None
    status: "TaskStatus" = "pending"
    priority: "int" = 0
    max_retries: "int" = 0
    retry_count: "int" = 0
    scheduled_at: "datetime | None" = None
    created_at: "datetime" = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: "datetime | None" = None
    completed_at: "datetime | None" = None
    heartbeat_at: "datetime | None" = None
    result: "Any" = None
    error: "str | None" = None
    key: "str | None" = None
    metadata: "dict[str, Any]" = field(default_factory=dict)

    def __post_init__(self) -> "None":
        if self.scheduled_at is not None:
            self.scheduled_at = _ensure_utc_datetime(self.scheduled_at)

    @property
    def is_terminal(self) -> "bool":
        """Whether the record is in a terminal state."""
        return self.status in TERMINAL_STATUSES

    @property
    def is_due(self) -> "bool":
        """Whether the record is eligible to be claimed now."""
        return self.scheduled_at is None or self.scheduled_at <= datetime.now(timezone.utc)


def _ensure_utc_datetime(value: "datetime") -> "datetime":
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)

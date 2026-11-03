"""Queue domain models, records, statistics, and result summaries."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID, uuid4

if TYPE_CHECKING:
    from litestar_queues.execution.base import DispatchRepairResult

__all__ = (
    "TERMINAL_STATUSES",
    "HeartbeatTouch",
    "HeartbeatTouchResult",
    "QueueBackendCapabilities",
    "QueueStatistics",
    "QueuedTaskRecord",
    "ScheduleAssertionResult",
    "StaleTaskRecoveryResult",
    "TaskRequest",
    "TaskReservation",
    "TaskStatus",
)

TaskStatus = Literal["pending", "scheduled", "running", "completed", "failed", "cancelled", "expired"]
"""Queue task lifecycle states."""

TERMINAL_STATUSES: "frozenset[TaskStatus]" = frozenset({"completed", "failed", "cancelled", "expired"})
"""Statuses that represent finished queue records."""


@dataclass(frozen=True, slots=True)
class HeartbeatTouch:
    """A fenced heartbeat update request for one running task."""

    task_id: "UUID"
    """Identifier of the running task whose lease is being extended."""

    expected_retry_count: "int | None"
    """Retry-count fence verifying the active execution attempt."""

    metadata_patch: "dict[str, Any] | None" = None
    """Optional key-value metadata patch merged during the heartbeat touch."""


@dataclass(slots=True)
class HeartbeatTouchResult:
    """Backend-neutral result for a bulk heartbeat update."""

    touched_task_ids: "set[UUID]" = field(default_factory=set)
    """Identifiers of tasks whose heartbeat timestamps were updated."""

    missed_task_ids: "set[UUID]" = field(default_factory=set)
    """Identifiers of tasks that were no longer running or failed the fence check."""


@dataclass(slots=True)
class QueueBackendCapabilities:
    """Behavior advertised by a queue backend."""

    supports_worker_wakeups: "bool" = False
    """Whether the backend emits push notifications when due work arrives."""

    wakeup_backend: "str | None" = None
    """Name of the active wakeup notification transport, or `None` when polling."""

    wakeups_durable: "bool" = False
    """Whether wakeup notifications survive transient listener disconnections."""

    supports_completion_events: "bool" = False
    """Whether the backend emits push notifications when tasks reach terminal status."""

    supports_maintenance: "bool" = False
    """Whether the backend supports distributed maintenance leases and cleanup."""


@dataclass(slots=True)
class QueueStatistics:
    """Operational status counts for a queue backend."""

    pending: "int" = 0
    """Count of due or immediate tasks waiting to be claimed."""

    scheduled: "int" = 0
    """Count of future-scheduled tasks waiting for `scheduled_at`."""

    running: "int" = 0
    """Count of tasks currently claimed by a worker or external execution transport."""

    completed: "int" = 0
    """Count of tasks that finished successfully."""

    failed: "int" = 0
    """Count of tasks that exhausted retries or failed permanently."""

    cancelled: "int" = 0
    """Count of tasks cancelled before or during execution."""

    expired: "int" = 0
    """Count of tasks whose `expires_at` deadline passed before execution started."""

    @property
    def total(self) -> "int":
        """Total number of known queue records."""
        return (
            self.pending + self.scheduled + self.running + self.completed + self.failed + self.cancelled + self.expired
        )


@dataclass(frozen=True, slots=True)
class ScheduleAssertionResult:
    """Summary of recurring schedule assertion and delivery repair."""

    created: "int" = 0
    """Count of missing recurring schedule records newly inserted."""

    repaired: "int" = 0
    """Count of existing recurring schedule records whose external dispatch was repaired."""

    repair: "DispatchRepairResult | None" = None
    """Structured external dispatch repair summary, if external repair ran."""

    @property
    def changed(self) -> "int":
        """Total schedule occurrences created or repaired."""
        return self.created + self.repaired


@dataclass(slots=True)
class StaleTaskRecoveryResult:
    """Summary of stale running task recovery."""

    requeued: "int" = 0
    """Count of stale running tasks returned to `pending` or `scheduled`."""

    failed: "int" = 0
    """Count of stale running tasks marked `failed` after exhausting retries."""

    skipped: "int" = 0
    """Count of stale running tasks left untouched due to policy or active state."""

    handler_needed: "int" = 0
    """Count of permanently failed stale tasks awaiting `on_stale_failure` callbacks."""

    requeued_task_ids: "list[UUID]" = field(default_factory=list)
    """Identifiers of tasks requeued during stale recovery."""

    failed_task_ids: "list[UUID]" = field(default_factory=list)
    """Identifiers of tasks transitioned to `failed` during stale recovery."""

    handler_needed_task_ids: "list[UUID]" = field(default_factory=list)
    """Identifiers of tasks requiring post-recovery failure handler invocation."""

    repair: "DispatchRepairResult | None" = None
    """Structured external dispatch repair summary for requeued external tasks."""

    def to_payload(self) -> "dict[str, int]":
        """Return a JSON-compatible event payload."""
        return {
            "requeued": self.requeued,
            "failed": self.failed,
            "skipped": self.skipped,
            "handler_needed": self.handler_needed,
        }


@dataclass(frozen=True, slots=True)
class TaskReservation:
    """A durable `unique_until="forever"` identity reservation.

    Records only the identity key, the originating task id/name, and the
    reservation time. It never carries args, kwargs, result, or error material.
    Routine terminal and event maintenance never delete it; an explicit
    administrative reset is the only removal path.
    """

    key: "str"
    """Unique task identity key held permanently by this reservation."""

    task_id: "UUID"
    """Identifier of the task record that established the reservation."""

    task_name: "str"
    """Registered task name associated with the reservation."""

    created_at: "datetime"
    """UTC timestamp when the permanent identity reservation was recorded."""


@dataclass(slots=True)
class TaskRequest:
    """A task submission for bulk enqueue via `enqueue_many`.

    Carries the task name and enqueue arguments needed to submit one item in a
    batch. The backend returns the resulting queue records in request order.
    """

    task_name: "str"
    """Registered task name to enqueue."""

    args: "tuple[Any, ...]" = ()
    """Positional arguments serialized with the task record."""

    kwargs: "dict[str, Any] | None" = None
    """Keyword arguments serialized with the task record."""

    queue: "str" = "default"
    """Target logical queue name."""

    priority: "int" = 0
    """Relative task priority (higher values are claimed first)."""

    max_retries: "int" = 0
    """Maximum retry attempts permitted after an execution failure."""

    scheduled_at: "datetime | None" = None
    """Optional UTC earliest execution timestamp."""

    expires_at: "datetime | None" = None
    """Optional UTC deadline after which an unstarted task expires."""

    key: "str | None" = None
    """Optional deduplication or uniqueness identity key."""

    execution_backend: "str" = "local"
    """Target execution transport name."""

    execution_profile: "str | None" = None
    """Optional named execution profile for the target execution backend."""

    metadata: "dict[str, Any] | None" = None
    """Optional JSON-serializable metadata dictionary stored with the record."""

    def __post_init__(self) -> "None":
        if self.scheduled_at is not None:
            self.scheduled_at = _ensure_utc_datetime(self.scheduled_at)
        if self.expires_at is not None:
            self.expires_at = _ensure_utc_datetime(self.expires_at)


@dataclass(slots=True)
class QueuedTaskRecord:
    """Backend-neutral representation of a queued task."""

    task_name: "str"
    """Registered task name to execute."""

    id: "UUID" = field(default_factory=uuid4)
    """Unique identifier for this task record."""

    args: "tuple[Any, ...]" = ()
    """Positional arguments persisted for the task invocation."""

    kwargs: "dict[str, Any]" = field(default_factory=dict)
    """Keyword arguments persisted for the task invocation."""

    queue: "str" = "default"
    """Logical queue name on which the task is scheduled."""

    execution_backend: "str" = "local"
    """Name of the execution transport responsible for running the task."""

    execution_profile: "str | None" = None
    """Optional execution profile name interpreted by the execution backend."""

    execution_ref: "str | None" = None
    """External transport execution reference or reservation sentinel."""

    worker_id: "str | None" = None
    """Identifier of the worker currently claiming or last executing the task."""

    status: "TaskStatus" = "pending"
    """Current lifecycle status of the task record."""

    priority: "int" = 0
    """Scheduling priority (higher integers are claimed before lower integers)."""

    max_retries: "int" = 0
    """Maximum number of retry attempts allowed on failure."""

    retry_count: "int" = 0
    """Number of retries consumed so far; also serves as the lease fence counter."""

    scheduled_at: "datetime | None" = None
    """UTC timestamp when a scheduled task becomes eligible for claiming."""

    expires_at: "datetime | None" = None
    """UTC deadline after which an unstarted task transitions to `expired`."""

    created_at: "datetime" = field(default_factory=lambda: datetime.now(UTC))
    """UTC timestamp when the record object was first created."""

    queued_at: "datetime" = field(default_factory=lambda: datetime.now(UTC))
    """UTC timestamp when the record entered the queue or was last requeued."""

    started_at: "datetime | None" = None
    """UTC timestamp when the current or most recent execution attempt started."""

    completed_at: "datetime | None" = None
    """UTC timestamp when the task reached a terminal status."""

    heartbeat_at: "datetime | None" = None
    """UTC timestamp of the most recent worker heartbeat while running."""

    result: "Any" = None
    """Deserialized return value from a completed task execution."""

    error: "str | None" = None
    """Formatted exception message from the most recent failed attempt."""

    key: "str | None" = None
    """Optional uniqueness or schedule identity key."""

    metadata: "dict[str, Any]" = field(default_factory=dict)
    """Backend-neutral metadata dictionary (retry backoff, trace context, schedule info)."""

    dispatch_checked_at: "datetime | None" = None
    """UTC timestamp when external dispatch reconciliation last inspected this record."""

    def __post_init__(self) -> "None":
        self.created_at = _ensure_utc_datetime(self.created_at)
        self.queued_at = _ensure_utc_datetime(self.queued_at)
        if self.dispatch_checked_at is not None:
            self.dispatch_checked_at = _ensure_utc_datetime(self.dispatch_checked_at)
        if self.scheduled_at is not None:
            self.scheduled_at = _ensure_utc_datetime(self.scheduled_at)
        if self.expires_at is not None:
            self.expires_at = _ensure_utc_datetime(self.expires_at)

    @property
    def is_terminal(self) -> "bool":
        """Whether the record is in a terminal state."""
        return self.status in TERMINAL_STATUSES

    @property
    def is_due(self) -> "bool":
        """Whether the record is eligible to be claimed now."""
        return self.scheduled_at is None or self.scheduled_at <= datetime.now(UTC)

    @property
    def is_expired(self) -> "bool":
        """Whether the not-started deadline has passed."""
        return self.expires_at is not None and self.expires_at <= datetime.now(UTC)


def _ensure_utc_datetime(value: "datetime") -> "datetime":
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)

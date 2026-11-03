"""Typed realtime event models for queue tasks."""

from datetime import UTC, datetime
from typing import Any, Literal, cast
from uuid import uuid4

import msgspec

__all__ = ("QueueEvent", "QueueEventActor", "QueueEventEntityRef", "QueueEventScope", "QueueEventType")

QueueEventScope = Literal["task", "queue", "worker", "global", "custom"]
QueueEventType = Literal[
    "task.started",
    "task.progress",
    "task.log",
    "task.event",
    "task.completed",
    "task.failed",
    "task.cancelled",
    "task.interrupted",
    "task.expired",
    "task.claim_lost",
    "task.stale_failed",
    "worker.heartbeat",
    "worker.stale_recovery",
]


class QueueEventActor(msgspec.Struct, rename="camel", kw_only=True):
    """Actor reference for a queue event."""

    type: "str | None" = None
    """Logical actor category (for example `"user"`, `"system"`, or `"service"`)."""

    id: "str | None" = None
    """Unique identifier of the actor within its category."""

    name: "str | None" = None
    """Optional human-readable display name for the actor."""

    def to_dict(self) -> "dict[str, Any]":
        """Return the camelCase wire mapping for this actor."""
        return cast("dict[str, Any]", msgspec.to_builtins(self))

    @classmethod
    def from_dict(cls, data: "dict[str, Any]") -> "QueueEventActor":
        """Build an actor reference from a camelCase mapping.

        Returns:
            The actor reference.
        """
        return msgspec.convert(data, cls)


class QueueEventEntityRef(msgspec.Struct, rename="camel", kw_only=True):
    """Entity reference for a queue event."""

    type: "str"
    """Domain entity type name (for example `"document"` or `"deployment"`)."""

    id: "str"
    """Unique domain entity identifier."""

    name: "str | None" = None
    """Optional human-readable display name for the entity."""

    def to_dict(self) -> "dict[str, Any]":
        """Return the camelCase wire mapping for this entity reference."""
        return cast("dict[str, Any]", msgspec.to_builtins(self))

    @classmethod
    def from_dict(cls, data: "dict[str, Any]") -> "QueueEventEntityRef":
        """Build an entity reference from a camelCase mapping.

        Returns:
            The entity reference.
        """
        return msgspec.convert(data, cls)


class QueueEvent(msgspec.Struct, rename="camel", kw_only=True):
    """Stable event envelope for queue lifecycle, progress, log, and custom events.

    The wire format is camelCase. Null-valued top-level fields are preserved so
    subscribers can rely on a stable schema for intermediate progress and log
    events. Payload contents are passed through verbatim.
    """

    type: "str"
    """Event type identifier (built-in `QueueEventType` or custom event name)."""

    scope: "QueueEventScope"
    """Routing scope of the event (`"task"`, `"queue"`, `"worker"`, `"global"`, or `"custom"`)."""

    id: "str" = msgspec.field(default_factory=lambda: uuid4().hex)
    """Unique hex UUID identifier for this event envelope."""

    scope_key: "str | None" = None
    """Optional tenant, workspace, or custom routing scope key."""

    task_id: "str | None" = None
    """Identifier of the associated task record, when task-scoped."""

    task_name: "str | None" = None
    """Registered name of the associated task, when task-scoped."""

    queue: "str | None" = None
    """Logical queue name associated with the event."""

    worker_id: "str | None" = None
    """Identifier of the worker emitting the event."""

    execution_backend: "str | None" = None
    """Name of the execution backend running the task."""

    execution_profile: "str | None" = None
    """Optional execution profile name active for the task."""

    attempt: "int | None" = None
    """One-based execution attempt counter."""

    sequence: "int | None" = None
    """Monotonic per-attempt event sequence number."""

    level: "str | None" = None
    """Optional log or event severity level."""

    message: "str | None" = None
    """Optional human-readable message accompanying the event."""

    progress_current: "int | float | None" = None
    """Optional current numeric progress value."""

    progress_total: "int | float | None" = None
    """Optional total numeric progress target."""

    progress_percent: "float | None" = None
    """Optional completion percentage (`0.0`-`100.0`)."""

    actor: "QueueEventActor | None" = None
    """Optional actor attribution reference."""

    entity: "QueueEventEntityRef | None" = None
    """Optional domain entity attribution reference."""

    payload: "dict[str, Any]" = msgspec.field(default_factory=dict)
    """Arbitrary JSON-serializable event payload dictionary."""

    occurred_at: "datetime" = msgspec.field(default_factory=lambda: datetime.now(UTC))
    """UTC timestamp when the event was created."""

    schema_version: "int" = 1
    """Wire envelope schema version (`1`)."""

    event_key: "str | None" = None
    """Optional idempotency or deduplication key for the event."""

    def to_dict(self) -> "dict[str, Any]":
        """Return the stable camelCase JSON-compatible event envelope.

        Null-valued top-level fields are preserved so subscribers can rely on a
        stable schema for intermediate progress and log events. Payload
        contents are passed through verbatim.
        """
        return cast("dict[str, Any]", msgspec.to_builtins(self))

    def to_json(self) -> "bytes":
        """Return the event envelope as camelCase JSON bytes."""
        return msgspec.json.encode(self)

    @classmethod
    def from_dict(cls, data: "dict[str, Any]") -> "QueueEvent":
        """Build an event from a camelCase mapping.

        Returns:
            The queue event.
        """
        return msgspec.convert(data, cls)

    @classmethod
    def from_json(cls, data: "str | bytes | bytearray") -> "QueueEvent":
        """Build an event from camelCase JSON text or bytes.

        Returns:
            The queue event.

        Raises:
            TypeError: If the decoded JSON value is not an object.
        """
        payload = bytes(data) if isinstance(data, bytearray) else data
        decoded = msgspec.json.decode(payload)
        if not isinstance(decoded, dict):
            msg = "Queue event JSON must decode to an object"
            raise TypeError(msg)
        return cls.from_dict(cast("dict[str, Any]", decoded))

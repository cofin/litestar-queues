"""Typed realtime event models for queue tasks."""

from datetime import datetime, timezone
from typing import Any, Literal, cast
from uuid import uuid4

import msgspec

__all__ = (
    "QueueEvent",
    "QueueEventActor",
    "QueueEventEntityRef",
    "QueueEventScope",
    "QueueEventType",
)

QueueEventScope = Literal["task", "queue", "worker", "global", "custom"]
QueueEventType = Literal[
    "task.started",
    "task.progress",
    "task.log",
    "task.event",
    "task.completed",
    "task.failed",
    "task.cancelled",
    "worker.heartbeat",
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class QueueEventActor(msgspec.Struct, rename="camel", kw_only=True):
    """Actor reference for a queue event."""

    type: str | None = None
    id: str | None = None
    name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return the camelCase wire mapping for this actor."""
        return cast("dict[str, Any]", msgspec.to_builtins(self))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueueEventActor":
        """Build an actor reference from a camelCase mapping.

        Returns:
            The actor reference.
        """
        return msgspec.convert(data, cls)


class QueueEventEntityRef(msgspec.Struct, rename="camel", kw_only=True):
    """Entity reference for a queue event."""

    type: str
    id: str
    name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return the camelCase wire mapping for this entity reference."""
        return cast("dict[str, Any]", msgspec.to_builtins(self))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueueEventEntityRef":
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

    type: str
    scope: QueueEventScope
    id: str = msgspec.field(default_factory=lambda: uuid4().hex)
    scope_key: str | None = None
    task_id: str | None = None
    task_name: str | None = None
    queue: str | None = None
    worker_id: str | None = None
    execution_backend: str | None = None
    execution_profile: str | None = None
    attempt: int | None = None
    sequence: int | None = None
    level: str | None = None
    message: str | None = None
    progress_current: int | float | None = None
    progress_total: int | float | None = None
    progress_percent: float | None = None
    actor: QueueEventActor | None = None
    entity: QueueEventEntityRef | None = None
    payload: dict[str, Any] = msgspec.field(default_factory=dict)
    occurred_at: datetime = msgspec.field(default_factory=_utc_now)
    schema_version: int = 1
    event_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return the stable camelCase JSON-compatible event envelope.

        Null-valued top-level fields are preserved so subscribers can rely on a
        stable schema for intermediate progress and log events. Payload
        contents are passed through verbatim.
        """
        return cast("dict[str, Any]", msgspec.to_builtins(self))

    def to_json(self) -> bytes:
        """Return the event envelope as camelCase JSON bytes."""
        from sqlspec.utils.serializers import to_json as _to_json

        return _to_json(self, as_bytes=True)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueueEvent":
        """Build an event from a camelCase mapping.

        Returns:
            The queue event.
        """
        return msgspec.convert(data, cls)

    @classmethod
    def from_json(cls, data: str | bytes | bytearray) -> "QueueEvent":
        """Build an event from camelCase JSON text or bytes.

        Returns:
            The queue event.

        Raises:
            TypeError: If the decoded JSON value is not an object.
        """
        from sqlspec.utils.serializers import from_json as _from_json

        payload = bytes(data) if isinstance(data, bytearray) else data
        decoded = _from_json(payload)
        if not isinstance(decoded, dict):
            msg = "Queue event JSON must decode to an object"
            raise TypeError(msg)
        return cls.from_dict(cast("dict[str, Any]", decoded))

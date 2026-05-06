"""Typed realtime event models for queue tasks."""

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal, cast
from uuid import uuid4

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


def _serialize_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _deserialize_datetime(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass(slots=True)
class QueueEventActor:
    """Actor reference for a queue event."""

    type: str | None = None
    id: str | None = None
    name: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        """Return a JSON-compatible actor mapping."""
        return {"type": self.type, "id": self.id, "name": self.name}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueueEventActor":
        """Build an actor reference from a mapping."""
        return cls(
            type=cast("str | None", data.get("type")),
            id=cast("str | None", data.get("id")),
            name=cast("str | None", data.get("name")),
        )


@dataclass(slots=True)
class QueueEventEntityRef:
    """Entity reference for a queue event."""

    type: str
    id: str
    name: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        """Return a JSON-compatible entity reference mapping."""
        return {"type": self.type, "id": self.id, "name": self.name}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueueEventEntityRef":
        """Build an entity reference from a mapping."""
        return cls(type=str(data["type"]), id=str(data["id"]), name=cast("str | None", data.get("name")))


@dataclass(slots=True)
class QueueEvent:
    """Stable event envelope for queue lifecycle, progress, log, and custom events."""

    type: str
    scope: QueueEventScope
    id: str = field(default_factory=lambda: uuid4().hex)
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
    payload: dict[str, Any] = field(default_factory=dict)
    occurred_at: datetime = field(default_factory=_utc_now)
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        """Return the stable JSON-compatible event envelope.

        Null-valued fields are intentionally preserved so subscribers can rely on
        a stable schema for intermediate progress and log events.
        """
        return {
            "id": self.id,
            "type": self.type,
            "scope": self.scope,
            "scope_key": self.scope_key,
            "task_id": self.task_id,
            "task_name": self.task_name,
            "queue": self.queue,
            "worker_id": self.worker_id,
            "execution_backend": self.execution_backend,
            "execution_profile": self.execution_profile,
            "attempt": self.attempt,
            "sequence": self.sequence,
            "level": self.level,
            "message": self.message,
            "progress_current": self.progress_current,
            "progress_total": self.progress_total,
            "progress_percent": self.progress_percent,
            "actor": self.actor.to_dict() if self.actor is not None else None,
            "entity": self.entity.to_dict() if self.entity is not None else None,
            "payload": self.payload,
            "occurred_at": _serialize_datetime(self.occurred_at),
            "schema_version": self.schema_version,
        }

    def to_json(self) -> str:
        """Return the event envelope as JSON text."""
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueueEvent":
        """Build an event from a JSON-compatible mapping."""
        actor_data = data.get("actor")
        entity_data = data.get("entity")
        occurred_at = data.get("occurred_at")
        return cls(
            id=str(data["id"]),
            type=str(data["type"]),
            scope=cast("QueueEventScope", data["scope"]),
            scope_key=cast("str | None", data.get("scope_key")),
            task_id=cast("str | None", data.get("task_id")),
            task_name=cast("str | None", data.get("task_name")),
            queue=cast("str | None", data.get("queue")),
            worker_id=cast("str | None", data.get("worker_id")),
            execution_backend=cast("str | None", data.get("execution_backend")),
            execution_profile=cast("str | None", data.get("execution_profile")),
            attempt=cast("int | None", data.get("attempt")),
            sequence=cast("int | None", data.get("sequence")),
            level=cast("str | None", data.get("level")),
            message=cast("str | None", data.get("message")),
            progress_current=cast("int | float | None", data.get("progress_current")),
            progress_total=cast("int | float | None", data.get("progress_total")),
            progress_percent=cast("float | None", data.get("progress_percent")),
            actor=QueueEventActor.from_dict(actor_data) if isinstance(actor_data, dict) else None,
            entity=QueueEventEntityRef.from_dict(entity_data) if isinstance(entity_data, dict) else None,
            payload=dict(cast("dict[str, Any]", data.get("payload") or {})),
            occurred_at=_deserialize_datetime(occurred_at) if occurred_at is not None else _utc_now(),
            schema_version=int(data.get("schema_version", 1)),
        )

    @classmethod
    def from_json(cls, data: str | bytes | bytearray) -> "QueueEvent":
        """Build an event from JSON text or bytes."""
        decoded = json.loads(data)
        if not isinstance(decoded, dict):
            msg = "Queue event JSON must decode to an object"
            raise ValueError(msg)
        return cls.from_dict(cast("dict[str, Any]", decoded))

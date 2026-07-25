"""Versioned JSON codec for the ephemeral SQLite backend.

Payloads are prefixed with a private magic/version marker and encoded with
Litestar's JSON serializer. No pickle or executable object codec is used, and no
error message interpolates task arguments, results, metadata, or the database
path.
"""

from datetime import datetime, timezone
from typing import Any, cast
from uuid import UUID

from litestar.exceptions import SerializationException
from litestar.serialization import decode_json, encode_json

from litestar_queues.backends.ephemeral.schema import UNREADABLE_ERROR, EphemeralDatabaseError
from litestar_queues.events.history import QueueEventLogRecord
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.models import QueuedTaskRecord

__all__ = (
    "decode_payload",
    "encode_payload",
    "event_from_payload",
    "event_to_payload",
    "record_from_payload",
    "record_to_payload",
)

MAGIC = b"LQEP\x01"
SCHEMA_VERSION = 1

_SERIALIZATION_ERROR = (
    "The ephemeral SQLite backend requires JSON-serializable task arguments, metadata, events, and results."
)
_RECORD_FIELDS = (
    "task_name",
    "id",
    "args",
    "kwargs",
    "queue",
    "execution_backend",
    "execution_profile",
    "execution_ref",
    "status",
    "priority",
    "max_retries",
    "retry_count",
    "scheduled_at",
    "created_at",
    "started_at",
    "completed_at",
    "heartbeat_at",
    "result",
    "error",
    "key",
    "metadata",
)
_RECORD_DATETIMES = ("scheduled_at", "created_at", "started_at", "completed_at", "heartbeat_at")
_EVENT_FIELDS = (
    "event_id",
    "event_type",
    "task_id",
    "task_name",
    "queue",
    "worker_id",
    "execution_backend",
    "execution_profile",
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
)


def _raise_corruption() -> "Any":
    raise EphemeralDatabaseError(UNREADABLE_ERROR)


def encode_payload(value: "object") -> "bytes":
    """Encode a mapping payload with the private magic prefix.

    Returns:
        The prefixed JSON payload.

    Raises:
        QueueConfigurationError: If the value is not JSON-serializable.
    """
    try:
        return MAGIC + encode_json(value)
    except (SerializationException, TypeError, ValueError):
        raise QueueConfigurationError(_SERIALIZATION_ERROR) from None


def decode_payload(payload: "bytes") -> "dict[str, Any]":
    """Decode a prefixed payload back to its mapping.

    Returns:
        The decoded mapping.

    Raises:
        EphemeralDatabaseError: If the prefix is missing or the body is unreadable.
    """
    if not isinstance(payload, (bytes, bytearray)) or not bytes(payload).startswith(MAGIC):
        _raise_corruption()
    try:
        decoded = decode_json(bytes(payload)[len(MAGIC) :])
    except (SerializationException, TypeError, ValueError):
        raise EphemeralDatabaseError(UNREADABLE_ERROR) from None
    if not isinstance(decoded, dict):
        _raise_corruption()
    return cast("dict[str, Any]", decoded)


def _iso(value: "datetime | None") -> "str | None":
    return None if value is None else value.astimezone(timezone.utc).isoformat()


def _parse_datetime(value: "object") -> "datetime | None":
    if value is None:
        return None
    if not isinstance(value, str):
        _raise_corruption()
    try:
        parsed = datetime.fromisoformat(cast("str", value))
    except ValueError:
        raise EphemeralDatabaseError(UNREADABLE_ERROR) from None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def record_to_payload(record: "QueuedTaskRecord") -> "bytes":
    """Encode one queue record as a canonical schema-1 mapping.

    Returns:
        The encoded payload blob.
    """
    payload: "dict[str, Any]" = {
        "schema_version": SCHEMA_VERSION,
        "task_name": record.task_name,
        "id": str(record.id),
        "args": list(record.args),
        "kwargs": dict(record.kwargs),
        "queue": record.queue,
        "execution_backend": record.execution_backend,
        "execution_profile": record.execution_profile,
        "execution_ref": record.execution_ref,
        "status": record.status,
        "priority": record.priority,
        "max_retries": record.max_retries,
        "retry_count": record.retry_count,
        "result": record.result,
        "error": record.error,
        "key": record.key,
        "metadata": dict(record.metadata),
    }
    for name in _RECORD_DATETIMES:
        payload[name] = _iso(getattr(record, name))
    return encode_payload(payload)


def record_from_payload(payload: "bytes") -> "QueuedTaskRecord":
    """Rebuild a queue record from its schema-1 payload.

    Returns:
        The reconstructed record.
    """
    decoded = decode_payload(payload)
    if decoded.get("schema_version") != SCHEMA_VERSION:
        _raise_corruption()
    missing = [name for name in _RECORD_FIELDS if name not in decoded]
    if missing:
        _raise_corruption()
    try:
        record_id = UUID(str(decoded["id"]))
    except (AttributeError, TypeError, ValueError):
        raise EphemeralDatabaseError(UNREADABLE_ERROR) from None
    args = decoded["args"]
    kwargs = decoded["kwargs"]
    metadata = decoded["metadata"]
    if not isinstance(args, list) or not isinstance(kwargs, dict) or not isinstance(metadata, dict):
        _raise_corruption()
    record = QueuedTaskRecord(
        task_name=decoded["task_name"],
        id=record_id,
        args=tuple(args),
        kwargs=dict(kwargs),
        queue=decoded["queue"],
        execution_backend=decoded["execution_backend"],
        execution_profile=decoded["execution_profile"],
        execution_ref=decoded["execution_ref"],
        status=decoded["status"],
        priority=decoded["priority"],
        max_retries=decoded["max_retries"],
        retry_count=decoded["retry_count"],
        result=decoded["result"],
        error=decoded["error"],
        key=decoded["key"],
        metadata=dict(metadata),
    )
    for name in _RECORD_DATETIMES:
        setattr(record, name, _parse_datetime(decoded[name]))
    return record


def event_to_payload(record: "QueueEventLogRecord") -> "bytes":
    """Encode one event-history record as a canonical schema-1 mapping.

    Returns:
        The encoded payload blob.
    """
    payload: "dict[str, Any]" = {"schema_version": SCHEMA_VERSION}
    for name in _EVENT_FIELDS:
        value = getattr(record, name)
        payload[name] = _iso(value) if isinstance(value, datetime) else value
    payload["detail"] = dict(record.detail)
    return encode_payload(payload)


def event_from_payload(payload: "bytes") -> "QueueEventLogRecord":
    """Rebuild an event-history record from its schema-1 payload.

    Returns:
        The reconstructed event record.
    """
    decoded = decode_payload(payload)
    if decoded.get("schema_version") != SCHEMA_VERSION:
        _raise_corruption()
    missing = [name for name in _EVENT_FIELDS if name not in decoded]
    if missing:
        _raise_corruption()
    occurred_at = _parse_datetime(decoded["occurred_at"])
    created_at = _parse_datetime(decoded["created_at"])
    detail = decoded["detail"]
    if occurred_at is None or created_at is None or not isinstance(detail, dict):
        _raise_corruption()
    values = {name: decoded[name] for name in _EVENT_FIELDS}
    values["detail"] = dict(detail)
    values["occurred_at"] = occurred_at
    values["created_at"] = created_at
    return QueueEventLogRecord(**values)

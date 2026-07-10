"""Internal helpers for backend-managed queue event history records."""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from litestar_queues.events.log import QueueEventLogRecord

if TYPE_CHECKING:
    from litestar_queues.events.models import QueueEvent

__all__ = (
    "event_log_record_from_event",
    "event_log_record_sort_key",
    "optional_float",
    "optional_int",
    "optional_str",
    "parse_datetime",
    "utc_now",
)


def event_log_record_from_event(event: "QueueEvent", *, created_at: "datetime | None" = None) -> "QueueEventLogRecord":
    """Convert a queue event envelope into a backend-neutral history record.

    Returns:
        Backend-neutral event-history record.
    """
    detail = dict(event.payload)
    return QueueEventLogRecord(
        event_id=event.id,
        event_type=event.type,
        task_id=event.task_id,
        task_name=event.task_name,
        queue=event.queue,
        worker_id=event.worker_id,
        execution_backend=event.execution_backend,
        execution_profile=event.execution_profile,
        stage=optional_str(detail.get("stage")),
        level=event.level,
        message=event.message,
        detail=detail,
        progress_current=optional_float(event.progress_current),
        progress_total=optional_float(event.progress_total),
        progress_percent=optional_float(event.progress_percent),
        duration_ms=optional_float(detail.get("duration_ms")),
        sequence=event.sequence,
        occurred_at=parse_datetime(event.occurred_at),
        created_at=created_at or utc_now(),
    )


def event_log_record_sort_key(record: "QueueEventLogRecord") -> "tuple[datetime, int, str]":
    """Return SQLSpec-compatible ascending event-history sort key."""
    return (record.occurred_at, record.sequence if record.sequence is not None else 0, record.event_id)


def parse_datetime(value: "Any") -> "datetime":
    """Return a timezone-aware UTC datetime."""
    if isinstance(value, datetime):
        parsed = value
    else:
        if isinstance(value, bytes | bytearray):
            value = bytes(value).decode()
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def utc_now() -> "datetime":
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc)


def optional_str(value: "Any") -> "str | None":
    """Return string values only."""
    return value if isinstance(value, str) else None


def optional_float(value: "Any") -> "float | None":
    """Return numeric values as floats, excluding booleans."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    return None


def optional_int(value: "Any") -> "int | None":
    """Return optional integer values."""
    if value is None:
        return None
    return int(cast("int", value))

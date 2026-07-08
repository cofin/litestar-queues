"""Live queue-event transport sizing helpers."""

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, cast

import msgspec

if TYPE_CHECKING:
    from litestar_queues.events.models import QueueEvent

__all__ = ("QueueEventSizeEstimator", "estimate_event_payload_bytes", "split_event_batch_by_size")

QueueEventSizeEstimator = Callable[["QueueEvent"], int]


def estimate_event_payload_bytes(event: "QueueEvent") -> int:
    """Return the direct JSON payload size for one queue event."""
    return len(event.to_json())


def split_event_batch_by_size(
    event: "QueueEvent", *, max_bytes: int, size_estimator: QueueEventSizeEstimator = estimate_event_payload_bytes
) -> "tuple[QueueEvent, ...]":
    """Split package-owned batch events into complete QueueEvent payloads.

    Returns:
        The original event when no split is needed, otherwise complete event
        payloads that each fit inside the configured limit.
    """
    if max_bytes < 1:
        msg = "max_bytes must be greater than zero."
        raise ValueError(msg)

    items = _extract_batch_items(event)
    if items is None or size_estimator(event) <= max_bytes:
        return (event,)

    chunks: list[QueueEvent] = []
    current: list[dict[str, Any]] = []
    for item in items:
        candidate = [*current, item]
        if size_estimator(_replace_batch_items(event, candidate)) <= max_bytes:
            current = candidate
            continue
        if current:
            chunks.append(_replace_batch_items(event, current))
            current = []
        single = _replace_batch_items(event, [item])
        if size_estimator(single) > max_bytes:
            msg = "A single queue event batch item exceeds the transport payload limit."
            raise ValueError(msg)
        current = [item]

    if current:
        chunks.append(_replace_batch_items(event, current))
    return tuple(chunks) or (event,)


def _extract_batch_items(event: "QueueEvent") -> "list[dict[str, Any]] | None":
    payload = event.payload
    if payload.get("batch") is not True:
        return None
    items = payload.get("items")
    if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
        return None
    return cast("list[dict[str, Any]]", items)


def _replace_batch_items(event: "QueueEvent", items: "Sequence[dict[str, Any]]") -> "QueueEvent":
    from litestar_queues.events.models import QueueEvent

    data = cast("dict[str, Any]", msgspec.to_builtins(event))
    payload = dict(cast("dict[str, Any]", data.get("payload") or {}))
    payload["items"] = list(items)
    payload["count"] = len(items)
    data["payload"] = payload
    return QueueEvent.from_dict(data)

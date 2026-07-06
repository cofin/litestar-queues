import base64
from typing import TYPE_CHECKING, Any

import msgspec
import pytest

from litestar_queues.events import QueueEvent, QueueEventPublisher
from litestar_queues.events.chunking import estimate_event_payload_bytes, split_event_batch_by_size
from litestar_queues.events.litestar import ChannelsQueueEventSink

if TYPE_CHECKING:
    from collections.abc import Sequence

pytestmark = pytest.mark.anyio


class _RecordingChannelsBackend:
    def __init__(self) -> None:
        self.published: list[tuple[bytes | str, tuple[str, ...]]] = []

    def publish(self, data: bytes | str, channels: "Sequence[str]") -> None:
        self.published.append((data, tuple(channels)))


class _RecordingEventLog:
    def __init__(self) -> None:
        self.events: list[QueueEvent] = []

    async def publish_event(self, event: QueueEvent) -> None:
        self.events.append(event)

    async def flush_events(self) -> None:
        return None

    async def list_events(
        self, *, task_id: str | None = None, task_name: str | None = None, limit: int | None = None
    ) -> list[Any]:
        return []

    async def summarize_stages(self, *, task_name: str | None = None) -> list[Any]:
        return []

    async def cleanup_before(self, before: Any) -> int:
        return 0


def test_size_estimator_can_account_for_transport_wrapper() -> None:
    event = QueueEvent(
        type="task.event", scope="task", task_id="task-1", payload={"message": "loaded", "duration_ms": 7}
    )

    assert _estimate_wrapped_event_bytes(event) > estimate_event_payload_bytes(event)


def test_split_event_batch_by_size_preserves_complete_events() -> None:
    items = [{"message": "a" * 100}, {"message": "b" * 100}, {"message": "c" * 100}]
    event = QueueEvent(
        type="task.event",
        scope="task",
        task_id="task-1",
        event_key="batch-1",
        payload={"batch": True, "count": len(items), "items": items, "source": "test"},
    )
    single_item_event = QueueEvent(
        type="task.event",
        scope="task",
        task_id="task-1",
        event_key="batch-1",
        payload={"batch": True, "count": 1, "items": [items[0]], "source": "test"},
    )
    max_bytes = _estimate_wrapped_event_bytes(single_item_event) + 16

    chunks = split_event_batch_by_size(event, max_bytes=max_bytes, size_estimator=_estimate_wrapped_event_bytes)

    assert len(chunks) > 1
    assert all(isinstance(chunk, QueueEvent) for chunk in chunks)
    assert {(chunk.type, chunk.scope, chunk.task_id, chunk.event_key) for chunk in chunks} == {
        ("task.event", "task", "task-1", "batch-1")
    }
    flattened_items = [item for chunk in chunks for item in chunk.payload["items"]]
    assert flattened_items == items
    assert all(chunk.payload["count"] == len(chunk.payload["items"]) for chunk in chunks)
    assert all(_estimate_wrapped_event_bytes(chunk) <= max_bytes for chunk in chunks)


def test_split_event_batch_by_size_leaves_ordinary_events_alone() -> None:
    event = QueueEvent(type="task.log", scope="task", task_id="task-1", payload={"items": [{"message": "plain"}]})

    assert split_event_batch_by_size(event, max_bytes=1) == (event,)


async def test_channels_sink_publishes_each_batch_chunk() -> None:
    items = [{"message": "a" * 100}, {"message": "b" * 100}, {"message": "c" * 100}]
    event = QueueEvent(
        type="task.event", scope="task", task_id="task-1", payload={"batch": True, "count": len(items), "items": items}
    )
    single_item_event = QueueEvent(
        type="task.event", scope="task", task_id="task-1", payload={"batch": True, "count": 1, "items": [items[0]]}
    )
    backend = _RecordingChannelsBackend()
    sink = ChannelsQueueEventSink(
        backend,
        max_payload_bytes=_estimate_wrapped_event_bytes(single_item_event) + 16,
        payload_size_estimator=_estimate_wrapped_event_bytes,
    )

    await sink.publish(event, channels=["events"])

    assert len(backend.published) > 1
    decoded = [QueueEvent.from_json(data) for data, _ in backend.published]
    assert [item for chunk in decoded for item in chunk.payload["items"]] == items
    assert all(decoded_event.payload["batch"] is True for decoded_event in decoded)
    assert all("chunk" not in decoded_event.payload for decoded_event in decoded)
    assert {channels for _, channels in backend.published} == {("events",)}


async def test_publisher_records_original_event_before_live_chunking() -> None:
    items = [{"message": "a" * 100}, {"message": "b" * 100}, {"message": "c" * 100}]
    event = QueueEvent(
        type="task.event", scope="task", task_id="task-1", payload={"batch": True, "count": len(items), "items": items}
    )
    single_item_event = QueueEvent(
        type="task.event", scope="task", task_id="task-1", payload={"batch": True, "count": 1, "items": [items[0]]}
    )
    backend = _RecordingChannelsBackend()
    event_log = _RecordingEventLog()
    sink = ChannelsQueueEventSink(
        backend,
        max_payload_bytes=_estimate_wrapped_event_bytes(single_item_event) + 16,
        payload_size_estimator=_estimate_wrapped_event_bytes,
    )

    await QueueEventPublisher(sink, event_log=event_log).publish(event, channels=["events"])

    assert event_log.events == [event]
    assert event_log.events[0].payload["items"] == items
    assert len(backend.published) > 1


async def test_single_oversized_batch_item_raises_and_publisher_strict_controls_propagation() -> None:
    event = QueueEvent(
        type="task.event",
        scope="task",
        task_id="task-1",
        payload={"batch": True, "count": 1, "items": [{"message": "x" * 100}]},
    )
    max_bytes = _estimate_wrapped_event_bytes(event) - 1
    sink = ChannelsQueueEventSink(
        _RecordingChannelsBackend(), max_payload_bytes=max_bytes, payload_size_estimator=_estimate_wrapped_event_bytes
    )

    with pytest.raises(ValueError, match="exceeds the transport payload limit"):
        await sink.publish(event, channels=["events"])

    await QueueEventPublisher(sink, strict=False).publish(event, channels=["events"])
    with pytest.raises(ValueError, match="exceeds the transport payload limit"):
        await QueueEventPublisher(sink, strict=True).publish(event, channels=["events"])


def _estimate_wrapped_event_bytes(event: QueueEvent) -> int:
    payload = {
        "payload": {"data_b64": base64.b64encode(event.to_json()).decode("ascii")},
        "metadata": {"transport": "test"},
    }
    return len(msgspec.json.encode(payload))

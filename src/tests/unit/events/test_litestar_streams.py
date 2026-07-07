import base64
import json
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast

import msgspec
import pytest

from litestar_queues.events import QueueEvent
from litestar_queues.events.litestar import ChannelsQueueEventSink, stream_queue_events

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

pytestmark = pytest.mark.anyio


class FakeSubscriber:
    def __init__(self, events: "Sequence[bytes]") -> "None":
        self._events = events

    async def iter_events(self) -> "AsyncIterator[bytes]":
        for event in self._events:
            yield event


class FakeChannelsPlugin:
    def __init__(self, events: "Sequence[bytes]") -> "None":
        self.events = events
        self.subscribed_channels: "list[str] | None" = None
        self.history: "int | None" = None
        self.closed = False

    @asynccontextmanager
    async def start_subscription(
        self, channels: "Sequence[str]", history: "int | None" = None
    ) -> "AsyncIterator[FakeSubscriber]":
        self.subscribed_channels = list(channels)
        self.history = history
        try:
            yield FakeSubscriber(self.events)
        finally:
            self.closed = True


class FakeSocket:
    def __init__(self, channels_plugin: "FakeChannelsPlugin") -> "None":
        self.channels_plugin = channels_plugin
        self.accepted = False
        self.sent_json: "list[dict[str, object]]" = []

    async def accept(self) -> "None":
        self.accepted = True

    async def send_json(self, data: "dict[str, object]") -> "None":
        self.sent_json.append(data)


async def test_stream_queue_events_subscribes_and_skips_malformed_and_duplicates() -> "None":
    event = QueueEvent(type="task.progress", scope="task", task_id="task-1", message="working")
    plugin = FakeChannelsPlugin([
        b"not-json",
        event.to_json(),
        event.to_json(),
        json.dumps({"id": "not-an-event"}).encode(),
    ])
    socket = FakeSocket(plugin)

    await stream_queue_events(socket, ["litestar_queues:task:task_1:events"], history=5, channels_backend=plugin)

    assert socket.accepted
    assert plugin.subscribed_channels == ["litestar_queues:task:task_1:events"]
    assert plugin.history == 5
    assert plugin.closed
    assert socket.sent_json == [event.to_dict()]


async def test_stream_queue_events_dedups_by_event_key() -> "None":
    """Two events with the same event_key but different ids emit only once."""
    first = QueueEvent(type="task.progress", scope="task", id="evt-1", task_id="task-1", event_key="dedup-1")
    second = QueueEvent(type="task.progress", scope="task", id="evt-2", task_id="task-1", event_key="dedup-1")
    plugin = FakeChannelsPlugin([first.to_json(), second.to_json()])
    socket = FakeSocket(plugin)

    await stream_queue_events(socket, ["litestar_queues:task:task_1:events"], channels_backend=plugin)

    assert len(socket.sent_json) == 1
    assert socket.sent_json[0]["id"] == "evt-1"
    assert socket.sent_json[0]["eventKey"] == "dedup-1"


async def test_stream_queue_events_falls_back_to_event_id_when_no_event_key() -> "None":
    """Events with no event_key still dedup on event.id."""
    event_a = QueueEvent(type="task.progress", scope="task", id="evt-a", task_id="task-1")
    event_b = QueueEvent(type="task.progress", scope="task", id="evt-b", task_id="task-1")
    plugin = FakeChannelsPlugin([
        event_a.to_json(),
        event_a.to_json(),  # duplicate of evt-a
        event_b.to_json(),
    ])
    socket = FakeSocket(plugin)

    await stream_queue_events(socket, ["litestar_queues:task:task_1:events"], channels_backend=plugin)

    sent_ids = [payload["id"] for payload in socket.sent_json]
    assert sent_ids == ["evt-a", "evt-b"]


async def test_stream_queue_events_dedup_cache_is_bounded(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """Old dedup keys should age out instead of growing for the socket lifetime."""
    from litestar_queues.events import litestar as litestar_events

    monkeypatch.setattr(litestar_events, "_STREAM_DEDUP_MAX_KEYS", 2)
    first = QueueEvent(type="task.progress", scope="task", id="evt-a", task_id="task-1")
    second = QueueEvent(type="task.progress", scope="task", id="evt-b", task_id="task-1")
    third = QueueEvent(type="task.progress", scope="task", id="evt-c", task_id="task-1")
    plugin = FakeChannelsPlugin([first.to_json(), second.to_json(), third.to_json(), first.to_json()])
    socket = FakeSocket(plugin)

    await stream_queue_events(socket, ["litestar_queues:task:task_1:events"], channels_backend=plugin)

    sent_ids = [payload["id"] for payload in socket.sent_json]
    assert sent_ids == ["evt-a", "evt-b", "evt-c", "evt-a"]


async def test_publish_many_groups_single_channel_into_one_call() -> "None":
    backend = _BatchPublishingChannelsBackend()
    sink = ChannelsQueueEventSink(cast("Any", backend))
    first = QueueEvent(type="task.progress", scope="task", task_id="task-1")
    second = QueueEvent(type="task.log", scope="task", task_id="task-1")

    await sink.publish_many(((first, ("events",)), (second, ("events",))))

    assert len(backend.published_many) == 1
    payloads, channels = backend.published_many[0]
    assert channels == ("events",)
    assert [QueueEvent.from_json(payload).type for payload in payloads] == ["task.progress", "task.log"]


async def test_publish_many_fallback_loops_when_no_batch_api() -> "None":
    backend = _WaitPublishingChannelsBackend()
    sink = ChannelsQueueEventSink(backend)
    first = QueueEvent(type="task.progress", scope="task", task_id="task-1")
    second = QueueEvent(type="task.log", scope="task", task_id="task-1")

    await sink.publish_many(((first, ("events",)), (second, ("events",))))

    assert len(backend.published) == 2
    assert [QueueEvent.from_json(payload).type for payload, _ in backend.published] == ["task.progress", "task.log"]
    assert {channels for _, channels in backend.published} == {("events",)}


async def test_publish_many_honours_max_payload_bytes() -> "None":
    items = [{"message": "a" * 100}, {"message": "b" * 100}, {"message": "c" * 100}]
    event = QueueEvent(
        type="task.event", scope="task", task_id="task-1", payload={"batch": True, "count": len(items), "items": items}
    )
    single_item_event = QueueEvent(
        type="task.event", scope="task", task_id="task-1", payload={"batch": True, "count": 1, "items": [items[0]]}
    )
    max_bytes = _estimate_wrapped_event_bytes(single_item_event) + 16
    backend = _BatchPublishingChannelsBackend()
    sink = ChannelsQueueEventSink(
        cast("Any", backend), max_payload_bytes=max_bytes, payload_size_estimator=_estimate_wrapped_event_bytes
    )

    await sink.publish_many(((event, ("events",)),))

    [(payloads, channels)] = backend.published_many
    assert channels == ("events",)
    assert len(payloads) > 1
    decoded = [QueueEvent.from_json(payload) for payload in payloads]
    assert [item for chunk in decoded for item in chunk.payload["items"]] == items
    assert all(_estimate_wrapped_event_bytes(decoded_event) <= max_bytes for decoded_event in decoded)


class _BatchPublishingChannelsBackend:
    def __init__(self) -> None:
        self.published_many: "list[tuple[tuple[bytes | str, ...], tuple[str, ...]]]" = []

    def wait_published_many(self, data: "Sequence[bytes | str]", channels: "Sequence[str]") -> None:
        self.published_many.append((tuple(data), tuple(channels)))


class _WaitPublishingChannelsBackend:
    def __init__(self) -> None:
        self.published: "list[tuple[bytes | str, tuple[str, ...]]]" = []

    def wait_published(self, data: "bytes | str", channels: "Sequence[str]") -> None:
        self.published.append((data, tuple(channels)))


def _estimate_wrapped_event_bytes(event: "QueueEvent") -> "int":
    payload = {
        "payload": {"data_b64": base64.b64encode(event.to_json()).decode("ascii")},
        "metadata": {"transport": "test"},
    }
    return len(msgspec.json.encode(payload))

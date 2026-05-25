import json
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager

import pytest

from litestar_queues.events import QueueEvent
from litestar_queues.events.litestar import stream_queue_events

pytestmark = pytest.mark.anyio


class FakeSubscriber:
    def __init__(self, events: Sequence[bytes]) -> None:
        self._events = events

    async def iter_events(self) -> AsyncIterator[bytes]:
        for event in self._events:
            yield event


class FakeChannelsPlugin:
    def __init__(self, events: Sequence[bytes]) -> None:
        self.events = events
        self.subscribed_channels: list[str] | None = None
        self.history: int | None = None
        self.closed = False

    @asynccontextmanager
    async def start_subscription(
        self, channels: Sequence[str], history: int | None = None
    ) -> AsyncIterator[FakeSubscriber]:
        self.subscribed_channels = list(channels)
        self.history = history
        try:
            yield FakeSubscriber(self.events)
        finally:
            self.closed = True


class FakeSocket:
    def __init__(self, channels_plugin: FakeChannelsPlugin) -> None:
        self.channels_plugin = channels_plugin
        self.accepted = False
        self.sent_json: list[dict[str, object]] = []

    async def accept(self) -> None:
        self.accepted = True

    async def send_json(self, data: dict[str, object]) -> None:
        self.sent_json.append(data)


async def test_stream_queue_events_subscribes_and_skips_malformed_and_duplicates() -> None:
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


async def test_stream_queue_events_dedups_by_event_key() -> None:
    """Two events with the same event_key but different ids emit only once."""
    first = QueueEvent(type="task.progress", scope="task", id="evt-1", task_id="task-1", event_key="dedup-1")
    second = QueueEvent(type="task.progress", scope="task", id="evt-2", task_id="task-1", event_key="dedup-1")
    plugin = FakeChannelsPlugin([first.to_json(), second.to_json()])
    socket = FakeSocket(plugin)

    await stream_queue_events(socket, ["litestar_queues:task:task_1:events"], channels_backend=plugin)

    assert len(socket.sent_json) == 1
    assert socket.sent_json[0]["id"] == "evt-1"
    assert socket.sent_json[0]["eventKey"] == "dedup-1"


async def test_stream_queue_events_falls_back_to_event_id_when_no_event_key() -> None:
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

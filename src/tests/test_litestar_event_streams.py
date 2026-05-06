import json
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

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
        self.sent_json: list[dict[str, Any]] = []

    async def accept(self) -> None:
        self.accepted = True

    async def send_json(self, data: dict[str, Any]) -> None:
        self.sent_json.append(data)


async def test_stream_queue_events_subscribes_and_skips_malformed_and_duplicates() -> None:
    event = QueueEvent(type="task.progress", scope="task", task_id="task-1", message="working")
    plugin = FakeChannelsPlugin([
        b"not-json",
        event.to_json().encode(),
        event.to_json().encode(),
        json.dumps({"id": "not-an-event"}).encode(),
    ])
    socket = FakeSocket(plugin)

    await stream_queue_events(socket, ["litestar_queues:task:task_1:events"], history=5, channels_backend=plugin)

    assert socket.accepted
    assert plugin.subscribed_channels == ["litestar_queues:task:task_1:events"]
    assert plugin.history == 5
    assert plugin.closed
    assert socket.sent_json == [event.to_dict()]

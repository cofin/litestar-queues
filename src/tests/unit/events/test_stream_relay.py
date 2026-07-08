import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from litestar_queues.events import QueueEvent
from litestar_queues.events.streaming import stream_queue_events_hardened

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

pytestmark = pytest.mark.anyio


class _FakeSubscriber:
    def __init__(self, events: "Sequence[bytes]", *, delay_between_events: float = 0.0) -> None:
        self._events = events
        self._delay_between_events = delay_between_events

    async def iter_events(self) -> "AsyncIterator[bytes]":
        for index, event in enumerate(self._events):
            if index:
                await asyncio.sleep(self._delay_between_events)
            yield event


class _FakeChannelsPlugin:
    def __init__(self, events: "Sequence[bytes]", *, delay_between_events: float = 0.0) -> None:
        self.events = events
        self.delay_between_events = delay_between_events
        self.closed = False
        self.history: int | None = None
        self.subscribed_channels: list[str] | None = None

    @asynccontextmanager
    async def start_subscription(
        self, channels: "Sequence[str]", history: int | None = None
    ) -> "AsyncIterator[_FakeSubscriber]":
        self.history = history
        self.subscribed_channels = list(channels)
        try:
            yield _FakeSubscriber(self.events, delay_between_events=self.delay_between_events)
        finally:
            self.closed = True


class _RecordingSocket:
    def __init__(self) -> None:
        self.accepted = False
        self.sent_json: list[dict[str, object]] = []
        self._sending = False
        self.reentered_send = False

    async def accept(self) -> None:
        self.accepted = True

    async def send_json(self, data: dict[str, object]) -> None:
        if self._sending:
            self.reentered_send = True
            msg = "send_json was re-entered"
            raise AssertionError(msg)
        self._sending = True
        try:
            await asyncio.sleep(0)
            self.sent_json.append(data)
        finally:
            self._sending = False


async def test_hardened_relay_emits_heartbeat_between_events() -> None:
    first = QueueEvent(type="task.progress", scope="task", id="evt-1", task_id="task-1")
    second = QueueEvent(type="task.progress", scope="task", id="evt-2", task_id="task-1")
    plugin = _FakeChannelsPlugin([first.to_json(), second.to_json()], delay_between_events=0.03)
    socket = _RecordingSocket()

    await stream_queue_events_hardened(
        socket, ["litestar_queues:task:task_1:events"], history=4, channels_backend=plugin, heartbeat_interval=0.005
    )

    assert socket.accepted
    assert plugin.subscribed_channels == ["litestar_queues:task:task_1:events"]
    assert plugin.history == 4
    assert plugin.closed
    assert not socket.reentered_send
    assert socket.sent_json[0]["id"] == "evt-1"
    second_index = next(index for index, payload in enumerate(socket.sent_json) if payload.get("id") == "evt-2")
    ping_indices = [index for index, payload in enumerate(socket.sent_json) if payload == {"type": "ping"}]
    assert any(0 < index < second_index for index in ping_indices)


async def test_hardened_relay_cancels_heartbeat_when_event_stream_ends() -> None:
    plugin = _FakeChannelsPlugin([])
    socket = _RecordingSocket()

    await stream_queue_events_hardened(
        socket, ["litestar_queues:task:task_1:events"], channels_backend=plugin, heartbeat_interval=0.005
    )
    sent_count = len(socket.sent_json)
    await asyncio.sleep(0.02)

    assert socket.accepted
    assert plugin.closed
    assert len(socket.sent_json) == sent_count


def test_no_taskgroup_in_shipped_streaming_code() -> None:
    streaming_source = Path("src/litestar_queues/events/streaming.py").read_text()

    assert "TaskGroup" not in streaming_source

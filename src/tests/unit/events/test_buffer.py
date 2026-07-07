import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from litestar_queues.events import EventBufferConfig, QueueEvent
from litestar_queues.exceptions import QueueEventBufferFull

if TYPE_CHECKING:
    from collections.abc import Sequence

pytestmark = pytest.mark.anyio


class _RecordingSink:
    def __init__(self) -> "None":
        self.published: "list[tuple[QueueEvent, tuple[str, ...]]]" = []
        self.published_event = asyncio.Event()

    async def publish(self, event: "QueueEvent", channels: "Sequence[str]") -> "None":
        self.published.append((event, tuple(channels)))
        self.published_event.set()

    @property
    def event_types(self) -> "list[str]":
        return [event.type for event, _ in self.published]


def _event(event_type: "str", *, task_id: "str | None" = "task-a", scope: "str" = "task") -> "QueueEvent":
    return QueueEvent(type=event_type, scope=scope, task_id=task_id, scope_key=None if scope == "task" else task_id)


def _ignore_drop(_scope: "str") -> "None":
    return None


async def test_add_below_size_does_not_flush() -> "None":
    from litestar_queues.events.buffer import LiveEventBuffer

    sink = _RecordingSink()
    buffer = LiveEventBuffer(EventBufferConfig(buffer_size=3), sink_publish=sink.publish, record_drop=_ignore_drop)

    await buffer.add(_event("task.progress"), ("tasks",))
    await buffer.add(_event("task.log"), ("tasks",))

    assert sink.published == []

    await buffer.flush()

    assert sink.event_types == ["task.progress", "task.log"]


async def test_size_threshold_triggers_eager_flush() -> "None":
    from litestar_queues.events.buffer import LiveEventBuffer

    sink = _RecordingSink()
    buffer = LiveEventBuffer(EventBufferConfig(buffer_size=2), sink_publish=sink.publish, record_drop=_ignore_drop)

    await buffer.add(_event("task.progress"), ("tasks",))
    await buffer.add(_event("task.log"), ("tasks",))

    assert sink.event_types == ["task.progress", "task.log"]


async def test_task_scoped_flush_drains_only_that_task() -> "None":
    from litestar_queues.events.buffer import LiveEventBuffer

    sink = _RecordingSink()
    buffer = LiveEventBuffer(EventBufferConfig(buffer_size=10), sink_publish=sink.publish, record_drop=_ignore_drop)

    await buffer.add(_event("task.progress.1", task_id="task-a"), ("task-a",))
    await buffer.add(_event("task.progress.2", task_id="task-b"), ("task-b",))
    await buffer.add(_event("task.log", task_id="task-a"), ("task-a",))

    await buffer.flush(key="task-a")

    assert sink.event_types == ["task.progress.1", "task.log"]

    await buffer.flush()

    assert sink.event_types == ["task.progress.1", "task.log", "task.progress.2"]


async def test_drop_oldest_drops_and_records_metric(caplog: "pytest.LogCaptureFixture") -> "None":
    from litestar_queues.events.buffer import LiveEventBuffer

    drops: "list[str]" = []
    sink = _RecordingSink()
    buffer = LiveEventBuffer(
        EventBufferConfig(buffer_size=10, max_pending=2, overflow="drop_oldest"),
        sink_publish=sink.publish,
        record_drop=drops.append,
    )

    await buffer.add(_event("one"), ("tasks",))
    await buffer.add(_event("two"), ("tasks",))
    await buffer.add(_event("three"), ("tasks",))
    await buffer.add(_event("four"), ("tasks",))
    await buffer.flush()

    assert sink.event_types == ["three", "four"]
    assert drops == ["task", "task"]
    assert caplog.text.count("Queue event buffer full; dropping event") == 1


async def test_drop_newest_refuses_incoming() -> "None":
    from litestar_queues.events.buffer import LiveEventBuffer

    drops: "list[str]" = []
    sink = _RecordingSink()
    buffer = LiveEventBuffer(
        EventBufferConfig(buffer_size=10, max_pending=2, overflow="drop_newest"),
        sink_publish=sink.publish,
        record_drop=drops.append,
    )

    await buffer.add(_event("one"), ("tasks",))
    await buffer.add(_event("two"), ("tasks",))
    await buffer.add(_event("three"), ("tasks",))
    await buffer.flush()

    assert sink.event_types == ["one", "two"]
    assert drops == ["task"]


async def test_error_policy_raises() -> "None":
    from litestar_queues.events.buffer import LiveEventBuffer

    sink = _RecordingSink()
    buffer = LiveEventBuffer(
        EventBufferConfig(buffer_size=10, max_pending=1, overflow="error"),
        sink_publish=sink.publish,
        record_drop=_ignore_drop,
    )

    await buffer.add(_event("one"), ("tasks",))

    with pytest.raises(QueueEventBufferFull):
        await buffer.add(_event("two"), ("tasks",))


async def test_block_waits_on_flush_not_caller() -> "None":
    from litestar_queues.events.buffer import LiveEventBuffer

    sink = _RecordingSink()
    buffer = LiveEventBuffer(
        EventBufferConfig(buffer_size=10, max_pending=1, overflow="block"),
        sink_publish=sink.publish,
        record_drop=_ignore_drop,
    )

    await buffer.add(_event("one"), ("tasks",))
    blocked_add = asyncio.create_task(buffer.add(_event("two"), ("tasks",)))
    await asyncio.sleep(0)

    assert not blocked_add.done()

    await buffer.flush()
    await asyncio.wait_for(blocked_add, timeout=1)
    await buffer.flush()

    assert sink.event_types == ["one", "two"]


async def test_interval_flush() -> "None":
    from litestar_queues.events.buffer import LiveEventBuffer

    sink = _RecordingSink()
    buffer = LiveEventBuffer(
        EventBufferConfig(buffer_size=10, flush_interval=0.01), sink_publish=sink.publish, record_drop=_ignore_drop
    )

    buffer.start()
    try:
        await buffer.add(_event("one"), ("tasks",))
        await asyncio.wait_for(sink.published_event.wait(), timeout=1)
    finally:
        await buffer.stop()

    assert sink.event_types == ["one"]


async def test_stop_drains_remainder_before_return() -> "None":
    from litestar_queues.events.buffer import LiveEventBuffer

    sink = _RecordingSink()
    buffer = LiveEventBuffer(EventBufferConfig(buffer_size=10), sink_publish=sink.publish, record_drop=_ignore_drop)

    await buffer.add(_event("one"), ("tasks",))
    await buffer.stop()

    assert sink.event_types == ["one"]


def test_no_taskgroup() -> None:
    assert "TaskGroup" not in Path("src/litestar_queues/events/buffer.py").read_text()

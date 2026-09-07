import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from litestar_queues.events import EventBufferConfig, QueueEvent, QueueEventPublisher, QueueEventScope
from litestar_queues.exceptions import QueueEventBufferFull

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

pytestmark = pytest.mark.anyio


class _RecordingSink:
    def __init__(self) -> "None":
        self.published: "list[tuple[QueueEvent, tuple[str, ...]]]" = []
        self.published_event = asyncio.Event()

    async def publish(self, event: "QueueEvent", channels: "Sequence[str]") -> "None":
        self.published.append((event, tuple(channels)))
        self.published_event.set()

    async def publish_many(self, batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> "None":
        for event, channels in batch:
            await self.publish(event, channels=channels)

    @property
    def event_types(self) -> "list[str]":
        return [event.type for event, _ in self.published]


def _event(event_type: "str", *, task_id: "str | None" = "task-a", scope: "QueueEventScope" = "task") -> "QueueEvent":
    return QueueEvent(type=event_type, scope=scope, task_id=task_id, scope_key=None if scope == "task" else task_id)


def _ignore_drop(_scope: "str") -> "None":
    return None


async def test_add_below_size_does_not_flush() -> "None":
    from litestar_queues.events.buffer import LiveEventBuffer

    sink = _RecordingSink()
    buffer = LiveEventBuffer(EventBufferConfig(batch_size=3), sink_publish=sink.publish_many, record_drop=_ignore_drop)

    await buffer.add(_event("task.progress"), ("tasks",))
    await buffer.add(_event("task.log"), ("tasks",))

    assert sink.published == []

    await buffer.flush()

    assert sink.event_types == ["task.progress", "task.log"]


async def test_size_threshold_triggers_eager_flush() -> "None":
    from litestar_queues.events.buffer import LiveEventBuffer

    sink = _RecordingSink()
    buffer = LiveEventBuffer(EventBufferConfig(batch_size=2), sink_publish=sink.publish_many, record_drop=_ignore_drop)

    await buffer.add(_event("task.progress"), ("tasks",))
    await buffer.add(_event("task.log"), ("tasks",))

    assert sink.event_types == ["task.progress", "task.log"]


async def test_task_scoped_flush_drains_only_that_task() -> "None":
    from litestar_queues.events.buffer import LiveEventBuffer

    sink = _RecordingSink()
    buffer = LiveEventBuffer(EventBufferConfig(batch_size=10), sink_publish=sink.publish_many, record_drop=_ignore_drop)

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
        EventBufferConfig(batch_size=10, max_pending=2, overflow="drop_oldest"),
        sink_publish=sink.publish_many,
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
        EventBufferConfig(batch_size=10, max_pending=2, overflow="drop_newest"),
        sink_publish=sink.publish_many,
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
        EventBufferConfig(batch_size=10, max_pending=1, overflow="error"),
        sink_publish=sink.publish_many,
        record_drop=_ignore_drop,
    )

    await buffer.add(_event("one"), ("tasks",))

    with pytest.raises(QueueEventBufferFull):
        await buffer.add(_event("two"), ("tasks",))


async def test_block_waits_on_flush_not_caller() -> "None":
    from litestar_queues.events.buffer import LiveEventBuffer

    sink = _RecordingSink()
    buffer = LiveEventBuffer(
        EventBufferConfig(batch_size=10, max_pending=1, overflow="block"),
        sink_publish=sink.publish_many,
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
        EventBufferConfig(batch_size=10, flush_interval=0.01), sink_publish=sink.publish_many, record_drop=_ignore_drop
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
    buffer = LiveEventBuffer(EventBufferConfig(batch_size=10), sink_publish=sink.publish_many, record_drop=_ignore_drop)

    await buffer.add(_event("one"), ("tasks",))
    await buffer.stop()

    assert sink.event_types == ["one"]


async def test_flush_uses_publish_many_when_available() -> "None":
    sink = _BatchAwareSink()
    publisher = QueueEventPublisher(
        sink, buffer_config=EventBufferConfig(batch_size=10, flush_interval=60), publish_global_lifecycle=False
    )

    await publisher.publish(_event("task.progress"))
    await publisher.publish(_event("task.log"))
    await publisher.flush_buffer()

    assert sink.published == []
    assert len(sink.published_many) == 1
    batch = sink.published_many[0]
    assert [event.type for event, _ in batch] == ["task.progress", "task.log"]


async def test_flush_falls_back_to_publish_loop() -> "None":
    sink = _PublishOnlySink()
    publisher = QueueEventPublisher(
        sink, buffer_config=EventBufferConfig(batch_size=10, flush_interval=60), publish_global_lifecycle=False
    )

    await publisher.publish(_event("task.progress"))
    await publisher.publish(_event("task.log"))
    await publisher.flush_buffer()

    assert [event.type for event, _ in sink.published] == ["task.progress", "task.log"]


def test_no_taskgroup() -> None:
    assert "TaskGroup" not in Path("src/litestar_queues/events/buffer.py").read_text()


class _BatchAwareSink:
    def __init__(self) -> None:
        self.published: "list[tuple[QueueEvent, tuple[str, ...]]]" = []
        self.published_many: "list[tuple[tuple[QueueEvent, tuple[str, ...]], ...]]" = []

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> "None":
        self.published.append((event, tuple(channels)))

    async def publish_many(self, batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> "None":
        self.published_many.append(tuple((event, tuple(channels)) for event, channels in batch))


class _PublishOnlySink:
    def __init__(self) -> None:
        self.published: "list[tuple[QueueEvent, tuple[str, ...]]]" = []

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> "None":
        self.published.append((event, tuple(channels)))


async def test_scoped_flush_waits_for_extracted_batch_to_finish() -> None:
    from litestar_queues.events.buffer import LiveEventBuffer

    entered = asyncio.Event()
    finish = asyncio.Event()
    delivered: "list[str]" = []

    async def publish(batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> None:
        entered.set()
        await finish.wait()
        delivered.extend(event.type for event, _ in batch)

    buffer = LiveEventBuffer(EventBufferConfig(batch_size=10), sink_publish=publish, record_drop=_ignore_drop)
    await buffer.add(_event("task.progress"), ("task-a",))
    first = asyncio.create_task(buffer.flush())
    await entered.wait()
    second = asyncio.create_task(buffer.flush(key="task-a"))
    try:
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not second.done()
        finish.set()
        await asyncio.gather(first, second)
        assert delivered == ["task.progress"]
    finally:
        finish.set()
        await asyncio.gather(first, second, return_exceptions=True)
        await buffer.stop()


@pytest.mark.parametrize("child", [False, True])
async def test_reentrant_terminal_is_deferred_until_whole_batch_finishes(child: bool) -> None:
    from litestar_queues.events.buffer import LiveEventBuffer

    delivered: "list[str]" = []

    async def terminal() -> None:
        delivered.append("terminal")

    async def publish(batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> None:
        for event, _ in batch:
            delivered.append(event.type)
            if event.type == "one":
                operation = buffer.publish_immediate(key="task-a", release=terminal)
                if child:
                    await asyncio.create_task(operation)
                else:
                    await operation

    buffer = LiveEventBuffer(EventBufferConfig(batch_size=10), sink_publish=publish, record_drop=_ignore_drop)
    await buffer.add(_event("one"), ("tasks",))
    await buffer.add(_event("two"), ("tasks",))
    await asyncio.wait_for(buffer.flush(), timeout=1)
    assert delivered == ["one", "two", "terminal"]
    await buffer.stop()


async def test_stale_child_terminal_waits_for_new_active_batch() -> None:
    from litestar_queues.events.buffer import LiveEventBuffer

    child_start = asyncio.Event()
    blocked = asyncio.Event()
    finish = asyncio.Event()
    delivered: "list[str]" = []
    children = []

    async def delayed() -> None:
        await child_start.wait()
        await buffer.publish_immediate(key="task-a", release=terminal)

    async def terminal() -> None:
        delivered.append("terminal")

    async def publish(batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> None:
        for event, _ in batch:
            if event.type == "one":
                children.append(asyncio.create_task(delayed()))
            else:
                blocked.set()
                await finish.wait()
            delivered.append(event.type)

    buffer = LiveEventBuffer(EventBufferConfig(batch_size=10), sink_publish=publish, record_drop=_ignore_drop)
    await buffer.add(_event("one"), ("tasks",))
    await buffer.flush()
    await buffer.add(_event("two"), ("tasks",))
    flushing = asyncio.create_task(buffer.flush())
    await blocked.wait()
    try:
        child_start.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert delivered == ["one"] and not children[0].done()
        finish.set()
        await asyncio.gather(flushing, *children)
        assert delivered == ["one", "two", "terminal"]
    finally:
        child_start.set()
        finish.set()
        await asyncio.gather(flushing, *children, return_exceptions=True)
        await buffer.stop()


async def test_reentrant_block_overflow_rejects_before_admission() -> None:
    from litestar_queues.events.buffer import LiveEventBuffer

    delivered: "list[str]" = []

    async def publish(batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> None:
        for event, _ in batch:
            delivered.append(event.type)
            if event.type == "one":
                await buffer.add(_event("two"), ("tasks",))
                with pytest.raises(QueueEventBufferFull):
                    await buffer.add(_event("rejected"), ("tasks",))

    buffer = LiveEventBuffer(
        EventBufferConfig(batch_size=10, max_pending=1, overflow="block"),
        sink_publish=publish,
        record_drop=_ignore_drop,
    )
    await buffer.add(_event("one"), ("tasks",))
    await asyncio.wait_for(buffer.flush(), timeout=1)
    await buffer.flush()
    assert delivered == ["one", "two"]
    await buffer.stop()


async def test_reentrant_control_queue_is_bounded() -> None:
    from litestar_queues.events.buffer import LiveEventBuffer

    delivered: "list[str]" = []

    async def terminal() -> None:
        delivered.append("terminal")

    async def publish(batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> None:
        delivered.extend(event.type for event, _ in batch)
        await buffer.publish_immediate(key="task-a", release=terminal)
        with pytest.raises(QueueEventBufferFull):
            await buffer.publish_immediate(key="task-a", release=terminal)

    buffer = LiveEventBuffer(
        EventBufferConfig(batch_size=10, max_pending=1), sink_publish=publish, record_drop=_ignore_drop
    )
    await buffer.add(_event("one"), ("tasks",))
    await buffer.flush()
    assert delivered == ["one", "terminal"]
    await buffer.stop()


async def test_cancelling_waiting_flush_does_not_wait_for_active_sink() -> None:
    from litestar_queues.events.buffer import LiveEventBuffer

    blocked = asyncio.Event()
    finish = asyncio.Event()

    async def publish(_batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> None:
        blocked.set()
        await finish.wait()

    buffer = LiveEventBuffer(EventBufferConfig(batch_size=10), sink_publish=publish, record_drop=_ignore_drop)
    await buffer.add(_event("one"), ("tasks",))
    flushing = asyncio.create_task(buffer.flush())
    await blocked.wait()
    waiting = asyncio.create_task(buffer.flush())
    try:
        await asyncio.sleep(0)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(waiting, timeout=1)
        assert not flushing.done()
    finally:
        finish.set()
        await asyncio.gather(flushing, waiting, return_exceptions=True)
        await buffer.stop()


async def test_reentrant_eager_add_rejects_before_exceeding_deferred_capacity() -> None:
    from litestar_queues.events.buffer import LiveEventBuffer

    delivered: "list[str]" = []

    async def publish(batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> None:
        for event, _ in batch:
            delivered.append(event.type)
            if event.type == "one":
                await buffer.add(_event("two"), ("tasks",))
                with pytest.raises(QueueEventBufferFull):
                    await buffer.add(_event("rejected"), ("tasks",))

    buffer = LiveEventBuffer(
        EventBufferConfig(batch_size=1, max_pending=1), sink_publish=publish, record_drop=_ignore_drop
    )
    await buffer.add(_event("one"), ("tasks",))
    await buffer.flush()
    assert delivered == ["one", "two"]
    await buffer.stop()


@pytest.mark.parametrize("deferred", [False, True])
async def test_cancelled_batch_retains_unattempted_terminal_release(deferred: bool) -> None:
    from litestar_queues.events.buffer import LiveEventBuffer

    blocked = asyncio.Event()
    attempts = []

    async def terminal() -> None:
        attempts.append("terminal")

    async def publish(batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> None:
        for event, _ in batch:
            attempts.append(event.type)
            if deferred and event.type == "one":
                await buffer.add(_event("two"), ("tasks",))
                await buffer.publish_immediate(key="task-a", release=terminal)
            else:
                blocked.set()
                await asyncio.Event().wait()

    buffer = LiveEventBuffer(EventBufferConfig(batch_size=10), sink_publish=publish, record_drop=_ignore_drop)
    await buffer.add(_event("one"), ("tasks",))
    operation = buffer.flush() if deferred else buffer.publish_immediate(key="task-a", release=terminal)
    flushing = asyncio.create_task(operation)
    try:
        await asyncio.wait_for(blocked.wait(), timeout=1)
        flushing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await flushing
        await buffer.flush()
        assert attempts == (["one", "two", "terminal"] if deferred else ["one", "terminal"])
    finally:
        if not flushing.done():
            flushing.cancel()
        await asyncio.gather(flushing, return_exceptions=True)
        await buffer.stop()


async def test_batch_failure_attempts_terminal_before_raising_original() -> None:
    from litestar_queues.events.buffer import LiveEventBuffer

    error = RuntimeError("first batch failure")
    attempts = []

    async def publish(_batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> None:
        attempts.append("batch")
        raise error

    async def terminal() -> None:
        attempts.append("terminal")

    buffer = LiveEventBuffer(EventBufferConfig(batch_size=10), sink_publish=publish, record_drop=_ignore_drop)
    await buffer.add(_event("one"), ("tasks",))
    with pytest.raises(RuntimeError) as raised:
        await buffer.publish_immediate(key="task-a", release=terminal)
    assert raised.value is error and attempts == ["batch", "terminal"]
    await buffer.stop()


async def test_stop_drains_later_pending_events_after_timer_failure() -> None:
    from litestar_queues.events.buffer import LiveEventBuffer

    error = RuntimeError("timer delivery failed")
    attempts = []

    async def publish(batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> None:
        for event, _ in batch:
            attempts.append(event.type)
            if event.type == "one":
                raise error

    buffer = LiveEventBuffer(
        EventBufferConfig(batch_size=10, flush_interval=0.01), sink_publish=publish, record_drop=_ignore_drop
    )
    await buffer.add(_event("one"), ("tasks",))
    buffer.start()
    assert buffer._task is not None
    await asyncio.gather(buffer._task, return_exceptions=True)
    await buffer.add(_event("two"), ("tasks",))
    with pytest.raises(RuntimeError) as raised:
        await buffer.stop()
    assert raised.value is error
    assert attempts == ["one", "two"]
    await buffer.stop()


async def test_stop_preserves_timer_error_over_later_final_drain_failure() -> None:
    from litestar_queues.events.buffer import LiveEventBuffer

    blocked = asyncio.Event()
    finish = asyncio.Event()
    original = RuntimeError("original timer failure")
    secondary = ValueError("later final drain failure")
    attempts = []

    async def publish(batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> None:
        for event, _ in batch:
            attempts.append(event.type)
            if event.type == "one":
                blocked.set()
                await finish.wait()
                raise original
            raise secondary

    buffer = LiveEventBuffer(
        EventBufferConfig(batch_size=10, flush_interval=0.01), sink_publish=publish, record_drop=_ignore_drop
    )
    await buffer.add(_event("one"), ("tasks",))
    buffer.start()
    try:
        await asyncio.wait_for(blocked.wait(), timeout=1)
        await buffer.add(_event("two"), ("tasks",))
        finish.set()
        assert buffer._task is not None
        await asyncio.gather(buffer._task, return_exceptions=True)
        with pytest.raises(RuntimeError) as raised:
            await buffer.stop()
        assert raised.value is original and attempts == ["one", "two"]
    finally:
        finish.set()
        await buffer.stop()


@pytest.mark.parametrize("stale", [False, True])
async def test_bound_release_does_not_borrow_workers_unrelated_active_token(stale: bool) -> None:
    from litestar_queues.events.buffer import LiveEventBuffer

    entered = asyncio.Event()
    finish = asyncio.Event()
    callbacks: "asyncio.Queue[Callable[[], Awaitable[None]]]" = asyncio.Queue()
    workers: "list[asyncio.Task[None]]" = []
    saved: "list[Callable[[], Awaitable[None]]]" = []
    delivered: "list[str]" = []

    async def terminal() -> None:
        delivered.append("terminal")

    async def operation() -> None:
        await buffer.publish_immediate(key="task-a", release=terminal)

    async def worker() -> None:
        callback = await callbacks.get()
        await callback()

    async def publish(batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> None:
        for event, _ in batch:
            if event.type == "old":
                saved.append(buffer.bind_release(operation))
            else:
                workers.append(asyncio.create_task(worker()))
                entered.set()
                await finish.wait()
            delivered.append(event.type)

    buffer = LiveEventBuffer(EventBufferConfig(batch_size=10), sink_publish=publish, record_drop=_ignore_drop)
    if stale:
        await buffer.add(_event("old"), ("tasks",))
        await buffer.flush()
        bound = saved[0]
    else:
        bound = buffer.bind_release(operation)
    await buffer.add(_event("active"), ("tasks",))
    flushing = asyncio.create_task(buffer.flush())
    await entered.wait()
    try:
        callbacks.put_nowait(bound)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not workers[0].done()
        finish.set()
        await asyncio.gather(flushing, *workers)
        assert delivered == (["old", "active", "terminal"] if stale else ["active", "terminal"])
    finally:
        finish.set()
        await asyncio.gather(flushing, *workers, return_exceptions=True)
        await buffer.stop()

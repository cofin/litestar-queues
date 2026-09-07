"""History commitment and live ordering through the actual publisher pipeline."""

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from copy import deepcopy
from typing import Any

import pytest

from litestar_queues.events import (
    EventBufferConfig,
    EventHistoryConfig,
    QueueChannels,
    QueueEvent,
    QueueEventActor,
    QueueEventEntityRef,
    QueueEventPublisher,
)
from litestar_queues.events._history_buffer import _HistoryBuffer
from litestar_queues.events._log_records import event_log_record_from_event
from litestar_queues.events.history import QueueEventLogRecord
from litestar_queues.exceptions import QueueConfigurationError, QueueEventBufferFull

pytestmark = pytest.mark.anyio


class CoordinatedHistory:
    def __init__(self, config: EventHistoryConfig) -> None:
        self.accepted: list[QueueEvent] = []
        self.committed: list[QueueEventLogRecord] = []
        self.fail_write = False
        self.release_started: dict[str, asyncio.Event] = {}
        self.buffer = _HistoryBuffer(config, self.write)

    async def write(self, records: Sequence[QueueEventLogRecord]) -> None:
        if self.fail_write:
            message = "history is unavailable"
            raise ConnectionError(message)
        self.committed.extend(records)

    async def publish_event_after_commit(
        self, event: QueueEvent, *, release: Callable[[], Awaitable[None]], barrier: bool = False
    ) -> None:
        self.accepted.append(event)
        started = self.release_started.setdefault(event.id, asyncio.Event())

        async def observed_release() -> None:
            started.set()
            await release()

        await self.buffer.enqueue(event_log_record_from_event(event), release=observed_release, barrier=barrier)

    async def aclose(self) -> None:
        await self.buffer.stop()


class ObservingSink:
    def __init__(self, history: CoordinatedHistory) -> None:
        self.history = history
        self.received: list[tuple[QueueEvent, tuple[str, ...]]] = []
        self.callback: Callable[[QueueEvent], Awaitable[None]] | None = None

    async def publish(self, event: QueueEvent, *, channels: Sequence[str]) -> None:
        assert event.id in {record.event_id for record in self.history.committed}
        if self.callback is not None:
            await self.callback(event)
        self.received.append((event, tuple(channels)))


@pytest.fixture
async def pipeline() -> Any:
    opened: list[tuple[CoordinatedHistory, QueueEventPublisher]] = []

    def build(
        *,
        history_strict: bool = True,
        live_strict: bool = True,
        live_batch_size: int = 20,
        live_flush_interval: float = 60,
        **config: Any,
    ) -> Any:
        history = CoordinatedHistory(
            EventHistoryConfig(**{
                "batch_size": 20,
                "max_pending": 40,
                "flush_interval": 60,
                "strict": history_strict,
                **config,
            })
        )
        sink = ObservingSink(history)
        publisher = QueueEventPublisher(
            sink,
            event_log=history,
            strict=live_strict,
            buffer_config=EventBufferConfig(batch_size=live_batch_size, flush_interval=live_flush_interval),
        )
        opened.append((history, publisher))
        return history, sink, publisher

    yield build
    for history, publisher in opened:
        with suppress(Exception, asyncio.CancelledError):
            await history.aclose()
        with suppress(Exception, asyncio.CancelledError):
            await publisher.stop_buffer()


def event(name: str, *, terminal: bool = False) -> QueueEvent:
    return QueueEvent(id=name, type="task.completed" if terminal else "task.log", scope="task", task_id="same-task")


@pytest.mark.parametrize("history_strict", [False, True])
@pytest.mark.parametrize("live_strict", [False, True])
async def test_history_and_live_share_one_isolated_envelope(
    pipeline: Any, history_strict: bool, live_strict: bool
) -> None:
    history, sink, publisher = pipeline(history_strict=history_strict, live_strict=live_strict)
    envelope = QueueEvent(
        type="task.log",
        scope="task",
        task_id="original-task",
        queue="original-queue",
        actor=QueueEventActor(type="user", id="original-user"),
        entity=QueueEventEntityRef(type="file", id="original-file"),
        payload={"nested": {"items": [1]}},
    )
    accepted = deepcopy(envelope)
    await publisher.publish(envelope)
    envelope.task_id = "changed-task"
    envelope.queue = "changed-queue"
    assert envelope.actor is not None and envelope.entity is not None
    envelope.actor.id = "changed-user"
    envelope.entity.id = "changed-file"
    envelope.payload["nested"]["items"].append(2)
    assert sink.received == [] and history.committed == []
    await history.buffer.flush()
    await publisher.flush_buffer()
    assert sink.received[0][0] == accepted
    assert history.accepted[0] == accepted
    assert history.committed[0].detail == accepted.payload
    assert history.committed[0].actor_id == "original-user"
    assert history.committed[0].entity == "file:original-file"
    assert sink.received[0][1] == (QueueChannels.task("original-task"), QueueChannels.queue("original-queue"))


@pytest.mark.parametrize("history_strict", [False, True])
@pytest.mark.parametrize("live_strict", [False, True])
async def test_copy_failure_never_reaches_history_admission(
    pipeline: Any, history_strict: bool, live_strict: bool
) -> None:
    class Uncopyable:
        def __deepcopy__(self, memo: dict[int, Any]) -> None:
            message = "copy refused"
            raise ValueError(message)

    history, sink, publisher = pipeline(history_strict=history_strict, live_strict=live_strict)
    with pytest.raises(ValueError, match="copy refused"):
        await publisher.publish(QueueEvent(type="task.log", scope="task", payload={"nested": [Uncopyable()]}))
    assert history.accepted == history.committed == sink.received == []


@pytest.mark.parametrize("history_strict", [False, True])
@pytest.mark.parametrize("live_strict", [False, True])
async def test_history_admission_rejections_always_propagate(
    pipeline: Any, history_strict: bool, live_strict: bool
) -> None:
    history, sink, publisher = pipeline(
        history_strict=history_strict, live_strict=live_strict, batch_size=1, max_pending=1
    )
    history.fail_write = True
    if history_strict:
        with pytest.raises(ConnectionError):
            await publisher.publish(event("retained"))
    else:
        await publisher.publish(event("retained"))
    with pytest.raises(QueueEventBufferFull):
        await publisher.publish(event("full"))
    assert sink.received == []
    history.fail_write = False
    await history.aclose()
    with pytest.raises(QueueConfigurationError, match="closing or closed"):
        await publisher.publish(event("closed"))


async def test_non_strict_failed_terminal_history_withholds_all_live(pipeline: Any) -> None:
    history, sink, publisher = pipeline(history_strict=False)
    history.fail_write = True
    await publisher.publish(event("ordinary"))
    await publisher.publish(event("terminal", terminal=True))
    assert sink.received == [] and history.committed == []
    history.fail_write = False
    await history.buffer.flush()
    assert [item.id for item, _ in sink.received] == ["ordinary", "terminal"]


async def test_terminal_waits_for_an_extracted_live_batch(pipeline: Any) -> None:
    history, sink, publisher = pipeline()
    entered, unblock = asyncio.Event(), asyncio.Event()

    async def observe(envelope: QueueEvent) -> None:
        if envelope.id == "ordinary":
            entered.set()
            await unblock.wait()

    sink.callback = observe
    await publisher.publish(event("ordinary"))
    await history.buffer.flush()
    first_flush = asyncio.create_task(publisher.flush_buffer())
    await asyncio.wait_for(entered.wait(), timeout=1)
    terminal = asyncio.create_task(publisher.publish(event("terminal", terminal=True)))
    try:
        await asyncio.sleep(0)
        assert not terminal.done()
        assert sink.received == []
    finally:
        unblock.set()
    await asyncio.wait_for(asyncio.gather(first_flush, terminal), timeout=1)
    assert [item.id for item, _ in sink.received] == ["ordinary", "terminal"]


@pytest.mark.parametrize("child", [False, True])
async def test_reentrant_terminal_publication_waits_for_commit_without_deadlock(pipeline: Any, child: bool) -> None:
    history, sink, publisher = pipeline()
    order: list[str] = []

    async def publish_inner() -> None:
        await publisher.publish(event("inner", terminal=True))
        assert "inner" in {record.event_id for record in history.committed}
        order.append("inner-accepted")

    async def observe(envelope: QueueEvent) -> None:
        order.append(f"start:{envelope.id}")
        if envelope.id == "outer":
            if child:
                await asyncio.create_task(publish_inner())
            else:
                await publish_inner()
        order.append(f"end:{envelope.id}")

    sink.callback = observe
    await asyncio.wait_for(publisher.publish(event("outer", terminal=True)), timeout=1)
    await publisher.flush_buffer()
    assert order == ["start:outer", "inner-accepted", "end:outer", "start:inner", "end:inner"]


async def test_delayed_child_cannot_bypass_a_later_live_drain(pipeline: Any) -> None:
    _history, sink, publisher = pipeline()
    start_child, child_started = asyncio.Event(), asyncio.Event()
    second_started, unblock_second = asyncio.Event(), asyncio.Event()
    child_task: asyncio.Task[None] | None = None

    async def delayed_child() -> None:
        await start_child.wait()
        child_started.set()
        await publisher.publish(event("child", terminal=True))

    async def observe(envelope: QueueEvent) -> None:
        nonlocal child_task
        if envelope.id == "first":
            child_task = asyncio.create_task(delayed_child())
        elif envelope.id == "second":
            second_started.set()
            await unblock_second.wait()

    sink.callback = observe
    await publisher.publish(event("first", terminal=True))
    second = asyncio.create_task(publisher.publish(event("second", terminal=True)))
    await asyncio.wait_for(second_started.wait(), timeout=1)
    start_child.set()
    await child_started.wait()
    try:
        await asyncio.sleep(0)
        assert child_task is not None and not child_task.done()
        assert [item.id for item, _ in sink.received] == ["first"]
    finally:
        unblock_second.set()
    assert child_task is not None
    await asyncio.wait_for(asyncio.gather(second, child_task), timeout=1)
    assert [item.id for item, _ in sink.received] == ["first", "second", "child"]


@pytest.mark.parametrize("child", [False, True])
@pytest.mark.parametrize("timer", [False, True])
async def test_reentrant_terminal_does_not_wait_behind_a_history_release_blocked_on_live(
    pipeline: Any, child: bool, timer: bool
) -> None:
    history, sink, publisher = pipeline(live_batch_size=2, live_flush_interval=0.01 if timer else 60)
    first_started, publish_inner_now = asyncio.Event(), asyncio.Event()
    order: list[str] = []

    async def publish_inner() -> None:
        await publisher.publish(event("inner", terminal=True))
        assert "inner" in {record.event_id for record in history.committed}
        order.append("inner-accepted")

    async def observe(envelope: QueueEvent) -> None:
        if envelope.id == "first":
            first_started.set()
            await publish_inner_now.wait()
            if child:
                await asyncio.create_task(publish_inner())
            else:
                await publish_inner()
        order.append(envelope.id)

    sink.callback = observe
    await publisher.publish(event("first"))
    await history.buffer.flush()
    first = None
    if timer:
        publisher.start_buffer()
    else:
        first = asyncio.create_task(publisher.flush_buffer())
    await asyncio.wait_for(first_started.wait(), timeout=1)
    await publisher.publish(event("second"))
    await publisher.publish(event("third"))
    pending_history = asyncio.create_task(history.buffer.flush())
    await asyncio.wait_for(history.release_started["third"].wait(), timeout=1)
    publish_inner_now.set()
    pending = [pending_history] if first is None else [first, pending_history]
    await asyncio.wait_for(asyncio.gather(*pending), timeout=1)
    await history.buffer.flush()
    await publisher.flush_buffer()
    assert order == ["inner-accepted", "first", "second", "third", "inner"]


async def test_history_close_from_live_callback_refuses_without_closing(pipeline: Any) -> None:
    history, sink, publisher = pipeline()
    refused = []

    async def observe(envelope: QueueEvent) -> None:
        if envelope.id == "first":
            with pytest.raises(QueueConfigurationError, match="release callback"):
                await history.aclose()
            refused.append(True)

    sink.callback = observe
    await publisher.publish(event("first"))
    await history.buffer.flush()
    await publisher.flush_buffer()
    assert refused == [True]
    await publisher.publish(event("second", terminal=True))
    assert [item.id for item, _ in sink.received] == ["first", "second"]

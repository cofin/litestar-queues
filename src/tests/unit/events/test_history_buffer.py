"""Durable history admission, commit barriers and bounded live release."""

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from typing import Any

import pytest

from litestar_queues.events._history_buffer import _HistoryBuffer
from litestar_queues.events._log_records import event_log_record_from_event
from litestar_queues.events.history import EventHistoryConfig, QueueEventLogRecord
from litestar_queues.events.models import QueueEvent
from litestar_queues.exceptions import QueueConfigurationError, QueueEventBufferFull

pytestmark = pytest.mark.anyio


def test_history_record_snapshots_nested_detail() -> None:
    payload = {"nested": {"items": [1]}}
    record = event_log_record_from_event(QueueEvent(type="task.event", scope="task", payload=payload))
    payload["nested"]["items"].append(2)
    assert record.detail == {"nested": {"items": [1]}}


def _record(name: str, **payload: Any) -> QueueEventLogRecord:
    return event_log_record_from_event(QueueEvent(id=name, type="task.event", scope="task", payload=payload))


@pytest.fixture
async def buffers() -> Any:
    opened: list[_HistoryBuffer] = []

    def build(writer: Callable[[Sequence[QueueEventLogRecord]], Awaitable[None]], **config: Any) -> _HistoryBuffer:
        buffer = _HistoryBuffer(EventHistoryConfig(**{"flush_interval": 60.0, **config}), writer)
        opened.append(buffer)
        return buffer

    yield build
    for buffer in opened:
        with suppress(Exception, asyncio.CancelledError):
            await buffer.stop()


async def test_sparse_history_releases_after_timer_commit(buffers: Any) -> None:
    order: list[str] = []
    released = asyncio.Event()

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        order.append("commit")

    async def release() -> None:
        order.append("live")
        released.set()

    buffer = buffers(write, flush_interval=0.01)
    await buffer.enqueue(_record("sparse"), release=release)
    assert order == []
    await asyncio.wait_for(released.wait(), timeout=1)
    assert order == ["commit", "live"]
    await buffer.stop()


async def test_history_capacity_includes_inflight_and_committed_callbacks(buffers: Any) -> None:
    writing = asyncio.Event()
    allow_write = asyncio.Event()
    releasing = asyncio.Event()
    allow_release = asyncio.Event()
    batches: list[list[str]] = []

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        batches.append([record.event_id for record in batch])
        writing.set()
        await allow_write.wait()

    async def release() -> None:
        releasing.set()
        await allow_release.wait()

    buffer = buffers(write, batch_size=1, max_pending=2)
    first = asyncio.create_task(buffer.enqueue(_record("first"), release=release))
    await writing.wait()
    second = asyncio.create_task(buffer.enqueue(_record("second")))
    await asyncio.sleep(0)
    with pytest.raises(QueueEventBufferFull):
        await buffer.enqueue(_record("overflow-inflight"))
    allow_write.set()
    await releasing.wait()
    with pytest.raises(QueueEventBufferFull):
        await buffer.enqueue(_record("overflow-release"))
    allow_release.set()
    await asyncio.gather(first, second)
    assert batches == [["first"], ["second"]]
    await buffer.enqueue(_record("after-release"))


async def test_overlapping_flushes_serialize_bounded_batches(buffers: Any) -> None:
    batches: list[list[str]] = []
    active = 0

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        nonlocal active
        active += 1
        assert active == 1
        batches.append([record.event_id for record in batch])
        await asyncio.sleep(0)
        active -= 1

    buffer = buffers(write, batch_size=3)
    await buffer.enqueue(_record("one"))
    await buffer.enqueue(_record("two"))
    await asyncio.gather(buffer.flush(), buffer.flush(), buffer.enqueue(_record("three"), barrier=True))
    assert [item for batch in batches for item in batch] == ["one", "two", "three"]
    assert all(len(batch) <= 3 for batch in batches)


async def test_barrier_uses_fixed_admission_boundary(buffers: Any) -> None:
    entered = asyncio.Event()
    proceed = asyncio.Event()
    written: list[str] = []

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        entered.set()
        await proceed.wait()
        written.extend(record.event_id for record in batch)

    buffer = buffers(write, batch_size=10)
    await buffer.enqueue(_record("before"))
    first_flush = asyncio.create_task(buffer.flush())
    await entered.wait()
    await buffer.enqueue(_record("after"))
    proceed.set()
    await first_flush
    assert written == ["before"]
    await buffer.flush()
    assert written == ["before", "after"]


@pytest.mark.parametrize("strict", [False, True])
async def test_failed_write_retains_snapshot_and_withholds_live(buffers: Any, strict: bool) -> None:
    attempted: list[QueueEventLogRecord] = []
    released: list[str] = []
    failing = True

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        attempted.extend(batch)
        if failing:
            msg = "temporary"
            raise RuntimeError(msg)

    async def release() -> None:
        released.append("live")

    buffer = buffers(write, strict=strict)
    original = _record("stable", nested={"values": [1]})
    if strict:
        with pytest.raises(RuntimeError, match="temporary"):
            await buffer.enqueue(original, release=release, barrier=True)
    else:
        await buffer.enqueue(original, release=release, barrier=True)
    original.detail["nested"]["values"].append(2)
    assert released == []
    failing = False
    await buffer.flush()
    assert len(attempted) == 2
    assert attempted[0] is attempted[1]
    assert attempted[1].detail == {"nested": {"values": [1]}}
    assert attempted[1].created_at == original.created_at
    assert released == ["live"]


async def test_copy_failure_is_not_admitted(buffers: Any) -> None:
    class Uncopyable:
        def __deepcopy__(self, memo: dict[int, Any]) -> Any:
            msg = "cannot snapshot"
            raise ValueError(msg)

    written: list[str] = []

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        written.extend(record.event_id for record in batch)

    buffer = buffers(write, max_pending=1, batch_size=1)
    broken = _record("broken")
    broken.detail["opaque"] = Uncopyable()
    with pytest.raises(ValueError, match="cannot snapshot"):
        await buffer.enqueue(broken)
    await buffer.enqueue(_record("valid"))
    assert written == ["valid"]


@pytest.mark.parametrize("boundary", ["enqueue", "flush", "stop"])
async def test_strict_timer_failure_surfaces_once(buffers: Any, boundary: str) -> None:
    attempted = asyncio.Event()
    failing = True
    writes = 0

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        nonlocal writes
        writes += 1
        attempted.set()
        if failing:
            msg = "latched timer failure"
            raise RuntimeError(msg)

    buffer = buffers(write, strict=True, flush_interval=0.01)
    await buffer.enqueue(_record("retained"))
    await asyncio.wait_for(attempted.wait(), timeout=1)
    await asyncio.sleep(0)
    failing = False
    with pytest.raises(RuntimeError, match="latched timer failure"):
        if boundary == "enqueue":
            await buffer.enqueue(_record("not-admitted"))
        elif boundary == "flush":
            await buffer.flush()
        else:
            await buffer.stop()
    if boundary == "stop":
        await buffer.stop()
    else:
        assert writes == 1
        await buffer.flush()
    assert writes == 2


async def test_non_strict_timer_retries_at_interval(buffers: Any) -> None:
    attempts: list[float] = []
    completed = asyncio.Event()

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        attempts.append(asyncio.get_running_loop().time())
        if len(attempts) == 1:
            msg = "retry later"
            raise RuntimeError(msg)
        completed.set()

    buffer = buffers(write, flush_interval=0.03)
    await buffer.enqueue(_record("retry"))
    await asyncio.wait_for(completed.wait(), timeout=1)
    assert len(attempts) == 2
    assert attempts[1] - attempts[0] >= 0.025


async def test_permanent_failure_stops_admission_and_timer_retry(buffers: Any) -> None:
    failed = asyncio.Event()
    attempts = 0

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        nonlocal attempts
        attempts += 1
        failed.set()
        msg = "conflicting event identity"
        raise QueueConfigurationError(msg)

    buffer = buffers(write, flush_interval=0.01)
    await buffer.enqueue(_record("conflict"))
    await asyncio.wait_for(failed.wait(), timeout=1)
    await asyncio.sleep(0)
    with pytest.raises(QueueConfigurationError, match="conflicting"):
        await buffer.enqueue(_record("rejected"))
    with pytest.raises(QueueConfigurationError, match="conflicting"):
        await buffer.flush()
    with pytest.raises(QueueConfigurationError, match="conflicting"):
        await buffer.stop()
    await buffer.stop()
    assert attempts == 1


@pytest.mark.parametrize("child", [False, True])
async def test_reentrant_release_waits_for_commit_without_deadlock(buffers: Any, child: bool) -> None:
    order: list[str] = []

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        order.extend(f"commit:{record.event_id}" for record in batch)

    async def inner_release() -> None:
        order.append("live:inner")

    async def publish_inner() -> None:
        await buffer.enqueue(_record("inner"), release=inner_release, barrier=True)
        await buffer.flush()
        order.append("inner-accepted")

    async def outer_release() -> None:
        order.append("live:outer-start")
        if child:
            await asyncio.create_task(publish_inner())
        else:
            await publish_inner()
        order.append("live:outer-end")

    buffer = buffers(write)
    await asyncio.wait_for(buffer.enqueue(_record("outer"), release=outer_release, barrier=True), timeout=1)
    assert order == [
        "commit:outer",
        "live:outer-start",
        "commit:inner",
        "inner-accepted",
        "live:outer-end",
        "live:inner",
    ]


async def test_delayed_child_cannot_reuse_expired_release_token(buffers: Any) -> None:
    start_child = asyncio.Event()
    child_committed = asyncio.Event()
    child_released = asyncio.Event()
    second_started = asyncio.Event()
    finish_second = asyncio.Event()
    child_task: asyncio.Task[None] | None = None

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        if any(record.event_id == "child" for record in batch):
            child_committed.set()

    async def child_release() -> None:
        child_released.set()

    async def delayed_child() -> None:
        await start_child.wait()
        await buffer.enqueue(_record("child"), release=child_release, barrier=True)

    async def first_release() -> None:
        nonlocal child_task
        child_task = asyncio.create_task(delayed_child())

    async def second_release() -> None:
        second_started.set()
        await finish_second.wait()

    buffer = buffers(write)
    await buffer.enqueue(_record("first"), release=first_release, barrier=True)
    second = asyncio.create_task(buffer.enqueue(_record("second"), release=second_release, barrier=True))
    await second_started.wait()
    start_child.set()
    await child_committed.wait()
    await asyncio.sleep(0)
    assert child_task is not None and not child_task.done()
    assert not child_released.is_set()
    finish_second.set()
    await asyncio.gather(second, child_task)
    assert child_released.is_set()


@pytest.mark.parametrize("strict", [False, True])
async def test_live_failure_does_not_repeat_history_and_attempts_later_callbacks(buffers: Any, strict: bool) -> None:
    written: list[str] = []
    released: list[str] = []

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        written.extend(record.event_id for record in batch)

    async def bad_release() -> None:
        released.append("bad")
        msg = "live failed"
        raise RuntimeError(msg)

    async def good_release() -> None:
        released.append("good")

    buffer = buffers(write, strict=strict)
    await buffer.enqueue(_record("bad"), release=bad_release)
    await buffer.enqueue(_record("good"), release=good_release)
    with pytest.raises(RuntimeError, match="live failed"):
        await buffer.stop()
    await buffer.stop()
    assert written == ["bad", "good"]
    assert released == ["bad", "good"]


async def test_background_live_failure_latches_even_without_history_strict(buffers: Any) -> None:
    attempted = asyncio.Event()
    writes = 0

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        nonlocal writes
        writes += 1

    async def fail() -> None:
        attempted.set()
        msg = "background live failed"
        raise RuntimeError(msg)

    buffer = buffers(write, flush_interval=0.01, strict=False)
    await buffer.enqueue(_record("live"), release=fail)
    await asyncio.wait_for(attempted.wait(), timeout=1)
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="background live failed"):
        await buffer.flush()
    await buffer.flush()
    assert writes == 1


async def test_stop_rejects_admission_and_waits_through_cancellation(buffers: Any) -> None:
    started = asyncio.Event()
    finish = asyncio.Event()
    released: list[str] = []

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        started.set()
        await finish.wait()

    async def release() -> None:
        released.append("live")

    buffer = buffers(write)
    await buffer.enqueue(_record("closing"), release=release)
    stop = asyncio.create_task(buffer.stop())
    await started.wait()
    with pytest.raises(QueueConfigurationError, match="closing or closed"):
        await buffer.enqueue(_record("too-late"))
    stop.cancel()
    await asyncio.sleep(0)
    assert not stop.done()
    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await stop
    assert released == ["live"]
    await buffer.stop()
    with pytest.raises(QueueConfigurationError, match="closing or closed"):
        buffer.start()


async def test_reentrant_stop_refuses_without_closing_buffer(buffers: Any) -> None:
    writes: list[str] = []

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        writes.extend(record.event_id for record in batch)

    async def release() -> None:
        with pytest.raises(QueueConfigurationError, match="own release callback"):
            await buffer.stop()

    buffer = buffers(write)
    await buffer.enqueue(_record("first"), release=release, barrier=True)
    await buffer.enqueue(_record("still-open"), barrier=True)
    assert writes == ["first", "still-open"]


async def test_non_strict_close_reports_unresolved_records(buffers: Any, caplog: pytest.LogCaptureFixture) -> None:
    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        msg = "unavailable"
        raise RuntimeError(msg)

    buffer = buffers(write)
    await buffer.enqueue(_record("retained"))
    await buffer.stop()
    assert "closed with 1 unresolved records" in caplog.text


@pytest.mark.parametrize("background", [False, True])
async def test_flush_observes_release_failure_from_another_drainer(buffers: Any, background: bool) -> None:
    first_live = asyncio.Event()
    finish_first = asyncio.Event()
    second_committed = asyncio.Event()

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        if any(record.event_id == "second" for record in batch):
            second_committed.set()

    async def first_release() -> None:
        first_live.set()
        await finish_first.wait()

    async def second_release() -> None:
        msg = "shared release failed"
        raise RuntimeError(msg)

    buffer = buffers(write, flush_interval=0.01 if background else 60)
    tasks: list[asyncio.Task[None]] = []
    first = asyncio.create_task(buffer.enqueue(_record("first"), release=first_release, barrier=True))
    tasks.append(first)
    try:
        await first_live.wait()
        await buffer.enqueue(_record("second"), release=second_release)
        if not background:
            tasks.append(asyncio.create_task(buffer.flush()))
        await asyncio.wait_for(second_committed.wait(), timeout=1)
        observer = asyncio.create_task(buffer.flush())
        tasks.append(observer)
        await asyncio.sleep(0)
        finish_first.set()
        with pytest.raises(RuntimeError, match="shared release failed"):
            await observer
        await buffer.flush()
    finally:
        finish_first.set()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_timer_callback_cancellation_is_observed_without_stranding_future_entries(buffers: Any) -> None:
    cancelled_release = asyncio.Event()
    later_release = asyncio.Event()

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        return None

    async def cancel() -> None:
        cancelled_release.set()
        raise asyncio.CancelledError

    async def later() -> None:
        later_release.set()

    buffer = buffers(write, flush_interval=0.01)
    await buffer.enqueue(_record("cancelled-callback"), release=cancel)
    await asyncio.wait_for(cancelled_release.wait(), timeout=1)
    await asyncio.sleep(0)
    with pytest.raises(asyncio.CancelledError):
        await buffer.flush()
    await buffer.enqueue(_record("later"), release=later)
    await asyncio.wait_for(later_release.wait(), timeout=1)


async def test_cancelled_flush_does_not_wait_for_another_callers_live_sink(buffers: Any) -> None:
    started = asyncio.Event()
    finish = asyncio.Event()

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        return None

    async def release() -> None:
        started.set()
        await finish.wait()

    buffer = buffers(write)
    first = asyncio.create_task(buffer.enqueue(_record("blocked-live"), release=release, barrier=True))
    second: asyncio.Task[None] | None = None
    try:
        await started.wait()
        second = asyncio.create_task(buffer.flush())
        await asyncio.sleep(0)
        second.cancel()
        done, _ = await asyncio.wait({second}, timeout=0.1)
        assert second in done
        with pytest.raises(asyncio.CancelledError):
            await second
    finally:
        finish.set()
        await asyncio.gather(first, *([second] if second is not None else []), return_exceptions=True)


async def test_blocked_live_callback_does_not_delay_sparse_history_commit(buffers: Any) -> None:
    first_live = asyncio.Event()
    finish_first = asyncio.Event()
    second_committed = asyncio.Event()

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        if any(record.event_id == "second-sparse" for record in batch):
            second_committed.set()

    async def release() -> None:
        first_live.set()
        await finish_first.wait()

    buffer = buffers(write, flush_interval=0.01)
    await buffer.enqueue(_record("first-sparse"), release=release)
    try:
        await asyncio.wait_for(first_live.wait(), timeout=1)
        await buffer.enqueue(_record("second-sparse"))
        await asyncio.wait_for(second_committed.wait(), timeout=0.1)
        assert not finish_first.is_set()
    finally:
        finish_first.set()


@pytest.mark.parametrize("owned_task", ["timer", "release"])
async def test_owned_tasks_terminate_on_cancellation_and_restart_for_new_work(buffers: Any, owned_task: str) -> None:
    written: list[str] = []

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        written.extend(record.event_id for record in batch)

    buffer = buffers(write)
    buffer.start()
    await asyncio.sleep(0)
    task = buffer._timer if owned_task == "timer" else buffer._release_task
    assert task is not None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)
    if owned_task == "timer":
        with pytest.raises(asyncio.CancelledError):
            await buffer.flush()
    await buffer.enqueue(_record("resumed"), barrier=True)
    assert written == ["resumed"]


async def test_close_continues_after_callback_cancellation(buffers: Any) -> None:
    released: list[str] = []

    async def write(batch: Sequence[QueueEventLogRecord]) -> None:
        return None

    async def cancel() -> None:
        released.append("cancel")
        raise asyncio.CancelledError

    async def finish() -> None:
        released.append("finish")

    buffer = buffers(write)
    await buffer.enqueue(_record("first"), release=cancel)
    await buffer.enqueue(_record("second"), release=finish)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(buffer.stop(), timeout=1)
    await buffer.stop()
    assert released == ["cancel", "finish"]

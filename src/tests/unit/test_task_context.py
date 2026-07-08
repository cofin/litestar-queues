from typing import cast

import pytest

from litestar_queues import QueueConfig, QueueService, task
from litestar_queues.events import (
    EventBufferConfig,
    EventConfig,
    InMemoryQueueEventSink,
    QueueEventPublisher,
    TaskExecutionContext,
    beat,
    get_current_task_context,
    publish_task_event,
    publish_task_log,
    publish_task_progress,
    require_current_task_context,
)
from litestar_queues.events.context import _bind_beat_sink, _bind_task_context, _reset_beat_sink, _reset_task_context

pytestmark = pytest.mark.anyio


def test_current_task_context_helpers_outside_task() -> "None":
    assert get_current_task_context() is None
    with pytest.raises(RuntimeError, match="No queue task execution context"):
        require_current_task_context()


async def test_task_context_is_bound_and_helpers_publish_task_events() -> "None":
    sink = InMemoryQueueEventSink()
    captured_context: "TaskExecutionContext | None" = None

    @task("tasks.with_context")
    async def with_context(*, _task_context: "TaskExecutionContext") -> "str":
        nonlocal captured_context
        captured_context = _task_context
        assert get_current_task_context() is _task_context
        await _task_context.progress(current=None, total=10, message="extracting", payload={"workspace_id": None})
        await publish_task_log("loaded page", level="debug", payload={"page": 1})
        await publish_task_event("task.custom", message="custom", payload={"value": "ok"})
        return "ok"

    async with QueueService(QueueConfig(execution_backend="immediate", event=EventConfig(sink=sink))) as service:
        result = await service.enqueue(with_context)
        await result.refresh()

    assert result.status == "completed"
    assert captured_context is not None
    assert captured_context.task_id == str(result.id)
    assert captured_context.task_name == "tasks.with_context"
    assert captured_context.queue == "default"
    assert get_current_task_context() is None

    event_types = [event.type for event in sink.events]
    assert event_types == ["task.started", "task.progress", "task.log", "task.custom", "task.completed"]
    progress = sink.events[1]
    assert progress.progress_current is None
    assert progress.progress_total == 10
    assert progress.message == "extracting"
    assert progress.payload["workspace_id"] is None
    assert all(event.task_id == str(result.id) for event in sink.events)


async def test_task_context_keyword_is_not_injected_when_callable_does_not_accept_it() -> "None":
    sink = InMemoryQueueEventSink()
    received_kwargs: "dict[str, object] | None" = None

    @task("tasks.plain")
    async def plain() -> "dict[str, object]":
        nonlocal received_kwargs
        received_kwargs = {}
        await publish_task_progress(percent=50)
        return received_kwargs

    async with QueueService(QueueConfig(execution_backend="immediate", event=EventConfig(sink=sink))) as service:
        result = await service.enqueue(plain)
        await result.refresh()

    assert result.status == "completed"
    assert received_kwargs == {}
    assert "task.progress" in [event.type for event in sink.events]


async def test_context_progress_immediate_flushes_prior() -> "None":
    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink, buffer_config=EventBufferConfig(buffer_size=10))
    context = _build_context(event_publisher=publisher)

    await context.log("buffered")

    assert sink.events == []

    await context.progress(current=1, total=2, immediate=True)

    assert [event.type for event in sink.events] == ["task.log", "task.progress"]
    assert [event.message for event in sink.events] == ["buffered", None]


async def test_module_helper_immediate_flushes_prior() -> "None":
    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink, buffer_config=EventBufferConfig(buffer_size=10))
    context = _build_context(event_publisher=publisher)

    await context.log("buffered")

    token = _bind_task_context(context)
    try:
        await publish_task_progress(current=1, total=2, immediate=True)
    finally:
        _reset_task_context(token)

    assert [event.type for event in sink.events] == ["task.log", "task.progress"]


def test_beat_outside_context_is_noop() -> "None":
    beat("row 30000")


def test_ctx_beat_forwards_to_bound_sink() -> "None":
    sink = _RecordingBeatSink()
    context = _build_context()

    token = _bind_beat_sink(sink)
    try:
        context.beat("row 30000")
    finally:
        _reset_beat_sink(token)

    assert sink.records == [("task-1", "row 30000")]


def test_module_beat_forwards_current_context_to_bound_sink() -> "None":
    sink = _RecordingBeatSink()
    context = _build_context()

    context_token = _bind_task_context(context)
    sink_token = _bind_beat_sink(sink)
    try:
        beat("row 30000")
    finally:
        _reset_beat_sink(sink_token)
        _reset_task_context(context_token)

    assert sink.records == [("task-1", "row 30000")]


def test_beat_performs_no_await_and_no_backend_call() -> "None":
    sink = _RecordingBeatSink()
    context = _build_context(event_publisher=_FailingPublisher())

    token = _bind_beat_sink(sink)
    try:
        context.beat("sync progress")
    finally:
        _reset_beat_sink(token)

    assert sink.records == [("task-1", "sync progress")]


def _build_context(*, event_publisher: "object | None" = None) -> "TaskExecutionContext":
    return TaskExecutionContext(
        task_id="task-1",
        task_name="tasks.context",
        queue="default",
        worker_id="worker-1",
        execution_backend="local",
        execution_profile=None,
        attempt=1,
        event_publisher=cast("QueueEventPublisher", event_publisher or _FailingPublisher()),
    )


class _RecordingBeatSink:
    __slots__ = ("records",)

    def __init__(self) -> "None":
        self.records: "list[tuple[str, str | None]]" = []

    def record_beat(self, task_id: "str", detail: "str | None") -> "None":
        self.records.append((task_id, detail))


class _FailingPublisher:
    async def publish(self, *_args: "object", **_kwargs: "object") -> "None":
        msg = "beat must not publish events"
        raise AssertionError(msg)

from typing import Any

import pytest

from litestar_queues import QueueConfig, QueueService, task
from litestar_queues.events import (
    InMemoryQueueEventSink,
    QueueEventConfig,
    TaskExecutionContext,
    get_current_task_context,
    publish_task_event,
    publish_task_log,
    publish_task_progress,
    require_current_task_context,
)

pytestmark = pytest.mark.anyio


def test_current_task_context_helpers_outside_task() -> None:
    assert get_current_task_context() is None
    with pytest.raises(RuntimeError, match="No queue task execution context"):
        require_current_task_context()


async def test_task_context_is_bound_and_helpers_publish_task_events() -> None:
    sink = InMemoryQueueEventSink()
    captured_context: TaskExecutionContext | None = None

    @task("tasks.with_context")
    async def with_context(*, _task_context: TaskExecutionContext) -> str:
        nonlocal captured_context
        captured_context = _task_context
        assert get_current_task_context() is _task_context
        await _task_context.progress(current=None, total=10, message="extracting", payload={"workspace_id": None})
        await publish_task_log("loaded page", level="debug", payload={"page": 1})
        await publish_task_event("task.custom", message="custom", payload={"value": "ok"})
        return "ok"

    async with QueueService(
        QueueConfig(
            execution_backend="immediate",
            event_config=QueueEventConfig(enabled=True, sink=sink),
        )
    ) as service:
        result = await service.enqueue(with_context)
        await result.refresh()

    assert result.status == "completed"
    assert captured_context is not None
    assert captured_context.task_id == str(result.id)
    assert captured_context.task_name == "tasks.with_context"
    assert captured_context.queue == "default"
    assert get_current_task_context() is None

    event_types = [event.type for event in sink.events]
    assert event_types == [
        "task.started",
        "task.progress",
        "task.log",
        "task.custom",
        "task.completed",
    ]
    progress = sink.events[1]
    assert progress.progress_current is None
    assert progress.progress_total == 10
    assert progress.message == "extracting"
    assert progress.payload["workspace_id"] is None
    assert all(event.task_id == str(result.id) for event in sink.events)


async def test_task_context_keyword_is_not_injected_when_callable_does_not_accept_it() -> None:
    sink = InMemoryQueueEventSink()
    received_kwargs: dict[str, Any] | None = None

    @task("tasks.plain")
    async def plain() -> dict[str, Any]:
        nonlocal received_kwargs
        received_kwargs = {}
        await publish_task_progress(percent=50)
        return received_kwargs

    async with QueueService(
        QueueConfig(
            execution_backend="immediate",
            event_config=QueueEventConfig(enabled=True, sink=sink),
        )
    ) as service:
        result = await service.enqueue(plain)
        await result.refresh()

    assert result.status == "completed"
    assert received_kwargs == {}
    assert "task.progress" in [event.type for event in sink.events]

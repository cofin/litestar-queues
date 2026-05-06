from collections.abc import Sequence

import pytest

from litestar_queues import QueueConfig, QueueService, Worker, task
from litestar_queues.events import InMemoryQueueEventSink, QueueEvent, QueueEventConfig, publish_task_progress

pytestmark = pytest.mark.anyio


class FailingSink:
    async def publish(self, event: QueueEvent, *, channels: Sequence[str]) -> None:
        msg = "event sink unavailable"
        raise RuntimeError(msg)


async def test_worker_emits_started_progress_and_terminal_events_in_order() -> None:
    sink = InMemoryQueueEventSink()

    @task("tasks.worker_events")
    async def worker_events() -> str:
        await publish_task_progress(current=1, total=2, message="halfway")
        return "ok"

    async with QueueService(
        QueueConfig(
            execution_backend="local",
            event_config=QueueEventConfig(enabled=True, sink=sink),
        )
    ) as service:
        result = await service.enqueue(worker_events)
        worker = Worker(service)

        assert await worker.run_once() == 1
        await result.refresh()

    assert result.status == "completed"
    assert [event.type for event in sink.events] == ["task.started", "task.progress", "task.completed"]
    assert sink.events[1].message == "halfway"
    assert sink.events[-1].task_id == str(result.id)


async def test_worker_emits_failed_terminal_event_for_failed_attempt() -> None:
    sink = InMemoryQueueEventSink()

    @task("tasks.worker_failure")
    async def worker_failure() -> None:
        msg = "boom"
        raise RuntimeError(msg)

    async with QueueService(
        QueueConfig(
            execution_backend="local",
            event_config=QueueEventConfig(enabled=True, sink=sink),
        )
    ) as service:
        result = await service.enqueue(worker_failure)
        worker = Worker(service)

        assert await worker.run_once() == 1
        await result.refresh()

    assert result.status == "failed"
    assert [event.type for event in sink.events] == ["task.started", "task.failed"]
    assert sink.events[-1].message == "boom"


async def test_event_publish_failure_does_not_fail_successful_task_by_default() -> None:
    @task("tasks.event_sink_failure")
    async def event_sink_failure() -> str:
        await publish_task_progress(percent=100)
        return "ok"

    async with QueueService(
        QueueConfig(
            execution_backend="local",
            event_config=QueueEventConfig(enabled=True, sink=FailingSink()),
        )
    ) as service:
        result = await service.enqueue(event_sink_failure)
        worker = Worker(service)

        assert await worker.run_once() == 1
        await result.refresh()

    assert result.status == "completed"
    assert result.result == "ok"

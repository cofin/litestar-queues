import asyncio
import logging
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest

from litestar_queues import QueueConfig, QueueService, Worker, job_cancelled, task
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.events import (
    EventConfig,
    InMemoryQueueEventSink,
    QueueEvent,
    publish_task_log,
    publish_task_progress,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

pytestmark = pytest.mark.anyio


async def test_worker_emits_started_progress_and_terminal_events_in_order() -> "None":
    sink = InMemoryQueueEventSink()

    @task("tasks.worker_events")
    async def worker_events() -> "str":
        await publish_task_progress(current=1, total=2, message="halfway")
        return "ok"

    async with QueueService(
        QueueConfig(execution_backend="local", event=EventConfig(enabled=True, sink=sink))
    ) as service:
        result = await service.enqueue(worker_events)
        worker = Worker(service)

        assert await worker.run_once() == 1
        await result.wait(timeout=1, poll_interval=0.01)

    assert result.status == "completed"
    assert [event.type for event in sink.events] == ["task.started", "task.progress", "task.completed"]
    assert sink.events[1].message == "halfway"
    assert sink.events[-1].task_id == str(result.id)


async def test_worker_emits_failed_terminal_event_for_failed_attempt() -> "None":
    sink = InMemoryQueueEventSink()

    @task("tasks.worker_failure")
    async def worker_failure() -> "None":
        msg = "boom"
        raise RuntimeError(msg)

    async with QueueService(
        QueueConfig(execution_backend="local", event=EventConfig(enabled=True, sink=sink))
    ) as service:
        result = await service.enqueue(worker_failure)
        worker = Worker(service)

        assert await worker.run_once() == 1
        await result.wait(timeout=1, poll_interval=0.01)

    assert result.status == "failed"
    assert [event.type for event in sink.events] == ["task.started", "task.failed"]
    assert sink.events[-1].message == "boom"
    assert sink.events[-1].payload == {"status": "failed", "retry_count": 0, "will_retry": False}


async def test_worker_emits_cancelled_event_for_cancelled_attempt() -> "None":
    sink = InMemoryQueueEventSink()
    started = asyncio.Event()

    @task("tasks.worker_cancelled")
    async def worker_cancelled() -> "None":
        started.set()
        await asyncio.Event().wait()

    queue_backend = InMemoryQueueBackend()
    async with QueueService(
        QueueConfig(execution_backend="local", event=EventConfig(enabled=True, sink=sink)), queue_backend=queue_backend
    ) as service:
        result = await service.enqueue(worker_cancelled)
        claimed = await queue_backend.claim_task(result.id)
        assert claimed is not None
        runner = asyncio.create_task(service.execute_record(claimed))
        await asyncio.wait_for(started.wait(), timeout=1)
        runner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await runner

    assert [event.type for event in sink.events] == ["task.started", "task.cancelled"]


async def test_worker_marks_job_cancelled_error_terminal_without_retry() -> "None":
    sink = InMemoryQueueEventSink()

    @task("tasks.worker_job_cancelled", retries=3)
    async def worker_job_cancelled() -> "None":
        job_cancelled("domain cancellation")

    async with QueueService(
        QueueConfig(execution_backend="local", event=EventConfig(enabled=True, sink=sink))
    ) as service:
        result = await service.enqueue(worker_job_cancelled)
        worker = Worker(service)

        assert await worker.run_once() == 1
        await result.wait(timeout=1, poll_interval=0.01)

    assert result.status == "cancelled"
    assert result.record is not None
    assert result.record.retry_count == 0
    assert result.record.error is None
    assert [event.type for event in sink.events] == ["task.started", "task.cancelled"]
    assert sink.events[-1].message == "domain cancellation"
    assert sink.events[-1].payload == {"status": "cancelled", "retry_count": 0}


async def test_worker_emits_claim_lost_event_when_terminal_fence_rejects_stale_attempt() -> "None":
    sink = InMemoryQueueEventSink()
    queue_backend = InMemoryQueueBackend()

    @task("tasks.worker_claim_lost", retries=1)
    async def worker_claim_lost() -> "str":
        return "too late"

    async with QueueService(
        QueueConfig(execution_backend="local", event=EventConfig(enabled=True, sink=sink)), queue_backend=queue_backend
    ) as service:
        result = await service.enqueue(worker_claim_lost)
        claimed = await queue_backend.claim_task(result.id)
        assert claimed is not None
        stale_claim = replace(claimed)
        claimed.heartbeat_at = datetime.now(timezone.utc) - timedelta(minutes=10)
        stale_result = await queue_backend.requeue_stale_running(stale_after=timedelta(seconds=1))

        updated = await service.execute_record(stale_claim)

    assert stale_result.requeued == 1
    assert updated.status == "pending"
    assert [event.type for event in sink.events] == ["task.started", "task.claim_lost"]
    claim_lost = sink.events[-1]
    assert claim_lost.payload["phase"] == "complete"
    assert claim_lost.payload["expected_retry_count"] == 0
    assert claim_lost.payload["current_status"] == "pending"
    assert claim_lost.payload["current_retry_count"] == 1


async def test_worker_emits_stale_failed_event_for_terminal_stale_recovery() -> "None":
    sink = InMemoryQueueEventSink()
    queue_backend = InMemoryQueueBackend()

    @task("tasks.worker_stale_failed")
    async def worker_stale_failed() -> "None":
        return None

    async with QueueService(
        QueueConfig(execution_backend="local", event=EventConfig(enabled=True, sink=sink)), queue_backend=queue_backend
    ) as service:
        result = await service.enqueue(worker_stale_failed, requeue_on_stale=False)
        claimed = await queue_backend.claim_task(result.id)
        assert claimed is not None
        claimed.heartbeat_at = datetime.now(timezone.utc) - timedelta(minutes=10)

        stale_result = await service.recover_stale_tasks(stale_after=timedelta(seconds=1), worker_id="worker-1")

    assert stale_result.failed == 1
    assert stale_result.handler_needed == 1
    assert [event.type for event in sink.events] == ["task.stale_failed", "worker.stale_recovery"]
    stale_failed = sink.events[0]
    assert stale_failed.task_id == str(result.id)
    assert stale_failed.payload["status"] == "failed"
    assert stale_failed.payload["retry_count"] == 0
    assert stale_failed.payload["requeue_on_stale"] is False
    assert stale_failed.payload["handler_needed"] is True


async def test_quiet_success_suppresses_success_python_log_but_keeps_lifecycle_events(
    caplog: "pytest.LogCaptureFixture",
) -> "None":
    sink = InMemoryQueueEventSink()

    @task("tasks.worker_visible_success", log_level="info")
    async def worker_visible_success() -> "str":
        await publish_task_log("visible task log", level="info")
        return "visible"

    @task("tasks.worker_quiet_success", log_level="info", quiet_success=True)
    async def worker_quiet_success() -> "str":
        await publish_task_log("quiet task log", level="info")
        return "quiet"

    async with QueueService(
        QueueConfig(execution_backend="local", event=EventConfig(enabled=True, sink=sink))
    ) as service:
        with caplog.at_level(logging.INFO, logger="litestar_queues.service"):
            visible_result = await service.enqueue(worker_visible_success)
            quiet_result = await service.enqueue(worker_quiet_success)
            worker = Worker(service)
            assert await worker.run_once() == 1
            await visible_result.wait(timeout=1, poll_interval=0.01)
            assert await worker.run_once() == 1
            await quiet_result.wait(timeout=1, poll_interval=0.01)

    assert visible_result.status == "completed"
    assert quiet_result.status == "completed"
    event_types = [event.type for event in sink.events]
    assert event_types.count("task.completed") == 2
    assert [event.message for event in sink.events if event.type == "task.log"] == [
        "visible task log",
        "quiet task log",
    ]

    completed_logs = [
        record
        for record in caplog.records
        if record.name == "litestar_queues.service" and record.getMessage() == "Queue task completed"
    ]
    assert [getattr(record, "queue_task_name", None) for record in completed_logs] == ["tasks.worker_visible_success"]
    assert completed_logs[0].levelno == logging.INFO


async def test_event_publish_failure_does_not_fail_successful_task_by_default() -> "None":
    @task("tasks.event_sink_failure")
    async def event_sink_failure() -> "str":
        await publish_task_progress(percent=100)
        return "ok"

    async with QueueService(
        QueueConfig(execution_backend="local", event=EventConfig(enabled=True, sink=_FailingSink()))
    ) as service:
        result = await service.enqueue(event_sink_failure)
        worker = Worker(service)

        assert await worker.run_once() == 1
        await result.wait(timeout=1, poll_interval=0.01)

    assert result.status == "completed"
    assert result.result == "ok"


class _FailingSink:
    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> "None":
        msg = "event sink unavailable"
        raise RuntimeError(msg)

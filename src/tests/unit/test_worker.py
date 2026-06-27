import asyncio
import os
import threading
from contextlib import suppress
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from litestar import Litestar
from litestar.testing import AsyncTestClient

from litestar_queues import (
    QueueConfig,
    QueuePlugin,
    QueueService,
    Worker,
    get_current_task_context,
    non_retryable,
    task,
)
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.events import InMemoryQueueEventSink, QueueEventConfig
from litestar_queues.models import StaleTaskRecoveryResult
from litestar_queues.task import clear_task_registry

if TYPE_CHECKING:
    from litestar_queues.models import QueuedTaskRecord

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def clean_task_registry() -> None:
    clear_task_registry()


async def test_worker_run_once_processes_pending_local_task() -> None:
    @task("tasks.worker")
    async def worker_task(value: int) -> int:
        return value + 1

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        result = await service.enqueue(worker_task, 41)
        worker = Worker(service)

        assert await worker.run_once() == 1
        await result.refresh()

    assert result.status == "completed"
    assert result.result == 42


async def test_worker_retries_failed_task_until_success() -> None:
    attempts = 0

    @task("tasks.flaky", retries=1)
    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            msg = "not yet"
            raise RuntimeError(msg)
        return "ok"

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        result = await service.enqueue(flaky)
        worker = Worker(service)

        assert await worker.run_once() == 1
        await result.refresh()
        pending_status = result.status
        assert pending_status == "pending"

        assert await worker.run_once() == 1
        await result.refresh()

    assert attempts == 2
    completed_status = result.status
    assert completed_status == "completed"
    assert result.result == "ok"


async def test_worker_non_retryable_failure_skips_retries_and_injects_job_id() -> None:
    captured_job_id: str | None = None

    @task("tasks.permanent", retries=3)
    async def permanent_failure(*, _job_id: str) -> None:
        nonlocal captured_job_id
        captured_job_id = _job_id
        non_retryable("permanent failure")

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        result = await service.enqueue(permanent_failure)
        worker = Worker(service)

        assert await worker.run_once() == 1
        await result.refresh()

    assert captured_job_id == str(result.id)
    assert result.status == "failed"
    assert result.error == "permanent failure"
    assert result.record is not None
    assert result.record.retry_count == 0


async def test_worker_processes_batch_with_configured_concurrency() -> None:
    started = 0
    both_started = asyncio.Event()
    release = asyncio.Event()

    @task("tasks.concurrent")
    async def concurrent_task(value: int) -> int:
        nonlocal started
        started += 1
        if started == 2:
            both_started.set()
        await release.wait()
        return value

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        first = await service.enqueue(concurrent_task, 1)
        second = await service.enqueue(concurrent_task, 2)
        worker = Worker(service, batch_size=2, max_concurrency=2)

        run_once = asyncio.create_task(worker.run_once())
        await asyncio.wait_for(both_started.wait(), timeout=1)
        release.set()

        assert await run_once == 2
        await first.refresh()
        await second.refresh()

    assert first.status == "completed"
    assert second.status == "completed"


class _LimitRecordingInMemoryQueueBackend(InMemoryQueueBackend):
    __slots__ = ("list_limits",)

    def __init__(self) -> None:
        super().__init__()
        self.list_limits: list[int] = []

    async def list_pending(
        self, *, limit: int = 1, queue: str | None = None, execution_backend: str | None = None
    ) -> list["QueuedTaskRecord"]:
        self.list_limits.append(limit)
        return await super().list_pending(limit=limit, queue=queue, execution_backend=execution_backend)


async def test_worker_does_not_over_claim_beyond_available_concurrency() -> None:
    backend = _LimitRecordingInMemoryQueueBackend()

    @task("tasks.capacity")
    async def capacity(value: int) -> int:
        return value

    async with QueueService(QueueConfig(execution_backend="local"), queue_backend=backend) as service:
        for value in range(5):
            await service.enqueue(capacity, value)
        worker = Worker(service, batch_size=10, max_concurrency=2)

        assert await worker.run_once() == 2

    assert backend.list_limits[0] == 2


async def test_worker_queue_filter_restricts_claimed_records() -> None:
    @task("tasks.filtered")
    async def filtered(value: str) -> str:
        return value

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        default_result = await service.enqueue(filtered, "default")
        priority_result = await service.enqueue(filtered.using(queue="priority"), "priority")
        worker = Worker(service, queues=("priority",))

        assert await worker.run_once() == 1
        await default_result.refresh()
        await priority_result.refresh()

    assert default_result.status == "pending"
    assert priority_result.status == "completed"


async def test_worker_start_wakes_from_backend_notifications() -> None:
    @task("tasks.notified_worker")
    async def notified_worker(value: int) -> int:
        return value + 1

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        worker = Worker(service, poll_interval=60)
        worker_task = asyncio.create_task(worker.start())
        await asyncio.sleep(0)

        result = await service.enqueue(notified_worker, 41)
        await result.wait(timeout=1, poll_interval=0.01)

        await worker.stop()
        with suppress(asyncio.CancelledError):
            await asyncio.wait_for(worker_task, timeout=1)

    assert result.status == "completed"
    assert result.result == 42


class _CountingInMemoryQueueBackend(InMemoryQueueBackend):
    __slots__ = ("requeue_calls",)

    def __init__(self) -> None:
        super().__init__()
        self.requeue_calls: list[timedelta] = []

    async def requeue_stale_running(self, *, stale_after: timedelta) -> StaleTaskRecoveryResult:
        self.requeue_calls.append(stale_after)
        return await super().requeue_stale_running(stale_after=stale_after)


async def test_worker_periodic_requeue_calls_backend_on_cadence() -> None:
    backend = _CountingInMemoryQueueBackend()
    async with QueueService(QueueConfig(execution_backend="local"), queue_backend=backend) as service:
        worker = Worker(service, stale_after=timedelta(seconds=30), stale_check_interval=0.0)

        await worker._maybe_requeue_stale()
        await worker._maybe_requeue_stale()

    assert len(backend.requeue_calls) >= 2
    assert backend.requeue_calls[0] == timedelta(seconds=30)


async def test_worker_skips_periodic_requeue_when_stale_after_is_none() -> None:
    backend = _CountingInMemoryQueueBackend()
    async with QueueService(QueueConfig(execution_backend="local"), queue_backend=backend) as service:
        worker = Worker(service)  # stale_after defaults to None

        for _ in range(3):
            await worker._maybe_requeue_stale()

    assert backend.requeue_calls == []


async def test_worker_periodic_requeue_respects_cadence_window() -> None:
    backend = _CountingInMemoryQueueBackend()
    async with QueueService(QueueConfig(execution_backend="local"), queue_backend=backend) as service:
        worker = Worker(
            service,
            stale_after=timedelta(seconds=30),
            stale_check_interval=3600.0,  # one hour: only the first call fires
        )

        await worker._maybe_requeue_stale()
        await worker._maybe_requeue_stale()
        await worker._maybe_requeue_stale()

    assert len(backend.requeue_calls) == 1


async def test_worker_default_worker_id_uses_pid() -> None:
    async with QueueService(QueueConfig()) as service:
        worker = Worker(service)

    assert worker.worker_id == f"worker-{os.getpid()}"


async def test_worker_explicit_worker_id_overrides_default() -> None:
    async with QueueService(QueueConfig()) as service:
        worker = Worker(service, worker_id="worker-alpha-7")

    assert worker.worker_id == "worker-alpha-7"


async def test_worker_id_propagates_into_published_events() -> None:
    sink = InMemoryQueueEventSink()

    @task("tasks.worker_id_event")
    async def worker_id_task() -> str:
        return "ok"

    async with QueueService(
        QueueConfig(execution_backend="local", event_config=QueueEventConfig(enabled=True, sink=sink))
    ) as service:
        await service.enqueue(worker_id_task)
        worker = Worker(service, worker_id="worker-test")

        assert await worker.run_once() == 1

    assert sink.events, "Expected lifecycle events to be published"
    lifecycle_types = {"task.started", "task.completed", "task.failed"}
    lifecycle_events = [event for event in sink.events if event.type in lifecycle_types]
    assert lifecycle_events, "Expected at least one lifecycle event"
    for event in lifecycle_events:
        assert event.worker_id == "worker-test"


async def test_worker_stop_cancels_stuck_task_after_drain_timeout() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    @task("tasks.stuck")
    async def stuck() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        result = await service.enqueue(stuck)
        worker = Worker(service, graceful_shutdown_timeout=0.01, final_cancel_timeout=0.2)
        worker_task = asyncio.create_task(worker.start())
        await asyncio.wait_for(started.wait(), timeout=1)

        await worker.stop()
        await asyncio.wait_for(worker_task, timeout=1)
        await result.refresh()

    assert cancelled.is_set()
    assert result.status == "running"


async def test_plugin_shutdown_waits_for_in_flight_worker_task() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    @task("tasks.plugin_drain")
    async def plugin_drain() -> str:
        started.set()
        await release.wait()
        return "ok"

    plugin = QueuePlugin(
        QueueConfig(
            execution_backend="local",
            start_worker=True,
            worker_poll_interval=0.01,
            worker_graceful_shutdown_timeout=1,
        )
    )
    app = Litestar(plugins=[plugin])

    async with AsyncTestClient(app=app):
        service = app.state[plugin.config.queue_service_state_key]
        result = await service.enqueue(plugin_drain)
        await asyncio.wait_for(started.wait(), timeout=1)
        release.set()

    await result.refresh()
    assert result.status == "completed"


async def test_sync_task_uses_configured_executor_and_preserves_task_context() -> None:
    @task("tasks.sync_context")
    def sync_context(*, _job_id: str) -> dict[str, str | None]:
        context = get_current_task_context()
        assert context is not None
        return {
            "job_id": _job_id,
            "context_task_id": context.task_id,
            "thread_name": threading.current_thread().name,
        }

    async with QueueService(
        QueueConfig(execution_backend="immediate", sync_executor_max_workers=1, sync_executor_thread_name_prefix="lq")
    ) as service:
        result = await service.enqueue(sync_context)

    assert isinstance(result.result, dict)
    assert result.result["job_id"] == result.result["context_task_id"]
    assert str(result.result["thread_name"]).startswith("lq")

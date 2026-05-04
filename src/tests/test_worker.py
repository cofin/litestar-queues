import asyncio
from contextlib import suppress
from uuid import UUID

import pytest

from litestar_queues import QueueConfig, QueueService, Worker, non_retryable, task
from litestar_queues.task import clear_task_registry

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
    captured_job_id: UUID | None = None

    @task("tasks.permanent", retries=3)
    async def permanent_failure(*, _job_id: UUID) -> None:
        nonlocal captured_job_id
        captured_job_id = _job_id
        non_retryable("permanent failure")

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        result = await service.enqueue(permanent_failure)
        worker = Worker(service)

        assert await worker.run_once() == 1
        await result.refresh()

    assert captured_job_id == result.id
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

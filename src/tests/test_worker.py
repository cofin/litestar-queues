import pytest

from litestar_queues import QueueConfig, QueueService, Worker, task
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

from datetime import UTC, datetime, timedelta

import pytest

from litestar_queues import QueueConfig, QueueService, task
from litestar_queues.backends import InMemoryStorageBackend
from litestar_queues.task import clear_task_registry

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def clean_task_registry() -> None:
    clear_task_registry()


async def test_memory_backend_deduplicates_active_keys_and_replaces_terminal_keys() -> None:
    backend = InMemoryStorageBackend()

    first = await backend.enqueue("tasks.sync", kwargs={"account_id": "acct-1"}, key="sync:acct-1")
    duplicate = await backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")

    assert duplicate.id == first.id
    assert duplicate.kwargs == {"account_id": "acct-1"}

    await backend.complete_task(first.id, result={"ok": True})
    replacement = await backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")

    assert replacement.id != first.id
    assert replacement.kwargs == {"account_id": "acct-2"}


async def test_memory_backend_claims_due_tasks_by_priority_and_marks_lifecycle() -> None:
    backend = InMemoryStorageBackend()
    later = datetime.now(UTC) + timedelta(minutes=5)

    low = await backend.enqueue("tasks.low", priority=1)
    scheduled = await backend.enqueue("tasks.later", priority=100, scheduled_at=later)
    high = await backend.enqueue("tasks.high", priority=10)

    claimed = await backend.claim_next()

    assert claimed is not None
    assert claimed.id == high.id
    assert claimed.status == "running"
    assert claimed.started_at is not None
    assert await backend.get_task(low.id) is low
    assert (await backend.get_task(scheduled.id)) is scheduled


async def test_memory_backend_fail_task_retries_then_fails_permanently() -> None:
    backend = InMemoryStorageBackend()
    record = await backend.enqueue("tasks.flaky", max_retries=1)

    await backend.claim_task(record.id)
    retried = await backend.fail_task(record.id, "first failure")

    assert retried is record
    assert retried.status == "pending"
    assert retried.retry_count == 1

    await backend.claim_task(record.id)
    failed = await backend.fail_task(record.id, "second failure")

    assert failed is record
    assert failed.status == "failed"
    assert failed.error == "second failure"
    assert failed.completed_at is not None


async def test_queue_service_local_enqueue_persists_until_worker_processes_record() -> None:
    @task("tasks.upper", retries=1)
    async def uppercase(value: str) -> str:
        return value.upper()

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        result = await service.enqueue(uppercase, "queue")

        pending_status = result.status
        assert pending_status == "pending"

        record = await service.claim_next()
        assert record is not None
        await service.execute_record(record)
        await result.refresh()

    completed_status = result.status
    assert completed_status == "completed"
    assert result.result == "QUEUE"

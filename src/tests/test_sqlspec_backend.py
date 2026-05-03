from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import AsyncIterator

import pytest

pytest.importorskip("aiosqlite")
pytest.importorskip("sqlspec")

from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues import QueueConfig, QueueService, task
from litestar_queues.backends import get_storage_backend_class, list_storage_backends
from litestar_queues.backends.sqlspec import SQLSpecStorageBackend
from litestar_queues.task import clear_task_registry

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def clean_task_registry() -> None:
    clear_task_registry()


@pytest.fixture
async def sqlspec_backend(tmp_path: Path) -> AsyncIterator[SQLSpecStorageBackend]:
    backend = SQLSpecStorageBackend(sqlspec_config=_sqlite_config(tmp_path / "queue.db"))
    await backend.open()
    try:
        yield backend
    finally:
        await backend.close()


def _sqlite_config(path: Path) -> AiosqliteConfig:
    return AiosqliteConfig(connection_config={"database": str(path)})


async def test_sqlspec_backend_is_registered_without_advanced_alchemy() -> None:
    assert "sqlspec" in list_storage_backends()
    assert get_storage_backend_class("sqlspec") is SQLSpecStorageBackend


async def test_sqlspec_backend_deduplicates_active_keys_and_replaces_terminal_keys(
    sqlspec_backend: SQLSpecStorageBackend,
) -> None:
    first = await sqlspec_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-1"}, key="sync:acct-1")
    duplicate = await sqlspec_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")

    assert duplicate.id == first.id
    assert duplicate.kwargs == {"account_id": "acct-1"}

    await sqlspec_backend.complete_task(first.id, result={"ok": True})
    replacement = await sqlspec_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")

    assert replacement.id != first.id
    assert replacement.kwargs == {"account_id": "acct-2"}
    keyed = await sqlspec_backend.get_task_by_key("sync:acct-1")
    assert keyed is not None
    assert keyed.id == replacement.id


async def test_sqlspec_backend_claims_due_tasks_by_priority(sqlspec_backend: SQLSpecStorageBackend) -> None:
    later = datetime.now(UTC) + timedelta(minutes=5)

    low = await sqlspec_backend.enqueue("tasks.low", priority=1)
    scheduled = await sqlspec_backend.enqueue("tasks.later", priority=100, scheduled_at=later)
    high = await sqlspec_backend.enqueue("tasks.high", priority=10)

    claimed = await sqlspec_backend.claim_next()

    assert claimed is not None
    assert claimed.id == high.id
    assert claimed.status == "running"
    assert claimed.started_at is not None
    stored_low = await sqlspec_backend.get_task(low.id)
    stored_scheduled = await sqlspec_backend.get_task(scheduled.id)
    assert stored_low is not None
    assert stored_scheduled is not None
    assert stored_low.status == "pending"
    assert stored_scheduled.status == "scheduled"


async def test_sqlspec_backend_fail_task_retries_then_fails_permanently(
    sqlspec_backend: SQLSpecStorageBackend,
) -> None:
    record = await sqlspec_backend.enqueue("tasks.flaky", max_retries=1)

    await sqlspec_backend.claim_task(record.id)
    retried = await sqlspec_backend.fail_task(record.id, "first failure")

    assert retried is not None
    assert retried.status == "pending"
    assert retried.retry_count == 1

    await sqlspec_backend.claim_task(record.id)
    failed = await sqlspec_backend.fail_task(record.id, "second failure")

    assert failed is not None
    assert failed.status == "failed"
    assert failed.error == "second failure"
    assert failed.completed_at is not None


async def test_sqlspec_backend_cancels_heartbeats_and_requeues_stale_running(
    sqlspec_backend: SQLSpecStorageBackend,
) -> None:
    pending = await sqlspec_backend.enqueue("tasks.cancel")

    assert await sqlspec_backend.cancel_task(pending.id)
    assert not await sqlspec_backend.cancel_task(pending.id)

    cancelled = await sqlspec_backend.get_task(pending.id)
    assert cancelled is not None
    assert cancelled.status == "cancelled"

    running = await sqlspec_backend.enqueue("tasks.heartbeat")
    claimed = await sqlspec_backend.claim_task(running.id)

    assert claimed is not None
    assert claimed.heartbeat_at is not None

    await sqlspec_backend.touch_heartbeat(claimed.id)
    touched = await sqlspec_backend.get_task(claimed.id)

    assert touched is not None
    assert touched.heartbeat_at is not None
    assert touched.heartbeat_at >= claimed.heartbeat_at

    assert await sqlspec_backend.requeue_stale_running(stale_after=timedelta(seconds=0)) == 1
    requeued = await sqlspec_backend.get_task(claimed.id)

    assert requeued is not None
    assert requeued.status == "pending"
    assert requeued.retry_count == 1


async def test_queue_service_uses_sqlspec_backend_from_config(tmp_path: Path) -> None:
    @task("tasks.lower", retries=1)
    async def lowercase(value: str) -> str:
        return value.lower()

    config = QueueConfig(
        storage_backend="sqlspec",
        storage_backend_config={"sqlspec_config": _sqlite_config(tmp_path / "service.db")},
        execution_backend="local",
    )
    async with QueueService(config) as service:
        result = await service.enqueue(lowercase, "QUEUE")

        pending_status = result.status
        assert pending_status == "pending"

        record = await service.claim_next()
        assert record is not None
        await service.execute_record(record)
        await result.refresh()

    completed_status = result.status
    assert completed_status == "completed"
    assert result.result == "queue"

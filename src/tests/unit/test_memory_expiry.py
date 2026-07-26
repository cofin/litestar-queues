from datetime import datetime, timedelta, timezone

import pytest

from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.backends.base import BaseQueueBackend
from litestar_queues.backends.memory import backend as memory_backend_module
from tests.helpers._timing import MutableClock
from tests.integration._expiry_contract import assert_claim_many_preserves_expired_dispatch_reservation

pytestmark = pytest.mark.anyio


async def test_base_expire_overdue_is_a_safe_noop() -> "None":
    assert await BaseQueueBackend().expire_overdue() == []


async def test_memory_claim_fences_expired_record() -> "None":
    backend = InMemoryQueueBackend()
    record = await backend.enqueue("tasks.expiring", expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))

    claimed = await backend.claim_task(record.id)
    stored = await backend.get_task(record.id)

    assert claimed is None
    assert stored is not None
    assert stored.status == "expired"
    assert stored.completed_at is not None


async def test_memory_list_pending_excludes_expired() -> "None":
    backend = InMemoryQueueBackend()
    expired = await backend.enqueue("tasks.expired", expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
    available = await backend.enqueue("tasks.available")

    pending = await backend.list_pending(limit=10)

    assert [record.id for record in pending] == [available.id]
    assert expired.id not in {record.id for record in pending}


async def test_memory_claim_many_fences_and_transitions_expired_records() -> "None":
    backend = InMemoryQueueBackend()
    expired = await backend.enqueue("tasks.expired_batch", expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
    available = await backend.enqueue("tasks.available_batch")

    claimed = await backend.claim_many(limit=10)
    stored_expired = await backend.get_task(expired.id)

    assert [record.id for record in claimed] == [available.id]
    assert stored_expired is not None
    assert stored_expired.status == "expired"


async def test_memory_claim_many_preserves_expired_dispatch_reservation(monkeypatch: "pytest.MonkeyPatch") -> "None":
    clock = MutableClock()
    monkeypatch.setattr(memory_backend_module, "_utc_now", clock)

    await assert_claim_many_preserves_expired_dispatch_reservation(InMemoryQueueBackend(), clock)


async def test_memory_expire_overdue_transitions_once_and_respects_limit() -> "None":
    backend = InMemoryQueueBackend()
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    records = [await backend.enqueue(f"tasks.expire_{index}", expires_at=past) for index in range(3)]

    first = await backend.expire_overdue(limit=2)
    second = await backend.expire_overdue(limit=2)
    third = await backend.expire_overdue(limit=2)

    assert [record.id for record in first] == [records[0].id, records[1].id]
    assert [record.id for record in second] == [records[2].id]
    assert third == []
    assert all(record.status == "expired" for record in [*first, *second])


async def test_memory_expire_overdue_ignores_future_and_running() -> "None":
    backend = InMemoryQueueBackend()
    future = datetime.now(timezone.utc) + timedelta(minutes=1)
    future_record = await backend.enqueue("tasks.future", expires_at=future)
    running = await backend.enqueue("tasks.running", expires_at=future)
    claimed = await backend.claim_task(running.id)
    assert claimed is not None
    running.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)

    expired = await backend.expire_overdue()

    assert expired == []
    assert future_record.status == "pending"
    assert running.status == "running"


async def test_memory_statistics_and_cleanup_include_expired() -> "None":
    backend = InMemoryQueueBackend()
    record = await backend.enqueue(
        "tasks.cleanup_expired", expires_at=datetime.now(timezone.utc) - timedelta(seconds=1)
    )
    expired = await backend.expire_overdue()

    statistics = await backend.get_statistics()
    removed = await backend.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1))

    assert [item.id for item in expired] == [record.id]
    assert statistics.expired == 1
    assert removed == 1
    assert await backend.get_task(record.id) is None

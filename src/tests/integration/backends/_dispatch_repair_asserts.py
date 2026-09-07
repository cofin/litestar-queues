"""Shared storage contracts for bounded delivery repair and nullable reference CAS."""

import asyncio
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from litestar_queues.backends.base import BaseQueueBackend
    from litestar_queues.models import QueuedTaskRecord


async def _persisted_creation_order(
    backend: "BaseQueueBackend", records: "tuple[QueuedTaskRecord, ...]"
) -> "list[QueuedTaskRecord]":
    persisted = [await backend.get_task(record.id) for record in records]
    assert all(record is not None for record in persisted)
    expected = sorted(
        (record for record in persisted if record is not None), key=lambda record: (record.created_at, str(record.id))
    )
    # MySQL's existing second-precision created_at can round into the future.
    # Start scans after every persisted creation time so a new precise check
    # mark sorts after the never-checked records in this rotation assertion.
    delay = (max(record.created_at for record in expected) - datetime.now(timezone.utc)).total_seconds()
    assert delay <= 1
    if delay >= 0:
        await asyncio.sleep(delay + 0.001)
    return expected


async def assert_dispatch_repair_candidates(backend: "BaseQueueBackend") -> "None":
    now = datetime.now(timezone.utc)
    pending = await backend.enqueue("repair.pending", execution_backend="cloudtasks")
    future = await backend.enqueue(
        "repair.future", execution_backend="cloudtasks", scheduled_at=now + timedelta(hours=1)
    )
    healthy = await backend.enqueue("repair.healthy", execution_backend="cloudtasks")
    await backend.set_execution_ref(healthy.id, "cloudtasks", "healthy-delivery")
    await backend.enqueue("repair.local")
    await backend.enqueue("repair.other", execution_backend="cloudrun")
    await backend.enqueue("repair.expired", execution_backend="cloudtasks", expires_at=now - timedelta(seconds=1))
    running = await backend.enqueue("repair.running", execution_backend="cloudtasks")
    await backend.claim_task(running.id)
    completed = await backend.enqueue("repair.completed", execution_backend="cloudtasks")
    await backend.claim_task(completed.id)
    await backend.complete_task(completed.id)
    cancelled = await backend.enqueue("repair.cancelled", execution_backend="cloudtasks")
    await backend.cancel_task(cancelled.id)

    expected = await _persisted_creation_order(backend, (pending, future, healthy))
    first = await backend.list_dispatch_repair_candidates("cloudtasks", limit=2)
    assert [record.id for record in first.records] == [record.id for record in expected[:2]]
    assert first.examined == 2
    assert first.limit_reached is True
    check_time = first.records[0].dispatch_checked_at
    assert check_time is not None
    assert all(record.dispatch_checked_at == check_time for record in first.records)
    for record in first.records:
        stored = await backend.get_task(record.id)
        assert stored is not None
        assert stored.dispatch_checked_at == check_time
    second = await backend.list_dispatch_repair_candidates("cloudtasks", limit=1)
    assert [record.id for record in second.records] == [expected[2].id], (
        [(record.id, record.created_at, record.dispatch_checked_at) for record in expected],
        [(record.id, record.created_at, record.dispatch_checked_at) for record in first.records],
        [(record.id, record.created_at, record.dispatch_checked_at) for record in second.records],
    )
    assert second.examined == 1
    assert second.limit_reached is True
    all_candidates = await backend.list_dispatch_repair_candidates("cloudtasks", limit=10)
    assert {record.id for record in all_candidates.records} == {record.id for record in expected}
    assert all_candidates.examined == 3
    assert all_candidates.limit_reached is False
    stored_future = await backend.get_task(future.id)
    stored_healthy = await backend.get_task(healthy.id)
    assert stored_future is not None and stored_future.status == "scheduled"
    assert stored_healthy is not None and stored_healthy.execution_ref == "healthy-delivery"

    empty = await backend.list_dispatch_repair_candidates("missing", limit=1)
    assert empty.records == ()
    assert empty.examined == 0
    assert empty.limit_reached is False
    zero = await backend.list_dispatch_repair_candidates("cloudtasks", limit=0)
    assert zero.records == ()
    assert zero.examined == 0
    assert zero.limit_reached is False
    with pytest.raises(QueueConfigurationError, match="non-negative"):
        await backend.list_dispatch_repair_candidates("cloudtasks", limit=-1)


async def assert_scheduled_execution_ref_contenders(
    backend: "BaseQueueBackend", second_backend: "BaseQueueBackend | None" = None
) -> "None":
    future = await backend.enqueue(
        "repair.future_cas",
        execution_backend="cloudtasks",
        execution_profile="profile",
        scheduled_at=datetime.now(timezone.utc) + timedelta(hours=1),
        max_retries=3,
        metadata={"unchanged": True},
    )
    await backend.list_dispatch_repair_candidates("cloudtasks", limit=1)
    snapshot = await backend.get_task(future.id)
    assert snapshot is not None
    before = asdict(snapshot)
    barrier = asyncio.Event()
    ready: asyncio.Queue[None] = asyncio.Queue()

    async def contender(storage: "BaseQueueBackend", reference: "str") -> "QueuedTaskRecord | None":
        record = await storage.get_task(future.id)
        assert record is not None
        retry_count, prior_reference = record.retry_count, record.execution_ref
        ready.put_nowait(None)
        await barrier.wait()
        return await storage.reserve_scheduled_execution_ref(
            record.id, "cloudtasks", reference, expected_retry_count=retry_count, expected_execution_ref=prior_reference
        )

    contenders = (
        asyncio.create_task(contender(backend, "delivery-a")),
        asyncio.create_task(contender(second_backend or backend, "delivery-b")),
    )
    await ready.get()
    await ready.get()
    barrier.set()
    results = await asyncio.gather(*contenders)
    assert sum(record is not None for record in results) == 1
    stored = await backend.get_task(future.id)
    assert stored is not None
    before["execution_ref"] = stored.execution_ref
    assert asdict(stored) == before
    retry_count, prior_reference = stored.retry_count, stored.execution_ref
    assert prior_reference in {"delivery-a", "delivery-b"}
    replacement = await backend.reserve_scheduled_execution_ref(
        future.id, "cloudtasks", "replacement", expected_retry_count=retry_count, expected_execution_ref=prior_reference
    )
    assert replacement is not None
    assert replacement.execution_ref == "replacement"
    assert (
        await backend.reserve_external_dispatch(future.id, "cloudtasks", "due-only", expected_retry_count=retry_count)
        is None
    )


async def assert_scheduled_execution_ref_rejects_mismatches(backend: "BaseQueueBackend") -> "None":
    record = await backend.enqueue("repair.cas_mismatch", execution_backend="cloudtasks")
    snapshot = await backend.get_task(record.id)
    assert snapshot is not None
    before = asdict(snapshot)
    for task_id, backend_name, retry_count, reference in (
        (uuid4(), "cloudtasks", 0, None),
        (record.id, "cloudrun", 0, None),
        (record.id, "cloudtasks", 1, None),
        (record.id, "cloudtasks", 0, "missing"),
    ):
        assert (
            await backend.reserve_scheduled_execution_ref(
                task_id, backend_name, "unowned", expected_retry_count=retry_count, expected_execution_ref=reference
            )
            is None
        )
    stored = await backend.get_task(record.id)
    assert stored is not None
    assert asdict(stored) == before
    await backend.claim_task(record.id)
    assert (
        await backend.reserve_scheduled_execution_ref(
            record.id, "cloudtasks", "running", expected_retry_count=0, expected_execution_ref=None
        )
        is None
    )
    await backend.complete_task(record.id)
    assert (
        await backend.reserve_scheduled_execution_ref(
            record.id, "cloudtasks", "terminal", expected_retry_count=0, expected_execution_ref=None
        )
        is None
    )
    expired = await backend.enqueue(
        "repair.cas_expired",
        execution_backend="cloudtasks",
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    assert (
        await backend.reserve_scheduled_execution_ref(
            expired.id, "cloudtasks", "expired", expected_retry_count=0, expected_execution_ref=None
        )
        is None
    )

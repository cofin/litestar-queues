"""Shared pending-job expiration assertions for every queue backend."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from litestar_queues.backends import BaseQueueBackend


async def assert_expired_claim_is_fenced(queue_backend: "BaseQueueBackend") -> "None":
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    overdue = await queue_backend.enqueue("tasks.overdue", expires_at=past)
    live = await queue_backend.enqueue("tasks.live", expires_at=datetime.now(timezone.utc) + timedelta(hours=1))

    claimed = await queue_backend.claim_task(overdue.id)
    stored_overdue = await queue_backend.get_task(overdue.id)
    pending_ids = [record.id for record in await queue_backend.list_pending(limit=10)]

    assert claimed is None
    assert stored_overdue is not None
    assert stored_overdue.status == "expired"
    assert overdue.id not in pending_ids
    assert live.id in pending_ids


async def assert_expire_overdue_transitions_and_reports(queue_backend: "BaseQueueBackend") -> "None":
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    records = [await queue_backend.enqueue(f"tasks.expire.{index}", expires_at=past) for index in range(3)]

    expired = await queue_backend.expire_overdue(limit=len(records))
    statistics = await queue_backend.get_statistics()
    removed = await queue_backend.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1))

    assert len(expired) == len(records)
    assert {record.id for record in expired} == {record.id for record in records}
    assert all(record.status == "expired" for record in expired)
    assert statistics.expired == len(records)
    assert removed == len(records)


async def assert_expiry_fences_retry_requeue(queue_backend: "BaseQueueBackend") -> "None":
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=2)
    record = await queue_backend.enqueue("tasks.retry_expiry", max_retries=1, expires_at=expires_at)
    claimed = await queue_backend.claim_task(record.id)
    assert claimed is not None
    requeued = await queue_backend.fail_task(
        record.id, "retry after deadline", expected_retry_count=claimed.retry_count
    )
    assert requeued is not None
    assert requeued.status == "pending"
    assert requeued.expires_at is not None
    remaining = (requeued.expires_at - datetime.now(timezone.utc)).total_seconds()
    if remaining > 0:
        await asyncio.sleep(remaining + 0.25)

    reclaimed = await queue_backend.claim_task(record.id)
    stored = await queue_backend.get_task(record.id)

    assert reclaimed is None
    assert stored is not None
    assert stored.status == "expired"


async def assert_future_deadline_is_claimable(queue_backend: "BaseQueueBackend") -> "None":
    record = await queue_backend.enqueue(
        "tasks.future_expiry", expires_at=datetime.now(timezone.utc) + timedelta(hours=1)
    )

    claimed = await queue_backend.claim_task(record.id)
    assert claimed is not None
    completed = await queue_backend.complete_task(record.id, result="ok", expected_retry_count=claimed.retry_count)

    assert completed is not None
    assert completed.status == "completed"
    assert completed.result == "ok"


async def assert_external_dispatch_reservation_is_fenced(queue_backend: "BaseQueueBackend") -> "None":
    expired = await queue_backend.enqueue(
        "tasks.expired_dispatch",
        execution_backend="cloudrun",
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    live = await queue_backend.enqueue(
        "tasks.live_dispatch",
        execution_backend="cloudrun",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )

    assert await queue_backend.reserve_external_dispatch(expired.id, "cloudrun", "reservation:expired") is None
    reserved = await queue_backend.reserve_external_dispatch(live.id, "cloudrun", "reservation:owner")
    assert reserved is not None
    assert reserved.execution_ref == "reservation:owner"
    duplicate = await queue_backend.reserve_external_dispatch(live.id, "cloudrun", "reservation:competitor")
    swept = await queue_backend.expire_overdue()
    wrong_release = await queue_backend.release_external_dispatch(
        live.id, "reservation:competitor", "cloudrun"
    )
    released = await queue_backend.release_external_dispatch(live.id, "reservation:owner", "cloudrun")

    assert duplicate is None
    assert live.id not in {record.id for record in swept}
    assert wrong_release is None
    assert released is not None
    assert released.execution_ref is None

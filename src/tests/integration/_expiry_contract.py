"""Shared pending-job expiration assertions for every queue backend."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from litestar_queues.backends import BaseQueueBackend
    from tests.helpers._timing import MutableClock

from litestar_queues.backends.base import EXTERNAL_DISPATCH_RESERVATION_PREFIX


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


async def assert_claim_many_reports_owned_expirations(queue_backend: "BaseQueueBackend") -> "None":
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    overdue = await queue_backend.enqueue("tasks.batch_overdue", expires_at=past)
    live = await queue_backend.enqueue("tasks.batch_live", expires_at=datetime.now(timezone.utc) + timedelta(hours=1))

    claimed, expired = await queue_backend.claim_many_with_expired(limit=1)

    assert [record.id for record in claimed] == [live.id]
    assert [record.id for record in expired] == [overdue.id]
    assert expired[0].status == "expired"


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
    expired_reservation = f"{EXTERNAL_DISPATCH_RESERVATION_PREFIX}expired"
    owner_reservation = f"{EXTERNAL_DISPATCH_RESERVATION_PREFIX}owner"
    competitor_reservation = f"{EXTERNAL_DISPATCH_RESERVATION_PREFIX}competitor"
    first_reservation = f"{EXTERNAL_DISPATCH_RESERVATION_PREFIX}first"
    second_reservation = f"{EXTERNAL_DISPATCH_RESERVATION_PREFIX}second"
    expired = await queue_backend.enqueue(
        "tasks.expired_dispatch",
        execution_backend="cloudrun",
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    live = await queue_backend.enqueue(
        "tasks.live_dispatch", execution_backend="cloudrun", expires_at=datetime.now(timezone.utc) + timedelta(hours=1)
    )

    assert await queue_backend.reserve_external_dispatch(expired.id, "cloudrun", expired_reservation) is None
    reserved = await queue_backend.reserve_external_dispatch(live.id, "cloudrun", owner_reservation)
    assert reserved is not None
    assert reserved.execution_ref == owner_reservation
    duplicate = await queue_backend.reserve_external_dispatch(live.id, "cloudrun", competitor_reservation)
    swept = await queue_backend.expire_overdue()
    wrong_release = await queue_backend.release_external_dispatch(live.id, competitor_reservation, "cloudrun")
    released = await queue_backend.release_external_dispatch(live.id, owner_reservation, "cloudrun")

    finalizable = await queue_backend.enqueue(
        "tasks.finalizable_dispatch",
        queue="finalize-fence",
        execution_backend="cloudrun",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    first = await queue_backend.reserve_external_dispatch(finalizable.id, "cloudrun", first_reservation)
    assert first is not None
    assert await queue_backend.release_external_dispatch(finalizable.id, first_reservation, "cloudrun") is not None
    second = await queue_backend.reserve_external_dispatch(finalizable.id, "cloudrun", second_reservation)
    assert second is not None
    reserved_claimed, reserved_expired = await queue_backend.claim_many_with_expired(
        limit=1, queues=("finalize-fence",), execution_backend="cloudrun"
    )
    stale_finalized = await queue_backend.finalize_external_dispatch(
        finalizable.id, first_reservation, "cloudrun", "operations/stale"
    )
    finalized = await queue_backend.finalize_external_dispatch(
        finalizable.id, second_reservation, "cloudrun", "operations/current"
    )

    assert duplicate is None
    assert live.id not in {record.id for record in swept}
    assert wrong_release is None
    assert released is not None
    assert released.execution_ref is None
    assert reserved_claimed == []
    assert reserved_expired == []
    assert stale_finalized is None
    assert finalized is not None
    assert finalized.execution_ref == "operations/current"


async def assert_claim_many_preserves_expired_dispatch_reservation(
    queue_backend: "BaseQueueBackend", clock: "MutableClock"
) -> "None":
    reservation = f"{EXTERNAL_DISPATCH_RESERVATION_PREFIX}leased"
    leased = await queue_backend.enqueue(
        "tasks.leased_dispatch",
        queue="lease-fence",
        execution_backend="cloudrun",
        expires_at=clock() + timedelta(hours=1),
    )
    leased_record = await queue_backend.reserve_external_dispatch(leased.id, "cloudrun", reservation)
    assert leased_record is not None
    clock.advance(timedelta(hours=2))

    claimed, expired = await queue_backend.claim_many_with_expired(
        limit=1, queues=("lease-fence",), execution_backend="cloudrun"
    )
    stored = await queue_backend.get_task(leased.id)

    assert claimed == []
    assert leased.id not in {record.id for record in expired}
    assert stored is not None
    assert stored.status in {"pending", "scheduled"}
    assert stored.execution_ref == reservation


async def assert_finalized_dispatch_claims_after_queue_deadline(queue_backend: "BaseQueueBackend") -> "None":
    record = await queue_backend.enqueue(
        "tasks.finalized_before_deadline",
        execution_backend="cloudrun",
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    finalized = await queue_backend.set_execution_ref(record.id, "cloudrun", "operations/accepted-before-deadline")
    assert finalized is not None

    claimed, expired = await queue_backend.claim_task_with_expired(record.id)

    assert claimed is not None
    assert claimed.id == record.id
    assert claimed.status == "running"
    assert claimed.execution_ref == "operations/accepted-before-deadline"
    assert expired is None

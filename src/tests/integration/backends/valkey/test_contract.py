"""Valkey backend contract suite against a real Valkey 8 container.

Mirror of ``backends/redis/test_contract.py`` against ``ValkeyQueueBackend``.
The Valkey wire protocol is API-compatible with Redis so the test bodies
are identical apart from the fixture name (``valkey_backend``) and the
notification-capability label (``valkey-pubsub``).
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

import pytest

pytest.importorskip("valkey")

from litestar_queues import EnqueueSpec
from litestar_queues.models import HeartbeatTouch

if TYPE_CHECKING:
    from litestar_queues.backends.valkey import ValkeyQueueBackend

pytestmark = pytest.mark.anyio


async def test_valkey_backend_keeps_claim_fallback_without_batch_capability(
    valkey_backend: "ValkeyQueueBackend",
) -> "None":
    """Valkey mirrors Redis: no native batch claim, correctness fallback only.

    The Valkey sorted set is ordered by due time, so a bounded atomic
    ``claim_many`` needs a ready-by-priority index migration. The inherited
    ``claim_next`` loop must preserve priority ordering and exclusive ownership.
    """

    high = await valkey_backend.enqueue("tasks.valkey.batch.high", priority=10)
    mid = await valkey_backend.enqueue("tasks.valkey.batch.mid", priority=5)
    low = await valkey_backend.enqueue("tasks.valkey.batch.low", priority=1)

    claimed = await valkey_backend.claim_many(limit=2)

    assert [record.id for record in claimed] == [high.id, mid.id]
    assert all(record.status == "running" for record in claimed)
    stored_low = await valkey_backend.get_task(low.id)
    assert stored_low is not None
    assert stored_low.status == "pending"

    remaining = await valkey_backend.claim_many(limit=5)
    assert [record.id for record in remaining] == [low.id]
    assert remaining[0].status == "running"
    assert await valkey_backend.claim_many(limit=5) == []


async def test_valkey_backend_deduplicates_active_keys_and_replaces_terminal_keys(
    valkey_backend: "ValkeyQueueBackend",
) -> "None":
    first = await valkey_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-1"}, key="sync:acct-1")
    duplicate = await valkey_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")

    assert duplicate.id == first.id
    assert duplicate.kwargs == {"account_id": "acct-1"}

    await valkey_backend.claim_task(first.id)
    await valkey_backend.complete_task(first.id, result={"ok": True})
    replacement = await valkey_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")
    keyed = await valkey_backend.get_task_by_key("sync:acct-1")

    assert replacement.id != first.id
    assert replacement.kwargs == {"account_id": "acct-2"}
    assert keyed is not None
    assert keyed.id == replacement.id


async def test_valkey_backend_claims_due_tasks_once_by_priority_and_filters_execution(
    valkey_backend: "ValkeyQueueBackend",
) -> "None":
    later = datetime.now(timezone.utc) + timedelta(minutes=5)

    low = await valkey_backend.enqueue("tasks.low", priority=1, execution_backend="local")
    await valkey_backend.enqueue("tasks.later", priority=100, scheduled_at=later, execution_backend="local")
    high = await valkey_backend.enqueue("tasks.high", priority=10, execution_backend="cloudrun")

    local_pending = await valkey_backend.list_pending(limit=10, execution_backend="local")
    cloud_pending = await valkey_backend.list_pending(limit=10, execution_backend="cloudrun")

    assert [record.id for record in local_pending] == [low.id]
    assert [record.id for record in cloud_pending] == [high.id]

    claimed_results = await asyncio.gather(valkey_backend.claim_task(high.id), valkey_backend.claim_task(high.id))
    claimed = [record for record in claimed_results if record is not None]

    assert len(claimed) == 1
    assert claimed[0].id == high.id
    assert claimed[0].status == "running"
    assert claimed[0].started_at is not None
    stored_low = await valkey_backend.get_task(low.id)
    assert stored_low is not None
    assert stored_low.status == "pending"


async def test_valkey_enqueue_many_records_remain_claimable_when_batch_marker_is_dropped(
    valkey_backend: "ValkeyQueueBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    async def drop_marker(self: "ValkeyQueueBackend", records: "object") -> "None":
        del self, records

    monkeypatch.setattr(type(valkey_backend), "notify_new_tasks", drop_marker)

    records = await valkey_backend.enqueue_many([
        EnqueueSpec(task_name=f"tasks.batch.{index}", kwargs={"index": index}) for index in range(25)
    ])
    pending = await valkey_backend.list_pending(limit=30)
    claimed = [await valkey_backend.claim_task(record.id) for record in pending]

    assert [record.kwargs["index"] for record in records] == list(range(25))
    assert {record.id for record in pending} == {record.id for record in records}
    assert {record.id for record in claimed if record is not None} == {record.id for record in records}


async def test_valkey_backend_retries_cancels_heartbeats_and_cleans_up(valkey_backend: "ValkeyQueueBackend") -> "None":
    flaky = await valkey_backend.enqueue("tasks.flaky", max_retries=1)

    await valkey_backend.claim_task(flaky.id)
    retried = await valkey_backend.fail_task(flaky.id, "first failure")
    assert retried is not None
    assert retried.status == "pending"
    assert retried.retry_count == 1

    await valkey_backend.claim_task(flaky.id)
    failed = await valkey_backend.fail_task(flaky.id, "second failure")
    assert failed is not None
    assert failed.status == "failed"
    assert failed.error == "second failure"
    assert failed.completed_at is not None
    assert failed.heartbeat_at is None

    cancellable = await valkey_backend.enqueue("tasks.cancel")
    assert await valkey_backend.cancel_task(cancellable.id) is True
    assert await valkey_backend.cancel_task(cancellable.id) is False

    running = await valkey_backend.enqueue("tasks.running", execution_backend="cloudrun", max_retries=1)
    claimed = await valkey_backend.claim_task(running.id)
    assert claimed is not None

    await valkey_backend.set_execution_ref(claimed.id, "cloudrun", "jobs/abc-123", execution_profile="batch-small")
    await valkey_backend.null_heartbeats([claimed.id])
    running_external = await valkey_backend.list_running_external()
    stale_result = await valkey_backend.requeue_stale_running(stale_after=timedelta(seconds=0))

    assert [record.id for record in running_external] == [claimed.id]
    assert running_external[0].execution_ref == "jobs/abc-123"
    assert stale_result.requeued == 1
    requeued = await valkey_backend.get_task(claimed.id)
    assert requeued is not None
    assert requeued.status == "pending"
    assert requeued.retry_count == 1

    completed = await valkey_backend.enqueue("tasks.completed")
    await valkey_backend.claim_task(completed.id)
    completed_record = await valkey_backend.complete_task(completed.id, result={"ok": True})
    assert completed_record is not None
    assert completed_record.heartbeat_at is None
    statistics = await valkey_backend.get_statistics()
    completed_records = await valkey_backend.list_completed_by_task("tasks.completed")
    cleanup_count = await valkey_backend.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1))

    assert statistics.failed == 1
    assert statistics.cancelled == 1
    assert statistics.completed == 1
    assert [record.id for record in completed_records] == [completed.id]
    assert cleanup_count >= 3
    assert await valkey_backend.get_task(completed.id) is None


async def test_valkey_backend_touch_heartbeats_fences_and_merges_metadata(
    valkey_backend: "ValkeyQueueBackend",
) -> "None":
    empty_result = await valkey_backend.touch_heartbeats([])
    assert empty_result.touched_task_ids == set()
    assert empty_result.missed_task_ids == set()

    record = await valkey_backend.enqueue(
        "tasks.valkey.heartbeat.metadata", max_retries=1, metadata={"existing": "kept"}
    )
    claimed = await valkey_backend.claim_task(record.id)
    assert claimed is not None

    result = await valkey_backend.touch_heartbeats([
        HeartbeatTouch(task_id=claimed.id, expected_retry_count=claimed.retry_count + 1),
        HeartbeatTouch(
            task_id=claimed.id, expected_retry_count=claimed.retry_count, metadata_patch={"progress_detail": "row 5"}
        ),
    ])
    touched = await valkey_backend.get_task(claimed.id)

    assert result.touched_task_ids == {claimed.id}
    assert result.missed_task_ids == {claimed.id}
    assert touched is not None
    assert touched.metadata == {"existing": "kept", "progress_detail": "row 5"}


async def test_valkey_backend_claim_many_orders_by_priority_then_created(
    valkey_backend: "ValkeyQueueBackend",
) -> "None":
    first_high = await valkey_backend.enqueue("tasks.h1", priority=5)
    await asyncio.sleep(0.005)
    second_high = await valkey_backend.enqueue("tasks.h2", priority=5)
    await asyncio.sleep(0.005)
    first_low = await valkey_backend.enqueue("tasks.l1", priority=1)
    await asyncio.sleep(0.005)
    second_low = await valkey_backend.enqueue("tasks.l2", priority=1)

    claimed = await valkey_backend.claim_many(limit=4)

    assert [record.id for record in claimed] == [first_high.id, second_high.id, first_low.id, second_low.id]
    assert all(record.status == "running" for record in claimed)


async def test_valkey_backend_claim_many_filters_queue_and_execution_backend(
    valkey_backend: "ValkeyQueueBackend",
) -> "None":
    match = await valkey_backend.enqueue("tasks.match", queue="a", execution_backend="local")
    wrong_eb = await valkey_backend.enqueue("tasks.wrong_eb", queue="a", execution_backend="cloudrun")
    wrong_queue = await valkey_backend.enqueue("tasks.wrong_queue", queue="b", execution_backend="local")
    wrong_both = await valkey_backend.enqueue("tasks.wrong_both", queue="b", execution_backend="cloudrun")

    claimed = await valkey_backend.claim_many(limit=10, queues=("a",), execution_backend="local")

    assert [record.id for record in claimed] == [match.id]
    for other in (wrong_eb, wrong_queue, wrong_both):
        stored = await valkey_backend.get_task(other.id)
        assert stored is not None
        assert stored.status == "pending"


async def test_valkey_backend_claim_many_promotes_due_scheduled(valkey_backend: "ValkeyQueueBackend") -> "None":
    soon = datetime.now(timezone.utc) + timedelta(milliseconds=200)
    far = datetime.now(timezone.utc) + timedelta(minutes=5)
    due_soon = await valkey_backend.enqueue("tasks.soon", scheduled_at=soon)
    scheduled_far = await valkey_backend.enqueue("tasks.far", scheduled_at=far)
    await asyncio.sleep(0.3)

    claimed = await valkey_backend.claim_many(limit=10)

    assert [record.id for record in claimed] == [due_soon.id]
    assert claimed[0].status == "running"
    stored_far = await valkey_backend.get_task(scheduled_far.id)
    assert stored_far is not None
    assert stored_far.status == "scheduled"


async def test_valkey_backend_scheduled_zset_holds_future_tasks(valkey_backend: "ValkeyQueueBackend") -> "None":
    far = datetime.now(timezone.utc) + timedelta(minutes=5)
    scheduled = await valkey_backend.enqueue("tasks.future", scheduled_at=far)

    client = cast("Any", await valkey_backend._get_client())
    scheduled_members = {str(member) for member in await client.zrange(valkey_backend._scheduled_key, 0, -1)}
    ready_members = {str(member) for member in await client.zrange(valkey_backend._ready_key, 0, -1)}

    assert str(scheduled.id) in scheduled_members
    assert str(scheduled.id) not in ready_members


async def test_valkey_backend_complete_clears_heartbeat_and_publishes(valkey_backend: "ValkeyQueueBackend") -> "None":
    record = await valkey_backend.enqueue("tasks.publish")
    claimed = await valkey_backend.claim_task(record.id)
    assert claimed is not None

    client = cast("Any", await valkey_backend._get_client())
    pubsub = client.pubsub()
    await pubsub.subscribe(valkey_backend._completion_channel)
    await asyncio.sleep(0.2)

    completed = await valkey_backend.complete_task(record.id, result={"ok": True})

    message = None
    deadline = asyncio.get_running_loop().time() + 2.0
    while asyncio.get_running_loop().time() < deadline:
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
        if message is not None:
            break
    await pubsub.aclose()

    assert completed is not None
    assert completed.heartbeat_at is None
    assert message is not None
    assert str(message["data"]) == str(record.id)


async def _drain_messages(pubsub: "object", *, window: "float") -> "list[object]":
    messages: "list[object]" = []
    deadline = asyncio.get_running_loop().time() + window
    while asyncio.get_running_loop().time() < deadline:
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)  # type: ignore[attr-defined]
        if message is not None:
            messages.append(message)
    return messages


async def _status_memberships(backend: "ValkeyQueueBackend", task_id: "object") -> "list[str]":
    client = cast("Any", await backend._get_client())
    statuses = ("pending", "scheduled", "running", "completed", "failed", "cancelled")
    return [
        status
        for status in statuses
        if str(task_id) in {str(member) for member in await client.smembers(backend._status_key(status))}
    ]


async def _zset_members(backend: "ValkeyQueueBackend") -> "tuple[set[str], set[str]]":
    client = cast("Any", await backend._get_client())
    ready = {str(member) for member in await client.zrange(backend._ready_key, 0, -1)}
    scheduled = {str(member) for member in await client.zrange(backend._scheduled_key, 0, -1)}
    return ready, scheduled


async def test_valkey_backend_enqueue_places_record_in_single_status_set(
    valkey_backend: "ValkeyQueueBackend",
) -> "None":
    record = await valkey_backend.enqueue("tasks.single")

    client = cast("Any", await valkey_backend._get_client())
    statuses = ("pending", "scheduled", "running", "completed", "failed", "cancelled")
    memberships = [
        status
        for status in statuses
        if str(record.id) in {str(member) for member in await client.smembers(valkey_backend._status_key(status))}
    ]

    assert memberships == ["pending"]


async def test_valkey_backend_enqueue_future_scheduled_indexes_without_publish(
    valkey_backend: "ValkeyQueueBackend",
) -> "None":
    client = cast("Any", await valkey_backend._get_client())
    pubsub = client.pubsub()
    await pubsub.subscribe(valkey_backend._notification_channel)
    await asyncio.sleep(0.2)

    far = datetime.now(timezone.utc) + timedelta(minutes=5)
    record = await valkey_backend.enqueue("tasks.future", scheduled_at=far)
    messages = await _drain_messages(pubsub, window=0.3)
    await pubsub.aclose()

    scheduled_members = {str(member) for member in await client.zrange(valkey_backend._scheduled_key, 0, -1)}
    ready_members = {str(member) for member in await client.zrange(valkey_backend._ready_key, 0, -1)}

    assert record.status == "scheduled"
    assert str(record.id) in scheduled_members
    assert str(record.id) not in ready_members
    assert messages == []


async def test_valkey_backend_enqueue_due_publishes_single_notification(valkey_backend: "ValkeyQueueBackend") -> "None":
    client = cast("Any", await valkey_backend._get_client())
    pubsub = client.pubsub()
    await pubsub.subscribe(valkey_backend._notification_channel)
    await asyncio.sleep(0.2)

    await valkey_backend.enqueue("tasks.due")
    messages = await _drain_messages(pubsub, window=0.5)
    await pubsub.aclose()

    assert len(messages) == 1


async def test_valkey_backend_enqueue_many_coalesces_single_notification(
    valkey_backend: "ValkeyQueueBackend",
) -> "None":
    client = cast("Any", await valkey_backend._get_client())
    pubsub = client.pubsub()
    await pubsub.subscribe(valkey_backend._notification_channel)
    await asyncio.sleep(0.2)

    records = await valkey_backend.enqueue_many([EnqueueSpec(task_name=f"tasks.batch.{index}") for index in range(5)])
    messages = await _drain_messages(pubsub, window=0.5)
    await pubsub.aclose()

    assert len(records) == 5
    assert len(messages) == 1


async def test_valkey_backend_cancel_leaves_single_status_membership(valkey_backend: "ValkeyQueueBackend") -> "None":
    record = await valkey_backend.enqueue("tasks.cancel.membership")

    assert await valkey_backend.cancel_task(record.id) is True

    ready, scheduled = await _zset_members(valkey_backend)
    assert await _status_memberships(valkey_backend, record.id) == ["cancelled"]
    assert str(record.id) not in ready
    assert str(record.id) not in scheduled


async def test_valkey_backend_stale_requeue_leaves_single_membership_in_ready(
    valkey_backend: "ValkeyQueueBackend",
) -> "None":
    record = await valkey_backend.enqueue("tasks.stale.requeue", max_retries=1)
    await valkey_backend.claim_task(record.id)

    result = await valkey_backend.requeue_stale_running(stale_after=timedelta(seconds=0))

    assert result.requeued == 1
    ready, scheduled = await _zset_members(valkey_backend)
    assert await _status_memberships(valkey_backend, record.id) == ["pending"]
    assert str(record.id) in ready
    assert str(record.id) not in scheduled
    requeued = await valkey_backend.get_task(record.id)
    assert requeued is not None
    assert requeued.status == "pending"
    assert requeued.retry_count == 1
    assert requeued.heartbeat_at is None


async def test_valkey_backend_stale_failure_leaves_single_failed_membership(
    valkey_backend: "ValkeyQueueBackend",
) -> "None":
    record = await valkey_backend.enqueue("tasks.stale.fail")
    await valkey_backend.claim_task(record.id)

    result = await valkey_backend.requeue_stale_running(stale_after=timedelta(seconds=0))

    assert result.failed == 1
    ready, scheduled = await _zset_members(valkey_backend)
    assert await _status_memberships(valkey_backend, record.id) == ["failed"]
    assert str(record.id) not in ready
    assert str(record.id) not in scheduled
    failed = await valkey_backend.get_task(record.id)
    assert failed is not None
    assert failed.status == "failed"
    assert failed.heartbeat_at is None


async def test_valkey_backend_concurrent_keyed_enqueue_yields_one_record(
    valkey_backend: "ValkeyQueueBackend",
) -> "None":
    first, second = await asyncio.gather(
        valkey_backend.enqueue("tasks.keyed.race", kwargs={"n": 1}, key="race:1"),
        valkey_backend.enqueue("tasks.keyed.race", kwargs={"n": 2}, key="race:1"),
    )

    assert first.id == second.id
    keyed = await valkey_backend.get_task_by_key("race:1")
    assert keyed is not None
    assert keyed.id == first.id
    statistics = await valkey_backend.get_statistics()
    assert statistics.pending == 1
    assert await _status_memberships(valkey_backend, first.id) == ["pending"]


async def test_valkey_backend_claim_task_honors_due_gating_and_fences(valkey_backend: "ValkeyQueueBackend") -> "None":
    later = datetime.now(timezone.utc) + timedelta(minutes=5)
    scheduled = await valkey_backend.enqueue("tasks.claim.future", scheduled_at=later)

    assert await valkey_backend.claim_task(scheduled.id) is None
    stored = await valkey_backend.get_task(scheduled.id)
    assert stored is not None
    assert stored.status == "scheduled"

    due = await valkey_backend.enqueue("tasks.claim.due")
    claimed = await valkey_backend.claim_task(due.id)
    assert claimed is not None
    assert claimed.status == "running"
    assert claimed.started_at is not None
    assert await valkey_backend.claim_task(due.id) is None

    ready, _ = await _zset_members(valkey_backend)
    assert await _status_memberships(valkey_backend, due.id) == ["running"]
    assert str(due.id) not in ready


async def test_valkey_forever_reservation_returns_owner_on_conflict(valkey_backend: "ValkeyQueueBackend") -> "None":
    from tests.integration._uniqueness_contract import assert_reserve_returns_owner_on_conflict

    await assert_reserve_returns_owner_on_conflict(valkey_backend)


async def test_valkey_forever_reset_is_only_deletion_path(valkey_backend: "ValkeyQueueBackend") -> "None":
    from tests.integration._uniqueness_contract import assert_reset_is_only_deletion_path

    await assert_reset_is_only_deletion_path(valkey_backend)


async def test_valkey_forever_tombstone_survives_terminal_cleanup(valkey_backend: "ValkeyQueueBackend") -> "None":
    from tests.integration._uniqueness_contract import assert_tombstone_survives_terminal_cleanup

    await assert_tombstone_survives_terminal_cleanup(valkey_backend)


async def test_valkey_forever_concurrent_reservation_single_winner(valkey_backend: "ValkeyQueueBackend") -> "None":
    from tests.integration._uniqueness_contract import assert_concurrent_reservation_has_single_winner

    await assert_concurrent_reservation_has_single_winner(valkey_backend)

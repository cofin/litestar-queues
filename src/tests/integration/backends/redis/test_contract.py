"""Redis backend contract suite against a real Redis 7 container.

Covers real-server semantics: ZSET score filtering, HSET round-trip,
atomic token-checked Lua release, ZADD/zrem on stale-task recovery.
The fixtures (``redis_client`` + ``redis_backend``) live in this
directory's conftest.
"""

import asyncio
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest

pytest.importorskip("redis")

from litestar_queues import EnqueueSpec
from litestar_queues.backends import get_queue_backend_class, list_queue_backends
from litestar_queues.models import HeartbeatTouch

if TYPE_CHECKING:
    from litestar_queues.backends.redis import RedisQueueBackend

pytestmark = pytest.mark.anyio


def test_top_level_litestar_queues_import_does_not_pull_in_redis_or_valkey() -> "None":
    """Importing ``litestar_queues`` must NOT import the redis or valkey clients."""
    code = """
import builtins
import sys

original_import = builtins.__import__

def blocked_import(name, *args, **kwargs):
    if name in {"redis", "valkey"} or name.startswith(("redis.", "valkey.")):
        raise ModuleNotFoundError(name)
    return original_import(name, *args, **kwargs)

builtins.__import__ = blocked_import
import litestar_queues
from litestar_queues import InMemoryQueueBackend

assert "InMemoryQueueBackend" in litestar_queues.__all__
assert "RedisQueueBackend" not in litestar_queues.__all__
assert "ValkeyQueueBackend" not in litestar_queues.__all__
assert "redis" not in sys.modules
assert "valkey" not in sys.modules
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr


def test_redis_valkey_backends_are_registered() -> "None":
    from litestar_queues.backends.redis import RedisQueueBackend
    from litestar_queues.backends.valkey import ValkeyQueueBackend

    assert get_queue_backend_class("redis") is RedisQueueBackend
    assert get_queue_backend_class("valkey") is ValkeyQueueBackend
    assert "redis" in list_queue_backends()
    assert "valkey" in list_queue_backends()


async def test_redis_backend_deduplicates_active_keys_and_replaces_terminal_keys(
    redis_backend: "RedisQueueBackend",
) -> "None":
    first = await redis_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-1"}, key="sync:acct-1")
    duplicate = await redis_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")

    assert duplicate.id == first.id
    assert duplicate.kwargs == {"account_id": "acct-1"}

    await redis_backend.claim_task(first.id)
    await redis_backend.complete_task(first.id, result={"ok": True})
    replacement = await redis_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")
    keyed = await redis_backend.get_task_by_key("sync:acct-1")

    assert replacement.id != first.id
    assert replacement.kwargs == {"account_id": "acct-2"}
    assert keyed is not None
    assert keyed.id == replacement.id


async def test_redis_backend_claims_due_tasks_once_by_priority_and_filters_execution(
    redis_backend: "RedisQueueBackend",
) -> "None":
    later = datetime.now(timezone.utc) + timedelta(minutes=5)

    low = await redis_backend.enqueue("tasks.low", priority=1, execution_backend="local")
    await redis_backend.enqueue("tasks.later", priority=100, scheduled_at=later, execution_backend="local")
    high = await redis_backend.enqueue("tasks.high", priority=10, execution_backend="cloudrun")

    local_pending = await redis_backend.list_pending(limit=10, execution_backend="local")
    cloud_pending = await redis_backend.list_pending(limit=10, execution_backend="cloudrun")

    assert [record.id for record in local_pending] == [low.id]
    assert [record.id for record in cloud_pending] == [high.id]

    claimed_results = await asyncio.gather(redis_backend.claim_task(high.id), redis_backend.claim_task(high.id))
    claimed = [record for record in claimed_results if record is not None]

    assert len(claimed) == 1
    assert claimed[0].id == high.id
    assert claimed[0].status == "running"
    assert claimed[0].started_at is not None
    stored_low = await redis_backend.get_task(low.id)
    assert stored_low is not None
    assert stored_low.status == "pending"


async def test_redis_enqueue_many_records_remain_claimable_when_batch_marker_is_dropped(
    redis_backend: "RedisQueueBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    async def drop_marker(self: "RedisQueueBackend", records: "object") -> "None":
        del self, records

    monkeypatch.setattr(type(redis_backend), "notify_new_tasks", drop_marker)

    records = await redis_backend.enqueue_many([
        EnqueueSpec(task_name=f"tasks.batch.{index}", kwargs={"index": index}) for index in range(25)
    ])
    pending = await redis_backend.list_pending(limit=30)
    claimed = [await redis_backend.claim_task(record.id) for record in pending]

    assert [record.kwargs["index"] for record in records] == list(range(25))
    assert {record.id for record in pending} == {record.id for record in records}
    assert {record.id for record in claimed if record is not None} == {record.id for record in records}


async def test_redis_backend_retries_cancels_heartbeats_and_cleans_up(redis_backend: "RedisQueueBackend") -> "None":
    flaky = await redis_backend.enqueue("tasks.flaky", max_retries=1)

    await redis_backend.claim_task(flaky.id)
    retried = await redis_backend.fail_task(flaky.id, "first failure")
    assert retried is not None
    assert retried.status == "pending"
    assert retried.retry_count == 1

    await redis_backend.claim_task(flaky.id)
    failed = await redis_backend.fail_task(flaky.id, "second failure")
    assert failed is not None
    assert failed.status == "failed"
    assert failed.error == "second failure"
    assert failed.completed_at is not None
    assert failed.heartbeat_at is None

    cancellable = await redis_backend.enqueue("tasks.cancel")
    assert await redis_backend.cancel_task(cancellable.id) is True
    assert await redis_backend.cancel_task(cancellable.id) is False

    running = await redis_backend.enqueue("tasks.running", execution_backend="cloudrun", max_retries=1)
    claimed = await redis_backend.claim_task(running.id)
    assert claimed is not None

    await redis_backend.set_execution_ref(claimed.id, "cloudrun", "jobs/abc-123", execution_profile="batch-small")
    await redis_backend.null_heartbeats([claimed.id])
    running_external = await redis_backend.list_running_external()
    stale_result = await redis_backend.requeue_stale_running(stale_after=timedelta(seconds=0))

    assert [record.id for record in running_external] == [claimed.id]
    assert running_external[0].execution_ref == "jobs/abc-123"
    assert stale_result.requeued == 1
    requeued = await redis_backend.get_task(claimed.id)
    assert requeued is not None
    assert requeued.status == "pending"
    assert requeued.retry_count == 1

    completed = await redis_backend.enqueue("tasks.completed")
    await redis_backend.claim_task(completed.id)
    completed_record = await redis_backend.complete_task(completed.id, result={"ok": True})
    assert completed_record is not None
    assert completed_record.heartbeat_at is None
    statistics = await redis_backend.get_statistics()
    completed_records = await redis_backend.list_completed_by_task("tasks.completed")
    cleanup_count = await redis_backend.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1))

    assert statistics.failed == 1
    assert statistics.cancelled == 1
    assert statistics.completed == 1
    assert [record.id for record in completed_records] == [completed.id]
    assert cleanup_count >= 3
    assert await redis_backend.get_task(completed.id) is None


async def test_redis_backend_touch_heartbeats_fences_and_merges_metadata(redis_backend: "RedisQueueBackend") -> "None":
    empty_result = await redis_backend.touch_heartbeats([])
    assert empty_result.touched_task_ids == set()
    assert empty_result.missed_task_ids == set()

    record = await redis_backend.enqueue("tasks.redis.heartbeat.metadata", max_retries=1, metadata={"existing": "kept"})
    claimed = await redis_backend.claim_task(record.id)
    assert claimed is not None

    result = await redis_backend.touch_heartbeats([
        HeartbeatTouch(task_id=claimed.id, expected_retry_count=claimed.retry_count + 1),
        HeartbeatTouch(
            task_id=claimed.id, expected_retry_count=claimed.retry_count, metadata_patch={"progress_detail": "row 5"}
        ),
    ])
    touched = await redis_backend.get_task(claimed.id)

    assert result.touched_task_ids == {claimed.id}
    assert result.missed_task_ids == {claimed.id}
    assert touched is not None
    assert touched.metadata == {"existing": "kept", "progress_detail": "row 5"}


async def test_redis_backend_rejects_unserializable_results(redis_backend: "RedisQueueBackend") -> "None":
    record = await redis_backend.enqueue("tasks.unserializable")
    claimed = await redis_backend.claim_task(record.id)

    assert claimed is not None
    with pytest.raises(TypeError, match="not JSON serializable"):
        await redis_backend.complete_task(record.id, result=object())

    stored = await redis_backend.get_task(record.id)
    assert stored is not None
    assert stored.status == "running"
    assert stored.result is None


async def test_redis_backend_claim_many_orders_by_priority_then_created(redis_backend: "RedisQueueBackend") -> "None":
    first_high = await redis_backend.enqueue("tasks.h1", priority=5)
    await asyncio.sleep(0.005)
    second_high = await redis_backend.enqueue("tasks.h2", priority=5)
    await asyncio.sleep(0.005)
    first_low = await redis_backend.enqueue("tasks.l1", priority=1)
    await asyncio.sleep(0.005)
    second_low = await redis_backend.enqueue("tasks.l2", priority=1)

    claimed = await redis_backend.claim_many(limit=4)

    assert [record.id for record in claimed] == [first_high.id, second_high.id, first_low.id, second_low.id]
    assert all(record.status == "running" for record in claimed)


async def test_redis_backend_claim_many_filters_queue_and_execution_backend(
    redis_backend: "RedisQueueBackend",
) -> "None":
    match = await redis_backend.enqueue("tasks.match", queue="a", execution_backend="local")
    wrong_eb = await redis_backend.enqueue("tasks.wrong_eb", queue="a", execution_backend="cloudrun")
    wrong_queue = await redis_backend.enqueue("tasks.wrong_queue", queue="b", execution_backend="local")
    wrong_both = await redis_backend.enqueue("tasks.wrong_both", queue="b", execution_backend="cloudrun")

    claimed = await redis_backend.claim_many(limit=10, queues=("a",), execution_backend="local")

    assert [record.id for record in claimed] == [match.id]
    for other in (wrong_eb, wrong_queue, wrong_both):
        stored = await redis_backend.get_task(other.id)
        assert stored is not None
        assert stored.status == "pending"


async def test_redis_backend_claim_many_promotes_due_scheduled(redis_backend: "RedisQueueBackend") -> "None":
    soon = datetime.now(timezone.utc) + timedelta(milliseconds=200)
    far = datetime.now(timezone.utc) + timedelta(minutes=5)
    due_soon = await redis_backend.enqueue("tasks.soon", scheduled_at=soon)
    scheduled_far = await redis_backend.enqueue("tasks.far", scheduled_at=far)
    await asyncio.sleep(0.3)

    claimed = await redis_backend.claim_many(limit=10)

    assert [record.id for record in claimed] == [due_soon.id]
    assert claimed[0].status == "running"
    stored_far = await redis_backend.get_task(scheduled_far.id)
    assert stored_far is not None
    assert stored_far.status == "scheduled"


async def test_redis_backend_scheduled_zset_holds_future_tasks(redis_backend: "RedisQueueBackend") -> "None":
    far = datetime.now(timezone.utc) + timedelta(minutes=5)
    scheduled = await redis_backend.enqueue("tasks.future", scheduled_at=far)

    client = await redis_backend._get_client()
    scheduled_members = {str(member) for member in await client.zrange(redis_backend._scheduled_key, 0, -1)}
    ready_members = {str(member) for member in await client.zrange(redis_backend._ready_key, 0, -1)}

    assert str(scheduled.id) in scheduled_members
    assert str(scheduled.id) not in ready_members


async def test_redis_backend_complete_clears_heartbeat_and_publishes(redis_backend: "RedisQueueBackend") -> "None":
    record = await redis_backend.enqueue("tasks.publish")
    claimed = await redis_backend.claim_task(record.id)
    assert claimed is not None

    client = await redis_backend._get_client()
    pubsub = client.pubsub()
    await pubsub.subscribe(redis_backend._completion_channel)
    await asyncio.sleep(0.2)

    completed = await redis_backend.complete_task(record.id, result={"ok": True})

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


async def _status_memberships(backend: "RedisQueueBackend", task_id: "object") -> "list[str]":
    client = await backend._get_client()
    statuses = ("pending", "scheduled", "running", "completed", "failed", "cancelled")
    return [
        status
        for status in statuses
        if str(task_id) in {str(member) for member in await client.smembers(backend._status_key(status))}
    ]


async def _zset_members(backend: "RedisQueueBackend") -> "tuple[set[str], set[str]]":
    client = await backend._get_client()
    ready = {str(member) for member in await client.zrange(backend._ready_key, 0, -1)}
    scheduled = {str(member) for member in await client.zrange(backend._scheduled_key, 0, -1)}
    return ready, scheduled


async def test_redis_backend_enqueue_places_record_in_single_status_set(redis_backend: "RedisQueueBackend") -> "None":
    record = await redis_backend.enqueue("tasks.single")

    client = await redis_backend._get_client()
    statuses = ("pending", "scheduled", "running", "completed", "failed", "cancelled")
    memberships = [
        status
        for status in statuses
        if str(record.id) in {str(member) for member in await client.smembers(redis_backend._status_key(status))}
    ]

    assert memberships == ["pending"]


async def test_redis_backend_enqueue_future_scheduled_indexes_without_publish(
    redis_backend: "RedisQueueBackend",
) -> "None":
    client = await redis_backend._get_client()
    pubsub = client.pubsub()
    await pubsub.subscribe(redis_backend._notification_channel)
    await asyncio.sleep(0.2)

    far = datetime.now(timezone.utc) + timedelta(minutes=5)
    record = await redis_backend.enqueue("tasks.future", scheduled_at=far)
    messages = await _drain_messages(pubsub, window=0.3)
    await pubsub.aclose()

    scheduled_members = {str(member) for member in await client.zrange(redis_backend._scheduled_key, 0, -1)}
    ready_members = {str(member) for member in await client.zrange(redis_backend._ready_key, 0, -1)}

    assert record.status == "scheduled"
    assert str(record.id) in scheduled_members
    assert str(record.id) not in ready_members
    assert messages == []


async def test_redis_backend_enqueue_due_publishes_single_notification(redis_backend: "RedisQueueBackend") -> "None":
    client = await redis_backend._get_client()
    pubsub = client.pubsub()
    await pubsub.subscribe(redis_backend._notification_channel)
    await asyncio.sleep(0.2)

    await redis_backend.enqueue("tasks.due")
    messages = await _drain_messages(pubsub, window=0.5)
    await pubsub.aclose()

    assert len(messages) == 1


async def test_redis_backend_enqueue_many_coalesces_single_notification(redis_backend: "RedisQueueBackend") -> "None":
    client = await redis_backend._get_client()
    pubsub = client.pubsub()
    await pubsub.subscribe(redis_backend._notification_channel)
    await asyncio.sleep(0.2)

    records = await redis_backend.enqueue_many([EnqueueSpec(task_name=f"tasks.batch.{index}") for index in range(5)])
    messages = await _drain_messages(pubsub, window=0.5)
    await pubsub.aclose()

    assert len(records) == 5
    assert len(messages) == 1


async def test_redis_backend_cancel_leaves_single_status_membership(redis_backend: "RedisQueueBackend") -> "None":
    record = await redis_backend.enqueue("tasks.cancel.membership")

    assert await redis_backend.cancel_task(record.id) is True

    ready, scheduled = await _zset_members(redis_backend)
    assert await _status_memberships(redis_backend, record.id) == ["cancelled"]
    assert str(record.id) not in ready
    assert str(record.id) not in scheduled


async def test_redis_backend_stale_requeue_leaves_single_membership_in_ready(
    redis_backend: "RedisQueueBackend",
) -> "None":
    record = await redis_backend.enqueue("tasks.stale.requeue", max_retries=1)
    await redis_backend.claim_task(record.id)

    result = await redis_backend.requeue_stale_running(stale_after=timedelta(seconds=0))

    assert result.requeued == 1
    ready, scheduled = await _zset_members(redis_backend)
    assert await _status_memberships(redis_backend, record.id) == ["pending"]
    assert str(record.id) in ready
    assert str(record.id) not in scheduled
    requeued = await redis_backend.get_task(record.id)
    assert requeued is not None
    assert requeued.status == "pending"
    assert requeued.retry_count == 1
    assert requeued.heartbeat_at is None


async def test_redis_backend_stale_failure_leaves_single_failed_membership(
    redis_backend: "RedisQueueBackend",
) -> "None":
    record = await redis_backend.enqueue("tasks.stale.fail")
    await redis_backend.claim_task(record.id)

    result = await redis_backend.requeue_stale_running(stale_after=timedelta(seconds=0))

    assert result.failed == 1
    ready, scheduled = await _zset_members(redis_backend)
    assert await _status_memberships(redis_backend, record.id) == ["failed"]
    assert str(record.id) not in ready
    assert str(record.id) not in scheduled
    failed = await redis_backend.get_task(record.id)
    assert failed is not None
    assert failed.status == "failed"
    assert failed.heartbeat_at is None


async def test_redis_backend_concurrent_keyed_enqueue_yields_one_record(redis_backend: "RedisQueueBackend") -> "None":
    first, second = await asyncio.gather(
        redis_backend.enqueue("tasks.keyed.race", kwargs={"n": 1}, key="race:1"),
        redis_backend.enqueue("tasks.keyed.race", kwargs={"n": 2}, key="race:1"),
    )

    assert first.id == second.id
    keyed = await redis_backend.get_task_by_key("race:1")
    assert keyed is not None
    assert keyed.id == first.id
    statistics = await redis_backend.get_statistics()
    assert statistics.pending == 1
    assert await _status_memberships(redis_backend, first.id) == ["pending"]


async def test_redis_backend_claim_task_honors_due_gating_and_fences(redis_backend: "RedisQueueBackend") -> "None":
    later = datetime.now(timezone.utc) + timedelta(minutes=5)
    scheduled = await redis_backend.enqueue("tasks.claim.future", scheduled_at=later)

    assert await redis_backend.claim_task(scheduled.id) is None
    stored = await redis_backend.get_task(scheduled.id)
    assert stored is not None
    assert stored.status == "scheduled"

    due = await redis_backend.enqueue("tasks.claim.due")
    claimed = await redis_backend.claim_task(due.id)
    assert claimed is not None
    assert claimed.status == "running"
    assert claimed.started_at is not None
    assert await redis_backend.claim_task(due.id) is None

    ready, _ = await _zset_members(redis_backend)
    assert await _status_memberships(redis_backend, due.id) == ["running"]
    assert str(due.id) not in ready

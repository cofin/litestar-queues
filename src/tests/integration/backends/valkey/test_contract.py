"""Valkey backend contract suite against a real Valkey 8 container.

Mirror of ``backends/redis/test_contract.py`` against ``ValkeyQueueBackend``.
The Valkey wire protocol is API-compatible with Redis so the test bodies
are identical apart from the fixture name (``valkey_backend``) and the
notification-capability label (``valkey-pubsub``).
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

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
    assert valkey_backend.capabilities.supports_batch_claim is False

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


async def test_valkey_backend_releases_locks_by_token_via_lua_script(valkey_backend: "ValkeyQueueBackend") -> "None":
    """Verify the token-checked release script against real Valkey Lua semantics."""
    client = await valkey_backend._get_client()
    lock_key = valkey_backend._lock_key("task:test")

    await client.set(lock_key, "new-owner")
    await valkey_backend._release_lock(client, lock_key, "old-owner")

    assert await client.get(lock_key) == "new-owner"

    await valkey_backend._release_lock(client, lock_key, "new-owner")

    assert await client.get(lock_key) is None


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
    await valkey_backend.complete_task(completed.id, result={"ok": True})
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

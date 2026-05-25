"""Valkey backend contract suite against a real Valkey 8 container.

Mirror of ``backends/redis/test_contract.py`` against ``ValkeyQueueBackend``.
The Valkey wire protocol is API-compatible with Redis so the test bodies
are identical apart from the fixture name (``valkey_backend``) and the
notification-capability label (``valkey-pubsub``).
"""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

pytest.importorskip("valkey")

if TYPE_CHECKING:
    from litestar_queues.backends.valkey import ValkeyQueueBackend

pytestmark = pytest.mark.anyio


async def test_valkey_backend_deduplicates_active_keys_and_replaces_terminal_keys(
    valkey_backend: "ValkeyQueueBackend",
) -> None:
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
) -> None:
    later = datetime.now(UTC) + timedelta(minutes=5)

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


async def test_valkey_backend_releases_locks_by_token_via_lua_script(valkey_backend: "ValkeyQueueBackend") -> None:
    """Verify the token-checked release script against real Valkey Lua semantics."""
    client = await valkey_backend._get_client()
    lock_key = valkey_backend._lock_key("task:test")

    await client.set(lock_key, "new-owner")
    await valkey_backend._release_lock(client, lock_key, "old-owner")

    assert await client.get(lock_key) == "new-owner"

    await valkey_backend._release_lock(client, lock_key, "new-owner")

    assert await client.get(lock_key) is None


async def test_valkey_backend_retries_cancels_heartbeats_and_cleans_up(valkey_backend: "ValkeyQueueBackend") -> None:
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

    running = await valkey_backend.enqueue("tasks.running", execution_backend="cloudrun")
    claimed = await valkey_backend.claim_task(running.id)
    assert claimed is not None

    await valkey_backend.set_execution_ref(claimed.id, "cloudrun", "jobs/abc-123", execution_profile="batch-small")
    await valkey_backend.null_heartbeats([claimed.id])
    running_external = await valkey_backend.list_running_external()
    stale_count = await valkey_backend.requeue_stale_running(stale_after=timedelta(seconds=0))

    assert [record.id for record in running_external] == [claimed.id]
    assert running_external[0].execution_ref == "jobs/abc-123"
    assert stale_count == 1
    requeued = await valkey_backend.get_task(claimed.id)
    assert requeued is not None
    assert requeued.status == "pending"
    assert requeued.retry_count == 1

    completed = await valkey_backend.enqueue("tasks.completed")
    await valkey_backend.claim_task(completed.id)
    await valkey_backend.complete_task(completed.id, result={"ok": True})
    statistics = await valkey_backend.get_statistics()
    completed_records = await valkey_backend.list_completed_by_task("tasks.completed")
    cleanup_count = await valkey_backend.cleanup_terminal(datetime.now(UTC) + timedelta(seconds=1))

    assert statistics.failed == 1
    assert statistics.cancelled == 1
    assert statistics.completed == 1
    assert [record.id for record in completed_records] == [completed.id]
    assert cleanup_count >= 3
    assert await valkey_backend.get_task(completed.id) is None

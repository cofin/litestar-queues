"""Redis backend contract suite against a real Redis 7 container.

Covers real-server semantics: ZSET score filtering, HSET round-trip,
atomic token-checked Lua release, ZADD/zrem on stale-task recovery.
The fixtures (``redis_client`` + ``redis_backend``) live in this
directory's conftest.
"""

import asyncio
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

pytest.importorskip("redis")

from litestar_queues.backends import get_queue_backend_class, list_queue_backends

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
    later = datetime.now(UTC) + timedelta(minutes=5)

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


async def test_redis_backend_releases_locks_by_token_via_lua_script(redis_backend: "RedisQueueBackend") -> "None":
    """Verify the token-checked release script against real Redis Lua semantics."""
    client = await redis_backend._get_client()
    lock_key = redis_backend._lock_key("task:test")

    await client.set(lock_key, "new-owner")
    await redis_backend._release_lock(client, lock_key, "old-owner")

    assert await client.get(lock_key) == "new-owner"

    await redis_backend._release_lock(client, lock_key, "new-owner")

    assert await client.get(lock_key) is None


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
    await redis_backend.complete_task(completed.id, result={"ok": True})
    statistics = await redis_backend.get_statistics()
    completed_records = await redis_backend.list_completed_by_task("tasks.completed")
    cleanup_count = await redis_backend.cleanup_terminal(datetime.now(UTC) + timedelta(seconds=1))

    assert statistics.failed == 1
    assert statistics.cancelled == 1
    assert statistics.completed == 1
    assert [record.id for record in completed_records] == [completed.id]
    assert cleanup_count >= 3
    assert await redis_backend.get_task(completed.id) is None

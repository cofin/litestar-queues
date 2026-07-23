"""Real-Redis pub/sub notification wakeup test.

Verifies that ``RedisQueueBackend.wait_for_wakeups`` unblocks when a
sibling enqueue publishes to the configured channel. Timeout is generous
(2.0s) to absorb container jitter — the fake-backed test used 0.5s.
"""

import asyncio
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

pytest.importorskip("redis")

from litestar_queues import TaskRequest

if TYPE_CHECKING:
    from litestar_queues.backends.redis import RedisQueueBackend

pytestmark = pytest.mark.anyio


async def test_redis_backend_pubsub_notifications_wake_waiters(redis_backend: "RedisQueueBackend") -> "None":
    waiter = asyncio.create_task(redis_backend.wait_for_wakeups(timeout=2.0))
    # Real Redis requires the SUBSCRIBE roundtrip to land before the
    # sibling enqueue publishes; a bare ``asyncio.sleep(0)`` is too brief.
    await asyncio.sleep(0.2)

    record = await redis_backend.enqueue("tasks.notified", queue="critical", execution_backend="local")

    assert await waiter is True
    assert redis_backend.capabilities.supports_worker_wakeups is True
    assert redis_backend.capabilities.wakeups_durable is False
    assert redis_backend.capabilities.wakeup_backend == "redis-pubsub"
    assert await redis_backend.wait_for_wakeups(timeout=0.01) is False
    assert record.status == "pending"


async def test_redis_backend_enqueue_many_publishes_one_batch_notification(
    redis_backend: "RedisQueueBackend",
) -> "None":
    waiter = asyncio.create_task(redis_backend.wait_for_wakeups(timeout=2.0))
    await asyncio.sleep(0.2)

    records = await redis_backend.enqueue_many([TaskRequest(task_name=f"tasks.batch.{index}") for index in range(5)])

    assert await waiter is True
    assert len(records) == 5
    assert await redis_backend.wait_for_wakeups(timeout=0.05) is False


async def test_redis_backend_wait_for_completion_wakes_on_terminal(redis_backend: "RedisQueueBackend") -> "None":
    record = await redis_backend.enqueue("tasks.awaited")
    claimed = await redis_backend.claim_task(record.id)
    assert claimed is not None

    waiter = asyncio.create_task(redis_backend.wait_for_completion(record.id, timeout=2.0))
    await asyncio.sleep(0.2)
    completed = await redis_backend.complete_task(record.id, result={"ok": True})

    assert completed is not None
    assert await waiter is True
    assert await redis_backend.wait_for_completion(uuid4(), timeout=0.05) is False


async def test_redis_backend_reuses_subscription_after_timeout(redis_backend: "RedisQueueBackend") -> "None":
    assert await redis_backend.wait_for_wakeups(timeout=0.1) is False
    pubsub = redis_backend._pubsub
    assert pubsub is not None
    assert redis_backend._pending_read.has_pending is True

    assert await redis_backend.wait_for_wakeups(timeout=0.1) is False
    # No re-subscription across empty poll timeouts.
    assert redis_backend._pubsub is pubsub
    assert redis_backend._pending_read.has_pending is True

    await redis_backend.enqueue("tasks.reuse", execution_backend="local")

    # The notification wakes the retained receive on the same subscription.
    assert await redis_backend.wait_for_wakeups(timeout=2.0) is True
    assert redis_backend._pubsub is pubsub
    assert bool(redis_backend._pending_read.has_pending) is False


async def test_redis_backend_close_while_reading_leaves_no_task(redis_backend: "RedisQueueBackend") -> "None":
    assert await redis_backend.wait_for_wakeups(timeout=0.1) is False
    assert redis_backend._pending_read.has_pending is True

    await redis_backend.close()
    assert bool(redis_backend._pending_read.has_pending) is False
    assert redis_backend._pubsub is None
    # Double close is idempotent.
    await redis_backend.close()

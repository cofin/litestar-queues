"""Real-Redis pub/sub notification wakeup test.

Verifies that ``RedisQueueBackend.wait_for_notifications`` unblocks when a
sibling enqueue publishes to the configured channel. Timeout is generous
(2.0s) to absorb container jitter — the fake-backed test used 0.5s.
"""

import asyncio
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

pytest.importorskip("redis")

from litestar_queues import EnqueueSpec

if TYPE_CHECKING:
    from litestar_queues.backends.redis import RedisQueueBackend

pytestmark = pytest.mark.anyio


async def test_redis_backend_pubsub_notifications_wake_waiters(redis_backend: "RedisQueueBackend") -> "None":
    waiter = asyncio.create_task(redis_backend.wait_for_notifications(timeout=2.0))
    # Real Redis requires the SUBSCRIBE roundtrip to land before the
    # sibling enqueue publishes; a bare ``asyncio.sleep(0)`` is too brief.
    await asyncio.sleep(0.2)

    record = await redis_backend.enqueue("tasks.notified", queue="critical", execution_backend="local")

    assert await waiter is True
    assert redis_backend.capabilities.supports_notifications is True
    assert redis_backend.capabilities.notifications_durable is False
    assert redis_backend.capabilities.notification_backend == "redis-pubsub"
    assert await redis_backend.wait_for_notifications(timeout=0.01) is False
    assert record.status == "pending"


async def test_redis_backend_enqueue_many_publishes_one_batch_notification(
    redis_backend: "RedisQueueBackend",
) -> "None":
    waiter = asyncio.create_task(redis_backend.wait_for_notifications(timeout=2.0))
    await asyncio.sleep(0.2)

    records = await redis_backend.enqueue_many([EnqueueSpec(task_name=f"tasks.batch.{index}") for index in range(5)])

    assert await waiter is True
    assert len(records) == 5
    assert await redis_backend.wait_for_notifications(timeout=0.05) is False


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

"""Real-Redis pub/sub notification wakeup test.

Verifies that ``RedisQueueBackend.wait_for_notifications`` unblocks when a
sibling enqueue publishes to the configured channel. Timeout is generous
(2.0s) to absorb container jitter — the fake-backed test used 0.5s.
"""

import asyncio
from typing import TYPE_CHECKING

import pytest

pytest.importorskip("redis")

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

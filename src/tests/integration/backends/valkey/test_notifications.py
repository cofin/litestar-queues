"""Real-Valkey pub/sub notification wakeup test.

Mirror of ``backends/redis/test_notifications.py`` against a real Valkey
container. The notification-backend label switches to ``valkey-pubsub``.
"""

import asyncio
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

pytest.importorskip("valkey")

from litestar_queues import TaskRequest
from tests.helpers.redis_protocol import wait_for_channel_subscribers

if TYPE_CHECKING:
    from litestar_queues.backends.valkey import ValkeyQueueBackend

pytestmark = pytest.mark.anyio


async def test_valkey_backend_pubsub_notifications_wake_waiters(valkey_backend: "ValkeyQueueBackend") -> "None":
    waiter = asyncio.create_task(valkey_backend.wait_for_wakeups(timeout=2.0))
    await wait_for_channel_subscribers(valkey_backend, valkey_backend._wakeup_channel)

    record = await valkey_backend.enqueue("tasks.notified", queue="critical", execution_backend="local")

    assert await waiter is True
    assert valkey_backend.capabilities.supports_worker_wakeups is True
    assert valkey_backend.capabilities.wakeups_durable is False
    assert valkey_backend.capabilities.wakeup_backend == "valkey-pubsub"
    assert await valkey_backend.wait_for_wakeups(timeout=0.01) is False
    assert record.status == "pending"


async def test_valkey_backend_enqueue_many_publishes_one_batch_notification(
    valkey_backend: "ValkeyQueueBackend",
) -> "None":
    waiter = asyncio.create_task(valkey_backend.wait_for_wakeups(timeout=2.0))
    await wait_for_channel_subscribers(valkey_backend, valkey_backend._wakeup_channel)

    records = await valkey_backend.enqueue_many([TaskRequest(task_name=f"tasks.batch.{index}") for index in range(5)])

    assert await waiter is True
    assert len(records) == 5
    assert await valkey_backend.wait_for_wakeups(timeout=0.05) is False


async def test_valkey_backend_wait_for_completion_wakes_on_terminal(valkey_backend: "ValkeyQueueBackend") -> "None":
    record = await valkey_backend.enqueue("tasks.awaited")
    claimed = await valkey_backend.claim_task(record.id)
    assert claimed is not None

    waiter = asyncio.create_task(valkey_backend.wait_for_completion(record.id, timeout=2.0))
    await wait_for_channel_subscribers(valkey_backend, valkey_backend._completion_channel)
    completed = await valkey_backend.complete_task(record.id, result={"ok": True})

    assert completed is not None
    assert await waiter is True
    assert await valkey_backend.wait_for_completion(uuid4(), timeout=0.05) is False


async def test_valkey_backend_shares_completion_subscription_between_waiters(
    valkey_backend: "ValkeyQueueBackend",
) -> "None":
    record = await valkey_backend.enqueue("tasks.awaited.concurrent")
    claimed = await valkey_backend.claim_task(record.id)
    assert claimed is not None

    first = asyncio.create_task(valkey_backend.wait_for_completion(record.id, timeout=2.0))
    second = asyncio.create_task(valkey_backend.wait_for_completion(record.id, timeout=2.0))
    await wait_for_channel_subscribers(valkey_backend, valkey_backend._completion_channel, expected=1)
    completed = await valkey_backend.complete_task(record.id)

    assert completed is not None
    assert await asyncio.gather(first, second) == [True, True]
    assert valkey_backend._completion_reader_task is not None


async def test_valkey_backend_reuses_subscription_after_timeout(valkey_backend: "ValkeyQueueBackend") -> "None":
    assert await valkey_backend.wait_for_wakeups(timeout=0.1) is False
    pubsub = valkey_backend._pubsub
    assert pubsub is not None
    assert valkey_backend._pending_read.has_pending is True

    assert await valkey_backend.wait_for_wakeups(timeout=0.1) is False
    # No re-subscription across empty poll timeouts.
    assert valkey_backend._pubsub is pubsub
    assert valkey_backend._pending_read.has_pending is True

    await valkey_backend.enqueue("tasks.reuse", execution_backend="local")

    # The notification wakes the retained receive on the same subscription.
    assert await valkey_backend.wait_for_wakeups(timeout=2.0) is True
    assert valkey_backend._pubsub is pubsub
    assert bool(valkey_backend._pending_read.has_pending) is False


async def test_valkey_backend_close_while_reading_leaves_no_task(valkey_backend: "ValkeyQueueBackend") -> "None":
    assert await valkey_backend.wait_for_wakeups(timeout=0.1) is False
    assert valkey_backend._pending_read.has_pending is True

    await valkey_backend.close()
    assert bool(valkey_backend._pending_read.has_pending) is False
    assert valkey_backend._pubsub is None
    # Double close is idempotent.
    await valkey_backend.close()

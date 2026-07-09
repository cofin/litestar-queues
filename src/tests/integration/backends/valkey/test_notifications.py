"""Real-Valkey pub/sub notification wakeup test.

Mirror of ``backends/redis/test_notifications.py`` against a real Valkey
container. The notification-backend label switches to ``valkey-pubsub``.
"""

import asyncio
from typing import TYPE_CHECKING

import pytest

pytest.importorskip("valkey")

from litestar_queues import EnqueueSpec

if TYPE_CHECKING:
    from litestar_queues.backends.valkey import ValkeyQueueBackend

pytestmark = pytest.mark.anyio


async def test_valkey_backend_pubsub_notifications_wake_waiters(valkey_backend: "ValkeyQueueBackend") -> "None":
    waiter = asyncio.create_task(valkey_backend.wait_for_notifications(timeout=2.0))
    # Real Valkey requires the SUBSCRIBE roundtrip to land before the
    # sibling enqueue publishes; a bare ``asyncio.sleep(0)`` is too brief.
    await asyncio.sleep(0.2)

    record = await valkey_backend.enqueue("tasks.notified", queue="critical", execution_backend="local")

    assert await waiter is True
    assert valkey_backend.capabilities.supports_notifications is True
    assert valkey_backend.capabilities.notifications_durable is False
    assert valkey_backend.capabilities.notification_backend == "valkey-pubsub"
    assert await valkey_backend.wait_for_notifications(timeout=0.01) is False
    assert record.status == "pending"


async def test_valkey_backend_enqueue_many_publishes_one_batch_notification(
    valkey_backend: "ValkeyQueueBackend",
) -> "None":
    waiter = asyncio.create_task(valkey_backend.wait_for_notifications(timeout=2.0))
    await asyncio.sleep(0.2)

    records = await valkey_backend.enqueue_many([EnqueueSpec(task_name=f"tasks.batch.{index}") for index in range(5)])

    assert await waiter is True
    assert len(records) == 5
    assert await valkey_backend.wait_for_notifications(timeout=0.05) is False

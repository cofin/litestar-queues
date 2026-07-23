"""Shared maintenance coordination and bounded-operation assertions.

Reused by every persistent-backend contract suite so the distributed ownership and
bounded cleanup/recovery contracts are validated identically across Redis,
Valkey, SQLSpec, and Advanced Alchemy.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from litestar_queues.backends import BaseQueueBackend

MAINTENANCE_NAME = "queue-maintenance"


async def assert_cross_instance_coordination(first: "BaseQueueBackend", second: "BaseQueueBackend") -> "None":
    """Two independently opened backends must share one token-fenced ownership."""
    ttl = timedelta(seconds=60)
    assert first.capabilities.supports_maintenance is True
    assert second.capabilities.supports_maintenance is True

    assert await first.acquire_maintenance(MAINTENANCE_NAME, "token-a", ttl=ttl) is True
    # A second, independently opened instance sees the held ownership and is denied.
    assert await second.acquire_maintenance(MAINTENANCE_NAME, "token-b", ttl=ttl) is False
    # A non-owner cannot release the ownership (token fencing).
    assert await second.release_maintenance(MAINTENANCE_NAME, "token-b") is False
    # The owner releases and the other instance can then acquire.
    assert await first.release_maintenance(MAINTENANCE_NAME, "token-a") is True
    assert await second.acquire_maintenance(MAINTENANCE_NAME, "token-b", ttl=ttl) is True
    assert await second.release_maintenance(MAINTENANCE_NAME, "token-b") is True


async def assert_coordination_expiry(backend: "BaseQueueBackend") -> "None":
    """An expired ownership is reacquirable by a fresh holder."""
    assert await backend.acquire_maintenance(MAINTENANCE_NAME, "token-a", ttl=timedelta(milliseconds=50)) is True
    await asyncio.sleep(0.2)
    assert await backend.acquire_maintenance(MAINTENANCE_NAME, "token-b", ttl=timedelta(seconds=60)) is True
    assert await backend.release_maintenance(MAINTENANCE_NAME, "token-b") is True


async def assert_bounded_cleanup_terminal(
    backend: "BaseQueueBackend", *, prefix: "str" = "tasks.cleanup.bound"
) -> "None":
    """Terminal cleanup removes at most ``limit`` per call and never double-deletes."""
    for index in range(5):
        record = await backend.enqueue(f"{prefix}.{index}")
        claimed = await backend.claim_task(record.id)
        assert claimed is not None
        await backend.complete_task(claimed.id)

    before = datetime.now(timezone.utc) + timedelta(seconds=1)
    assert await backend.cleanup_terminal(before, limit=2) == 2
    assert await backend.cleanup_terminal(before, limit=2) == 2
    assert await backend.cleanup_terminal(before, limit=2) == 1
    assert await backend.cleanup_terminal(before, limit=2) == 0


async def assert_bounded_stale_recovery(backend: "BaseQueueBackend", *, prefix: "str" = "tasks.stale.bound") -> "None":
    """Stale recovery recovers at most ``limit`` per call with zero overlap."""
    records = []
    for index in range(4):
        record = await backend.enqueue(f"{prefix}.{index}", max_retries=3, metadata={"requeue_on_stale": True})
        claimed = await backend.claim_task(record.id)
        assert claimed is not None
        records.append(record)

    assert (await backend.requeue_stale_running(stale_after=timedelta(seconds=-2), limit=2)).requeued == 2
    assert (await backend.requeue_stale_running(stale_after=timedelta(seconds=-2), limit=2)).requeued == 2
    assert (await backend.requeue_stale_running(stale_after=timedelta(seconds=-2), limit=2)).requeued == 0
    for record in records:
        stored = await backend.get_task(record.id)
        assert stored is not None
        assert stored.status == "pending"
        assert stored.retry_count == 1

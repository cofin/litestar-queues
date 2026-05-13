"""Real-Redis fixtures for backend integration tests.

Opens a redis-py async client against the pytest-databases ``redis_service``
container fixture. Each test gets a fresh ``flushdb`` so dedup keys, ZSETs,
and pubsub channels start clean; backends additionally namespace under a
``uuid.uuid4().hex`` key prefix for xdist + shared-container safety.
"""

import uuid
from typing import TYPE_CHECKING, Any

import pytest

pytest.importorskip("redis")

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from redis.asyncio import Redis

    from litestar_queues.backends.redis import RedisQueueBackend


@pytest.fixture
async def redis_client(redis_service: Any) -> "AsyncIterator[Redis]":
    """Yield an opened redis-py async client against the pytest-databases Redis container."""
    from redis.asyncio import Redis

    client: Redis = Redis(
        host=redis_service.host,
        port=redis_service.port,
        db=0,
        decode_responses=False,
    )
    try:
        await client.flushdb()
        yield client
    finally:
        await client.aclose()


@pytest.fixture
async def redis_backend(redis_client: "Redis") -> "AsyncIterator[RedisQueueBackend]":
    """Yield an opened ``RedisQueueBackend`` namespaced under a unique prefix."""
    from litestar_queues.backends.redis import RedisQueueBackend

    prefix = f"litestar_queues:test:redis:{uuid.uuid4().hex}"
    backend = RedisQueueBackend(
        client=redis_client,
        key_prefix=prefix,
        notifications=True,
        notification_channel=f"{prefix}:notifications",
        lock_timeout=0.1,
    )
    await backend.open()
    try:
        yield backend
    finally:
        await backend.close()

"""Real-Redis fixtures for backend integration tests."""

import uuid
from typing import TYPE_CHECKING, Protocol

import pytest

pytest.importorskip("redis")

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from litestar_queues.backends.redis import RedisQueueBackend


class RedisService(Protocol):
    """pytest-databases Redis service attributes used by the backend fixture."""

    host: "str"
    port: "int"
    db: "int"


@pytest.fixture
async def redis_backend(redis_service: "RedisService") -> "AsyncIterator[RedisQueueBackend]":
    """Yield an opened ``RedisQueueBackend`` namespaced under a unique prefix."""
    from litestar_queues.backends.redis import RedisBackendConfig, RedisQueueBackend

    prefix = f"litestar_queues:test:redis:{uuid.uuid4().hex}"
    backend = RedisQueueBackend(
        backend_config=RedisBackendConfig(
            url=f"redis://{redis_service.host}:{redis_service.port}/{redis_service.db}",
            key_prefix=prefix,
            notifications=True,
            notification_channel=f"{prefix}:notifications",
        )
    )
    await backend.open()
    try:
        yield backend
    finally:
        await backend.close()

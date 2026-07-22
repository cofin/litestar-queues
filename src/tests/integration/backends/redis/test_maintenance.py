"""Redis distributed maintenance-lease and bounded-operation contract."""

import uuid
from typing import TYPE_CHECKING

import pytest

pytest.importorskip("redis")

from tests.integration.backends._maintenance_asserts import (
    assert_bounded_cleanup_terminal,
    assert_bounded_stale_recovery,
    assert_cross_instance_lease,
    assert_lease_expiry,
)

if TYPE_CHECKING:
    from litestar_queues.backends.redis import RedisQueueBackend
    from tests.integration.backends.redis.conftest import RedisService

pytestmark = pytest.mark.anyio


async def test_redis_backend_bounded_cleanup_terminal(redis_backend: "RedisQueueBackend") -> "None":
    await assert_bounded_cleanup_terminal(redis_backend)


async def test_redis_backend_bounded_stale_recovery(redis_backend: "RedisQueueBackend") -> "None":
    await assert_bounded_stale_recovery(redis_backend)


async def test_redis_backend_lease_expiry(redis_backend: "RedisQueueBackend") -> "None":
    await assert_lease_expiry(redis_backend)


async def test_redis_backend_lease_is_not_process_local(redis_service: "RedisService") -> "None":
    """Two independently opened Redis backends share the namespaced lease key."""
    from litestar_queues.backends.redis import RedisBackendConfig, RedisQueueBackend

    prefix = f"litestar_queues:test:lease:{uuid.uuid4().hex}"
    url = f"redis://{redis_service.host}:{redis_service.port}/{redis_service.db}"
    first = RedisQueueBackend(backend_config=RedisBackendConfig(url=url, key_prefix=prefix, notifications=False))
    second = RedisQueueBackend(backend_config=RedisBackendConfig(url=url, key_prefix=prefix, notifications=False))
    await first.open()
    await second.open()
    try:
        await assert_cross_instance_lease(first, second)
    finally:
        await first.close()
        await second.close()

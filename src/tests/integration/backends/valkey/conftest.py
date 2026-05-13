"""Real-Valkey fixtures for backend integration tests.

Mirror of ``backends/redis/conftest.py`` against the pytest-databases
``valkey_service`` container fixture. The ``valkey.asyncio.Valkey`` client
is API-compatible with redis-py for the protocol surface this backend
uses, so per-test ``flushdb`` + per-backend uuid-prefixed namespace covers
both shared-container and xdist scenarios.
"""

import uuid
from typing import TYPE_CHECKING, Any

import pytest

pytest.importorskip("valkey")

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from valkey.asyncio import Valkey

    from litestar_queues.backends.valkey import ValkeyQueueBackend


@pytest.fixture
async def valkey_client(valkey_service: Any) -> "AsyncIterator[Valkey]":
    """Yield an opened valkey-py async client against the pytest-databases Valkey container."""
    from valkey.asyncio import Valkey

    client: Valkey = Valkey(
        host=valkey_service.host,
        port=valkey_service.port,
        db=0,
        decode_responses=False,
    )
    try:
        await client.flushdb()
        yield client
    finally:
        await client.aclose()


@pytest.fixture
async def valkey_backend(valkey_client: "Valkey") -> "AsyncIterator[ValkeyQueueBackend]":
    """Yield an opened ``ValkeyQueueBackend`` namespaced under a unique prefix."""
    from litestar_queues.backends.valkey import ValkeyQueueBackend

    prefix = f"litestar_queues:test:valkey:{uuid.uuid4().hex}"
    backend = ValkeyQueueBackend(
        client=valkey_client,
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

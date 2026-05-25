"""Real-Valkey fixtures for backend integration tests."""

import uuid
from typing import TYPE_CHECKING, Protocol

import pytest

pytest.importorskip("valkey")

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from litestar_queues.backends.valkey import ValkeyQueueBackend


class ValkeyService(Protocol):
    """pytest-databases Valkey service attributes used by the backend fixture."""

    host: str
    port: int
    db: int


@pytest.fixture
async def valkey_backend(valkey_service: ValkeyService) -> "AsyncIterator[ValkeyQueueBackend]":
    """Yield an opened ``ValkeyQueueBackend`` namespaced under a unique prefix."""
    from litestar_queues.backends.valkey import ValkeyQueueBackend

    prefix = f"litestar_queues:test:valkey:{uuid.uuid4().hex}"
    backend = ValkeyQueueBackend(
        url=f"redis://{valkey_service.host}:{valkey_service.port}/{valkey_service.db}",
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

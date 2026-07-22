"""Valkey distributed maintenance-lease and bounded-operation contract."""

import uuid
from typing import TYPE_CHECKING

import pytest

pytest.importorskip("valkey")

from tests.integration.backends._maintenance_asserts import (
    assert_bounded_cleanup_terminal,
    assert_bounded_stale_recovery,
    assert_cross_instance_lease,
    assert_lease_expiry,
)

if TYPE_CHECKING:
    from litestar_queues.backends.valkey import ValkeyQueueBackend
    from tests.integration.backends.valkey.conftest import ValkeyService

pytestmark = pytest.mark.anyio


async def test_valkey_backend_bounded_cleanup_terminal(valkey_backend: "ValkeyQueueBackend") -> "None":
    await assert_bounded_cleanup_terminal(valkey_backend)


async def test_valkey_backend_bounded_stale_recovery(valkey_backend: "ValkeyQueueBackend") -> "None":
    await assert_bounded_stale_recovery(valkey_backend)


async def test_valkey_backend_lease_expiry(valkey_backend: "ValkeyQueueBackend") -> "None":
    await assert_lease_expiry(valkey_backend)


async def test_valkey_backend_lease_is_not_process_local(valkey_service: "ValkeyService") -> "None":
    """Two independently opened Valkey backends share the namespaced lease key."""
    from litestar_queues.backends.valkey import ValkeyBackendConfig, ValkeyQueueBackend

    prefix = f"litestar_queues:test:lease:{uuid.uuid4().hex}"
    url = f"redis://{valkey_service.host}:{valkey_service.port}/{valkey_service.db}"
    first = ValkeyQueueBackend(backend_config=ValkeyBackendConfig(url=url, key_prefix=prefix, notifications=False))
    second = ValkeyQueueBackend(backend_config=ValkeyBackendConfig(url=url, key_prefix=prefix, notifications=False))
    await first.open()
    await second.open()
    try:
        await assert_cross_instance_lease(first, second)
    finally:
        await first.close()
        await second.close()

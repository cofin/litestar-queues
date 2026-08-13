"""Real-Valkey proof that a worker-control hint cancels a saturated worker."""

import uuid
from typing import TYPE_CHECKING, Protocol

import pytest

pytest.importorskip("valkey")

from litestar_queues import QueueConfig, WorkerConfig
from litestar_queues.backends.valkey import ValkeyBackendConfig, ValkeyQueueBackend
from tests.helpers.redis_protocol import wait_for_channel_subscribers
from tests.integration._worker_control_contract import (
    assert_control_hint_cancels_saturated_worker,
    assert_durable_poll_cancels_without_control_hint,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class ValkeyService(Protocol):
    """pytest-databases Valkey service attributes used here."""

    host: "str"
    port: "int"
    db: "int"


pytestmark = pytest.mark.anyio


@pytest.fixture
async def valkey_control_pair(valkey_service: "ValkeyService") -> "AsyncIterator[tuple[ValkeyQueueBackend, ...]]":
    """Yield two independently connected backends sharing one private namespace."""
    config = QueueConfig(
        namespace=f"lq_ctl_{uuid.uuid4().hex[:12]}",
        queue_backend="valkey",
        worker=WorkerConfig(placement="external"),
        initialize_schedules=False,
    )
    url = f"redis://{valkey_service.host}:{valkey_service.port}/{valkey_service.db}"
    backends = tuple(
        ValkeyQueueBackend(config, backend_config=ValkeyBackendConfig(url=url, worker_wakeups=True)) for _ in range(2)
    )
    for backend in backends:
        await backend.open()
    try:
        yield backends
    finally:
        for backend in backends:
            await backend.close()


async def test_valkey_control_hint_cancels_saturated_worker(
    valkey_control_pair: "tuple[ValkeyQueueBackend, ...]",
) -> "None":
    worker_backend, control_backend = valkey_control_pair

    async def listener_ready() -> "None":
        await wait_for_channel_subscribers(worker_backend, worker_backend._control_channel, timeout=5.0)

    await assert_control_hint_cancels_saturated_worker(
        worker_backend=worker_backend,
        control_backend=control_backend,
        backend_name="valkey",
        wait_for_listener=listener_ready,
    )


async def test_valkey_dropped_control_hint_still_cancels_via_durable_poll(
    valkey_control_pair: "tuple[ValkeyQueueBackend, ...]",
) -> "None":
    worker_backend, control_backend = valkey_control_pair

    await assert_durable_poll_cancels_without_control_hint(
        worker_backend=worker_backend, control_backend=control_backend, backend_name="valkey"
    )

"""Postgres LISTEN/NOTIFY proof that a control hint cancels a saturated worker."""

import uuid
from contextlib import suppress
from typing import TYPE_CHECKING, Any, cast

import pytest

pytest.importorskip("aiosqlite")
pytest.importorskip("sqlspec")

from litestar_queues import QueueConfig, WorkerConfig
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend
from tests.helpers._timing import wait_until
from tests.integration._worker_control_contract import (
    assert_control_hint_cancels_saturated_worker,
    assert_durable_poll_cancels_without_control_hint,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from litestar_queues.backends.sqlspec.typing import SQLSpecManager, SQLSpecSessionConfig
    from tests.integration._backends import PostgresService

pytestmark = pytest.mark.anyio

_QUEUE_TABLE = "lq_worker_control"


@pytest.fixture
async def postgres_control_pair(
    postgres_service: "PostgresService",
) -> "AsyncIterator[tuple[SQLSpecQueueBackend, ...]]":
    """Yield two asyncpg-backed backends sharing one private namespace."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    config = QueueConfig(
        namespace=f"lq_ctl_{uuid.uuid4().hex[:12]}",
        queue_backend="sqlspec",
        worker=WorkerConfig(placement="external"),
        initialize_schedules=False,
    )

    def sqlspec_config() -> "AsyncpgConfig":
        return AsyncpgConfig(
            connection_config={
                "host": postgres_service.host,
                "port": postgres_service.port,
                "user": postgres_service.user,
                "password": postgres_service.password,
                "database": postgres_service.database,
            }
        )

    backends = tuple(
        SQLSpecQueueBackend(
            config, backend_config=SQLSpecBackendConfig(sqlspec_config=sqlspec_config(), queue_table_name=_QUEUE_TABLE)
        )
        for _ in range(2)
    )
    for backend in backends:
        await backend.open()
    await backends[0].create_schema()
    try:
        yield backends
    finally:
        with suppress(Exception):
            await _drop_tables(backends[0])
        for backend in backends:
            await backend.close()


async def _drop_tables(backend: "SQLSpecQueueBackend") -> "None":
    from litestar_queues.backends.sqlspec.backend import _bridge_session

    assert backend._sqlspec is not None
    assert backend._sqlspec_config is not None
    async with _bridge_session(
        cast("SQLSpecManager", backend._sqlspec), cast("SQLSpecSessionConfig", backend._sqlspec_config)
    ) as driver:
        await driver.execute_script(f'DROP TABLE IF EXISTS "{_QUEUE_TABLE}"')


async def test_sqlspec_postgres_control_hint_cancels_saturated_worker(
    postgres_control_pair: "tuple[SQLSpecQueueBackend, ...]",
) -> "None":
    worker_backend, control_backend = postgres_control_pair
    assert worker_backend.capabilities.supports_worker_wakeups is True

    async def listener_ready() -> "None":
        await wait_until(
            lambda: cast("Any", worker_backend)._control_stream is not None,
            timeout=5.0,
            message="worker did not open its LISTEN/NOTIFY control stream",
        )

    await assert_control_hint_cancels_saturated_worker(
        worker_backend=worker_backend,
        control_backend=control_backend,
        backend_name="sqlspec",
        wait_for_listener=listener_ready,
    )


async def test_sqlspec_postgres_dropped_control_hint_still_cancels_via_durable_poll(
    postgres_control_pair: "tuple[SQLSpecQueueBackend, ...]",
) -> "None":
    worker_backend, control_backend = postgres_control_pair

    await assert_durable_poll_cancels_without_control_hint(
        worker_backend=worker_backend, control_backend=control_backend, backend_name="sqlspec"
    )

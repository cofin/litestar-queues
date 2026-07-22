"""SQLSpec distributed maintenance-lease and bounded-operation contract."""

from typing import TYPE_CHECKING

import pytest

pytest.importorskip("aiosqlite")
pytest.importorskip("sqlspec")

from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend
from tests.integration.backends._maintenance_asserts import (
    assert_bounded_cleanup_terminal,
    assert_bounded_stale_recovery,
    assert_cross_instance_lease,
    assert_lease_expiry,
)

if TYPE_CHECKING:
    from pathlib import Path

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend as _Backend

pytestmark = pytest.mark.anyio


async def test_sqlspec_backend_bounded_cleanup_terminal(sqlspec_backend: "_Backend") -> "None":
    await assert_bounded_cleanup_terminal(sqlspec_backend)


async def test_sqlspec_backend_bounded_stale_recovery(sqlspec_backend: "_Backend") -> "None":
    await assert_bounded_stale_recovery(sqlspec_backend)


async def test_sqlspec_backend_lease_expiry(sqlspec_backend: "_Backend") -> "None":
    await assert_lease_expiry(sqlspec_backend)


async def test_sqlspec_backend_lease_is_not_process_local(tmp_path: "Path") -> "None":
    """Two independently opened SQLSpec backends share the persisted lease row."""
    db_path = str(tmp_path / "lease.db")
    first = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(config=AiosqliteConfig(connection_config={"database": db_path}))
    )
    second = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(config=AiosqliteConfig(connection_config={"database": db_path}))
    )
    await first.open()
    await first.create_schema()
    await second.open()
    try:
        await assert_cross_instance_lease(first, second)
    finally:
        await first.close()
        await second.close()

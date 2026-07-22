"""Advanced Alchemy distributed maintenance-lease and bounded-operation contract."""

from typing import TYPE_CHECKING

import pytest

pytest.importorskip("aiosqlite")
pytest.importorskip("advanced_alchemy")

from tests.integration.backends._maintenance_asserts import (
    assert_bounded_cleanup_terminal,
    assert_bounded_stale_recovery,
    assert_cross_instance_lease,
    assert_lease_expiry,
)
from tests.integration.backends.advanced_alchemy._aa_schema import create_tables

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.anyio


def _config(db_path: "Path") -> "object":
    from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig

    return SQLAlchemyAsyncConfig(connection_string=f"sqlite+aiosqlite:///{db_path}")


async def test_advanced_alchemy_backend_bounded_operations(tmp_path: "Path") -> "None":
    from litestar_queues.backends.advanced_alchemy import (
        QueueMaintenanceLeaseModel,
        QueueTaskModel,
        SQLAlchemyBackend,
        SQLAlchemyBackendConfig,
    )

    config = _config(tmp_path / "aa-maintenance.db")
    await create_tables(config, QueueTaskModel, QueueMaintenanceLeaseModel)  # type: ignore[arg-type]
    backend = SQLAlchemyBackend(backend_config=SQLAlchemyBackendConfig(sqlalchemy_config=config))  # type: ignore[arg-type]
    await backend.open()
    try:
        await assert_bounded_cleanup_terminal(backend)
        await assert_bounded_stale_recovery(backend)
        await assert_lease_expiry(backend)
    finally:
        await backend.close()


async def test_advanced_alchemy_backend_lease_is_not_process_local(tmp_path: "Path") -> "None":
    """Two independently opened Advanced Alchemy backends share the persisted lease row."""
    from litestar_queues.backends.advanced_alchemy import (
        QueueMaintenanceLeaseModel,
        QueueTaskModel,
        SQLAlchemyBackend,
        SQLAlchemyBackendConfig,
    )

    db_path = tmp_path / "aa-lease.db"
    first_config = _config(db_path)
    await create_tables(first_config, QueueTaskModel, QueueMaintenanceLeaseModel)  # type: ignore[arg-type]
    first = SQLAlchemyBackend(backend_config=SQLAlchemyBackendConfig(sqlalchemy_config=first_config))  # type: ignore[arg-type]
    second = SQLAlchemyBackend(backend_config=SQLAlchemyBackendConfig(sqlalchemy_config=_config(db_path)))  # type: ignore[arg-type]
    await first.open()
    await second.open()
    try:
        await assert_cross_instance_lease(first, second)
    finally:
        await first.close()
        await second.close()

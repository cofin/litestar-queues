"""Heartbeat-pool isolation tests for the SQLSpec queue backend.

Covers the dedicated heartbeat pool feature (``heartbeat_pool_config``):
the default-pool path, isolated-write path, registration-failure fallback,
and concurrent-heartbeat correctness. All cases pin to aiosqlite.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pytest

pytest.importorskip("aiosqlite")
pytest.importorskip("sqlspec")

from sqlspec import SQLSpec

from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

if TYPE_CHECKING:
    from pathlib import Path

    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    from tests.integration.backends.sqlspec.conftest import SqliteConfigFactory

pytestmark = pytest.mark.anyio


async def test_sqlspec_backend_default_heartbeat_uses_main_pool(
    sqlspec_backend: SQLSpecQueueBackend,
) -> None:
    """When heartbeat_pool_config is None, heartbeat writes use the main pool."""
    record = await sqlspec_backend.enqueue("tasks.heartbeat")
    claimed = await sqlspec_backend.claim_task(record.id)

    assert claimed is not None
    assert sqlspec_backend._heartbeat_pool_enabled is False
    assert sqlspec_backend._heartbeat_pool_registered is False

    await sqlspec_backend.touch_heartbeat(claimed.id)
    touched = await sqlspec_backend.get_task(claimed.id)
    assert touched is not None
    assert touched.heartbeat_at is not None


async def test_sqlspec_backend_dedicated_heartbeat_pool_isolates_heartbeat_writes(
    tmp_path: "Path",
    monkeypatch: pytest.MonkeyPatch,
    sqlite_config_factory: "SqliteConfigFactory",
) -> None:
    """touch_heartbeat / null_heartbeats hit the dedicated pool only."""
    queue_path = tmp_path / "queue.db"
    main_config = sqlite_config_factory(queue_path)
    heartbeat_config = sqlite_config_factory(queue_path)
    sqlspec = SQLSpec()
    sqlspec.add_config(main_config)

    backend = SQLSpecQueueBackend(
        sqlspec=sqlspec,
        sqlspec_config=main_config,
        heartbeat_pool_config=heartbeat_config,
    )
    await backend.open()
    try:
        assert id(heartbeat_config) in sqlspec.configs
        assert backend._heartbeat_pool_registered is True

        record = await backend.enqueue("tasks.isolated_heartbeat")
        claimed = await backend.claim_task(record.id)
        assert claimed is not None

        original_session = SQLSpecQueueBackend._session
        original_heartbeat = SQLSpecQueueBackend._heartbeat_session
        main_calls = 0
        heartbeat_calls = 0

        @contextlib.asynccontextmanager
        async def counting_session(self: SQLSpecQueueBackend) -> AsyncIterator[object]:
            nonlocal main_calls
            main_calls += 1
            async with original_session(self) as driver:
                yield driver

        @contextlib.asynccontextmanager
        async def counting_heartbeat(self: SQLSpecQueueBackend) -> AsyncIterator[object]:
            nonlocal heartbeat_calls
            heartbeat_calls += 1
            async with original_heartbeat(self) as driver:
                yield driver

        monkeypatch.setattr(SQLSpecQueueBackend, "_session", counting_session)
        monkeypatch.setattr(SQLSpecQueueBackend, "_heartbeat_session", counting_heartbeat)

        await backend.touch_heartbeat(claimed.id)
        await backend.null_heartbeats([claimed.id])

        assert heartbeat_calls == 2
        assert main_calls == 0
    finally:
        await backend.close()


async def test_sqlspec_backend_heartbeat_pool_failure_falls_back_to_main(
    tmp_path: "Path",
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    sqlite_config_factory: "SqliteConfigFactory",
) -> None:
    """Dedicated pool registration failure does not prevent backend.open()."""
    main_config = sqlite_config_factory(tmp_path / "main.db")
    bad_heartbeat_config = sqlite_config_factory(tmp_path / "main.db")

    real_add_config = SQLSpec.add_config

    def failing_add_config(self: SQLSpec, config: "AiosqliteConfig") -> "AiosqliteConfig":
        if config is bad_heartbeat_config:
            msg = "simulated heartbeat pool registration failure"
            raise RuntimeError(msg)
        return real_add_config(self, config)

    monkeypatch.setattr(SQLSpec, "add_config", failing_add_config)

    backend = SQLSpecQueueBackend(
        sqlspec_config=main_config,
        heartbeat_pool_config=bad_heartbeat_config,
    )
    with caplog.at_level("WARNING", logger="litestar_queues"):
        await backend.open()
    try:
        assert backend._heartbeat_pool_registered is False
        assert backend._heartbeat_pool_enabled is False
        assert any("heartbeat pool registration failed" in entry.getMessage() for entry in caplog.records)
        record = await backend.enqueue("tasks.fallback_heartbeat")
        claimed = await backend.claim_task(record.id)
        assert claimed is not None
        await backend.touch_heartbeat(claimed.id)
        touched = await backend.get_task(claimed.id)
        assert touched is not None
        assert touched.heartbeat_at is not None
    finally:
        await backend.close()


async def test_sqlspec_backend_dedicated_heartbeat_pool_handles_concurrent_heartbeats(
    tmp_path: "Path",
    sqlite_config_factory: "SqliteConfigFactory",
) -> None:
    """Many concurrent heartbeats on the dedicated pool must not deadlock."""
    queue_path = tmp_path / "queue.db"
    main_config = sqlite_config_factory(queue_path)
    heartbeat_config = sqlite_config_factory(queue_path)
    backend = SQLSpecQueueBackend(
        sqlspec_config=main_config,
        heartbeat_pool_config=heartbeat_config,
    )
    await backend.open()
    try:
        records = [await backend.enqueue(f"tasks.bulk-{i}") for i in range(16)]
        claimed = []
        for record in records:
            c = await backend.claim_task(record.id)
            assert c is not None
            claimed.append(c)

        await asyncio.wait_for(
            asyncio.gather(*(backend.touch_heartbeat(record.id) for _ in range(4) for record in claimed)),
            timeout=10.0,
        )
        for record in claimed:
            stored = await backend.get_task(record.id)
            assert stored is not None
            assert stored.heartbeat_at is not None
    finally:
        await backend.close()

"""Heartbeat-sessionmaker isolation tests for the Advanced Alchemy backend.

Covers the dedicated heartbeat sessionmaker feature
(``heartbeat_session_maker``): the default fallthrough path, the isolated-
write path, and concurrent-heartbeat correctness. All cases pin to
aiosqlite so they run without Docker.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pytest

pytest.importorskip("aiosqlite")
pytest.importorskip("advanced_alchemy")
pytest.importorskip("sqlalchemy")

from advanced_alchemy.base import UUIDAuditBase
from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig
from sqlalchemy.ext.asyncio import async_sessionmaker

from litestar_queues.backends.advanced_alchemy import (
    AdvancedAlchemyBackendConfig,
    AdvancedAlchemyQueueBackend,
    QueueTaskModelMixin,
)

if TYPE_CHECKING:
    from pathlib import Path

    from litestar_queues.backends.advanced_alchemy.service import QueueTaskService

pytestmark = pytest.mark.anyio


def _sqlite_config(path: "Path") -> SQLAlchemyAsyncConfig:
    """Return an aiosqlite Advanced Alchemy config pointing at ``path``."""
    return SQLAlchemyAsyncConfig(connection_string=f"sqlite+aiosqlite:///{path}")


class HeartbeatQueueTask(UUIDAuditBase, QueueTaskModelMixin):
    __tablename__ = "heartbeat_queue_tasks"


async def test_advanced_alchemy_backend_default_heartbeat_uses_main_session(
    tmp_path: "Path",
) -> None:
    """When heartbeat_session_maker is None, heartbeat writes use the main session."""
    backend = AdvancedAlchemyQueueBackend(
        backend_config=AdvancedAlchemyBackendConfig(
            sqlalchemy_config=_sqlite_config(tmp_path / "queue.db"),
            model_class=HeartbeatQueueTask,
            create_schema=True,
        )
    )
    await backend.open()
    try:
        assert backend._heartbeat_session_maker is None

        record = await backend.enqueue("tasks.heartbeat")
        claimed = await backend.claim_task(record.id)
        assert claimed is not None

        await backend.touch_heartbeat(claimed.id)
        touched = await backend.get_task(claimed.id)
        assert touched is not None
        assert touched.heartbeat_at is not None
    finally:
        await backend.close()


async def test_advanced_alchemy_backend_dedicated_heartbeat_maker_isolates_writes(
    tmp_path: "Path",
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """touch_heartbeat / null_heartbeats use the dedicated heartbeat sessionmaker."""
    queue_path = tmp_path / "queue.db"
    main_config = _sqlite_config(queue_path)
    heartbeat_config = _sqlite_config(queue_path)
    heartbeat_maker = async_sessionmaker(heartbeat_config.get_engine(), expire_on_commit=False)

    backend = AdvancedAlchemyQueueBackend(
        backend_config=AdvancedAlchemyBackendConfig(
            sqlalchemy_config=main_config,
            model_class=HeartbeatQueueTask,
            heartbeat_session_maker=heartbeat_maker,
            create_schema=True,
        )
    )
    await backend.open()
    try:
        assert backend._heartbeat_session_maker is heartbeat_maker

        record = await backend.enqueue("tasks.isolated_heartbeat")
        claimed = await backend.claim_task(record.id)
        assert claimed is not None

        original_operation = AdvancedAlchemyQueueBackend._operation
        original_heartbeat = AdvancedAlchemyQueueBackend._heartbeat_operation
        operation_calls = 0
        heartbeat_calls = 0

        @contextlib.asynccontextmanager
        async def counting_operation(self: AdvancedAlchemyQueueBackend) -> AsyncIterator["QueueTaskService"]:
            nonlocal operation_calls
            operation_calls += 1
            async with original_operation(self) as service:
                yield service

        @contextlib.asynccontextmanager
        async def counting_heartbeat(self: AdvancedAlchemyQueueBackend) -> AsyncIterator["QueueTaskService"]:
            nonlocal heartbeat_calls
            heartbeat_calls += 1
            async with original_heartbeat(self) as service:
                yield service

        monkeypatch.setattr(AdvancedAlchemyQueueBackend, "_operation", counting_operation)
        monkeypatch.setattr(AdvancedAlchemyQueueBackend, "_heartbeat_operation", counting_heartbeat)

        await backend.touch_heartbeat(claimed.id)
        await backend.null_heartbeats([claimed.id])

        assert heartbeat_calls == 2
        assert operation_calls == 0
    finally:
        await backend.close()
        await heartbeat_config.get_engine().dispose()


async def test_advanced_alchemy_backend_dedicated_heartbeat_maker_handles_concurrent_heartbeats(
    tmp_path: "Path",
) -> None:
    """Many concurrent heartbeats over the dedicated maker must not deadlock."""
    queue_path = tmp_path / "queue.db"
    main_config = _sqlite_config(queue_path)
    heartbeat_config = _sqlite_config(queue_path)
    heartbeat_maker = async_sessionmaker(heartbeat_config.get_engine(), expire_on_commit=False)

    backend = AdvancedAlchemyQueueBackend(
        backend_config=AdvancedAlchemyBackendConfig(
            sqlalchemy_config=main_config,
            model_class=HeartbeatQueueTask,
            heartbeat_session_maker=heartbeat_maker,
            create_schema=True,
        )
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
        await heartbeat_config.get_engine().dispose()

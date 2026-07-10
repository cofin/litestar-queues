"""Heartbeat-sessionmaker isolation tests for the Advanced Alchemy backend.

Covers the dedicated heartbeat sessionmaker feature
(``heartbeat_session_maker``): the default fallthrough path, the isolated-
write path, and concurrent-heartbeat correctness. All cases pin to
aiosqlite so they run without Docker.
"""

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any

import pytest

pytest.importorskip("aiosqlite")
pytest.importorskip("advanced_alchemy")
pytest.importorskip("sqlalchemy")

from advanced_alchemy.base import UUIDAuditBase
from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from litestar_queues import HeartbeatTouch
from litestar_queues.backends.advanced_alchemy import QueueTaskModelMixin, SQLAlchemyBackend, SQLAlchemyBackendConfig
from tests.integration.backends.advanced_alchemy._aa_schema import create_tables

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from litestar_queues.backends.advanced_alchemy.service import QueueTaskService

pytestmark = pytest.mark.anyio


def _sqlite_config(path: "Path") -> "SQLAlchemyAsyncConfig":
    """Return an aiosqlite Advanced Alchemy config pointing at ``path``."""
    return SQLAlchemyAsyncConfig(connection_string=f"sqlite+aiosqlite:///{path}")


class HeartbeatQueueTask(UUIDAuditBase, QueueTaskModelMixin):
    __tablename__ = "heartbeat_queue_task"


async def test_advanced_alchemy_backend_default_heartbeat_uses_main_session(tmp_path: "Path") -> "None":
    """When heartbeat_session_maker is None, heartbeat writes use the main session."""
    config = _sqlite_config(tmp_path / "queue.db")
    await create_tables(config, HeartbeatQueueTask)
    backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(sqlalchemy_config=config, model_class=HeartbeatQueueTask)
    )
    await backend.open()
    try:
        assert backend._heartbeat_session_maker is None

        record = await backend.enqueue("tasks.heartbeat")
        claimed = await backend.claim_task(record.id)
        assert claimed is not None

        result = await backend.touch_heartbeats([
            HeartbeatTouch(task_id=claimed.id, expected_retry_count=claimed.retry_count)
        ])
        touched = await backend.get_task(claimed.id)
        assert result.touched_task_ids == {claimed.id}
        assert result.missed_task_ids == set()
        assert touched is not None
        assert touched.heartbeat_at is not None
    finally:
        await backend.close()


async def test_advanced_alchemy_backend_dedicated_heartbeat_maker_isolates_writes(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """touch_heartbeats / null_heartbeats use the dedicated heartbeat sessionmaker."""
    queue_path = tmp_path / "queue.db"
    main_config = _sqlite_config(queue_path)
    await create_tables(main_config, HeartbeatQueueTask)
    heartbeat_config = _sqlite_config(queue_path)
    heartbeat_maker = async_sessionmaker(heartbeat_config.get_engine(), expire_on_commit=False)

    backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(
            sqlalchemy_config=main_config, model_class=HeartbeatQueueTask, heartbeat_session_maker=heartbeat_maker
        )
    )
    await backend.open()
    try:
        assert backend._heartbeat_session_maker is heartbeat_maker

        record = await backend.enqueue("tasks.isolated_heartbeat")
        claimed = await backend.claim_task(record.id)
        assert claimed is not None

        original_operation = SQLAlchemyBackend._operation
        original_heartbeat = SQLAlchemyBackend._heartbeat_operation
        operation_calls = 0
        heartbeat_calls = 0

        @contextlib.asynccontextmanager
        async def counting_operation(self: "SQLAlchemyBackend") -> 'AsyncIterator["QueueTaskService"]':
            nonlocal operation_calls
            operation_calls += 1
            async with original_operation(self) as service:
                yield service

        @contextlib.asynccontextmanager
        async def counting_heartbeat(self: "SQLAlchemyBackend") -> 'AsyncIterator["QueueTaskService"]':
            nonlocal heartbeat_calls
            heartbeat_calls += 1
            async with original_heartbeat(self) as service:
                yield service

        monkeypatch.setattr(SQLAlchemyBackend, "_operation", counting_operation)
        monkeypatch.setattr(SQLAlchemyBackend, "_heartbeat_operation", counting_heartbeat)

        result = await backend.touch_heartbeats([
            HeartbeatTouch(task_id=claimed.id, expected_retry_count=claimed.retry_count)
        ])
        await backend.null_heartbeats([claimed.id])

        assert result.touched_task_ids == {claimed.id}
        assert heartbeat_calls == 2
        assert operation_calls == 0
    finally:
        await backend.close()
        await heartbeat_config.get_engine().dispose()


async def test_advanced_alchemy_backend_dedicated_heartbeat_maker_handles_concurrent_heartbeats(
    tmp_path: "Path",
) -> "None":
    """Many concurrent heartbeats over the dedicated maker must not deadlock."""
    queue_path = tmp_path / "queue.db"
    main_config = _sqlite_config(queue_path)
    await create_tables(main_config, HeartbeatQueueTask)
    heartbeat_config = _sqlite_config(queue_path)
    heartbeat_maker = async_sessionmaker(heartbeat_config.get_engine(), expire_on_commit=False)

    backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(
            sqlalchemy_config=main_config, model_class=HeartbeatQueueTask, heartbeat_session_maker=heartbeat_maker
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
            asyncio.gather(
                *(
                    backend.touch_heartbeats([
                        HeartbeatTouch(task_id=record.id, expected_retry_count=record.retry_count)
                    ])
                    for _ in range(4)
                    for record in claimed
                )
            ),
            timeout=10.0,
        )
        for record in claimed:
            stored = await backend.get_task(record.id)
            assert stored is not None
            assert stored.heartbeat_at is not None
    finally:
        await backend.close()
        await heartbeat_config.get_engine().dispose()


async def test_advanced_alchemy_backend_touch_heartbeats_groups_updates_in_one_session(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """A multi-record heartbeat tick uses one heartbeat session and one grouped update."""
    queue_path = tmp_path / "queue.db"
    main_config = _sqlite_config(queue_path)
    await create_tables(main_config, HeartbeatQueueTask)
    heartbeat_config = _sqlite_config(queue_path)
    heartbeat_maker = async_sessionmaker(heartbeat_config.get_engine(), expire_on_commit=False)

    backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(
            sqlalchemy_config=main_config, model_class=HeartbeatQueueTask, heartbeat_session_maker=heartbeat_maker
        )
    )
    await backend.open()
    try:
        records = [
            await backend.enqueue(f"tasks.grouped.{index}", metadata={"index": index, "keep": True})
            for index in range(3)
        ]
        claimed = []
        for record in records:
            c = await backend.claim_task(record.id)
            assert c is not None
            claimed.append(c)

        original_heartbeat = SQLAlchemyBackend._heartbeat_operation
        heartbeat_calls = 0

        @contextlib.asynccontextmanager
        async def counting_heartbeat(self: "SQLAlchemyBackend") -> 'AsyncIterator["QueueTaskService"]':
            nonlocal heartbeat_calls
            heartbeat_calls += 1
            async with original_heartbeat(self) as service:
                yield service

        original_execute = AsyncSession.execute
        select_calls = 0
        update_calls = 0

        async def counting_execute(self: "AsyncSession", statement: "Any", *args: "Any", **kwargs: "Any") -> "Any":
            nonlocal select_calls, update_calls
            statement_type = type(statement).__name__
            if statement_type == "Select":
                select_calls += 1
            elif statement_type == "Update":
                update_calls += 1
            return await original_execute(self, statement, *args, **kwargs)

        monkeypatch.setattr(SQLAlchemyBackend, "_heartbeat_operation", counting_heartbeat)
        monkeypatch.setattr(AsyncSession, "execute", counting_execute)

        result = await backend.touch_heartbeats([
            HeartbeatTouch(
                task_id=record.id,
                expected_retry_count=record.retry_count,
                metadata_patch={"progress_detail": f"step-{index}"},
            )
            for index, record in enumerate(claimed)
        ])

        assert result.touched_task_ids == {record.id for record in claimed}
        assert result.missed_task_ids == set()
        assert heartbeat_calls == 1
        assert select_calls == 1
        assert update_calls == 1
        for index, record in enumerate(claimed):
            stored = await backend.get_task(record.id)
            assert stored is not None
            assert stored.heartbeat_at is not None
            assert stored.metadata["keep"] is True
            assert stored.metadata["progress_detail"] == f"step-{index}"
    finally:
        await backend.close()
        await heartbeat_config.get_engine().dispose()

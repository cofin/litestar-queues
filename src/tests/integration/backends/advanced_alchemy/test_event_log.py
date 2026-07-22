"""Advanced Alchemy backend-managed queue event history tests."""

import pytest

pytest.importorskip("advanced_alchemy")
pytest.importorskip("aiosqlite")
pytest.importorskip("sqlalchemy")

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from advanced_alchemy.base import UUIDAuditBase
from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig

from litestar_queues import EventLogConfig, QueueConfig, QueueService, task
from litestar_queues.backends.advanced_alchemy import SQLAlchemyBackendConfig
from litestar_queues.backends.advanced_alchemy.mixins import QueueEventLogModelMixin, QueueTaskModelMixin
from litestar_queues.events import publish_task_event, publish_task_log, publish_task_progress
from litestar_queues.task import clear_task_registry
from tests.integration.backends.advanced_alchemy._aa_schema import create_tables

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.anyio


class AAEventQueueTask(UUIDAuditBase, QueueTaskModelMixin):
    __tablename__ = "aa_event_queue_task"


class AAEventQueueEvent(UUIDAuditBase, QueueEventLogModelMixin):
    __tablename__ = "aa_event_queue_task_event_log"


async def test_advanced_alchemy_event_log_records_queries_and_cleans_up(tmp_path: "Path") -> "None":
    clear_task_registry()

    @task("tasks.aa_event_history")
    async def aa_event_history_task() -> "str":
        await publish_task_log("loaded", payload={"stage": "load", "duration_ms": 7, "items": 3})
        await publish_task_progress(current=2, total=4, payload={"stage": "load", "duration_ms": 5})
        await publish_task_event("task.event", message="stored", payload={"stage": "store", "duration_ms": 11})
        return "ok"

    db_path = tmp_path / "aa-event-history.db"
    sqlalchemy_config = _sqlite_config(db_path)
    await create_tables(sqlalchemy_config, AAEventQueueTask, AAEventQueueEvent)
    event_log_config = EventLogConfig(buffer_size=100, flush_interval=60)
    backend_config = SQLAlchemyBackendConfig(
        sqlalchemy_config=sqlalchemy_config, model_class=AAEventQueueTask, event_log_model_class=AAEventQueueEvent
    )
    config = QueueConfig(queue_backend=backend_config, execution_backend="immediate", event_log=event_log_config)

    async with QueueService(config) as service:
        result = await service.enqueue(aa_event_history_task)

    reader_config = QueueConfig(
        queue_backend=SQLAlchemyBackendConfig(
            sqlalchemy_config=_sqlite_config(db_path),
            model_class=AAEventQueueTask,
            event_log_model_class=AAEventQueueEvent,
        ),
        event_log=event_log_config,
    )
    async with QueueService(reader_config) as reader:
        event_log = reader.get_queue_backend().get_event_log(event_log_config)
        assert event_log is not None

        records = await event_log.list_events(task_id=str(result.id))
        task_name_records = await event_log.list_events(task_name=aa_event_history_task.name, limit=2)
        cutoff = datetime.now(timezone.utc) + timedelta(seconds=1)
        first_deleted = await event_log.cleanup_before(cutoff, limit=2)
        after_first = await event_log.list_events(task_id=str(result.id))
        second_deleted = await event_log.cleanup_before(cutoff, limit=2)
        after_second = await event_log.list_events(task_id=str(result.id))
        third_deleted = await event_log.cleanup_before(cutoff, limit=2)
        after_third = await event_log.list_events(task_id=str(result.id))
        final_deleted = await event_log.cleanup_before(cutoff, limit=2)
        after_final = await event_log.list_events(task_id=str(result.id))

    assert [record.event_type for record in records] == [
        "task.started",
        "task.log",
        "task.progress",
        "task.event",
        "task.completed",
    ]
    assert [record.sequence for record in task_name_records] == [1, 2]
    custom = next(record for record in records if record.event_type == "task.event")
    assert custom.detail == {"stage": "store", "duration_ms": 11}
    assert custom.stage == "store"
    assert custom.duration_ms == 11
    assert first_deleted == 2
    assert [record.event_id for record in after_first] == [record.event_id for record in records[2:]]
    assert second_deleted == 2
    assert [record.event_id for record in after_second] == [record.event_id for record in records[4:]]
    assert third_deleted == 1
    assert after_third == []
    assert (final_deleted, after_final) == (0, [])


def _sqlite_config(path: "Path") -> "SQLAlchemyAsyncConfig":
    return SQLAlchemyAsyncConfig(connection_string=f"sqlite+aiosqlite:///{path}")

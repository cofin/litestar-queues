"""SQLSpec backend-managed queue event history tests."""

import importlib
import sqlite3
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

pytest.importorskip("aiosqlite")
pytest.importorskip("sqlspec")

from litestar_queues import InMemoryQueueEventSink, QueueConfig, QueueEventConfig, QueueEventLogConfig, QueueService, task
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.events import publish_task_log, publish_task_progress
from litestar_queues.task import clear_task_registry

if TYPE_CHECKING:
    from pathlib import Path

    from tests.integration.backends.sqlspec.conftest import SqliteConfigFactory

pytestmark = pytest.mark.anyio


async def test_sqlspec_event_log_records_and_queries_task_history(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    """SQLSpec event history persists buffered task events through service shutdown."""
    clear_task_registry()

    @task("tasks.sqlspec_event_history")
    async def event_history_task() -> "str":
        await publish_task_log("loaded", payload={"stage": "load", "duration_ms": 7, "items": 3})
        await publish_task_progress(current=2, total=4, payload={"stage": "load", "duration_ms": 5})
        await publish_task_log("stored", payload={"stage": "store", "duration_ms": 11})
        return "ok"

    db_path = tmp_path / "event-history.db"
    live_sink = InMemoryQueueEventSink()
    event_log_config = QueueEventLogConfig(enabled=True, buffer_size=100, flush_interval=60)
    config = QueueConfig(
        queue_backend=SQLSpecBackendConfig(config=sqlite_config_factory(db_path)),
        execution_backend="immediate",
        event_config=QueueEventConfig(enabled=True, sink=live_sink),
        event_log_config=event_log_config,
    )

    async with QueueService(config) as service:
        result = await service.enqueue(event_history_task)

    reader_config = QueueConfig(
        queue_backend=SQLSpecBackendConfig(config=sqlite_config_factory(db_path)),
        event_log_config=event_log_config,
    )
    async with QueueService(reader_config) as reader:
        event_log = reader.get_queue_backend().get_event_log(event_log_config)
        assert event_log is not None

        records = await event_log.list_events(task_id=str(result.id))
        task_name_records = await event_log.list_events(task_name=event_history_task.name, limit=2)
        summaries = await event_log.summarize_stages(task_name=event_history_task.name)
        deleted = await event_log.cleanup_before(datetime.now(timezone.utc) + timedelta(seconds=1))
        remaining = await event_log.list_events(task_id=str(result.id))

    assert [record.event_type for record in records] == [
        "task.started",
        "task.log",
        "task.progress",
        "task.log",
        "task.completed",
    ]
    assert [event.type for event in live_sink.events] == [record.event_type for record in records]
    assert [record.sequence for record in task_name_records] == [1, 2]

    load_record = next(record for record in records if record.event_type == "task.progress")
    assert load_record.detail["stage"] == "load"
    assert load_record.progress_current == 2
    assert load_record.progress_total == 4
    assert load_record.progress_percent == 50.0

    stage_summaries = {summary.stage: summary for summary in summaries}
    assert stage_summaries["load"].event_count == 2
    assert stage_summaries["load"].total_duration_ms == 12
    assert stage_summaries["store"].event_count == 1
    assert stage_summaries["store"].total_duration_ms == 11

    assert deleted == len(records)
    assert remaining == []


async def test_sqlspec_event_log_table_follows_event_log_enabled_lifecycle(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    """SQLSpec only creates the durable event table when event history is enabled."""
    disabled_db_path = tmp_path / "event-log-disabled.db"
    async with QueueService(
        QueueConfig(queue_backend=SQLSpecBackendConfig(config=sqlite_config_factory(disabled_db_path)))
    ):
        pass

    assert "litestar_queue_task_event_log" not in _sqlite_table_names(disabled_db_path)

    enabled_db_path = tmp_path / "event-log-enabled.db"
    async with QueueService(
        QueueConfig(
            queue_backend=SQLSpecBackendConfig(config=sqlite_config_factory(enabled_db_path)),
            event_log_config=QueueEventLogConfig(enabled=True),
        )
    ):
        pass

    assert "litestar_queue_task_event_log" in _sqlite_table_names(enabled_db_path)


async def test_sqlspec_event_log_table_name_follows_queue_table_name(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    """SQLSpec derives the default event-log table from the resolved queue table."""
    derived_db_path = tmp_path / "event-log-derived.db"
    async with QueueService(
        QueueConfig(
            queue_backend=SQLSpecBackendConfig(
                config=sqlite_config_factory(derived_db_path), table_name="custom_queue_task"
            ),
            event_log_config=QueueEventLogConfig(enabled=True),
        )
    ):
        pass

    derived_tables = _sqlite_table_names(derived_db_path)
    assert "custom_queue_task_event_log" in derived_tables
    assert "litestar_queue_task_event_log" not in derived_tables

    explicit_db_path = tmp_path / "event-log-explicit.db"
    async with QueueService(
        QueueConfig(
            queue_backend=SQLSpecBackendConfig(
                config=sqlite_config_factory(explicit_db_path),
                table_name="explicit_queue_task",
                event_log_table_name="queue_events",
            ),
            event_log_config=QueueEventLogConfig(enabled=True),
        )
    ):
        pass

    explicit_tables = _sqlite_table_names(explicit_db_path)
    assert "queue_events" in explicit_tables
    assert "explicit_queue_task_event_log" not in explicit_tables


async def test_sqlspec_event_log_migration_down_drops_event_table() -> "None":
    """The packaged event-log migration can remove the managed history table."""
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0002_create_queue_event_log")
    context = SimpleNamespace(
        config=AiosqliteConfig(extension_config={QUEUE_EXTENSION_NAME: {"event_log_enabled": True}})
    )

    down_statements = await migration.down(context)

    assert any("DROP TABLE" in statement and "litestar_queue_task_event_log" in statement for statement in down_statements)


def _sqlite_table_names(db_path: "Path") -> "set[str]":
    with sqlite3.connect(db_path) as connection:
        rows = connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        return {cast("str", row[0]) for row in rows}

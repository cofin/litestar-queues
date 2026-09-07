"""SQLSpec backend-managed queue event history tests."""

import asyncio
import contextlib
import importlib
import sqlite3
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

pytest.importorskip("aiosqlite")
pytest.importorskip("sqlspec")

from litestar_queues import (
    EventDeliveryConfig,
    EventHistoryConfig,
    InMemoryQueueEventSink,
    QueueConfig,
    QueueService,
    WorkerConfig,
    task,
)
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.events import (
    QueueEvent,
    QueueEventActor,
    QueueEventsConfig,
    publish_task_log,
    publish_task_progress,
)
from litestar_queues.events.query import QueueEventQuery
from litestar_queues.task import clear_task_registry
from tests.integration.backends.sqlspec._schema import bootstrap_queue_schema

if TYPE_CHECKING:
    from pathlib import Path

    from tests.integration.backends.sqlspec.conftest import SqliteConfigFactory

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize("adapter", ["aiosqlite", "sqlite"])
async def test_sparse_history_commits_without_another_publication(tmp_path: "Path", adapter: "str") -> "None":
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.adapters.sqlite import SqliteConfig

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    path = tmp_path / f"sparse-{adapter}.db"
    config_class = AiosqliteConfig if adapter == "aiosqlite" else SqliteConfig
    backend_config = SQLSpecBackendConfig(sqlspec_config=config_class(connection_config={"database": str(path)}))
    await bootstrap_queue_schema(backend_config, event_history_enabled=True)
    backend = SQLSpecQueueBackend(backend_config=backend_config)
    history = EventHistoryConfig(batch_size=20, flush_interval=0.02, strict=True)
    try:
        await backend.open()
        log = cast("Any", backend.get_event_log(history))
        event = QueueEvent(type="task.log", scope="task", message="sparse")
        await log.publish_event(event)
        deadline = asyncio.get_running_loop().time() + 1
        while True:
            with sqlite3.connect(path) as reader:
                count = reader.execute(
                    "SELECT COUNT(*) FROM queue_task_event_history WHERE event_id = ?", (event.id,)
                ).fetchone()[0]
            if count == 1 or asyncio.get_running_loop().time() >= deadline:
                break
            await asyncio.sleep(0.01)
        assert count == 1
    finally:
        await backend.close()


async def test_service_close_waits_for_history_flush_and_reopens_fresh(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch"
) -> None:
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    from litestar_queues.backends.sqlspec.event_log import SQLSpecQueueEventLog

    path = tmp_path / "service-history-lifecycle.db"
    backend_config = SQLSpecBackendConfig(sqlspec_config=AiosqliteConfig(connection_config={"database": str(path)}))
    await bootstrap_queue_schema(backend_config, event_history_enabled=True)
    service = QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend=backend_config,
            events=QueueEventsConfig(history=EventHistoryConfig(batch_size=20, flush_interval=60, strict=True)),
        )
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    write = SQLSpecQueueEventLog._write_transaction

    async def blocked_write(log: SQLSpecQueueEventLog, records: Any) -> None:
        entered.set()
        await release.wait()
        await write(log, records)

    monkeypatch.setattr(SQLSpecQueueEventLog, "_write_transaction", blocked_write)
    await service.open()
    first = service.get_event_log()
    assert first is not None
    await service.get_event_publisher().publish(QueueEvent(type="task.log", scope="task"))
    flushing = asyncio.create_task(first.flush_events())
    closing: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(entered.wait(), timeout=2)
        closing = asyncio.create_task(service.close())
        await asyncio.sleep(0)
        assert not closing.done()
    finally:
        release.set()
        await flushing
        if closing is not None:
            await closing
        else:
            await service.close()
    assert service.get_event_log() is None
    await service.open()
    fresh = service.get_event_log()
    assert fresh is not None and fresh is not first
    try:
        await service.get_event_publisher().publish(QueueEvent(type="task.log", scope="task"))
    finally:
        await service.close()
    with sqlite3.connect(path) as reader:
        assert reader.execute("SELECT COUNT(*) FROM queue_task_event_history").fetchone()[0] == 2


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
    event_log_config = EventHistoryConfig(batch_size=100, flush_interval=60)
    backend_config = SQLSpecBackendConfig(sqlspec_config=sqlite_config_factory(db_path))
    await bootstrap_queue_schema(backend_config, event_history_enabled=True)
    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend=backend_config,
        execution_backend="immediate",
        events=QueueEventsConfig(delivery=EventDeliveryConfig(sinks=(live_sink,)), history=event_log_config),
    )

    async with QueueService(config) as service:
        result = await service.enqueue(event_history_task)

    reader_config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend=SQLSpecBackendConfig(sqlspec_config=sqlite_config_factory(db_path)),
        events=QueueEventsConfig(history=event_log_config),
    )
    async with QueueService(reader_config) as reader:
        event_log = reader.get_queue_backend().get_event_log(event_log_config)
        assert event_log is not None

        records = (await event_log.query_events(QueueEventQuery(task_id=str(result.id)))).items
        task_name_records = (
            await event_log.query_events(QueueEventQuery(task_name=event_history_task.name, limit=2))
        ).items
        summaries = await event_log.summarize_stages(QueueEventQuery(task_name=event_history_task.name))
        cutoff = datetime.now(timezone.utc) + timedelta(seconds=1)
        first_deleted = await event_log.cleanup_events(before=cutoff, limit=2)
        after_first = (await event_log.query_events(QueueEventQuery(task_id=str(result.id)))).items
        second_deleted = await event_log.cleanup_events(before=cutoff, limit=2)
        after_second = (await event_log.query_events(QueueEventQuery(task_id=str(result.id)))).items
        third_deleted = await event_log.cleanup_events(before=cutoff, limit=2)
        after_third = (await event_log.query_events(QueueEventQuery(task_id=str(result.id)))).items
        final_deleted = await event_log.cleanup_events(before=cutoff, limit=2)
        after_final = (await event_log.query_events(QueueEventQuery(task_id=str(result.id)))).items

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

    assert first_deleted == 2
    assert [record.event_id for record in after_first] == [record.event_id for record in records[2:]]
    assert second_deleted == 2
    assert [record.event_id for record in after_second] == [record.event_id for record in records[4:]]
    assert third_deleted == 1
    assert after_third == []
    assert (final_deleted, after_final) == (0, [])


async def test_sqlspec_event_log_persists_and_filters_the_actor(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    """The actor type and id are stored columns, so history can be filtered by who acted."""
    db_path = tmp_path / "event-actor.db"
    event_log_config = EventHistoryConfig(batch_size=100, flush_interval=60)
    backend_config = SQLSpecBackendConfig(sqlspec_config=sqlite_config_factory(db_path))
    await bootstrap_queue_schema(backend_config, event_history_enabled=True)
    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend=backend_config,
        events=QueueEventsConfig(history=event_log_config),
    )

    async with QueueService(config) as service:
        event_log = service.get_queue_backend().get_event_log(event_log_config)
        assert event_log is not None
        for index, actor in enumerate((
            QueueEventActor(type="user", id="u-1", name="Alice"),
            QueueEventActor(type="service", id="svc-1"),
            None,
        )):
            await event_log.publish_event(
                QueueEvent(
                    type="task.log",
                    scope="task",
                    task_id=f"task-{index}",
                    task_name="tasks.actor",
                    sequence=index,
                    actor=actor,
                    payload={"index": index},
                )
            )

        recorded = (await event_log.query_events(QueueEventQuery(task_name="tasks.actor"))).items
        by_actor_id = (await event_log.query_events(QueueEventQuery(), extra={"actor_id": "u-1"})).items
        by_actor_type = (await event_log.query_events(QueueEventQuery(), extra={"actor_type": "service"})).items
        by_both = (
            await event_log.query_events(QueueEventQuery(), extra={"actor_id": "u-1", "actor_type": "service"})
        ).items

    assert [(record.actor_type, record.actor_id) for record in recorded] == [
        ("user", "u-1"),
        ("service", "svc-1"),
        (None, None),
    ]
    assert [record.detail["index"] for record in by_actor_id] == [0]
    assert [record.detail["index"] for record in by_actor_type] == [1]
    assert by_both == []


async def test_sqlspec_event_history_table_follows_event_history_enabled_lifecycle(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    """SQLSpec only creates the durable event table when event history is enabled."""
    disabled_db_path = tmp_path / "event-log-disabled.db"
    disabled_backend_config = SQLSpecBackendConfig(sqlspec_config=sqlite_config_factory(disabled_db_path))
    await bootstrap_queue_schema(disabled_backend_config)
    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend=disabled_backend_config)
    ):
        pass

    assert "queue_task_event_history" not in _sqlite_table_names(disabled_db_path)

    enabled_db_path = tmp_path / "event-log-enabled.db"
    enabled_backend_config = SQLSpecBackendConfig(sqlspec_config=sqlite_config_factory(enabled_db_path))
    await bootstrap_queue_schema(enabled_backend_config, event_history_enabled=True)
    async with QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend=enabled_backend_config,
            events=QueueEventsConfig(history=EventHistoryConfig()),
        )
    ):
        pass

    assert "queue_task_event_history" in _sqlite_table_names(enabled_db_path)


async def test_sqlspec_event_history_table_name_follows_queue_table_name(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    """SQLSpec derives the default event-log table from the resolved queue table."""
    derived_db_path = tmp_path / "event-log-derived.db"
    derived_backend_config = SQLSpecBackendConfig(
        sqlspec_config=sqlite_config_factory(derived_db_path), queue_table_name="custom_queue_task"
    )
    await bootstrap_queue_schema(derived_backend_config, event_history_enabled=True)
    async with QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend=derived_backend_config,
            events=QueueEventsConfig(history=EventHistoryConfig()),
        )
    ):
        pass

    derived_tables = _sqlite_table_names(derived_db_path)
    assert "custom_queue_task_event_history" in derived_tables
    assert "queue_task_event_history" not in derived_tables

    explicit_db_path = tmp_path / "event-log-explicit.db"
    explicit_backend_config = SQLSpecBackendConfig(
        sqlspec_config=sqlite_config_factory(explicit_db_path),
        queue_table_name="explicit_queue_task",
        event_history_table_name="queue_events",
    )
    await bootstrap_queue_schema(explicit_backend_config, event_history_enabled=True)
    async with QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend=explicit_backend_config,
            events=QueueEventsConfig(history=EventHistoryConfig()),
        )
    ):
        pass

    explicit_tables = _sqlite_table_names(explicit_db_path)
    assert "queue_events" in explicit_tables
    assert "explicit_queue_task_event_history" not in explicit_tables


async def test_sqlspec_event_log_migration_down_drops_event_table() -> "None":
    """The packaged migration can remove the managed history table."""
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    context = SimpleNamespace(
        config=AiosqliteConfig(extension_config={QUEUE_EXTENSION_NAME: {"event_history_enabled": True}})
    )

    down_statements = await migration.down(context)

    assert any("DROP TABLE" in statement and "queue_task_event_history" in statement for statement in down_statements)


def _sqlite_table_names(db_path: "Path") -> "set[str]":
    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        rows = connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        return {cast("str", row[0]) for row in rows}


@pytest.fixture(params=["aiosqlite", "sqlite", "postgres", "mysql"])
async def replay_backend(request: "pytest.FixtureRequest", tmp_path: "Path") -> "Any":
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.adapters.asyncmy import AsyncmyConfig
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig
    from sqlspec.adapters.sqlite import SqliteConfig

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend
    from tests.integration._names import table_name_for_test

    adapter = request.param
    adapter_config: Any
    if adapter == "postgres":
        service = request.getfixturevalue("postgres_service")
        adapter_config = PsycopgAsyncConfig(
            connection_config={
                "host": service.host,
                "port": service.port,
                "user": service.user,
                "password": service.password,
                "dbname": service.database,
            }
        )
    elif adapter == "mysql":
        service = request.getfixturevalue("mysql_84_service")
        adapter_config = AsyncmyConfig(
            connection_config={
                "host": service.host,
                "port": service.port,
                "user": service.user,
                "password": service.password,
                "db": service.db,
            }
        )
    else:
        config_class = AiosqliteConfig if adapter == "aiosqlite" else SqliteConfig
        adapter_config = config_class(connection_config={"database": str(tmp_path / "replay.db")})
    backend_config = SQLSpecBackendConfig(
        sqlspec_config=adapter_config, queue_table_name=table_name_for_test("history", adapter, request.node.nodeid)
    )
    config = QueueConfig(queue_backend=backend_config, events=QueueEventsConfig(history=EventHistoryConfig()))
    backend = SQLSpecQueueBackend(config=config, backend_config=backend_config)
    await backend.open()
    await backend.create_schema()
    try:
        yield backend
    finally:
        await backend.close()


async def test_history_replays_actual_commit_and_releases_to_independent_reader(
    replay_backend: "Any", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    backend = replay_backend
    log = backend.get_event_log(EventHistoryConfig(batch_size=1, flush_interval=60, strict=True))
    event = QueueEvent(
        type="task.log",
        scope="task",
        occurred_at=datetime(2026, 1, 2, 3, 4, 5, 987654, tzinfo=timezone.utc),
        actor=QueueEventActor(type="user", id="same"),
        progress_current=0.123456789,
        payload={"nested": {"value": 1}},
    )
    factory = log._session_factory
    commit_calls = 0

    class UncertainCommit:
        def __init__(self, driver: "Any") -> "None":
            self.driver = driver

        def __getattr__(self, name: "str") -> "Any":
            return getattr(self.driver, name)

        async def commit(self) -> "None":
            nonlocal commit_calls
            await self.driver.commit()
            commit_calls += 1
            if commit_calls == 1:
                message = "lost acknowledgement after commit"
                raise ConnectionError(message)

    @contextlib.asynccontextmanager
    async def uncertain_session() -> "Any":
        async with factory() as driver:
            yield UncertainCommit(driver)

    monkeypatch.setattr(log, "_session_factory", uncertain_session)
    visible = []

    async def release() -> "None":
        async with factory() as reader:
            rows = await reader.select(log._store.select_existing_event_ids([event.id]))
        visible.append(rows)

    with pytest.raises(ConnectionError, match="lost acknowledgement"):
        await log.publish_event_after_commit(event, release=release, barrier=True)
    assert visible == []
    async with factory() as reader:
        first = await reader.select(log._store.select_existing_event_ids([event.id]))
    assert len(first) == 1
    await log.flush_events()
    assert len(visible) == 1 and len(visible[0]) == 1
    assert visible[0][0]["created_at"] == first[0]["created_at"]
    await log.publish_event(event)
    async with factory() as reader:
        final = await reader.select(log._store.select_existing_event_ids([event.id]))
    assert len(final) == 1 and final[0]["created_at"] == first[0]["created_at"]


async def test_conflicting_history_id_is_fatal_without_live_release(replay_backend: "Any") -> "None":
    from msgspec.structs import replace

    from litestar_queues.exceptions import QueueConfigurationError

    log = replay_backend.get_event_log(EventHistoryConfig(batch_size=1, strict=True))
    event = QueueEvent(type="task.log", scope="task", payload={"value": "first"})
    await log.publish_event(event)
    released = []

    async def release() -> "None":
        released.append(True)

    with pytest.raises(QueueConfigurationError, match="Conflicting immutable"):
        await log.publish_event_after_commit(replace(event, payload={"value": "second"}), release=release, barrier=True)
    assert released == []
    with pytest.raises(QueueConfigurationError):
        await log.aclose()
    async with replay_backend._session() as reader:
        rows = await reader.select(log._store.select_existing_event_ids([event.id]))
    assert log._record_from_row(rows[0]).detail == {"value": "first"}


async def test_history_close_failure_reopens_with_a_distinct_log(
    replay_backend: "Any", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    from litestar_queues.exceptions import QueueConfigurationError

    backend = replay_backend
    history = EventHistoryConfig(batch_size=20, flush_interval=60, strict=True)
    old_log = backend.get_event_log(history)
    await old_log.publish_event(QueueEvent(type="task.log", scope="task"))

    async def fail_write(records: "Any") -> "None":
        message = "history unavailable"
        raise ConnectionError(message)

    monkeypatch.setattr(old_log._buffer, "_write_batch", fail_write)
    with pytest.raises(ConnectionError, match="history unavailable"):
        await backend.close()
    await backend.open()
    fresh_log = backend.get_event_log(history)
    assert fresh_log is not old_log
    with pytest.raises(QueueConfigurationError, match="closing or closed"):
        await old_log.publish_event(QueueEvent(type="task.log", scope="task"))
    event = QueueEvent(type="task.log", scope="task", message="reopened")
    await fresh_log.publish_event(event)
    await fresh_log.flush_events()
    async with backend._session() as reader:
        rows = await reader.select(fresh_log._store.select_existing_event_ids([event.id]))
    assert len(rows) == 1


async def test_duplicate_insert_race_retries_in_a_fresh_transaction(
    replay_backend: "Any", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    log = replay_backend.get_event_log(EventHistoryConfig(batch_size=1, flush_interval=60, strict=True))
    event = QueueEvent(type="task.log", scope="task", payload={"preserved": True})
    await log.publish_event(event)
    factory = log._session_factory
    sessions = 0
    insert_errors = []

    class RaceDriver:
        def __init__(self, driver: "Any", first: "bool") -> "None":
            self.driver = driver
            self.first = first

        def __getattr__(self, name: "str") -> "Any":
            return getattr(self.driver, name)

        async def select(self, statement: "Any", *args: "Any", **kwargs: "Any") -> "Any":
            # A row committed after the existence snapshot causes an actual
            # native duplicate-key error below; only the first lookup is stale.
            if self.first:
                self.first = False
                return []
            return await self.driver.select(statement, *args, **kwargs)

        async def execute_many(self, statement: "Any", params: "Any") -> "Any":
            try:
                return await self.driver.execute_many(statement, params)
            except Exception as exc:
                insert_errors.append(exc)
                raise

    @contextlib.asynccontextmanager
    async def raced_session() -> "Any":
        nonlocal sessions
        sessions += 1
        async with factory() as driver:
            yield RaceDriver(driver, sessions == 1)

    monkeypatch.setattr(log, "_session_factory", raced_session)
    await log.publish_event(event)
    assert sessions == 2
    assert len(insert_errors) == 1
    async with factory() as reader:
        rows = await reader.select(log._store.select_existing_event_ids([event.id]))
    assert len(rows) == 1

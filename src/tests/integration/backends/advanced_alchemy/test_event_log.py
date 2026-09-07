"""Advanced Alchemy backend-managed queue event history tests."""

import pytest

pytest.importorskip("advanced_alchemy")
pytest.importorskip("aiosqlite")
pytest.importorskip("sqlalchemy")

import asyncio
import sqlite3
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

from advanced_alchemy.base import UUIDAuditBase
from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig

from litestar_queues import EventHistoryConfig, QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends.advanced_alchemy import SQLAlchemyBackendConfig
from litestar_queues.backends.advanced_alchemy.mixins import QueueEventHistoryModelMixin, QueueTaskModelMixin
from litestar_queues.events import (
    QueueEvent,
    QueueEventsConfig,
    publish_task_event,
    publish_task_log,
    publish_task_progress,
)
from litestar_queues.events.query import QueueEventQuery
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.task import clear_task_registry
from tests.integration.backends.advanced_alchemy._aa_schema import create_tables

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence
    from pathlib import Path

    from litestar_queues.backends.advanced_alchemy import SQLAlchemyBackend
    from litestar_queues.events import QueueEventLogRecord
    from tests.integration.backends.advanced_alchemy._aa_schema import MappedModel

pytestmark = pytest.mark.anyio


class AAEventQueueTask(UUIDAuditBase, QueueTaskModelMixin):
    __tablename__ = "aa_event_queue_task"


class AAEventQueueEvent(UUIDAuditBase, QueueEventHistoryModelMixin):
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
    event_log_config = EventHistoryConfig(batch_size=100, flush_interval=60)
    backend_config = SQLAlchemyBackendConfig(
        sqlalchemy_config=sqlalchemy_config, model_class=AAEventQueueTask, event_history_model_class=AAEventQueueEvent
    )
    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend=backend_config,
        execution_backend="immediate",
        events=QueueEventsConfig(history=event_log_config),
    )

    async with QueueService(config) as service:
        result = await service.enqueue(aa_event_history_task)

    reader_config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend=SQLAlchemyBackendConfig(
            sqlalchemy_config=_sqlite_config(db_path),
            model_class=AAEventQueueTask,
            event_history_model_class=AAEventQueueEvent,
        ),
        events=QueueEventsConfig(history=event_log_config),
    )
    async with QueueService(reader_config) as reader:
        event_log = reader.get_queue_backend().get_event_log(event_log_config)
        assert event_log is not None

        records = (await event_log.query_events(QueueEventQuery(task_id=str(result.id)))).items
        task_name_records = (
            await event_log.query_events(QueueEventQuery(task_name=aa_event_history_task.name, limit=2))
        ).items
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


@pytest.fixture
async def aa_history_backend(
    advanced_alchemy_backend: "SQLAlchemyBackend", request: "pytest.FixtureRequest"
) -> "AsyncIterator[SQLAlchemyBackend]":
    from sqlalchemy import DateTime
    from sqlalchemy.dialects import mysql, postgresql
    from sqlalchemy.orm import mapped_column

    from litestar_queues.backends.advanced_alchemy import SQLAlchemyBackend
    from tests.integration._names import table_name_for_test

    config = advanced_alchemy_backend._sqlalchemy_config
    assert config is not None
    model_fields: dict[str, Any] = {"__tablename__": table_name_for_test("aa_history", "replay", request.node.nodeid)}
    if getattr(request, "param", None) is not None:
        model_fields["occurred_at"] = mapped_column(
            DateTime(timezone=True)
            .with_variant(postgresql.TIMESTAMP(timezone=True, precision=request.param), "postgresql")
            .with_variant(mysql.DATETIME(fsp=request.param), "mysql", "mariadb"),
            nullable=False,
        )
    model = type(f"History_{request.node.name}", (UUIDAuditBase, QueueEventHistoryModelMixin), model_fields)
    await create_tables(cast("SQLAlchemyAsyncConfig", config), model)
    backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(sqlalchemy_config=config, event_history_model_class=model)
    )
    await backend.open()
    try:
        yield backend
    finally:
        with suppress(Exception):
            await backend.close()
        async with config.get_engine().begin() as connection:
            await connection.run_sync(cast("MappedModel", model).__table__.drop)


async def test_aa_history_sparse_commit_before_release(aa_history_backend: "SQLAlchemyBackend") -> None:
    backend = aa_history_backend
    log = backend.get_event_log(EventHistoryConfig(batch_size=20, flush_interval=0.02, strict=True))
    event = QueueEvent(type="task.log", scope="task", message="sparse")
    released = asyncio.Event()

    async def release() -> None:
        async with backend._event_log_service() as service:
            total, rows = await service.query_events(QueueEventQuery())
        assert total == 1
        assert rows[0].event_id == event.id
        released.set()

    await log.publish_event_after_commit(event, release=release)
    await asyncio.wait_for(released.wait(), timeout=5)


async def test_aa_history_replay_round_trip_and_conflict(aa_history_backend: "SQLAlchemyBackend") -> None:
    backend = aa_history_backend
    log = backend.get_event_log(EventHistoryConfig(batch_size=20, strict=True))
    event = QueueEvent(
        type="task.log",
        scope="task",
        message="original",
        progress_current=0.123456789,
        progress_total=123.456789,
        progress_percent=12.3456789,
        occurred_at=datetime(2026, 1, 2, 3, 4, 5, 900000, tzinfo=timezone.utc),
        payload={"nested": {"value": [1, 2]}, "stage": "load", "duration_ms": 0.123456789},
    )
    await log.publish_event(event)
    await log.flush_events()
    original = (await log.query_events()).items[0]
    await log.publish_event(event)
    await log.flush_events()
    page = await log.query_events()
    assert page.total == 1
    assert page.items[0].created_at == original.created_at
    conflict = QueueEvent(
        type="task.log", scope="task", id=event.id, message="different", occurred_at=event.occurred_at
    )
    delivered = []

    async def release() -> None:
        delivered.append(True)

    with pytest.raises(QueueConfigurationError, match="Conflicting"):
        await log.publish_event_after_commit(conflict, release=release, barrier=True)
    assert not delivered


@pytest.mark.parametrize("aa_history_backend", [3], indirect=True)
async def test_aa_history_replay_custom_timestamp_precision(aa_history_backend: "SQLAlchemyBackend") -> None:
    log = aa_history_backend.get_event_log(EventHistoryConfig(batch_size=20, strict=True))
    event = QueueEvent(
        type="task.log", scope="task", occurred_at=datetime(2026, 1, 2, 3, 4, 5, 900501, tzinfo=timezone.utc)
    )
    await log.publish_event(event)
    await log.flush_events()
    original = (await log.query_events()).items[0]
    await log.publish_event(event)
    await log.flush_events()
    page = await log.query_events()
    assert page.total == 1
    assert page.items[0].occurred_at == original.occurred_at
    assert page.items[0].created_at == original.created_at


async def test_aa_history_uncertain_commit_retains_identity(aa_history_backend: "SQLAlchemyBackend") -> None:
    from litestar_queues.backends.advanced_alchemy.event_log import AdvancedAlchemyQueueEventLog

    backend = aa_history_backend
    attempts = 0

    @asynccontextmanager
    async def transaction() -> "AsyncIterator[Any]":
        nonlocal attempts
        async with backend._event_log_operation() as service:
            yield service
        attempts += 1
        if attempts == 1:
            message = "acknowledgement lost"
            raise RuntimeError(message)

    log = AdvancedAlchemyQueueEventLog(
        EventHistoryConfig(batch_size=20, strict=True),
        service_factory=backend._event_log_service,
        transaction_factory=transaction,
    )
    event = QueueEvent(
        type="task.log", scope="task", occurred_at=datetime(2026, 1, 2, 3, 4, 5, 900000, tzinfo=timezone.utc)
    )
    released = []

    async def release() -> None:
        released.append(True)

    try:
        with pytest.raises(RuntimeError, match="acknowledgement lost"):
            await log.publish_event_after_commit(event, release=release, barrier=True)
        assert not released
        await log.flush_events()
        assert released == [True]
        async with backend._event_log_service() as service:
            total, rows = await service.query_events(QueueEventQuery())
        assert total == 1
        assert rows[0].event_id == event.id
    finally:
        await log.aclose()


async def test_aa_history_close_failure_detaches_and_reopens(
    aa_history_backend: "SQLAlchemyBackend", monkeypatch: "pytest.MonkeyPatch"
) -> None:
    from litestar_queues.backends.advanced_alchemy.event_log import AdvancedAlchemyQueueEventLog

    backend = aa_history_backend

    @asynccontextmanager
    async def failed_transaction() -> "AsyncIterator[Any]":
        async with backend._event_log_operation() as service:
            yield service
            message = "write unavailable"
            raise RuntimeError(message)

    log = AdvancedAlchemyQueueEventLog(
        EventHistoryConfig(batch_size=20, strict=True),
        service_factory=backend._event_log_service,
        transaction_factory=failed_transaction,
    )
    monkeypatch.setattr(backend, "_event_log", log)
    listener_closed = []

    class Listener:
        async def close(self) -> None:
            listener_closed.append(True)

    monkeypatch.setattr(backend, "_notification_listener", Listener())
    await log.publish_event(QueueEvent(type="task.log", scope="task"))
    with pytest.raises(RuntimeError, match="write unavailable"):
        await backend.close()
    assert backend._event_log is None
    assert backend._notification_listener is None
    assert listener_closed == [True]
    assert not backend._opened
    await backend.open()
    fresh = backend.get_event_log(EventHistoryConfig(strict=True))
    assert fresh is not log
    await fresh.publish_event(QueueEvent(type="task.log", scope="task"))
    await fresh.flush_events()
    with pytest.raises(QueueConfigurationError, match=r"closed|closing"):
        await log.publish_event(QueueEvent(type="task.log", scope="task"))


@pytest.mark.parametrize("translated", [False, True])
async def test_aa_history_retries_duplicate_at_transaction_exit(translated: bool) -> None:
    from advanced_alchemy.exceptions import DuplicateKeyError
    from sqlalchemy.exc import IntegrityError

    from litestar_queues.backends.advanced_alchemy.event_log import AdvancedAlchemyQueueEventLog

    native = _sqlite_integrity_error(unique=True)
    error = DuplicateKeyError("duplicate") if translated else IntegrityError("INSERT", {}, native)
    attempts = []
    releases = []

    class Service:
        async def add_records(self, records: "Sequence[QueueEventLogRecord]") -> None:
            attempts.append(tuple(record.event_id for record in records))

    @asynccontextmanager
    async def transaction() -> "AsyncIterator[Any]":
        yield Service()
        if len(attempts) == 1:
            raise error

    async def release() -> None:
        releases.append(len(attempts))

    log = AdvancedAlchemyQueueEventLog(
        EventHistoryConfig(strict=True), service_factory=transaction, transaction_factory=transaction
    )
    try:
        await log.publish_event_after_commit(QueueEvent(type="task.log", scope="task"), release=release, barrier=True)
        assert len(attempts) == 2
        assert attempts[0] == attempts[1]
        assert releases == [2]
    finally:
        await log.aclose()


async def test_aa_history_does_not_retry_unrelated_integrity_at_exit() -> None:
    from sqlalchemy.exc import IntegrityError

    from litestar_queues.backends.advanced_alchemy.event_log import AdvancedAlchemyQueueEventLog

    error = IntegrityError("INSERT", {}, _sqlite_integrity_error(unique=False))
    attempts = []

    class Service:
        async def add_records(self, records: "Sequence[QueueEventLogRecord]") -> None:
            attempts.append(records)

    @asynccontextmanager
    async def transaction() -> "AsyncIterator[Any]":
        yield Service()
        raise error

    log = AdvancedAlchemyQueueEventLog(
        EventHistoryConfig(strict=True), service_factory=transaction, transaction_factory=transaction
    )
    await log.publish_event(QueueEvent(type="task.log", scope="task"))
    with pytest.raises(IntegrityError) as caught:
        await log.flush_events()
    assert caught.value is error
    assert len(attempts) == 1
    with pytest.raises(IntegrityError):
        await log.aclose()


def _sqlite_integrity_error(*, unique: bool) -> sqlite3.IntegrityError:
    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE events (id INTEGER UNIQUE NOT NULL)")
        connection.execute("INSERT INTO events VALUES (1)")
        try:
            connection.execute("INSERT INTO events VALUES (?)", (1 if unique else None,))
        except sqlite3.IntegrityError as error:
            return error
    message = "Expected native SQLite constraint error"
    raise AssertionError(message)


@pytest.mark.parametrize("cancelled", [False, True])
@pytest.mark.parametrize("suppress_primary", [False, True])
async def test_aa_history_primary_error_survives_transaction_cleanup(cancelled: bool, suppress_primary: bool) -> None:
    from litestar_queues.backends.advanced_alchemy.event_log import AdvancedAlchemyQueueEventLog

    primary = asyncio.CancelledError() if cancelled else RuntimeError("primary write")
    cleanup = RuntimeError("cleanup failure")

    class Service:
        async def add_records(self, records: "Sequence[QueueEventLogRecord]") -> None:
            raise primary

    @asynccontextmanager
    async def transaction() -> "AsyncIterator[Any]":
        try:
            yield Service()
        except BaseException:  # noqa: BLE001 - simulate a context suppressing cancellation and write errors.
            if suppress_primary:
                return
            raise cleanup from None

    log = AdvancedAlchemyQueueEventLog(
        EventHistoryConfig(strict=True), service_factory=transaction, transaction_factory=transaction
    )
    await log.publish_event(QueueEvent(type="task.log", scope="task"))
    with pytest.raises(type(primary)) as caught:
        await log.flush_events()
    assert caught.value is primary
    with suppress(asyncio.CancelledError, RuntimeError):
        await log.aclose()


def test_aa_history_duplicate_classification_rejects_untyped_numeric_errors() -> None:
    from litestar_queues.backends.advanced_alchemy.event_log import _is_duplicate_event_error

    assert not _is_duplicate_event_error(RuntimeError(1062))

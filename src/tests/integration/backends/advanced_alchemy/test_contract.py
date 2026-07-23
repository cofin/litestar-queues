"""Advanced Alchemy queue-backend contract suite.

Smoke tests (registration + import isolation + schema bootstrap config) stay
unparametrized. The 6 behavior tests consume the ``advanced_alchemy_backend``
fixture parametrized over ``AA_ENGINES`` (aiosqlite + Postgres + MySQL +
Oracle async configs) by the subdir conftest.
"""

import asyncio
import sys
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, timezone
from subprocess import run
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import pytest
from sqlalchemy.dialects import mysql, oracle, postgresql, sqlite

pytest.importorskip("advanced_alchemy")
pytest.importorskip("aiosqlite")
pytest.importorskip("sqlalchemy")

from advanced_alchemy.base import UUIDAuditBase
from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig
from advanced_alchemy.operations import MergeStatement

from litestar_queues import HeartbeatTouch, QueueConfig, QueueService, task
from litestar_queues.backends import get_queue_backend_class, list_queue_backends
from litestar_queues.backends.advanced_alchemy import QueueTaskModelMixin, SQLAlchemyBackend, SQLAlchemyBackendConfig
from litestar_queues.models import QueuedTaskRecord, TaskRequest
from litestar_queues.task import clear_task_registry
from tests.integration.backends.advanced_alchemy._aa_schema import create_tables

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from tests.integration._backends import PostgresService

pytestmark = pytest.mark.anyio

# Both async PostgreSQL drivers share the same dedicated LISTEN/NOTIFY listener
# seam and the same durable reconciliation. Notification behavior is proven on
# each against the SAME real PostgreSQL service.
_PG_NOTIFY_DRIVERS = (
    pytest.param(("postgresql+asyncpg", "asyncpg"), id="asyncpg"),
    pytest.param(("postgresql+psycopg", "psycopg"), id="psycopg"),
)


def _pg_notify_config(driver: "str", service: "PostgresService") -> "SQLAlchemyAsyncConfig":
    return SQLAlchemyAsyncConfig(
        connection_string=(
            f"{driver}://{service.user}:{service.password}@{service.host}:{service.port}/{service.database}"
        )
    )


@pytest.fixture(autouse=True)
def clean_task_registry() -> "None":
    clear_task_registry()


class ContractQueueTask(UUIDAuditBase, QueueTaskModelMixin):
    __tablename__ = "aa_contract_queue_task"


async def test_advanced_alchemy_backend_is_registered_without_sqlspec() -> "None":
    assert "advanced-alchemy" in list_queue_backends()
    assert get_queue_backend_class("advanced-alchemy") is SQLAlchemyBackend


def test_top_level_litestar_queues_import_does_not_require_advanced_alchemy() -> "None":
    """Importing ``litestar_queues`` must succeed without advanced_alchemy installed."""
    code = """
import builtins

blocked_prefixes = ("advanced_alchemy", "sqlalchemy")
blocked_package_prefixes = tuple(f"{name}." for name in blocked_prefixes)
original_import = builtins.__import__

def blocked_import(name, *args, **kwargs):
    if name in blocked_prefixes or name.startswith(blocked_package_prefixes):
        raise ModuleNotFoundError(name)
    return original_import(name, *args, **kwargs)

builtins.__import__ = blocked_import

import litestar_queues
from litestar_queues import InMemoryQueueBackend, QueueConfig

assert "InMemoryQueueBackend" in litestar_queues.__all__
assert "SQLAlchemyBackend" not in litestar_queues.__all__
assert QueueConfig().queue_backend == "memory"
"""
    result = run([sys.executable, "-c", code], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr


def test_advanced_alchemy_backend_uses_upstream_schema_lifecycle(tmp_path: "Path") -> "None":
    sqlalchemy_config = _sqlite_config(tmp_path / "configured.db")
    backend_config = SQLAlchemyBackendConfig(sqlalchemy_config=sqlalchemy_config, model_class=ContractQueueTask)

    assert backend_config.model_class is ContractQueueTask
    assert sqlalchemy_config.create_all is False
    assert not hasattr(backend_config, "create_schema")


def test_advanced_alchemy_mixin_uses_native_json_column_types() -> "None":
    column_type = ContractQueueTask.args_json.property.columns[0].type

    assert column_type.compile(dialect=_postgresql_dialect()) == "JSONB"
    assert column_type.compile(dialect=_sqlite_dialect()) == "JSON"


def test_advanced_alchemy_claim_statement_uses_skip_locked_for_locking_dialects() -> "None":
    from litestar_queues.backends.advanced_alchemy.service import (
        _build_claim_candidate_statement,
        _build_claim_lock_statement,
        _supports_batch_claim,
        _supports_skip_locked_claim,
    )

    claim_time = datetime.now(timezone.utc)
    postgres_statement = _build_claim_candidate_statement(
        ContractQueueTask,
        queue=None,
        execution_backend=None,
        now=claim_time,
        limit=1,
        skip_locked=_supports_skip_locked_claim("postgresql"),
    )
    sqlite_statement = _build_claim_candidate_statement(
        ContractQueueTask,
        queue=None,
        execution_backend=None,
        now=claim_time,
        limit=1,
        skip_locked=_supports_skip_locked_claim("sqlite"),
    )
    oracle_statement = _build_claim_candidate_statement(
        ContractQueueTask, queue=None, execution_backend=None, now=claim_time, limit=10, skip_locked=False
    )
    oracle_lock_statement = _build_claim_lock_statement(ContractQueueTask, uuid4())

    assert "FOR UPDATE SKIP LOCKED" in str(postgres_statement.compile(dialect=_postgresql_dialect()))
    assert "FOR UPDATE" not in str(sqlite_statement.compile(dialect=_sqlite_dialect()))
    oracle_sql = str(oracle_statement.compile(dialect=_oracle_dialect()))
    oracle_lock_sql = str(oracle_lock_statement.compile(dialect=_oracle_dialect()))
    assert "FOR UPDATE" not in oracle_sql
    assert "FETCH FIRST" in oracle_sql
    assert "FOR UPDATE SKIP LOCKED" in oracle_lock_sql
    assert not _supports_skip_locked_claim("mysql")
    assert not _supports_skip_locked_claim("mariadb")
    # Batch claim is gated on the PostgreSQL dialect, so both async PostgreSQL
    # drivers (asyncpg and psycopg share ``dialect.name == "postgresql"``)
    # advertise it while other dialects do not.
    assert _supports_batch_claim("postgresql")
    assert not _supports_batch_claim("oracle")
    assert not _supports_batch_claim("mysql")
    assert not _supports_batch_claim("sqlite")


async def test_advanced_alchemy_cas_claim_scans_past_first_failed_batch() -> "None":
    from litestar_queues.backends.advanced_alchemy.service import QueueTaskService

    records = [QueuedTaskRecord(task_name=f"tasks.cas.{index}") for index in range(11)]
    target = records[-1]
    service = _CasClaimService(records, target)

    claimed = await QueueTaskService.claim_next(cast("QueueTaskService", service), queue=None, execution_backend=None)

    assert claimed is target
    assert service.list_limits == [10, 20]


def test_advanced_alchemy_keyed_enqueue_uses_native_upsert_for_supported_dialects() -> "None":
    from litestar_queues.backends.advanced_alchemy.service import (
        _build_keyed_enqueue_upsert,
        _supports_native_keyed_enqueue,
    )

    values = {
        "id": uuid4(),
        "task_name": "tasks.native_upsert",
        "task_key": "native:upsert",
        "task_args": [],
        "task_kwargs": {},
        "metadata": {},
    }

    postgres_statement, postgres_params = _build_keyed_enqueue_upsert(
        ContractQueueTask.__table__, values, dialect_name="postgresql", key_column="task_key"
    )
    mysql_statement, mysql_params = _build_keyed_enqueue_upsert(
        ContractQueueTask.__table__, values, dialect_name="mysql", key_column="task_key"
    )
    oracle_statement, oracle_params = _build_keyed_enqueue_upsert(
        ContractQueueTask.__table__, values, dialect_name="oracle", key_column="task_key"
    )

    assert _supports_native_keyed_enqueue("postgresql")
    assert not _supports_native_keyed_enqueue("sqlite")
    assert "ON CONFLICT" in str(postgres_statement.compile(dialect=_postgresql_dialect()))
    assert "ON DUPLICATE KEY UPDATE" in str(mysql_statement.compile(dialect=_mysql_dialect()))
    assert isinstance(oracle_statement, MergeStatement)
    assert "MERGE INTO" in str(oracle_statement.compile(dialect=_oracle_dialect()))
    assert postgres_params == {}
    assert mysql_params == {}
    assert set(oracle_params).issubset(ContractQueueTask.__table__.c)


async def test_advanced_alchemy_backend_deduplicates_active_keys_and_replaces_terminal_keys(
    advanced_alchemy_backend: "SQLAlchemyBackend",
) -> "None":
    first = await advanced_alchemy_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-1"}, key="sync:acct-1")
    duplicate = await advanced_alchemy_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")

    assert duplicate.id == first.id
    assert duplicate.kwargs == {"account_id": "acct-1"}

    await advanced_alchemy_backend.complete_task(first.id, result={"ok": True})
    replacement = await advanced_alchemy_backend.enqueue(
        "tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1"
    )

    assert replacement.id != first.id
    assert replacement.kwargs == {"account_id": "acct-2"}
    keyed = await advanced_alchemy_backend.get_task_by_key("sync:acct-1")
    assert keyed is not None
    assert keyed.id == replacement.id


async def test_advanced_alchemy_enqueue_many_uses_one_operation_and_coalesces_due_wakeups(tmp_path: "Path") -> "None":
    config = _sqlite_config(tmp_path / "bulk.db")
    await create_tables(config, ContractQueueTask)
    backend = _CountingSQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(sqlalchemy_config=config, model_class=ContractQueueTask)
    )
    await backend.open()
    try:
        later = datetime.now(timezone.utc) + timedelta(minutes=5)
        records = await backend.enqueue_many([
            TaskRequest("tasks.bulk.first", key="bulk:active", kwargs={"value": 1}),
            TaskRequest("tasks.bulk.future", scheduled_at=later),
            TaskRequest("tasks.bulk.duplicate", key="bulk:active", kwargs={"value": 2}),
        ])

        assert backend.operation_count == 1
        assert [record.task_name for record in records] == ["tasks.bulk.first", "tasks.bulk.future", "tasks.bulk.first"]
        assert records[0].id == records[2].id
        assert records[1].status == "scheduled"
        assert len(backend.notification_batches) == 1
        assert [record.id for record in backend.notification_batches[0]] == [records[0].id]
    finally:
        await backend.close()


async def test_advanced_alchemy_enqueue_many_empty_batch_opens_no_operation(tmp_path: "Path") -> "None":
    config = _sqlite_config(tmp_path / "empty-bulk.db")
    await create_tables(config, ContractQueueTask)
    backend = _CountingSQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(sqlalchemy_config=config, model_class=ContractQueueTask)
    )
    await backend.open()
    try:
        assert await backend.enqueue_many([]) == []
        assert backend.operation_count == 0
        assert backend.notification_batches == []
    finally:
        await backend.close()


async def test_advanced_alchemy_enqueue_many_rolls_back_and_skips_notification_on_failure(tmp_path: "Path") -> "None":
    config = _sqlite_config(tmp_path / "bulk-failure.db")
    await create_tables(config, ContractQueueTask)
    backend = _CountingSQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(sqlalchemy_config=config, model_class=ContractQueueTask)
    )
    await backend.open()
    failing_service = type("FailingBulkService", (backend._service_class,), {"enqueue_many": _failing_enqueue_many})
    backend._service_class = failing_service
    try:
        with pytest.raises(RuntimeError, match="bulk failed"):
            await backend.enqueue_many([TaskRequest("tasks.bulk.failure")])

        assert backend.notification_batches == []
        assert (await backend.get_statistics()).total == 0
    finally:
        await backend.close()


async def test_advanced_alchemy_enqueue_many_handles_large_batch_with_bounded_operation(tmp_path: "Path") -> "None":
    config = _sqlite_config(tmp_path / "bulk-large.db")
    await create_tables(config, ContractQueueTask)
    backend = _CountingSQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(sqlalchemy_config=config, model_class=ContractQueueTask)
    )
    await backend.open()
    try:
        records = await backend.enqueue_many([TaskRequest(f"tasks.bulk.{index}") for index in range(1000)])
        pending = await backend.list_pending(limit=1000)

        assert len(records) == 1000
        assert len(pending) == 1000
        assert backend.operation_count == 1
        assert len(backend.notification_batches) == 1
    finally:
        await backend.close()


async def test_advanced_alchemy_claim_many_falls_back_safely_on_sqlite(
    advanced_alchemy_backend: "SQLAlchemyBackend",
) -> "None":
    first = await advanced_alchemy_backend.enqueue("tasks.claim.first", priority=1)
    second = await advanced_alchemy_backend.enqueue("tasks.claim.second", priority=10)
    future = await advanced_alchemy_backend.enqueue(
        "tasks.claim.future", priority=100, scheduled_at=datetime.now(timezone.utc) + timedelta(minutes=5)
    )

    claimed = await advanced_alchemy_backend.claim_many(limit=5)

    assert [record.id for record in claimed] == [second.id, first.id]
    assert all(record.status == "running" for record in claimed)
    stored_future = await advanced_alchemy_backend.get_task(future.id)
    assert stored_future is not None
    assert stored_future.status == "scheduled"


async def test_advanced_alchemy_capabilities_are_polling_only_without_postgres_notifications(
    tmp_path: "Path",
) -> "None":
    sqlite_backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(
            sqlalchemy_config=_sqlite_config(tmp_path / "polling.db"),
            model_class=ContractQueueTask,
            worker_wakeups=True,
        )
    )
    postgres_backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(
            sqlalchemy_config=SQLAlchemyAsyncConfig(connection_string="postgresql+asyncpg://user:pass@localhost/db"),
            model_class=ContractQueueTask,
            worker_wakeups=True,
        )
    )
    postgres_psycopg_backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(
            sqlalchemy_config=SQLAlchemyAsyncConfig(connection_string="postgresql+psycopg://user:pass@localhost/db"),
            model_class=ContractQueueTask,
            worker_wakeups=True,
        )
    )
    mysql_backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(
            sqlalchemy_config=SQLAlchemyAsyncConfig(connection_string="mysql+asyncmy://user:pass@localhost/db"),
            model_class=ContractQueueTask,
            worker_wakeups=True,
        )
    )
    oracle_backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(
            sqlalchemy_config=SQLAlchemyAsyncConfig(
                connection_string="oracle+oracledb_async://user:pass@localhost:1521/?service_name=FREEPDB1"
            ),
            model_class=ContractQueueTask,
            worker_wakeups=True,
        )
    )

    assert sqlite_backend.capabilities.supports_worker_wakeups is False
    assert sqlite_backend.capabilities.wakeup_backend is None
    assert sqlite_backend.capabilities.wakeups_durable is False
    assert mysql_backend.capabilities.supports_worker_wakeups is False
    # Oracle stays polling: Advanced Alchemy exposes no AQ/TxEventQ transport.
    assert oracle_backend.capabilities.supports_worker_wakeups is False
    assert oracle_backend.capabilities.wakeup_backend not in {"aq", "txeventq"}
    assert oracle_backend.capabilities.wakeup_backend is None
    # Both async PostgreSQL drivers advertise transient LISTEN/NOTIFY wakeups.
    for pg_backend in (postgres_backend, postgres_psycopg_backend):
        assert pg_backend.capabilities.supports_worker_wakeups is True
        assert pg_backend.capabilities.wakeup_backend == "postgres-listen-notify"
        assert pg_backend.capabilities.wakeups_durable is False


async def test_advanced_alchemy_wait_for_wakeups_uses_dedicated_listener(tmp_path: "Path") -> "None":
    backend = _FakeListenerSQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(
            sqlalchemy_config=SQLAlchemyAsyncConfig(connection_string="postgresql+asyncpg://user:pass@localhost/db"),
            model_class=ContractQueueTask,
            worker_wakeups=True,
        )
    )
    backend.listener.due_on_reconcile = True

    assert await backend.wait_for_wakeups(timeout=0.01) is True
    assert backend.listener.started == 1
    assert backend.listener.waits == []

    backend.listener.due_on_reconcile = False
    backend.listener.mark_notified()

    assert await backend.wait_for_wakeups(timeout=0.01) is True
    assert backend.listener.started == 1
    assert backend.listener.waits == [0.01]

    await backend.close()
    assert backend.listener.closed is True


@pytest.mark.parametrize("pg_notify_driver", _PG_NOTIFY_DRIVERS)
async def test_advanced_alchemy_postgres_notifications_wake_waiter(
    postgres_service: "PostgresService", pg_notify_driver: "tuple[str, str]"
) -> "None":
    driver, extra = pg_notify_driver
    pytest.importorskip(extra)
    table_name = f"aa_notify_{uuid4().hex}"
    model_class = _dynamic_queue_model(table_name)
    sqlalchemy_config = _pg_notify_config(driver, postgres_service)
    await create_tables(sqlalchemy_config, model_class)
    backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(
            sqlalchemy_config=sqlalchemy_config,
            model_class=model_class,
            worker_wakeups=True,
            wakeup_channel=f"lq_notify_{uuid4().hex}",
        )
    )
    await backend.open()
    try:
        waiter = asyncio.create_task(backend.wait_for_wakeups(timeout=2.0))
        # Give the dedicated LISTEN connection time to subscribe before the
        # enqueue commits and publishes its notification.
        await asyncio.sleep(0.2)
        record = await backend.enqueue("tasks.pg.notify")

        assert await waiter is True
        assert backend.capabilities.supports_worker_wakeups is True
        assert backend.capabilities.wakeup_backend == "postgres-listen-notify"

        claimed = await backend.claim_task(record.id)
        assert claimed is not None
        assert await backend.wait_for_wakeups(timeout=0.01) is False
    finally:
        with suppress(Exception):
            await _drop_dynamic_queue_model(backend, model_class)
        await backend.close()


@pytest.mark.parametrize("pg_notify_driver", _PG_NOTIFY_DRIVERS)
async def test_advanced_alchemy_postgres_notification_after_timeout_reuses_listener(
    postgres_service: "PostgresService", pg_notify_driver: "tuple[str, str]"
) -> "None":
    driver, extra = pg_notify_driver
    pytest.importorskip(extra)
    table_name = f"aa_notify_reuse_{uuid4().hex}"
    model_class = _dynamic_queue_model(table_name)
    sqlalchemy_config = _pg_notify_config(driver, postgres_service)
    await create_tables(sqlalchemy_config, model_class)
    backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(
            sqlalchemy_config=sqlalchemy_config,
            model_class=model_class,
            worker_wakeups=True,
            wakeup_channel=f"lq_reuse_{uuid4().hex}",
        )
    )
    await backend.open()
    try:
        # A first empty wait establishes the LISTEN connection and retains its read.
        assert await backend.wait_for_wakeups(timeout=0.2) is False
        listener = cast("Any", backend._notification_listener)
        assert listener is not None
        connection = listener._connection
        assert connection is not None
        assert listener._pending_read.has_pending is True

        # A concurrent waiter reuses the retained read; the enqueue's NOTIFY wakes it.
        waiter = asyncio.create_task(backend.wait_for_wakeups(timeout=2.0))
        await asyncio.sleep(0.1)
        await backend.enqueue("tasks.pg.reuse")

        assert await waiter is True
        assert listener._connection is connection
        assert bool(listener._pending_read.has_pending) is False
        # Identity check last: ``is`` widens ``listener`` back to the attribute's type.
        assert backend._notification_listener is listener
    finally:
        with suppress(Exception):
            await _drop_dynamic_queue_model(backend, model_class)
        await backend.close()
        assert backend._notification_listener is None


@pytest.mark.parametrize("pg_notify_driver", _PG_NOTIFY_DRIVERS)
async def test_advanced_alchemy_postgres_dropped_marker_is_reconciled_from_durable_table(
    postgres_service: "PostgresService", pg_notify_driver: "tuple[str, str]"
) -> "None":
    """A committed task enqueued before anyone listens is still claimed.

    The NOTIFY marker fires while no listener is subscribed (it is "dropped"),
    proving durable reconciliation on ``start()`` closes the startup race.
    """
    driver, extra = pg_notify_driver
    pytest.importorskip(extra)
    table_name = f"aa_notify_drop_{uuid4().hex}"
    model_class = _dynamic_queue_model(table_name)
    sqlalchemy_config = _pg_notify_config(driver, postgres_service)
    await create_tables(sqlalchemy_config, model_class)
    backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(
            sqlalchemy_config=sqlalchemy_config,
            model_class=model_class,
            worker_wakeups=True,
            wakeup_channel=f"lq_drop_{uuid4().hex}",
        )
    )
    await backend.open()
    try:
        # Commit + NOTIFY happen here, before any listener exists: the marker is lost.
        record = await backend.enqueue("tasks.pg.dropped")

        # The very first wait subscribes, then reconciles the durable table and
        # discovers the committed due row without any live notification.
        assert await backend.wait_for_wakeups(timeout=1.0) is True
        claimed = await backend.claim_task(record.id)
        assert claimed is not None
        assert claimed.status == "running"
    finally:
        with suppress(Exception):
            await _drop_dynamic_queue_model(backend, model_class)
        await backend.close()


@pytest.mark.parametrize("pg_notify_driver", _PG_NOTIFY_DRIVERS)
async def test_advanced_alchemy_postgres_uncommitted_row_does_not_wake_committed_does(
    postgres_service: "PostgresService", pg_notify_driver: "tuple[str, str]"
) -> "None":
    """An uncommitted enqueue produces no actionable wakeup; the commit does."""
    driver, extra = pg_notify_driver
    pytest.importorskip(extra)
    table_name = f"aa_notify_commit_{uuid4().hex}"
    model_class = _dynamic_queue_model(table_name)
    sqlalchemy_config = _pg_notify_config(driver, postgres_service)
    await create_tables(sqlalchemy_config, model_class)
    backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(
            sqlalchemy_config=sqlalchemy_config,
            model_class=model_class,
            worker_wakeups=True,
            wakeup_channel=f"lq_commit_{uuid4().hex}",
        )
    )
    await backend.open()
    session_maker = sqlalchemy_config.create_session_maker()
    try:
        async with session_maker() as session, session.begin():
            service = backend._service_class(session=session)
            await service.enqueue(
                "tasks.pg.uncommitted",
                args=(),
                kwargs={},
                queue="default",
                priority=0,
                max_retries=0,
                scheduled_at=None,
                key=None,
                execution_backend="local",
                execution_profile=None,
                metadata={},
            )
            await session.flush()
            # Reconciliation runs on a separate connection under READ
            # COMMITTED; the still-open insert is invisible and no marker
            # was published, so the waiter must not wake.
            assert await backend.wait_for_wakeups(timeout=0.4) is False
            # Transaction committed on block exit.
        # The committed due row is now visible to durable reconciliation.
        assert await backend.wait_for_wakeups(timeout=1.0) is True
    finally:
        with suppress(Exception):
            await _drop_dynamic_queue_model(backend, model_class)
        await backend.close()


async def test_advanced_alchemy_backend_claims_due_tasks_by_priority(
    advanced_alchemy_backend: "SQLAlchemyBackend",
) -> "None":
    later = datetime.now(timezone.utc) + timedelta(minutes=5)

    low = await advanced_alchemy_backend.enqueue("tasks.low", priority=1)
    scheduled = await advanced_alchemy_backend.enqueue("tasks.later", priority=100, scheduled_at=later)
    high = await advanced_alchemy_backend.enqueue("tasks.high", priority=10)

    claimed = await advanced_alchemy_backend.claim_next()

    assert claimed is not None
    assert claimed.id == high.id
    assert claimed.status == "running"
    assert claimed.started_at is not None
    stored_low = await advanced_alchemy_backend.get_task(low.id)
    stored_scheduled = await advanced_alchemy_backend.get_task(scheduled.id)
    assert stored_low is not None
    assert stored_scheduled is not None
    assert stored_low.status == "pending"
    assert stored_scheduled.status == "scheduled"


async def test_advanced_alchemy_backend_fail_task_retries_then_fails_permanently(
    advanced_alchemy_backend: "SQLAlchemyBackend",
) -> "None":
    record = await advanced_alchemy_backend.enqueue("tasks.flaky", max_retries=1)

    await advanced_alchemy_backend.claim_task(record.id)
    retried = await advanced_alchemy_backend.fail_task(record.id, "first failure")

    assert retried is not None
    assert retried.status == "pending"
    assert retried.retry_count == 1

    await advanced_alchemy_backend.claim_task(record.id)
    failed = await advanced_alchemy_backend.fail_task(record.id, "second failure")

    assert failed is not None
    assert failed.status == "failed"
    assert failed.error == "second failure"
    assert failed.completed_at is not None


async def test_advanced_alchemy_backend_preserves_json_string_results(
    advanced_alchemy_backend: "SQLAlchemyBackend",
) -> "None":
    record = await advanced_alchemy_backend.enqueue("tasks.string-result")
    claimed = await advanced_alchemy_backend.claim_task(record.id)

    assert claimed is not None
    completed = await advanced_alchemy_backend.complete_task(claimed.id, result="123")
    stored = await advanced_alchemy_backend.get_task(record.id)

    assert completed is not None
    assert completed.result == "123"
    assert stored is not None
    assert stored.result == "123"


async def test_advanced_alchemy_backend_cancels_heartbeats_and_requeues_stale_running(
    advanced_alchemy_backend: "SQLAlchemyBackend",
) -> "None":
    pending = await advanced_alchemy_backend.enqueue("tasks.cancel")

    assert await advanced_alchemy_backend.cancel_task(pending.id)
    assert not await advanced_alchemy_backend.cancel_task(pending.id)

    cancelled = await advanced_alchemy_backend.get_task(pending.id)
    assert cancelled is not None
    assert cancelled.status == "cancelled"

    running = await advanced_alchemy_backend.enqueue("tasks.heartbeat", max_retries=1, metadata={"existing": "kept"})
    claimed = await advanced_alchemy_backend.claim_task(running.id)

    assert claimed is not None
    assert claimed.heartbeat_at is not None

    result = await advanced_alchemy_backend.touch_heartbeats([
        HeartbeatTouch(
            task_id=claimed.id, expected_retry_count=claimed.retry_count, metadata_patch={"progress_detail": "row 5"}
        )
    ])
    touched = await advanced_alchemy_backend.get_task(claimed.id)

    assert result.touched_task_ids == {claimed.id}
    assert result.missed_task_ids == set()
    assert touched is not None
    assert touched.heartbeat_at is not None
    assert touched.heartbeat_at >= claimed.heartbeat_at
    assert touched.metadata == {"existing": "kept", "progress_detail": "row 5"}

    stale_result = await advanced_alchemy_backend.requeue_stale_running(stale_after=timedelta(seconds=0))
    assert stale_result.requeued == 1
    requeued = await advanced_alchemy_backend.get_task(claimed.id)

    assert requeued is not None
    assert requeued.status == "pending"
    assert requeued.retry_count == 1

    exhausted = await advanced_alchemy_backend.enqueue("tasks.exhausted", max_retries=0)
    exhausted_claim = await advanced_alchemy_backend.claim_task(exhausted.id)
    assert exhausted_claim is not None
    exhausted_result = await advanced_alchemy_backend.requeue_stale_running(stale_after=timedelta(seconds=0))
    exhausted_stored = await advanced_alchemy_backend.get_task(exhausted.id)

    assert exhausted_result.failed == 1
    assert exhausted_stored is not None
    assert exhausted_stored.status == "failed"
    assert exhausted_stored.error == "Task heartbeat stale"


async def test_advanced_alchemy_backend_touch_heartbeats_handles_mixed_metadata_patches(
    advanced_alchemy_backend: "SQLAlchemyBackend",
) -> "None":
    patched = await advanced_alchemy_backend.enqueue(
        "tasks.heartbeat.patched", metadata={"existing": "kept", "patch": "old"}
    )
    heartbeat_only = await advanced_alchemy_backend.enqueue("tasks.heartbeat.only", metadata={"existing": "untouched"})
    patched_claim = await advanced_alchemy_backend.claim_task(patched.id)
    heartbeat_only_claim = await advanced_alchemy_backend.claim_task(heartbeat_only.id)

    assert patched_claim is not None
    assert heartbeat_only_claim is not None
    assert patched_claim.heartbeat_at is not None
    assert heartbeat_only_claim.heartbeat_at is not None

    result = await advanced_alchemy_backend.touch_heartbeats([
        HeartbeatTouch(
            task_id=patched_claim.id, expected_retry_count=patched_claim.retry_count, metadata_patch={"patch": "new"}
        ),
        HeartbeatTouch(task_id=heartbeat_only_claim.id, expected_retry_count=heartbeat_only_claim.retry_count),
    ])
    touched_patched = await advanced_alchemy_backend.get_task(patched_claim.id)
    touched_heartbeat_only = await advanced_alchemy_backend.get_task(heartbeat_only_claim.id)

    assert result.touched_task_ids == {patched_claim.id, heartbeat_only_claim.id}
    assert result.missed_task_ids == set()
    assert touched_patched is not None
    assert touched_heartbeat_only is not None
    assert touched_patched.heartbeat_at is not None
    assert touched_heartbeat_only.heartbeat_at is not None
    assert touched_patched.heartbeat_at >= patched_claim.heartbeat_at
    assert touched_heartbeat_only.heartbeat_at >= heartbeat_only_claim.heartbeat_at
    assert touched_patched.metadata == {"existing": "kept", "patch": "new"}
    assert touched_heartbeat_only.metadata == {"existing": "untouched"}


async def test_advanced_alchemy_backend_operational_queries_and_execution_refs(
    advanced_alchemy_backend: "SQLAlchemyBackend",
) -> "None":
    local = await advanced_alchemy_backend.enqueue("tasks.local", priority=100, execution_backend="local")
    external = await advanced_alchemy_backend.enqueue(
        "tasks.remote", execution_backend="cloudrun", execution_profile="batch-small"
    )
    completed = await advanced_alchemy_backend.enqueue(
        "tasks.report", metadata={"encoded_at": datetime.now(timezone.utc)}
    )

    pending = await advanced_alchemy_backend.list_pending(limit=10, execution_backend="cloudrun")
    claimed = await advanced_alchemy_backend.claim_next(execution_backend="cloudrun")
    completed_claim = await advanced_alchemy_backend.claim_task(completed.id)

    assert [record.id for record in pending] == [external.id]
    assert claimed is not None
    assert claimed.id == external.id
    assert completed_claim is not None

    await advanced_alchemy_backend.set_execution_ref(
        claimed.id, "cloudrun", "jobs/abc-123", execution_profile="batch-small"
    )
    await advanced_alchemy_backend.complete_task(completed_claim.id, result={"ok": True})

    running_external = await advanced_alchemy_backend.list_running_external()
    stored_local = await advanced_alchemy_backend.get_task(local.id)
    statistics = await advanced_alchemy_backend.get_statistics()
    completed_records = await advanced_alchemy_backend.list_completed_by_task("tasks.report")
    cleanup_count = await advanced_alchemy_backend.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1))

    assert [record.id for record in running_external] == [external.id]
    assert running_external[0].execution_ref == "jobs/abc-123"
    assert stored_local is not None
    assert stored_local.status == "pending"
    assert statistics.pending == 1
    assert statistics.running == 1
    assert statistics.completed == 1
    assert completed_records[0].metadata["encoded_at"].endswith("Z")
    assert cleanup_count == 1
    assert await advanced_alchemy_backend.get_task(completed.id) is None


async def test_queue_service_uses_advanced_alchemy_backend(tmp_path: "Path") -> "None":
    @task("tasks.aa")
    async def aa_task() -> "str":
        return "ok"

    sqlalchemy_config = _sqlite_config(tmp_path / "service.db")
    await create_tables(sqlalchemy_config, ContractQueueTask)
    queue_config = QueueConfig(
        queue_backend=SQLAlchemyBackendConfig(sqlalchemy_config=sqlalchemy_config, model_class=ContractQueueTask)
    )

    async with QueueService(queue_config) as service:
        result = await service.enqueue("tasks.aa", execution_backend="local")
        stored = await service.get_task(result.id)

    assert result.status == "pending"
    assert stored is not None
    assert stored.task_name == "tasks.aa"


def _sqlite_config(path: "Path") -> "SQLAlchemyAsyncConfig":
    return SQLAlchemyAsyncConfig(connection_string=f"sqlite+aiosqlite:///{path}")


def _postgresql_dialect() -> "Any":
    return postgresql.dialect()  # type: ignore[no-untyped-call]


def _sqlite_dialect() -> "Any":
    return sqlite.dialect()


def _mysql_dialect() -> "Any":
    return mysql.dialect()


def _oracle_dialect() -> "Any":
    return oracle.dialect()  # type: ignore[no-untyped-call]


def _dynamic_queue_model(table_name: "str") -> "type[object]":
    return type(
        f"NotificationQueueTask{table_name[-8:]}",
        (UUIDAuditBase, QueueTaskModelMixin),
        {"__module__": __name__, "__tablename__": table_name},
    )


async def _drop_dynamic_queue_model(backend: "SQLAlchemyBackend", model_class: "type[object]") -> "None":
    sqlalchemy_config = backend._sqlalchemy_config
    if sqlalchemy_config is None:
        return
    engine = sqlalchemy_config.get_engine()
    async with engine.begin() as connection:
        await connection.run_sync(cast("Any", model_class).__table__.drop, checkfirst=True)


async def test_advanced_alchemy_forever_reservation_returns_owner_on_conflict(
    advanced_alchemy_backend: "SQLAlchemyBackend",
) -> "None":
    from tests.integration._uniqueness_contract import assert_reserve_returns_owner_on_conflict

    await assert_reserve_returns_owner_on_conflict(advanced_alchemy_backend)


async def test_advanced_alchemy_forever_reset_is_only_deletion_path(
    advanced_alchemy_backend: "SQLAlchemyBackend",
) -> "None":
    from tests.integration._uniqueness_contract import assert_reset_is_only_deletion_path

    await assert_reset_is_only_deletion_path(advanced_alchemy_backend)


async def test_advanced_alchemy_forever_reservation_survives_terminal_cleanup(
    advanced_alchemy_backend: "SQLAlchemyBackend",
) -> "None":
    from tests.integration._uniqueness_contract import assert_reservation_survives_terminal_cleanup

    await assert_reservation_survives_terminal_cleanup(advanced_alchemy_backend)


async def test_advanced_alchemy_forever_concurrent_reservation_single_winner(
    advanced_alchemy_backend: "SQLAlchemyBackend",
) -> "None":
    from tests.integration._uniqueness_contract import assert_concurrent_reservation_has_single_winner

    await assert_concurrent_reservation_has_single_winner(advanced_alchemy_backend)


class _CasClaimService:
    def __init__(self, records: "list[QueuedTaskRecord]", target: "QueuedTaskRecord") -> "None":
        self.records = records
        self.target = target
        self.list_limits: "list[int]" = []

    def _dialect_name(self) -> "str":
        return "sqlite"

    async def list_pending(
        self, *, limit: "int", queue: "str | None", execution_backend: "str | None"
    ) -> "list[QueuedTaskRecord]":
        del queue, execution_backend
        self.list_limits.append(limit)
        return self.records[:limit]

    async def claim_task(self, task_id: "object") -> "QueuedTaskRecord | None":
        if task_id == self.target.id:
            return self.target
        return None


async def _failing_enqueue_many(self: "Any", specs: "Any") -> "Any":
    del self, specs
    msg = "bulk failed"
    raise RuntimeError(msg)


class _CountingSQLAlchemyBackend(SQLAlchemyBackend):
    __slots__ = ("notification_batches", "operation_count")

    def __init__(self, *args: "Any", **kwargs: "Any") -> "None":
        super().__init__(*args, **kwargs)
        self.operation_count = 0
        self.notification_batches: "list[tuple[QueuedTaskRecord, ...]]" = []

    @asynccontextmanager
    async def _operation(self) -> "Any":
        self.operation_count += 1
        async with super()._operation() as service:
            yield service

    async def notify_new_tasks(self, records: "Sequence[QueuedTaskRecord]") -> "None":
        for record in records:
            if record.is_due and record.status in {"pending", "scheduled"}:
                self.notification_batches.append((record,))
                return


class _FakeNotificationListener:
    __slots__ = ("closed", "due_on_reconcile", "notified", "started", "waits")

    def __init__(self) -> "None":
        self.closed = False
        self.due_on_reconcile = False
        self.notified = False
        self.started = 0
        self.waits: "list[float | None]" = []

    async def start(self) -> "None":
        if self.started:
            return
        self.started += 1

    async def wait(self, timeout: "float | None") -> "bool":
        self.waits.append(timeout)
        notified = self.notified
        self.notified = False
        return notified

    async def close(self) -> "None":
        self.closed = True

    def mark_notified(self) -> "None":
        self.notified = True


class _FakeListenerSQLAlchemyBackend(SQLAlchemyBackend):
    __slots__ = ("listener",)

    def __init__(self, *args: "Any", **kwargs: "Any") -> "None":
        super().__init__(*args, **kwargs)
        self.listener = _FakeNotificationListener()

    def _create_notification_listener(self) -> "_FakeNotificationListener":
        return self.listener

    async def _has_due_tasks(self) -> "bool":
        return self.listener.due_on_reconcile

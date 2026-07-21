"""SQLSpec queue backend contract tests.

Two flavours of tests live here:

1. **Registry-parametrized tests** consume the ``queue_backend`` fixture exposed by
   the integration conftest, exercising shared queue-backend contracts across
   every registered backend (memory + SQLSpec adapters).
2. **SQLSpec-pinned tests** target SQLSpec-specific behaviour (config resolution,
   store factory dispatch, packaged migrations, etc.) and use the aiosqlite-pinned
   ``sqlspec_backend`` fixture defined in the local ``conftest.py``.
"""

import asyncio
import logging
import sqlite3
import sys
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from subprocess import run
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import pytest

pytest.importorskip("aiosqlite")
pytest.importorskip("sqlspec")

from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues import HeartbeatTouch, QueueConfig, QueueService, task
from litestar_queues.backends import InMemoryQueueBackend, get_queue_backend_class, list_queue_backends
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend
from litestar_queues.backends.sqlspec.backend import _bridge_session
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.stores import (
    AiomysqlQueueStore,
    AiosqliteQueueStore,
    ArrowOdbcQueueStore,
    AsyncmyQueueStore,
    AsyncpgQueueStore,
    CockroachAsyncpgQueueStore,
    CockroachPsycopgAsyncQueueStore,
    CockroachPsycopgSyncQueueStore,
    DuckDBQueueStore,
    MysqlConnectorAsyncQueueStore,
    MysqlConnectorSyncQueueStore,
    OracledbAsyncQueueStore,
    OracledbSyncQueueStore,
    PsqlpyQueueStore,
    PsycopgAsyncQueueStore,
    PsycopgSyncQueueStore,
    PymysqlQueueStore,
    SpannerQueueStore,
    SqliteQueueStore,
    create_queue_store,
)
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.models import QueuedTaskRecord
from tests.integration._backends import QUEUE_BACKENDS
from tests.integration._names import table_name_for_test
from tests.integration.backends.sqlspec._schema import bootstrap_queue_schema, run_queue_migrations

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping
    from pathlib import Path
    from uuid import UUID

    from pytest import FixtureRequest

    from litestar_queues.backends import BaseQueueBackend
    from tests.integration._backends import BackendCase, PostgresService
    from tests.integration.backends.sqlspec.conftest import SqliteConfigFactory

pytestmark = pytest.mark.anyio


class FakeSQLSpecConfig(SimpleNamespace):
    """Structural config used by SQLSpec store dispatch tests."""

    extension_config: "dict[str, object]"
    statement_config: "SimpleNamespace"
    connection_config: "dict[str, object]"


def _fake_adapter_config(
    adapter_name: "str",
    *,
    dialect: "str | None" = None,
    config_type_name: "str | None" = None,
    connection_config: "dict[str, object] | None" = None,
    extension_config: "dict[str, object] | None" = None,
    driver_features: "dict[str, object] | None" = None,
    supports_native_arrow_import: "bool | None" = None,
) -> "FakeSQLSpecConfig":
    class_attrs: "dict[str, object]" = {"__module__": f"sqlspec.adapters.{adapter_name}.config"}
    if supports_native_arrow_import is not None:
        class_attrs["supports_native_arrow_import"] = supports_native_arrow_import
    config_type = cast(
        "type[FakeSQLSpecConfig]",
        type(
            config_type_name or f"Fake{adapter_name.title().replace('_', '')}Config", (FakeSQLSpecConfig,), class_attrs
        ),
    )
    config = config_type()
    config.extension_config = extension_config or {}
    config.statement_config = SimpleNamespace(dialect=dialect)
    config.connection_config = connection_config or {}
    config.driver_features = driver_features or {}
    return config


# ---------------------------------------------------------------------------
# Registry-parametrized contract tests
# ---------------------------------------------------------------------------


async def test_backend_contract_enqueue_claim_complete_cycle(
    queue_backend: "BaseQueueBackend", queue_backend_case: "BackendCase"
) -> "None":
    """A backend must support the full enqueue → claim → complete cycle."""
    if "xfail-update-rows-affected" in queue_backend_case.capabilities:
        pytest.xfail(
            f"{queue_backend_case.name}: SQLSpec 0.55.0 psqlpy adapter reports rows_affected=0 for the "
            "UPDATE issued inside claim_task's begin() transaction (upstream defect, sqlspec#645)"
        )
    record = await queue_backend.enqueue("tasks.contract.cycle", priority=10)

    claimed = await queue_backend.claim_task(record.id)
    assert claimed is not None
    assert claimed.id == record.id
    assert claimed.status == "running"

    await queue_backend.complete_task(claimed.id, result={"ok": True})

    stored = await queue_backend.get_task(record.id)
    assert stored is not None
    assert stored.status == "completed"


async def test_backend_contract_concurrent_claim_next_never_double_claims(
    queue_backend: "BaseQueueBackend", queue_backend_case: "BackendCase"
) -> "None":
    """Concurrent ``claim_next`` must never hand the same task to two workers.

    On adapters that advertise SKIP LOCKED (Postgres family, MySQL 8+, …) the
    claim selects under ``FOR UPDATE SKIP LOCKED``; on the rest it falls back to
    the optimistic CAS claim. Either way the invariant is identical: no task is
    claimed twice, and every due task is eventually claimed exactly once. Runs
    against the real container behind each ``queue_backend`` case.

    Single-writer sync drivers (sqlite, duckdb, …) share one DBAPI connection
    and cannot be driven concurrently through the bridge; their CAS strategy is
    asserted by the capability-introspection tests instead.
    """
    if "sync-driver" in queue_backend_case.capabilities:
        pytest.skip(f"{queue_backend_case.name}: single-writer sync driver cannot claim concurrently")
    if "xfail-update-rows-affected" in queue_backend_case.capabilities:
        pytest.xfail(
            f"{queue_backend_case.name}: SQLSpec 0.55.0 psqlpy adapter reports rows_affected=0 for the "
            "UPDATE issued inside the SKIP LOCKED claim's begin() transaction (upstream defect, sqlspec#645)"
        )

    task_count = 8
    enqueued_ids = {(await queue_backend.enqueue("tasks.contract.contended", priority=5)).id for _ in range(task_count)}

    burst = await asyncio.gather(*(queue_backend.claim_next() for _ in range(task_count)))
    claimed = [record for record in burst if record is not None]
    claimed_ids = [record.id for record in claimed]

    # The CAS-loop fallback can leave stragglers under contention; drain them so
    # completeness is asserted without weakening the no-double-claim invariant.
    while (straggler := await queue_backend.claim_next()) is not None:
        claimed.append(straggler)
        claimed_ids.append(straggler.id)

    assert all(record.status == "running" for record in claimed)
    assert len(claimed_ids) == len(set(claimed_ids)), "a task was claimed by more than one worker"
    assert set(claimed_ids) == enqueued_ids, "every due task should be claimed exactly once"


async def test_backend_contract_requeue_stale_running_recovers_every_task(
    queue_backend: "BaseQueueBackend", queue_backend_case: "BackendCase"
) -> "None":
    """Stale recovery must requeue every stale running task.

    For SQLSpec backends the per-row stale-recovery writes are batched into a
    single ``StatementStack`` / ``execute_stack`` call (pipelined on Oracle
    >=23ai and psycopg, sequential elsewhere). This asserts the batched writes
    actually apply on the real container behind each adapter.
    """
    if "xfail-update-rows-affected" in queue_backend_case.capabilities:
        pytest.xfail(
            f"{queue_backend_case.name}: SQLSpec 0.55.0 psqlpy adapter reports rows_affected=0 for the "
            "UPDATE issued inside claim_task's begin() transaction (upstream defect, sqlspec#645)"
        )
    stale_count = 5
    records = [
        await queue_backend.enqueue(f"tasks.contract.stale.{index}", max_retries=3) for index in range(stale_count)
    ]
    for record in records:
        claimed = await queue_backend.claim_task(record.id)
        assert claimed is not None

    # A negative window puts the cutoff slightly in the future so every
    # just-claimed task counts as stale regardless of the adapter's timestamp
    # precision (Oracle/DuckDB truncate sub-second, so stale_after=0 would race).
    result = await queue_backend.requeue_stale_running(stale_after=timedelta(seconds=-2))

    assert result.requeued == stale_count
    for record in records:
        stored = await queue_backend.get_task(record.id)
        assert stored is not None
        assert stored.status == "pending"
        assert stored.retry_count == 1


# ---------------------------------------------------------------------------
# SQLSpec-pinned contract tests (aiosqlite via the ``sqlspec_backend`` fixture)
# ---------------------------------------------------------------------------


async def test_sqlspec_backend_supports_sync_sqlspec_config_via_sync_tools_bridge(tmp_path: "Path") -> "None":
    """SQLSpecQueueBackend must support sync SQLSpec configs via sqlspec.utils.sync_tools."""
    from sqlspec.adapters.sqlite import SqliteConfig

    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=SqliteConfig(connection_config={"database": str(tmp_path / "queue-sync.db")})
        )
    )
    await backend.open()
    await backend.create_schema()
    try:
        record = await backend.enqueue("tasks.sync_bridge", kwargs={"a": 1})
        claimed = await backend.claim_task(record.id)
        assert claimed is not None
        assert claimed.id == record.id
        assert claimed.status == "running"
        await backend.complete_task(claimed.id, result={"ok": True})
        stored = await backend.get_task(record.id)
        assert stored is not None
        assert stored.status == "completed"
        statistics = await backend.get_statistics()
        streamed = [record async for record in backend.iter_all()]
        assert statistics.completed == 1
        assert [record.id for record in streamed] == [stored.id]
    finally:
        await backend.close()


async def test_adbc_sqlite_completed_query_survives_prior_aiosqlite_query(tmp_path: "Path") -> "None":
    """ADBC SQLite completed queries must survive prior SQLite-family builder execution."""
    pytest.importorskip("adbc_driver_manager")
    pytest.importorskip("adbc_driver_sqlite")
    from sqlspec.adapters.adbc import AdbcConfig

    aiosqlite_backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=AiosqliteConfig(connection_config={"database": str(tmp_path / "aiosqlite.db")})
        )
    )
    await aiosqlite_backend.open()
    await aiosqlite_backend.create_schema()
    try:
        await _complete_report_task(aiosqlite_backend)
        aiosqlite_records = await aiosqlite_backend.list_completed_by_task("tasks.report")
    finally:
        await aiosqlite_backend.close()

    adbc_backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=AdbcConfig(
                connection_config={"driver_name": "adbc_driver_sqlite", "uri": str(tmp_path / "adbc-sqlite.db")}
            )
        )
    )
    await adbc_backend.open()
    await adbc_backend.create_schema()
    try:
        completed_id = await _complete_report_task(adbc_backend)
        adbc_records = await adbc_backend.list_completed_by_task("tasks.report")
    finally:
        await adbc_backend.close()

    assert [record.task_name for record in aiosqlite_records] == ["tasks.report"]
    assert [record.id for record in adbc_records] == [completed_id]


async def test_sqlspec_backend_is_registered_without_advanced_alchemy() -> "None":
    assert "sqlspec" in list_queue_backends()
    assert get_queue_backend_class("sqlspec") is SQLSpecQueueBackend


def test_sqlspec_backend_serializes_text_bound_datetimes_as_iso_by_default() -> "None":
    backend = SQLSpecQueueBackend()
    backend._store = cast("Any", SimpleNamespace(bind_datetime_as_text=True))

    serialized = backend._serialize_datetime(datetime(2026, 7, 2, 12, 34, 56, 789012, tzinfo=timezone.utc))

    assert serialized == "2026-07-02T12:34:56.789012+00:00"


def test_sqlspec_backend_serializes_arrow_odbc_datetimes_for_sql_server_datetime() -> "None":
    backend = SQLSpecQueueBackend()
    backend._store = create_queue_store(
        _fake_adapter_config(
            "arrow_odbc",
            dialect="tsql",
            config_type_name="ArrowOdbcConfig",
            driver_features={"dbms_name": "Microsoft SQL Server"},
        )
    )

    serialized = backend._serialize_datetime(datetime(2026, 7, 2, 12, 34, 56, 789012, tzinfo=timezone.utc))

    assert isinstance(backend._store, ArrowOdbcQueueStore)
    assert serialized == "2026-07-02 12:34:56.789"


def test_top_level_litestar_queues_import_does_not_pull_in_sqlspec() -> "None":
    """Importing ``litestar_queues`` must succeed without sqlspec installed."""
    code = """
import builtins
import sys

original_import = builtins.__import__

def blocked_import(name, *args, **kwargs):
    if name == "sqlspec" or name.startswith("sqlspec."):
        raise ModuleNotFoundError(name)
    return original_import(name, *args, **kwargs)

builtins.__import__ = blocked_import
import litestar_queues
from litestar_queues import InMemoryQueueBackend

assert "InMemoryQueueBackend" in litestar_queues.__all__
assert "SQLSpecQueueBackend" not in litestar_queues.__all__
assert "sqlspec" not in sys.modules
"""
    result = run([sys.executable, "-c", code], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr


def test_sqlspec_backend_store_factory_does_not_import_optional_adapter_drivers() -> "None":
    code = """
import builtins
from types import SimpleNamespace

blocked_prefixes = (
    "adbc_driver_manager",
    "adbc_driver_sqlite",
    "aiomysql",
    "aiosqlite",
    "asyncmy",
    "asyncpg",
    "arrow_odbc",
    "duckdb",
    "cockroach_asyncpg",
    "cockroach_psycopg",
    "mssql_python",
    "mysql.connector",
    "pymysql",
    "oracledb",
    "pymssql",
    "psqlpy",
    "psycopg",
    "sqlspec.adapters.adbc",
    "sqlspec.adapters.aiomysql",
    "sqlspec.adapters.aiosqlite",
    "sqlspec.adapters.asyncmy",
    "sqlspec.adapters.asyncpg",
    "sqlspec.adapters.arrow_odbc",
    "sqlspec.adapters.cockroach_asyncpg",
    "sqlspec.adapters.cockroach_psycopg",
    "sqlspec.adapters.duckdb",
    "sqlspec.adapters.mssql_python",
    "sqlspec.adapters.mysqlconnector",
    "sqlspec.adapters.pymysql",
    "sqlspec.adapters.oracledb",
    "sqlspec.adapters.pymssql",
    "sqlspec.adapters.psqlpy",
    "sqlspec.adapters.psycopg",
    "sqlspec.adapters.spanner",
    "google.cloud.spanner_v1",
)
blocked_package_prefixes = tuple(f"{name}." for name in blocked_prefixes)
original_import = builtins.__import__

def blocked_import(name, *args, **kwargs):
    if name in blocked_prefixes or name.startswith(blocked_package_prefixes):
        raise ModuleNotFoundError(name)
    return original_import(name, *args, **kwargs)

builtins.__import__ = blocked_import

from litestar_queues.backends.sqlspec.stores import (
    AdbcSqliteQueueStore,
    AiomysqlQueueStore,
    AiosqliteQueueStore,
    AsyncmyQueueStore,
    AsyncpgQueueStore,
    DuckDBQueueStore,
    CockroachAsyncpgQueueStore,
    CockroachPsycopgAsyncQueueStore,
    CockroachPsycopgSyncQueueStore,
    MysqlConnectorAsyncQueueStore,
    MysqlConnectorSyncQueueStore,
    OracledbAsyncQueueStore,
    OracledbSyncQueueStore,
    PsqlpyQueueStore,
    PsycopgAsyncQueueStore,
    PsycopgSyncQueueStore,
    SpannerQueueStore,
    SqliteQueueStore,
    create_queue_store,
)

def fake_config(adapter_name, dialect, config_type_name):
    config_type = type(config_type_name, (), {"__module__": f"sqlspec.adapters.{adapter_name}.config"})
    config = config_type()
    config.extension_config = {}
    config.statement_config = SimpleNamespace(dialect=dialect)
    config.connection_config = {}
    return config

expected = (
    ("adbc", "sqlite", "AdbcConfig", AdbcSqliteQueueStore),
    ("aiomysql", "mysql", "AiomysqlConfig", AiomysqlQueueStore),
    ("aiosqlite", "sqlite", "AiosqliteConfig", AiosqliteQueueStore),
    ("asyncmy", "mysql", "AsyncmyConfig", AsyncmyQueueStore),
    ("asyncpg", "postgres", "AsyncpgConfig", AsyncpgQueueStore),
    ("cockroach_asyncpg", "postgres", "CockroachAsyncpgConfig", CockroachAsyncpgQueueStore),
    ("cockroach_psycopg", "postgres", "CockroachPsycopgAsyncConfig", CockroachPsycopgAsyncQueueStore),
    ("cockroach_psycopg", "postgres", "CockroachPsycopgSyncConfig", CockroachPsycopgSyncQueueStore),
    ("duckdb", "duckdb", "DuckDBConfig", DuckDBQueueStore),
    ("mysqlconnector", "mysql", "MysqlConnectorSyncConfig", MysqlConnectorSyncQueueStore),
    ("mysqlconnector", "mysql", "MysqlConnectorAsyncConfig", MysqlConnectorAsyncQueueStore),
    ("oracledb", "oracle", "OracleSyncConfig", OracledbSyncQueueStore),
    ("oracledb", "oracle", "OracleAsyncConfig", OracledbAsyncQueueStore),
    ("psqlpy", "postgres", "PsqlpyConfig", PsqlpyQueueStore),
    ("psycopg", "postgres", "PsycopgSyncConfig", PsycopgSyncQueueStore),
    ("psycopg", "postgres", "PsycopgAsyncConfig", PsycopgAsyncQueueStore),
    ("spanner", "spanner", "SpannerConfig", SpannerQueueStore),
    ("sqlite", "sqlite", "SqliteConfig", SqliteQueueStore),
)

for adapter_name, dialect, config_type_name, expected_store in expected:
    store = create_queue_store(fake_config(adapter_name, dialect, config_type_name), table_name="queue_tasks")
    assert isinstance(store, expected_store), adapter_name
"""
    result = run([sys.executable, "-c", code], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr


async def test_sqlspec_backend_exposes_config_type_and_builder_store(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    backend_config = SQLSpecBackendConfig(queue_table_name="queue_tasks")
    store = create_queue_store(sqlite_config_factory(tmp_path / "queue.db"), table_name=backend_config.queue_table_name)

    assert backend_config.queue_table_name == "queue_tasks"
    assert isinstance(store, AiosqliteQueueStore)
    assert store.table_name == "queue_tasks"
    assert any('"queue_tasks"' in statement for statement in store.create_statements())

    insert_statement = store.insert_task({"id": "task-1", "task_name": "tasks.sync"}).build(dialect="sqlite")
    pending_statement = store.list_pending(now=datetime.now(timezone.utc).isoformat(), limit=10, queue="default").build(
        dialect="sqlite"
    )

    assert 'INSERT INTO "queue_tasks"' in insert_statement.sql
    assert "task-1" in insert_statement.parameters.values()
    assert 'FROM "queue_tasks"' in pending_statement.sql
    assert "queue" in pending_statement.sql


def test_postgres_and_duckdb_stores_build_bulk_heartbeat_updates() -> "None":
    """Postgres-family and DuckDB stores expose one VALUES-driven heartbeat update."""
    postgres_store = AsyncpgQueueStore(_fake_adapter_config("asyncpg", dialect="postgres"), table_name="queue_tasks")
    duckdb_store = DuckDBQueueStore(_fake_adapter_config("duckdb", dialect="duckdb"), table_name="queue_tasks")
    touches: "list[Mapping[str, Any]]" = [
        {"task_id": "task-1", "expected_retry_count": 0, "metadata_json": {"progress_detail": "row 1"}},
        {"task_id": "task-2", "expected_retry_count": None, "metadata_json": None},
    ]

    postgres_statement = postgres_store.bulk_touch_heartbeats(touches=touches, heartbeat_at="2026-07-06T22:00:00Z")
    duckdb_statement = duckdb_store.bulk_touch_heartbeats(touches=touches, heartbeat_at="2026-07-06T22:00:00Z")

    assert postgres_statement is not None
    assert duckdb_statement is not None
    assert postgres_statement.sql.count("VALUES") == 1
    assert duckdb_statement.sql.count("VALUES") == 1
    assert 'UPDATE "queue_tasks" AS target' in postgres_statement.sql
    assert 'UPDATE "queue_tasks" AS target' in duckdb_statement.sql
    assert 'RETURNING target."id" AS id' in postgres_statement.sql
    assert 'RETURNING target."id" AS id' in duckdb_statement.sql
    assert "CAST(:expected_retry_count_0 AS INTEGER)" in postgres_statement.sql
    assert 'target."metadata" || heartbeat_updates.metadata_json' in postgres_statement.sql
    assert "CAST(? AS INTEGER)" in duckdb_statement.sql
    assert "json_group_object(merged.key, merged.value)" in duckdb_statement.sql
    assert isinstance(postgres_statement.parameters, dict)
    assert isinstance(duckdb_statement.parameters, list)
    assert postgres_statement.parameters["task_id_0"] == "task-1"
    assert postgres_statement.parameters["expected_retry_count_1"] is None
    assert duckdb_statement.parameters == [
        "task-1",
        0,
        {"progress_detail": "row 1"},
        "task-2",
        None,
        None,
        "2026-07-06T22:00:00Z",
    ]


def test_sqlspec_claim_batch_sql_is_cached() -> "None":
    """Hot-path RETURNING builders are built once per store instance and reused by identity."""
    store = AsyncpgQueueStore(_fake_adapter_config("asyncpg", dialect="postgres"), table_name="queue_tasks")

    claim_first = store.claim_batch_returning_sql(queue_count=1, filter_execution_backend=True)
    claim_second = store.claim_batch_returning_sql(queue_count=1, filter_execution_backend=True)

    assert claim_first is claim_second
    assert store._cached_sql["claim_batch:1:1"] is claim_first
    assert store.complete_returning_sql(fence_retry_count=True) is store.complete_returning_sql(fence_retry_count=True)
    assert store.fail_returning_sql(fence_retry_count=True) is store.fail_returning_sql(fence_retry_count=True)
    assert store.insert_returning_sql() is store.insert_returning_sql()
    assert store.claim_batch_returning_sql(
        queue_count=0, filter_execution_backend=False
    ) is not store.claim_batch_returning_sql(queue_count=1, filter_execution_backend=True)
    assert store.complete_returning_sql(fence_retry_count=False) is not store.complete_returning_sql(
        fence_retry_count=True
    )


def test_sqlspec_claim_batch_returning_sql_shape() -> "None":
    """The batched claim statement locks, orders, filters, and returns full rows."""
    store = AsyncpgQueueStore(_fake_adapter_config("asyncpg", dialect="postgres"), table_name="queue_tasks")

    sql = store.claim_batch_returning_sql(queue_count=2, filter_execution_backend=True)

    assert "FOR UPDATE SKIP LOCKED" in sql
    assert 'ORDER BY "priority" DESC, "created_at" ASC' in sql
    assert "RETURNING" in sql
    assert "\"status\" = 'running'" in sql
    assert '"started_at" = :started_at' in sql
    assert '"heartbeat_at" = :heartbeat_at' in sql
    assert '"queue" IN (:queue_0, :queue_1)' in sql
    assert '"execution_backend" = :execution_backend' in sql
    assert "LIMIT :limit" in sql

    unfiltered = store.claim_batch_returning_sql(queue_count=0, filter_execution_backend=False)
    assert '"queue" IN' not in unfiltered
    assert '"execution_backend" = :execution_backend' not in unfiltered


def test_sqlspec_complete_returning_sql_clears_heartbeat_and_fences() -> "None":
    """The completion statement clears the heartbeat and fences on running status."""
    store = AsyncpgQueueStore(_fake_adapter_config("asyncpg", dialect="postgres"), table_name="queue_tasks")

    fenced = store.complete_returning_sql(fence_retry_count=True)

    assert "\"status\" = 'completed'" in fenced
    assert '"heartbeat_at" = NULL' in fenced
    assert "\"status\" = 'running'" in fenced
    assert '"retry_count" = :expected_retry_count' in fenced
    assert '"error" = NULL' in fenced
    assert ":expected_retry_count" not in store.complete_returning_sql(fence_retry_count=False)


def test_sqlspec_fail_returning_sql_uses_case_retry_branch() -> "None":
    """The fail statement branches retry vs terminal with a CASE and clears the heartbeat."""
    store = AsyncpgQueueStore(_fake_adapter_config("asyncpg", dialect="postgres"), table_name="queue_tasks")

    sql = store.fail_returning_sql(fence_retry_count=False)

    assert "CASE WHEN :retry = TRUE AND \"retry_count\" < \"max_retries\" THEN 'pending' ELSE 'failed' END" in sql
    assert '"heartbeat_at" = NULL' in sql
    assert '"error" = :error' in sql
    assert (
        '"completed_at" = CASE WHEN :retry = TRUE AND "retry_count" < "max_retries" '
        "THEN NULL ELSE CAST(:completed_at AS TIMESTAMPTZ) END" in sql
    )


def test_sqlspec_returning_columns_alias_remapped_columns() -> "None":
    """RETURNING aliases physical columns back to canonical names for record reads."""
    store = AsyncpgQueueStore(
        _fake_adapter_config("asyncpg", dialect="postgres"),
        table_name="queue_tasks",
        column_map={"task_key": "dedupe_key"},
    )

    insert_sql = store.insert_returning_sql()

    assert '"result" AS "result_json"' in insert_sql
    assert '"metadata" AS "metadata_json"' in insert_sql
    assert '"dedupe_key" AS "task_key"' in insert_sql
    assert 't."dedupe_key" AS "task_key"' in store.claim_batch_returning_sql(
        queue_count=0, filter_execution_backend=False
    )


def test_sqlspec_backend_accepts_adbc_sqlite_adapter() -> "None":
    store = create_queue_store(
        _fake_adapter_config(
            "adbc",
            dialect="sqlite",
            config_type_name="AdbcConfig",
            connection_config={"driver_name": "adbc_driver_sqlite", "uri": "/tmp/queue.db"},
        ),
        table_name="queue_tasks",
    )

    assert store.__class__.__name__ == "AdbcSqliteQueueStore"
    assert store.__class__.__module__.startswith("litestar_queues.backends.sqlspec.stores.adbc.")
    assert '"queue_tasks"' in "\n".join(store.create_statements())


def test_sqlspec_backend_store_factory_resolves_adapter_config_subclasses(tmp_path: "Path") -> "None":
    class CustomAiosqliteConfig(AiosqliteConfig):
        pass

    config = CustomAiosqliteConfig(connection_config={"database": str(tmp_path / "queue.db")})

    store = create_queue_store(config, table_name="queue_tasks")

    assert isinstance(store, AiosqliteQueueStore)


@pytest.mark.parametrize(
    ("adapter_name", "dialect", "config_type_name", "expected_store_name"),
    (("pymssql", "tsql", "PymssqlConfig", "PymssqlQueueStore"),),
)
def test_sqlspec_backend_store_factory_supports_sql_server_adapters(
    adapter_name: "str", dialect: "str | None", config_type_name: "str", expected_store_name: "str"
) -> "None":
    store = create_queue_store(
        _fake_adapter_config(adapter_name, dialect=dialect, config_type_name=config_type_name), table_name="queue_tasks"
    )

    assert store.__class__.__name__ == expected_store_name
    assert store.__class__.__module__.startswith(f"litestar_queues.backends.sqlspec.stores.{adapter_name}.")


@pytest.mark.parametrize(("adapter_name", "dialect", "config_type_name"), (("pymssql", "tsql", "PymssqlConfig"),))
def test_sqlspec_sql_server_queue_store_uses_sql_server_types(
    adapter_name: "str", dialect: "str | None", config_type_name: "str"
) -> "None":
    store = create_queue_store(
        _fake_adapter_config(adapter_name, dialect=dialect, config_type_name=config_type_name), table_name="queue_tasks"
    )

    ddl = "\n".join(store.create_statements())

    assert "NVARCHAR(255)" in ddl
    assert "NVARCHAR(MAX)" in ddl
    assert "DATETIME2(6)" in ddl
    assert " INT " in f" {ddl} "
    assert "CREATE UNIQUE INDEX" in ddl
    assert "[task_key] IS NOT NULL" in ddl
    assert store.supports_skip_locked is False


@pytest.mark.parametrize(("adapter_name", "dialect", "config_type_name"), (("bigquery", "bigquery", "BigQueryConfig"),))
def test_sqlspec_backend_rejects_unsupported_sqlspec_adapter(
    adapter_name: "str", dialect: "str | None", config_type_name: "str"
) -> "None":
    with pytest.raises(QueueConfigurationError, match=adapter_name):
        create_queue_store(
            _fake_adapter_config(adapter_name, dialect=dialect, config_type_name=config_type_name),
            table_name="queue_tasks",
        )


def test_sqlspec_backend_rejects_mssql_python_until_upstream_transaction_fix() -> "None":
    config = _fake_adapter_config("mssql_python", dialect="tsql", config_type_name="MssqlPythonConfig")

    with pytest.raises(QueueConfigurationError, match=r"mssql_python.*sqlspec/issues/642"):
        create_queue_store(config, table_name="queue_tasks")


@pytest.mark.parametrize(("dialect",), (("bigquery",), ("postgres",)))
def test_sqlspec_backend_rejects_non_sqlite_adbc_adapter(dialect: "str") -> "None":
    with pytest.raises(QueueConfigurationError, match="sqlite"):
        create_queue_store(
            _fake_adapter_config(
                "adbc",
                dialect=dialect,
                config_type_name="AdbcConfig",
                connection_config={"driver_name": "adbc_driver_sqlite", "uri": "/tmp/queue.db"},
            ),
            table_name="queue_tasks",
        )


def test_sqlspec_backend_rejects_arrow_odbc_unknown_target_dialect() -> "None":
    with pytest.raises(QueueConfigurationError, match="Supported target dialect"):
        create_queue_store(
            _fake_adapter_config("arrow_odbc", dialect=None, config_type_name="ArrowOdbcConfig"),
            table_name="queue_tasks",
        )


def test_sqlspec_backend_rejects_arrow_odbc_unsupported_target_dialect() -> "None":
    with pytest.raises(QueueConfigurationError, match="postgres"):
        create_queue_store(
            _fake_adapter_config("arrow_odbc", dialect="postgres", config_type_name="ArrowOdbcConfig"),
            table_name="queue_tasks",
        )


def test_sqlspec_backend_accepts_arrow_odbc_sql_server_target() -> "None":
    store = create_queue_store(
        _fake_adapter_config(
            "arrow_odbc",
            dialect="tsql",
            config_type_name="ArrowOdbcConfig",
            connection_config={
                "connection_string": (
                    "encrypt=no;TrustServerCertificate=yes;driver={ODBC Driver 18 for SQL Server};"
                    "server=localhost,1433;database=pytest_databases;UID=sa;PWD=Super-secret1"
                )
            },
        ),
        table_name="queue_tasks",
    )

    ddl = "\n".join(store.create_statements())

    assert store.__class__.__module__.startswith("litestar_queues.backends.sqlspec.stores.arrow_odbc.")
    assert "DATETIME" in ddl
    assert "NVARCHAR(MAX)" in ddl
    assert "[task_key] VARCHAR(255) UNIQUE" not in ddl
    assert "CREATE UNIQUE INDEX" in ddl
    assert "WHERE [task_key] IS NOT NULL" in ddl


@pytest.mark.parametrize(
    ("adapter_name", "config_type_name", "expected_store_name"),
    (
        ("cockroach_asyncpg", "CockroachAsyncpgConfig", "CockroachAsyncpgQueueStore"),
        ("cockroach_psycopg", "CockroachPsycopgAsyncConfig", "CockroachPsycopgAsyncQueueStore"),
        ("cockroach_psycopg", "CockroachPsycopgSyncConfig", "CockroachPsycopgSyncQueueStore"),
    ),
)
def test_sqlspec_backend_accepts_cockroach_sqlspec_adapters(
    adapter_name: "str", config_type_name: "str", expected_store_name: "str"
) -> "None":
    store = create_queue_store(
        _fake_adapter_config(adapter_name, dialect="postgres", config_type_name=config_type_name),
        table_name="queue_tasks",
    )

    created_statements = "\n".join(store.create_statements())
    assert store.__class__.__module__.startswith(f"litestar_queues.backends.sqlspec.stores.{adapter_name}.")
    assert store.__class__.__name__ == expected_store_name
    assert "WITH (fillfactor = 80)" not in created_statements
    assert "autovacuum_vacuum_scale_factor" not in created_statements
    assert store.supports_skip_locked is False


@pytest.mark.parametrize(
    ("adapter_name", "config_type_name", "expected"),
    (
        ("psycopg", "PsycopgAsyncConfig", '["alpha","beta"]'),
        ("cockroach_psycopg", "CockroachPsycopgAsyncConfig", '["alpha","beta"]'),
        ("psqlpy", "PsqlpyConfig", ["alpha", "beta"]),
    ),
)
def test_postgres_native_json_array_bind_shape_matches_adapter(
    adapter_name: "str", config_type_name: "str", expected: "object"
) -> "None":
    store = create_queue_store(
        _fake_adapter_config(adapter_name, dialect="postgres", config_type_name=config_type_name),
        table_name="queue_tasks",
    )

    assert store.serialize_json("args_json", ("alpha", "beta")) == expected


@pytest.mark.parametrize(
    (
        "adapter_name",
        "dialect",
        "config_type_name",
        "connection_config",
        "expected_store_type",
        "expected_sql_fragment",
    ),
    (
        ("aiomysql", "mysql", "AiomysqlConfig", {}, AiomysqlQueueStore, "ENGINE=InnoDB"),
        ("aiosqlite", "sqlite", "AiosqliteConfig", {}, AiosqliteQueueStore, '"queue_tasks"'),
        ("asyncmy", "mysql", "AsyncmyConfig", {}, AsyncmyQueueStore, "ENGINE=InnoDB"),
        ("asyncpg", "postgres", "AsyncpgConfig", {}, AsyncpgQueueStore, 'WHERE "status" IN'),
        (
            "cockroach_asyncpg",
            "postgres",
            "CockroachAsyncpgConfig",
            {},
            CockroachAsyncpgQueueStore,
            'WHERE "status" IN',
        ),
        (
            "cockroach_psycopg",
            "postgres",
            "CockroachPsycopgAsyncConfig",
            {},
            CockroachPsycopgAsyncQueueStore,
            'WHERE "status" IN',
        ),
        (
            "cockroach_psycopg",
            "postgres",
            "CockroachPsycopgSyncConfig",
            {},
            CockroachPsycopgSyncQueueStore,
            'WHERE "status" IN',
        ),
        ("duckdb", "duckdb", "DuckDBConfig", {}, DuckDBQueueStore, "JSON"),
        ("mysqlconnector", "mysql", "MysqlConnectorSyncConfig", {}, MysqlConnectorSyncQueueStore, "ENGINE=InnoDB"),
        ("mysqlconnector", "mysql", "MysqlConnectorAsyncConfig", {}, MysqlConnectorAsyncQueueStore, "ENGINE=InnoDB"),
        ("oracledb", "oracle", "OracleSyncConfig", {}, OracledbSyncQueueStore, "BLOB CHECK (task_args IS JSON)"),
        ("oracledb", "oracle", "OracleAsyncConfig", {}, OracledbAsyncQueueStore, "BLOB CHECK (task_args IS JSON)"),
        ("pymysql", "mysql", "PyMysqlConfig", {}, PymysqlQueueStore, "ENGINE=InnoDB"),
        ("psqlpy", "postgres", "PsqlpyConfig", {}, PsqlpyQueueStore, 'WHERE "status" IN'),
        ("psycopg", "postgres", "PsycopgSyncConfig", {}, PsycopgSyncQueueStore, 'WHERE "status" IN'),
        ("psycopg", "postgres", "PsycopgAsyncConfig", {}, PsycopgAsyncQueueStore, 'WHERE "status" IN'),
        ("spanner", "spanner", "SpannerConfig", {}, SpannerQueueStore, "CREATE UNIQUE NULL_FILTERED INDEX"),
        ("sqlite", "sqlite", "SqliteConfig", {}, SqliteQueueStore, '"queue_tasks"'),
    ),
)
async def test_sqlspec_backend_store_factory_covers_sqlspec_adapter_modules(
    adapter_name: "str",
    dialect: "str | None",
    config_type_name: "str",
    connection_config: "dict[str, object]",
    expected_store_type: "type[object]",
    expected_sql_fragment: "str",
) -> "None":
    store = create_queue_store(
        _fake_adapter_config(
            adapter_name, dialect=dialect, config_type_name=config_type_name, connection_config=connection_config
        ),
        table_name="queue_tasks",
    )

    assert isinstance(store, expected_store_type)
    assert store.__class__.__module__.startswith(f"litestar_queues.backends.sqlspec.stores.{adapter_name}.")
    assert expected_sql_fragment in "\n".join(store.create_statements())


def test_sqlspec_backend_registry_excludes_broken_mssql_python_adapter() -> "None":
    names = {case.name for case in QUEUE_BACKENDS}
    service_attrs = {case.name: case.service_attr for case in QUEUE_BACKENDS}

    assert "mssql-python" not in names
    assert "pymssql" in names
    assert service_attrs["pymssql"] == "mssql_service"


def test_sqlspec_spanner_store_uses_spanner_ddl_and_native_json_columns() -> "None":
    store = create_queue_store(_fake_adapter_config("spanner", dialect="spanner", config_type_name="SpannerConfig"))

    ddl = "\n".join(store.create_statements())

    assert isinstance(store, SpannerQueueStore)
    assert "STRING(64)" in ddl
    assert "INT64" in ddl
    assert "TIMESTAMP" in ddl
    assert "IF NOT EXISTS" not in ddl
    assert "PRIMARY KEY (`id`)" in ddl
    assert "`result_json` JSON NOT NULL" not in ddl
    assert "`metadata` JSON NOT NULL" in ddl
    assert "CREATE UNIQUE NULL_FILTERED INDEX" in ddl
    assert store._native_json_columns == frozenset({"args_json", "kwargs_json", "metadata_json", "result_json"})
    assert type(store.serialize_json("result_json", None)).__name__ == "JsonObject"
    assert type(store.serialize_json("metadata_json", {"source": "spanner"})).__name__ == "JsonObject"


async def test_sqlspec_backend_uses_spanner_update_ddl_for_schema_bootstrap() -> "None":
    class FakeOperation:
        def result(self) -> "None":
            return None

    class FakeDatabase:
        def __init__(self) -> "None":
            self.statement_batches: "list[tuple[str, ...]]" = []

        def update_ddl(self, ddl_statements: "list[str]") -> "FakeOperation":
            self.statement_batches.append(tuple(ddl_statements))
            return FakeOperation()

    database = FakeDatabase()
    config = _fake_adapter_config("spanner", dialect="spanner", config_type_name="SpannerSyncConfig")
    config.get_database = lambda: database
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(config=config, queue_table_name="queue_tasks", notifications=False)
    )

    await backend.open()
    await backend.create_schema()
    await backend.close()

    assert database.statement_batches
    assert all(len(batch) == 1 for batch in database.statement_batches)
    statements = [statement for batch in database.statement_batches for statement in batch]
    assert statements == create_queue_store(config, table_name="queue_tasks").create_statements()
    assert "PRIMARY KEY (`id`)" in statements[0]
    assert all("IF NOT EXISTS" not in statement for statement in statements)


@pytest.mark.parametrize(
    ("adapter_name", "dialect", "expected"),
    (
        ("asyncpg", "postgres", True),
        ("asyncmy", "mysql", True),
        ("pymysql", "mysql", True),
        ("psqlpy", "postgres", True),
        ("spanner", "spanner", False),
        ("aiosqlite", "sqlite", False),
        ("duckdb", "duckdb", False),
    ),
)
def test_sqlspec_store_supports_skip_locked_follows_data_dictionary_flags(
    adapter_name: "str", dialect: "str", expected: "bool"
) -> "None":
    """``supports_skip_locked`` gates off SQLSpec data-dictionary feature flags."""
    store = create_queue_store(_fake_adapter_config(adapter_name, dialect=dialect), table_name="queue_tasks")

    assert store.supports_skip_locked is expected


def test_sqlspec_oracledb_async_store_supports_skip_locked_from_data_dictionary() -> "None":
    """Async Oracle uses SQLSpec 0.52's Oracle SKIP LOCKED capability."""
    store = create_queue_store(
        _fake_adapter_config("oracledb", dialect="oracle", config_type_name="FakeOracleAsyncConfig"),
        table_name="queue_tasks",
    )

    assert isinstance(store, OracledbAsyncQueueStore)
    assert store.supports_skip_locked is True
    assert store.claim_select_stream_chunk_size == 1


def test_sqlspec_oracledb_sync_store_uses_cas_until_safe_streaming_claims() -> "None":
    """Sync Oracle stays on CAS because the sync bridge cannot bound locked rows."""
    store = create_queue_store(
        _fake_adapter_config("oracledb", dialect="oracle", config_type_name="FakeOracleSyncConfig"),
        table_name="queue_tasks",
    )

    assert isinstance(store, OracledbSyncQueueStore)
    assert store.supports_skip_locked is False


async def test_sqlspec_claim_next_skips_serialization_conflict_candidate(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """Optimistic CAS claims should treat serialization conflicts as claim contention."""
    from sqlspec.exceptions import SerializationConflictError

    first_id = uuid4()
    second_id = uuid4()
    claimed_record = QueuedTaskRecord(task_name="tasks.claimed", id=second_id, status="running")
    backend = SQLSpecQueueBackend()
    claim_attempts: "list[UUID]" = []

    class NoSkipLockedStore:
        supports_skip_locked = False

    async def select_pending_rows(
        self: "SQLSpecQueueBackend", *, limit: int, queue: str | None, execution_backend: str | None
    ) -> "list[dict[str, Any]]":
        del self, limit, queue, execution_backend
        return [{"id": str(first_id)}, {"id": str(second_id)}]

    async def claim_task(self: "SQLSpecQueueBackend", task_id: "UUID") -> "QueuedTaskRecord | None":
        del self
        claim_attempts.append(task_id)
        if task_id == first_id:
            msg = "restart transaction: WriteTooOldError"
            raise SerializationConflictError(msg)
        return claimed_record

    monkeypatch.setattr(SQLSpecQueueBackend, "_get_store", lambda _self: NoSkipLockedStore())
    monkeypatch.setattr(SQLSpecQueueBackend, "_select_pending_rows", select_pending_rows)
    monkeypatch.setattr(SQLSpecQueueBackend, "claim_task", claim_task)

    assert await backend.claim_next() is claimed_record
    assert claim_attempts == [first_id, second_id]


def test_sqlspec_store_supports_skip_locked_defaults_false_without_dialect() -> "None":
    """A config without dialect metadata degrades to optimistic CAS."""
    store = create_queue_store(_fake_adapter_config("aiosqlite", dialect=None), table_name="queue_tasks")

    assert store.supports_skip_locked is False


def test_sqlspec_store_select_claimable_uses_skip_locked_on_supporting_dialect() -> "None":
    """``select_claimable`` builds a due-task SELECT that locks rows with SKIP LOCKED."""
    store = create_queue_store(_fake_adapter_config("asyncpg", dialect="postgres"), table_name="queue_tasks")

    built = store.select_claimable(now="2026-01-01T00:00:00+00:00", limit=1, queue="default").build(dialect="postgres")

    assert "FOR UPDATE SKIP LOCKED" in built.sql
    assert 'FROM "queue_tasks"' in built.sql


def test_sqlspec_store_claim_tasks_builds_fenced_bulk_update() -> "None":
    """``claim_tasks`` transitions a batch of ids fenced on due status and time."""
    store = create_queue_store(_fake_adapter_config("asyncpg", dialect="postgres"), table_name="queue_tasks")

    built = store.claim_tasks(
        task_ids=["id-a", "id-b"],
        due_at="2026-01-01T00:00:00+00:00",
        started_at="2026-01-01T00:00:00+00:00",
        heartbeat_at="2026-01-01T00:00:00+00:00",
    ).build(dialect="postgres")
    sql_text = built.sql.upper()

    assert "UPDATE" in sql_text
    assert '"status" = ' in built.sql
    assert '"id" IN (' in built.sql
    assert '"status" IN (' in built.sql
    assert "SCHEDULED_AT" in sql_text
    assert "running" in str(built.parameters)


def test_sqlspec_store_select_tasks_by_ids_filters_to_requested_ids() -> "None":
    """``select_tasks_by_ids`` reloads exactly the requested rows for batch claim."""
    store = create_queue_store(_fake_adapter_config("asyncpg", dialect="postgres"), table_name="queue_tasks")

    built = store.select_tasks_by_ids(["id-a", "id-b"]).build(dialect="postgres")

    assert '"id" IN (' in built.sql
    assert 'FROM "queue_tasks"' in built.sql


async def test_sqlspec_aiosqlite_backend_falls_back_to_sequential_claim_many(
    sqlspec_backend: "SQLSpecQueueBackend",
) -> "None":
    """Adapters without RETURNING claims still batch through the sequential fallback."""
    high = await sqlspec_backend.enqueue("tasks.batch.high", priority=10)
    mid = await sqlspec_backend.enqueue("tasks.batch.mid", priority=5)
    low = await sqlspec_backend.enqueue("tasks.batch.low", priority=1)

    claimed = await sqlspec_backend.claim_many(limit=2)

    assert [record.id for record in claimed] == [high.id, mid.id]
    assert all(record.status == "running" for record in claimed)
    assert all(record.started_at is not None and record.heartbeat_at is not None for record in claimed)
    stored_low = await sqlspec_backend.get_task(low.id)
    assert stored_low is not None
    assert stored_low.status == "pending"


async def test_sqlspec_batch_claim_uses_single_transaction(
    queue_backend: "BaseQueueBackend", queue_backend_case: "BackendCase", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """Supported SQLSpec stores must claim a batch in exactly one session/transaction."""
    if not (
        isinstance(queue_backend, SQLSpecQueueBackend) and type(queue_backend._get_store()).supports_returning_claim
    ):
        pytest.skip(f"{queue_backend_case.name}: store has no native RETURNING batch claim")

    for index in range(5):
        await queue_backend.enqueue(f"tasks.batch.count.{index}", priority=5)

    original_session = SQLSpecQueueBackend._session
    session_calls = 0

    def counting_session(self: "SQLSpecQueueBackend", *args: "Any", **kwargs: "Any") -> "Any":
        nonlocal session_calls
        session_calls += 1
        return original_session(self, *args, **kwargs)

    monkeypatch.setattr(SQLSpecQueueBackend, "_session", counting_session)

    claimed = await queue_backend.claim_many(limit=5)

    assert len(claimed) == 5
    assert session_calls == 1


async def test_sqlspec_batch_claim_concurrent_workers_never_double_claim(
    queue_backend: "BaseQueueBackend", queue_backend_case: "BackendCase"
) -> "None":
    """Two and four concurrent batch claimers must skip locked rows, never duplicate."""
    if not (
        isinstance(queue_backend, SQLSpecQueueBackend) and type(queue_backend._get_store()).supports_returning_claim
    ):
        pytest.skip(f"{queue_backend_case.name}: store has no native RETURNING batch claim")

    for worker_count in (2, 4):
        task_count = 12
        enqueued = {
            (await queue_backend.enqueue(f"tasks.batch.contended.{worker_count}", priority=5)).id
            for _ in range(task_count)
        }

        bursts = await asyncio.gather(*(queue_backend.claim_many(limit=task_count) for _ in range(worker_count)))
        claimed = [record for burst in bursts for record in burst]
        claimed_ids = [record.id for record in claimed]
        while remainder := await queue_backend.claim_many(limit=task_count):
            claimed.extend(remainder)
            claimed_ids.extend(record.id for record in remainder)

        assert all(record.status == "running" for record in claimed)
        assert len(claimed_ids) == len(set(claimed_ids)), "a task was claimed by more than one batch worker"
        assert {task_id for task_id in claimed_ids if task_id in enqueued} == enqueued


async def test_sqlspec_sync_bridge_rolls_back_read_transactions_before_pool_return() -> "None":
    """Sync sessions must not return pooled connections with stale read snapshots."""
    manager = _FakeSyncSQLSpec()

    async with _bridge_session(manager, _FakeSyncConfig()) as driver:
        assert await driver.select("SELECT 1") == []

    assert manager.driver.rollback_count == 1


async def test_sqlspec_sync_bridge_skips_cleanup_rollback_after_commit() -> "None":
    """Committed sync sessions must not be rolled back during pool cleanup."""
    manager = _FakeSyncSQLSpec()

    async with _bridge_session(manager, _FakeSyncConfig()) as driver:
        await driver.begin()
        await driver.commit()

    assert manager.driver.begin_count == 1
    assert manager.driver.commit_count == 1
    assert manager.driver.rollback_count == 0


async def test_sqlspec_sync_bridge_can_skip_explicit_begin_for_driver_managed_transactions() -> "None":
    """Some sync drivers rely on their DB-API transaction lifecycle."""
    manager = _FakeSyncSQLSpec()

    async with _bridge_session(manager, _FakeSyncConfig(), skip_explicit_begin=True) as driver:
        await driver.begin()
        await driver.commit()

    assert manager.driver.begin_count == 0
    assert manager.driver.commit_count == 1
    assert manager.driver.rollback_count == 0


async def test_sqlspec_sync_bridge_can_skip_cleanup_rollback_for_driver_owned_sessions() -> "None":
    """Some sync drivers own cleanup in their session context manager."""
    manager = _FakeSyncSQLSpec()

    async with _bridge_session(manager, _FakeSyncConfig(), skip_cleanup_rollback=True) as driver:
        assert await driver.select("SELECT 1") == []

    assert manager.driver.rollback_count == 0
    assert manager.session.exit_count == 1


@pytest.mark.parametrize(
    ("table_name", "expected"),
    (
        ("queue_tasks", "queue_tasks"),
        ("tenant.queue_tasks", "tenant.queue_tasks"),
        ('"tenant"."queue_tasks"', "tenant.queue_tasks"),
        ("[tenant].[queue_tasks]", "tenant.queue_tasks"),
    ),
)
def test_sqlspec_backend_accepts_sqlspec_qualified_table_names(table_name: "str", expected: "str") -> "None":
    """Table-name validation follows SQLSpec's identifier splitter."""
    backend_config = SQLSpecBackendConfig(queue_table_name=table_name)
    store = create_queue_store(
        _fake_adapter_config("aiosqlite", dialect="sqlite"), table_name=backend_config.queue_table_name
    )

    assert backend_config.queue_table_name == expected
    assert store.table_name == expected
    assert ".".join(f'"{part}"' for part in expected.split(".")) in "\n".join(store.create_statements())


@pytest.mark.parametrize(
    "table_name", ("", "queue tasks", "queue;drop", "schema.", ".queue_tasks", "schema..queue_tasks")
)
def test_sqlspec_backend_rejects_invalid_table_names(table_name: "str") -> "None":
    with pytest.raises(QueueConfigurationError, match="Invalid SQLSpec queue table name"):
        SQLSpecBackendConfig(queue_table_name=table_name)


@pytest.mark.parametrize(
    ("adapter_name", "dialect", "config_type_name", "connection_config", "expected_fragment", "native_json_columns"),
    (
        ("aiosqlite", "sqlite", "AiosqliteConfig", {}, '"task_args" TEXT NOT NULL', frozenset()),
        ("duckdb", "duckdb", "DuckDBConfig", {}, '"task_args" JSON NOT NULL', frozenset()),
        (
            "asyncpg",
            "postgres",
            "AsyncpgConfig",
            {},
            '"task_args" JSONB NOT NULL',
            frozenset({"args_json", "kwargs_json", "metadata_json", "result_json"}),
        ),
        (
            "psqlpy",
            "postgres",
            "PsqlpyConfig",
            {},
            '"result" TEXT NOT NULL',
            frozenset({"args_json", "kwargs_json", "metadata_json"}),
        ),
        (
            "asyncmy",
            "mysql",
            "AsyncmyConfig",
            {},
            "`task_args` JSON NOT NULL",
            frozenset({"args_json", "kwargs_json", "metadata_json", "result_json"}),
        ),
        (
            "pymysql",
            "mysql",
            "PyMysqlConfig",
            {},
            "`task_args` JSON NOT NULL",
            frozenset({"args_json", "kwargs_json", "metadata_json", "result_json"}),
        ),
        (
            "spanner",
            "spanner",
            "SpannerConfig",
            {},
            "STRING(64)",
            frozenset({"args_json", "kwargs_json", "metadata_json", "result_json"}),
        ),
    ),
)
def test_sqlspec_store_capability_matrix_pins_json_and_bulk_capabilities(
    adapter_name: "str",
    dialect: "str | None",
    config_type_name: "str",
    connection_config: "dict[str, object]",
    expected_fragment: "str",
    native_json_columns: "frozenset[str]",
) -> "None":
    """Pin the matrix-visible JSON codec and bulk-ingest capability per store."""
    pytest.importorskip("pyarrow")
    store = create_queue_store(
        _fake_adapter_config(
            adapter_name,
            dialect=dialect,
            config_type_name=config_type_name,
            connection_config=connection_config,
            supports_native_arrow_import=True,
        ),
        table_name="queue_tasks",
    )

    assert expected_fragment in "\n".join(store.create_statements())
    assert store.supports_native_bulk_ingest is True
    assert store._native_json_columns == native_json_columns


@pytest.mark.parametrize(
    ("adapter_name", "config_type_name"),
    (
        ("aiomysql", "AiomysqlConfig"),
        ("asyncmy", "AsyncmyConfig"),
        ("pymysql", "PyMysqlConfig"),
        ("mysqlconnector", "MysqlConnectorSyncConfig"),
        ("mysqlconnector", "MysqlConnectorAsyncConfig"),
    ),
)
async def test_sqlspec_mysql_queue_store_uses_safe_index_prefixes(
    adapter_name: "str", config_type_name: "str"
) -> "None":
    store = create_queue_store(
        _fake_adapter_config(adapter_name, dialect="mysql", config_type_name=config_type_name), table_name="queue_tasks"
    )

    ddl = "\n".join(store.create_statements())

    assert "`status`(32), `queue`(191)" in ddl
    assert "`execution_backend`(191), `scheduled_at`" in ddl
    assert "`status`(32), `heartbeat_at`" in ddl
    assert "`task_key` VARCHAR(255) UNIQUE" in ddl


async def test_sqlspec_backend_deduplicates_active_keys_and_replaces_terminal_keys(
    sqlspec_backend: "SQLSpecQueueBackend",
) -> "None":
    first = await sqlspec_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-1"}, key="sync:acct-1")
    duplicate = await sqlspec_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")

    assert duplicate.id == first.id
    assert duplicate.kwargs == {"account_id": "acct-1"}

    await sqlspec_backend.complete_task(first.id, result={"ok": True})
    replacement = await sqlspec_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")

    assert replacement.id != first.id
    assert replacement.kwargs == {"account_id": "acct-2"}
    keyed = await sqlspec_backend.get_task_by_key("sync:acct-1")
    assert keyed is not None
    assert keyed.id == replacement.id


async def test_sqlspec_backend_reuses_winner_when_key_insert_races(monkeypatch: "pytest.MonkeyPatch") -> "None":
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(config=AiosqliteConfig(), queue_observability=False)
    )
    driver = _UniqueViolationDriver()
    winner = await InMemoryQueueBackend().enqueue("tasks.race", kwargs={"attempt": 1}, key="sync:race")

    @asynccontextmanager
    async def fake_session(_self: "SQLSpecQueueBackend") -> "AsyncIterator[_UniqueViolationDriver]":
        yield driver

    async def select_no_winner(_self: "SQLSpecQueueBackend", _driver: "Any", _key: "str") -> "None":
        return None

    async def get_winner(_self: "SQLSpecQueueBackend", key: "str") -> "QueuedTaskRecord | None":
        return winner if key == "sync:race" else None

    monkeypatch.setattr(SQLSpecQueueBackend, "_session", fake_session)
    monkeypatch.setattr(SQLSpecQueueBackend, "_select_task_by_key", select_no_winner)
    monkeypatch.setattr(SQLSpecQueueBackend, "_get_store", lambda _self: _InsertOnlyStore())
    monkeypatch.setattr(SQLSpecQueueBackend, "get_task_by_key", get_winner)

    record = await backend.enqueue("tasks.race", kwargs={"attempt": 2}, key="sync:race")

    assert record is winner
    assert driver.rolled_back is True


async def test_sqlspec_backend_claims_due_tasks_by_priority(sqlspec_backend: "SQLSpecQueueBackend") -> "None":
    later = datetime.now(timezone.utc) + timedelta(minutes=5)

    low = await sqlspec_backend.enqueue("tasks.low", priority=1)
    scheduled = await sqlspec_backend.enqueue("tasks.later", priority=100, scheduled_at=later)
    high = await sqlspec_backend.enqueue("tasks.high", priority=10)

    claimed = await sqlspec_backend.claim_next()

    assert claimed is not None
    assert claimed.id == high.id
    assert claimed.status == "running"
    assert claimed.started_at is not None
    stored_low = await sqlspec_backend.get_task(low.id)
    stored_scheduled = await sqlspec_backend.get_task(scheduled.id)
    assert stored_low is not None
    assert stored_scheduled is not None
    assert stored_low.status == "pending"
    assert stored_scheduled.status == "scheduled"


async def test_sqlspec_backend_fail_task_retries_then_fails_permanently(
    sqlspec_backend: "SQLSpecQueueBackend",
) -> "None":
    record = await sqlspec_backend.enqueue("tasks.flaky", max_retries=1)

    await sqlspec_backend.claim_task(record.id)
    retried = await sqlspec_backend.fail_task(record.id, "first failure")

    assert retried is not None
    assert retried.status == "pending"
    assert retried.retry_count == 1

    await sqlspec_backend.claim_task(record.id)
    failed = await sqlspec_backend.fail_task(record.id, "second failure")

    assert failed is not None
    assert failed.status == "failed"
    assert failed.error == "second failure"
    assert failed.completed_at is not None


async def test_sqlspec_backend_cancels_heartbeats_and_requeues_stale_running(
    sqlspec_backend: "SQLSpecQueueBackend",
) -> "None":
    pending = await sqlspec_backend.enqueue("tasks.cancel")

    assert await sqlspec_backend.cancel_task(pending.id)
    assert not await sqlspec_backend.cancel_task(pending.id)

    cancelled = await sqlspec_backend.get_task(pending.id)
    assert cancelled is not None
    assert cancelled.status == "cancelled"

    running = await sqlspec_backend.enqueue("tasks.heartbeat", max_retries=1)
    claimed = await sqlspec_backend.claim_task(running.id)

    assert claimed is not None
    assert claimed.heartbeat_at is not None

    result = await sqlspec_backend.touch_heartbeats([
        HeartbeatTouch(task_id=claimed.id, expected_retry_count=claimed.retry_count)
    ])
    touched = await sqlspec_backend.get_task(claimed.id)

    assert result.touched_task_ids == {claimed.id}
    assert result.missed_task_ids == set()
    assert touched is not None
    assert touched.heartbeat_at is not None
    assert touched.heartbeat_at >= claimed.heartbeat_at

    stale_result = await sqlspec_backend.requeue_stale_running(stale_after=timedelta(seconds=0))
    assert stale_result.requeued == 1
    requeued = await sqlspec_backend.get_task(claimed.id)

    assert requeued is not None
    assert requeued.status == "pending"
    assert requeued.retry_count == 1

    exhausted = await sqlspec_backend.enqueue("tasks.exhausted", max_retries=0)
    exhausted_claim = await sqlspec_backend.claim_task(exhausted.id)
    assert exhausted_claim is not None
    exhausted_result = await sqlspec_backend.requeue_stale_running(stale_after=timedelta(seconds=0))
    exhausted_stored = await sqlspec_backend.get_task(exhausted.id)

    assert exhausted_result.failed == 1
    assert exhausted_stored is not None
    assert exhausted_stored.status == "failed"
    assert exhausted_stored.error == "Task heartbeat stale"


async def test_sqlspec_backend_touch_heartbeats_merges_metadata_patch(sqlspec_backend: "SQLSpecQueueBackend") -> "None":
    record = await sqlspec_backend.enqueue("tasks.heartbeat.metadata", metadata={"existing": "kept"})
    claimed = await sqlspec_backend.claim_task(record.id)

    assert claimed is not None

    result = await sqlspec_backend.touch_heartbeats([
        HeartbeatTouch(
            task_id=claimed.id, expected_retry_count=claimed.retry_count, metadata_patch={"progress_detail": "row 5"}
        )
    ])
    touched = await sqlspec_backend.get_task(claimed.id)

    assert result.touched_task_ids == {claimed.id}
    assert result.missed_task_ids == set()
    assert touched is not None
    assert touched.metadata == {"existing": "kept", "progress_detail": "row 5"}


async def test_sqlspec_duckdb_touch_heartbeats_uses_bulk_path(
    duckdb_backend: "SQLSpecQueueBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    first = await duckdb_backend.enqueue(
        "tasks.heartbeat.duckdb.first",
        metadata={"existing": "kept", "nested": {"a": 1, "b": 2}, "nullable": "original"},
    )
    second = await duckdb_backend.enqueue("tasks.heartbeat.duckdb.second")
    mismatch = await duckdb_backend.enqueue("tasks.heartbeat.duckdb.mismatch")
    first_claim = await duckdb_backend.claim_task(first.id)
    second_claim = await duckdb_backend.claim_task(second.id)
    mismatch_claim = await duckdb_backend.claim_task(mismatch.id)

    assert first_claim is not None
    assert second_claim is not None
    assert mismatch_claim is not None

    async def fail_per_task_select(*_args: "object", **_kwargs: "object") -> "None":
        msg = "bulk heartbeat path must not use per-task _select_task"
        raise AssertionError(msg)

    with monkeypatch.context() as scoped:
        scoped.setattr(SQLSpecQueueBackend, "_select_task", fail_per_task_select)
        result = await duckdb_backend.touch_heartbeats([
            HeartbeatTouch(
                task_id=first_claim.id,
                expected_retry_count=first_claim.retry_count,
                metadata_patch={"progress_detail": "row 10", "nested": {"a": 3}, "nullable": None},
            ),
            HeartbeatTouch(task_id=second_claim.id, expected_retry_count=second_claim.retry_count),
            HeartbeatTouch(task_id=mismatch_claim.id, expected_retry_count=mismatch_claim.retry_count + 1),
        ])

    first_stored = await duckdb_backend.get_task(first.id)
    second_stored = await duckdb_backend.get_task(second.id)
    mismatch_stored = await duckdb_backend.get_task(mismatch.id)

    assert result.touched_task_ids == {first_claim.id, second_claim.id}
    assert result.missed_task_ids == {mismatch_claim.id}
    assert first_stored is not None
    assert first_stored.metadata == {
        "existing": "kept",
        "nested": {"a": 3},
        "nullable": None,
        "progress_detail": "row 10",
    }
    assert second_stored is not None
    assert second_stored.heartbeat_at is not None
    assert mismatch_stored is not None
    assert mismatch_stored.heartbeat_at == mismatch_claim.heartbeat_at


async def test_sqlspec_postgres_touch_heartbeats_uses_bulk_path(
    postgres_service: "PostgresService", request: "FixtureRequest", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    table_name = table_name_for_test("lq_heartbeat_asyncpg", "asyncpg", request.node.nodeid)
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=AsyncpgConfig(
                connection_config={
                    "host": postgres_service.host,
                    "port": postgres_service.port,
                    "user": postgres_service.user,
                    "password": postgres_service.password,
                    "database": postgres_service.database,
                }
            ),
            queue_table_name=table_name,
        )
    )
    await backend.open()
    await backend.create_schema()
    try:
        first = await backend.enqueue("tasks.heartbeat.postgres.first", metadata={"existing": "kept"})
        second = await backend.enqueue("tasks.heartbeat.postgres.second")
        mismatch = await backend.enqueue("tasks.heartbeat.postgres.mismatch")
        first_claim = await backend.claim_task(first.id)
        second_claim = await backend.claim_task(second.id)
        mismatch_claim = await backend.claim_task(mismatch.id)

        assert first_claim is not None
        assert second_claim is not None
        assert mismatch_claim is not None

        async def fail_per_task_select(*_args: "object", **_kwargs: "object") -> "None":
            msg = "bulk heartbeat path must not use per-task _select_task"
            raise AssertionError(msg)

        with monkeypatch.context() as scoped:
            scoped.setattr(SQLSpecQueueBackend, "_select_task", fail_per_task_select)
            result = await backend.touch_heartbeats([
                HeartbeatTouch(
                    task_id=first_claim.id,
                    expected_retry_count=first_claim.retry_count,
                    metadata_patch={"progress_detail": "row 20"},
                ),
                HeartbeatTouch(task_id=second_claim.id, expected_retry_count=second_claim.retry_count),
                HeartbeatTouch(task_id=mismatch_claim.id, expected_retry_count=mismatch_claim.retry_count + 1),
            ])

        first_stored = await backend.get_task(first.id)
        second_stored = await backend.get_task(second.id)
        mismatch_stored = await backend.get_task(mismatch.id)

        assert result.touched_task_ids == {first_claim.id, second_claim.id}
        assert result.missed_task_ids == {mismatch_claim.id}
        assert first_stored is not None
        assert first_stored.metadata == {"existing": "kept", "progress_detail": "row 20"}
        assert second_stored is not None
        assert second_stored.heartbeat_at is not None
        assert mismatch_stored is not None
        assert mismatch_stored.heartbeat_at == mismatch_claim.heartbeat_at
    finally:
        await backend.close()


@asynccontextmanager
async def _postgres_asyncpg_backend(
    postgres_service: "PostgresService", request: "FixtureRequest", prefix: "str"
) -> "AsyncIterator[SQLSpecQueueBackend]":
    """Yield an opened asyncpg-backed SQLSpec queue backend on a per-test table.

    Yields:
        The opened backend, closed on exit.
    """
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    table_name = table_name_for_test(prefix, "asyncpg", request.node.nodeid)
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=AsyncpgConfig(
                connection_config={
                    "host": postgres_service.host,
                    "port": postgres_service.port,
                    "user": postgres_service.user,
                    "password": postgres_service.password,
                    "database": postgres_service.database,
                }
            ),
            queue_table_name=table_name,
        )
    )
    await backend.open()
    await backend.create_schema()
    try:
        yield backend
    finally:
        await backend.close()


async def test_sqlspec_postgres_claim_many_single_statement_orders_and_fences(
    postgres_service: "PostgresService", request: "FixtureRequest"
) -> "None":
    """The native ``claim_many`` orders, filters, and never double-claims under SKIP LOCKED."""
    async with _postgres_asyncpg_backend(postgres_service, request, "lq_claim_many") as backend:
        assert type(backend._get_store()).supports_returning_claim is True

        high_first = await backend.enqueue("tasks.claim_many.high1", queue="q1", priority=10)
        high_second = await backend.enqueue("tasks.claim_many.high2", queue="q1", priority=10)
        mid = await backend.enqueue("tasks.claim_many.mid", queue="q1", priority=5)
        low = await backend.enqueue("tasks.claim_many.low", queue="q1", priority=1)
        other_queue = await backend.enqueue("tasks.claim_many.other_queue", queue="q2", priority=100)
        other_backend = await backend.enqueue(
            "tasks.claim_many.other_backend", queue="q1", priority=100, execution_backend="external"
        )

        claimed_order: "list[UUID]" = []
        for _ in range(4):
            batch = await backend.claim_many(limit=1, queues=("q1",), execution_backend="local")
            assert len(batch) == 1
            assert batch[0].status == "running"
            claimed_order.append(batch[0].id)

        assert claimed_order == [high_first.id, high_second.id, mid.id, low.id]
        assert await backend.claim_many(limit=5, queues=("q1",), execution_backend="local") == []

        untouched_queue = await backend.get_task(other_queue.id)
        untouched_backend = await backend.get_task(other_backend.id)
        assert untouched_queue is not None
        assert untouched_queue.status == "pending"
        assert untouched_backend is not None
        assert untouched_backend.status == "pending"

        concurrent_ids = {
            (await backend.enqueue("tasks.claim_many.concurrent", queue="q3", priority=5)).id for _ in range(8)
        }
        first, second = await asyncio.gather(
            backend.claim_many(limit=4, queues=("q3",)), backend.claim_many(limit=4, queues=("q3",))
        )
        claimed_ids = [record.id for record in (*first, *second)]
        assert len(claimed_ids) == len(set(claimed_ids)), "a task was claimed by more than one worker"
        while straggler := await backend.claim_many(limit=4, queues=("q3",)):
            claimed_ids.extend(record.id for record in straggler)
        assert len(claimed_ids) == len(set(claimed_ids))
        assert set(claimed_ids) == concurrent_ids


async def test_sqlspec_postgres_complete_clears_heartbeat_single_statement(
    postgres_service: "PostgresService", request: "FixtureRequest"
) -> "None":
    """The single-statement completion path clears the heartbeat and stores the result."""
    async with _postgres_asyncpg_backend(postgres_service, request, "lq_complete") as backend:
        record = await backend.enqueue("tasks.complete.single")
        claimed = await backend.claim_task(record.id)
        assert claimed is not None
        assert claimed.heartbeat_at is not None

        completed = await backend.complete_task(claimed.id, result={"ok": True})

        assert completed is not None
        assert completed.status == "completed"
        assert completed.heartbeat_at is None
        assert completed.result == {"ok": True}
        stored = await backend.get_task(record.id)
        assert stored is not None
        assert stored.status == "completed"
        assert stored.heartbeat_at is None


async def test_sqlspec_postgres_fail_retry_and_terminal_single_statement(
    postgres_service: "PostgresService", request: "FixtureRequest"
) -> "None":
    """The CASE-based fail path branches retry then terminal, clearing the heartbeat both times."""
    async with _postgres_asyncpg_backend(postgres_service, request, "lq_fail") as backend:
        record = await backend.enqueue("tasks.fail.single", max_retries=1)
        claimed = await backend.claim_task(record.id)
        assert claimed is not None

        retried = await backend.fail_task(record.id, "first failure")
        assert retried is not None
        assert retried.status == "pending"
        assert retried.retry_count == 1
        assert retried.heartbeat_at is None

        reclaimed = await backend.claim_task(record.id)
        assert reclaimed is not None
        assert reclaimed.retry_count == 1

        failed = await backend.fail_task(record.id, "second failure")
        assert failed is not None
        assert failed.status == "failed"
        assert failed.error == "second failure"
        assert failed.heartbeat_at is None
        assert failed.completed_at is not None


async def test_sqlspec_postgres_complete_fence_rejects_stale_retry_count(
    postgres_service: "PostgresService", request: "FixtureRequest"
) -> "None":
    """The completion fence rejects a stale ``expected_retry_count`` without mutating the row."""
    async with _postgres_asyncpg_backend(postgres_service, request, "lq_fence") as backend:
        record = await backend.enqueue("tasks.fence.stale", max_retries=1)
        claimed = await backend.claim_task(record.id)
        assert claimed is not None
        assert claimed.retry_count == 0

        retried = await backend.fail_task(record.id, "requeue")
        assert retried is not None
        assert retried.retry_count == 1
        reclaimed = await backend.claim_task(record.id)
        assert reclaimed is not None
        heartbeat = reclaimed.heartbeat_at

        rejected = await backend.complete_task(record.id, result="late", expected_retry_count=0)

        assert rejected is None
        stored = await backend.get_task(record.id)
        assert stored is not None
        assert stored.status == "running"
        assert stored.retry_count == 1
        assert stored.heartbeat_at == heartbeat
        assert stored.result is None


async def test_sqlspec_postgres_enqueue_returns_persisted_record(
    postgres_service: "PostgresService", request: "FixtureRequest"
) -> "None":
    """The RETURNING enqueue round-trips and keyed enqueue keeps deduplication."""
    async with _postgres_asyncpg_backend(postgres_service, request, "lq_enqueue") as backend:
        record = await backend.enqueue("tasks.enqueue.persisted", kwargs={"n": 1})
        stored = await backend.get_task(record.id)

        assert stored is not None
        assert stored.id == record.id
        assert stored.kwargs == {"n": 1}

        first = await backend.enqueue("tasks.enqueue.keyed", kwargs={"n": 1}, key="dedupe:enqueue")
        duplicate = await backend.enqueue("tasks.enqueue.keyed", kwargs={"n": 2}, key="dedupe:enqueue")

        assert duplicate.id == first.id
        assert duplicate.kwargs == {"n": 1}


async def test_sqlspec_duckdb_uses_legacy_fallback_paths(
    duckdb_backend: "SQLSpecQueueBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """DuckDB has no RETURNING gate, so enqueue/complete/fail route through the legacy methods."""
    store = duckdb_backend._get_store()
    assert type(store).supports_dml_returning is False
    assert type(store).supports_returning_claim is False

    invoked: "set[str]" = set()
    original_enqueue = SQLSpecQueueBackend._enqueue_legacy
    original_complete = SQLSpecQueueBackend._complete_task_legacy
    original_fail = SQLSpecQueueBackend._fail_task_legacy

    async def spy_enqueue(self: "SQLSpecQueueBackend", record: "QueuedTaskRecord") -> "QueuedTaskRecord":
        invoked.add("enqueue")
        return await original_enqueue(self, record)

    async def spy_complete(
        self: "SQLSpecQueueBackend", task_id: "UUID", *, result: "Any" = None, expected_retry_count: "int | None" = None
    ) -> "QueuedTaskRecord | None":
        invoked.add("complete")
        return await original_complete(self, task_id, result=result, expected_retry_count=expected_retry_count)

    async def spy_fail(
        self: "SQLSpecQueueBackend",
        task_id: "UUID",
        error: "str",
        *,
        retry: "bool" = True,
        expected_retry_count: "int | None" = None,
    ) -> "QueuedTaskRecord | None":
        invoked.add("fail")
        return await original_fail(self, task_id, error, retry=retry, expected_retry_count=expected_retry_count)

    monkeypatch.setattr(SQLSpecQueueBackend, "_enqueue_legacy", spy_enqueue)
    monkeypatch.setattr(SQLSpecQueueBackend, "_complete_task_legacy", spy_complete)
    monkeypatch.setattr(SQLSpecQueueBackend, "_fail_task_legacy", spy_fail)

    completed = await duckdb_backend.enqueue("tasks.legacy.complete")
    claimed = await duckdb_backend.claim_task(completed.id)
    assert claimed is not None
    done = await duckdb_backend.complete_task(claimed.id, result={"ok": True})
    assert done is not None
    assert done.status == "completed"

    flaky = await duckdb_backend.enqueue("tasks.legacy.fail", max_retries=1)
    claimed_flaky = await duckdb_backend.claim_task(flaky.id)
    assert claimed_flaky is not None
    retried = await duckdb_backend.fail_task(flaky.id, "boom")
    assert retried is not None
    assert retried.status == "pending"

    assert invoked == {"enqueue", "complete", "fail"}


async def test_sqlspec_backend_uses_sqlspec_json_serializer(sqlspec_backend: "SQLSpecQueueBackend") -> "None":
    encoded_at = datetime.now(timezone.utc)

    record = await sqlspec_backend.enqueue("tasks.metadata", metadata={"encoded_at": encoded_at})
    stored = await sqlspec_backend.get_task(record.id)

    assert stored is not None
    assert stored.metadata["encoded_at"] == encoded_at.isoformat().replace("+00:00", "Z")


def test_sqlspec_backend_does_not_create_sqlspec_litestar_plugin() -> "None":
    with pytest.raises(TypeError):
        SQLSpecQueueBackend(register_plugin=True)  # type: ignore[call-arg]


async def test_sqlspec_backend_can_start_with_packaged_migrations(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    db_path = tmp_path / "migrated.db"

    first_config = sqlite_config_factory(db_path)
    await run_queue_migrations(first_config)

    first = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(config=first_config))
    await first.open()
    await first.create_schema()
    await first.close()

    second_config = sqlite_config_factory(db_path)
    await run_queue_migrations(second_config)

    second = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(config=second_config))
    await second.open()
    await second.create_schema()
    try:
        record = await second.enqueue("tasks.migrated")
    finally:
        await second.close()

    assert record.task_name == "tasks.migrated"

    with sqlite3.connect(db_path) as connection:
        versions = [row[0] for row in connection.execute("SELECT version_num FROM ddl_migrations")]

    assert versions == ["ext_litestar_queues_0001"]


async def test_sqlspec_backend_packaged_migrations_do_not_mutate_adopter_config(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory", caplog: "pytest.LogCaptureFixture"
) -> "None":
    db_path = tmp_path / "migrated-config.db"
    sqlspec_config = sqlite_config_factory(db_path)
    original_extension_config = deepcopy(sqlspec_config.extension_config)
    original_migration_config = deepcopy(sqlspec_config.migration_config)
    await run_queue_migrations(sqlspec_config)

    backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(config=sqlspec_config))

    caplog.set_level(logging.WARNING, logger="sqlspec.migrations.base")
    await backend.open()
    await backend.create_schema()
    try:
        record = await backend.enqueue("tasks.migrated_config")
    finally:
        await backend.close()

    assert record.task_name == "tasks.migrated_config"
    assert not any("Extension litestar_queues not found" in entry.message for entry in caplog.records)
    assert deepcopy(sqlspec_config.extension_config) == original_extension_config
    assert deepcopy(sqlspec_config.migration_config) == original_migration_config


async def test_sqlspec_backend_uses_configured_table_name(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    db_path = tmp_path / "custom-table.db"
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(config=sqlite_config_factory(db_path), queue_table_name="queue_tasks")
    )

    await backend.open()
    await backend.create_schema()
    try:
        record = await backend.enqueue("tasks.custom_table")
    finally:
        await backend.close()

    assert record.task_name == "tasks.custom_table"
    with sqlite3.connect(db_path) as connection:
        table_names = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}

    assert "queue_tasks" in table_names
    assert "litestar_queue_task" not in table_names


async def test_sqlspec_backend_uses_structured_extension_config_when_explicit_values_are_absent(
    tmp_path: "Path",
) -> "None":
    db_path = tmp_path / "extension-config.db"
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        extension_config={QUEUE_EXTENSION_NAME: {"table_name": "extension_queue_tasks"}},
    )
    backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(config=sqlspec_config))

    await backend.open()
    await backend.create_schema()
    try:
        record = await backend.enqueue("tasks.extension_config")
    finally:
        await backend.close()

    assert record.task_name == "tasks.extension_config"
    with sqlite3.connect(db_path) as connection:
        table_names = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}

    assert "extension_queue_tasks" in table_names
    assert "litestar_queue_task" not in table_names


async def test_sqlspec_backend_explicit_config_values_override_sqlspec_extension_config(tmp_path: "Path") -> "None":
    db_path = tmp_path / "explicit-config.db"
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        extension_config={QUEUE_EXTENSION_NAME: {"table_name": "extension_queue_tasks"}},
    )
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(config=sqlspec_config, queue_table_name="explicit_queue_tasks")
    )

    await backend.open()
    await backend.create_schema()
    try:
        record = await backend.enqueue("tasks.explicit_config")
    finally:
        await backend.close()

    assert record.task_name == "tasks.explicit_config"
    with sqlite3.connect(db_path) as connection:
        table_names = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}

    assert "explicit_queue_tasks" in table_names
    assert "extension_queue_tasks" not in table_names


async def test_queue_service_uses_sqlspec_backend_from_config(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    @task("tasks.lower", retries=1)
    async def lowercase(value: "str") -> "str":
        return value.lower()

    backend_config = SQLSpecBackendConfig(config=sqlite_config_factory(tmp_path / "service.db"))
    await bootstrap_queue_schema(backend_config)
    config = QueueConfig(queue_backend=backend_config, execution_backend="local")

    async with QueueService(config) as service:
        result = await service.enqueue(lowercase, "QUEUE")

        pending_status = result.status
        assert pending_status == "pending"

        record = await service.get_queue_backend().claim_next()
        assert record is not None
        await service.execute_record(record)
        await result.refresh()

    completed_status = result.status
    assert completed_status == "completed"
    assert result.result == "queue"


async def _complete_report_task(backend: "BaseQueueBackend") -> "UUID":
    completed = await backend.enqueue("tasks.report")
    claimed = await backend.claim_task(completed.id)
    assert claimed is not None

    await backend.complete_task(claimed.id, result={"ok": True})

    return completed.id


class _InsertOnlyStore:
    bind_datetime_as_naive_utc = False
    bind_datetime_as_text = False

    def insert_task(self, params: "dict[str, object]") -> "str":
        del params
        return "insert into queue_tasks"

    def serialize_json(self, canonical: "str", value: "object") -> "object":
        del canonical
        return value


class _UniqueViolationDriver:
    def __init__(self) -> "None":
        self.rolled_back = False

    async def begin(self) -> "None":
        return None

    async def commit(self) -> "None":
        return None

    async def rollback(self) -> "None":
        self.rolled_back = True

    async def execute(self, statement: "object") -> "None":
        del statement
        msg = "UNIQUE constraint failed: litestar_queue_task.task_key"
        raise sqlite3.IntegrityError(msg)


class _FakeSyncConfig:
    is_async = False


class _FakeSyncSQLSpec:
    def __init__(self) -> "None":
        self.driver = _FakeSyncDriver()
        self.session = _FakeSyncSession(self.driver)

    def provide_session(self, config: "_FakeSyncConfig") -> "_FakeSyncSession":
        return self.session


class _FakeSyncSession:
    def __init__(self, driver: "_FakeSyncDriver") -> "None":
        self.driver = driver
        self.exit_count = 0

    def __enter__(self) -> "_FakeSyncDriver":
        return self.driver

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        _ = (exc_type, exc, traceback)
        self.exit_count += 1


class _FakeSyncDriver:
    def __init__(self) -> "None":
        self.begin_count = 0
        self.commit_count = 0
        self.rollback_count = 0
        self.selected: "list[object]" = []

    def begin(self) -> "None":
        self.begin_count += 1

    def commit(self) -> "None":
        self.commit_count += 1

    def select(self, statement: "object") -> "list[object]":
        self.selected.append(statement)
        return []

    def rollback(self) -> "None":
        self.rollback_count += 1

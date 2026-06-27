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
import sqlite3
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from subprocess import run
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

pytest.importorskip("aiosqlite")
pytest.importorskip("sqlspec")

from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues import QueueConfig, QueueService, task
from litestar_queues.backends import get_queue_backend_class, list_queue_backends
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.stores import (
    AdbcQueueStore,
    AiomysqlQueueStore,
    AiosqliteQueueStore,
    AsyncmyQueueStore,
    AsyncpgQueueStore,
    BigQueryQueueStore,
    CockroachAsyncpgQueueStore,
    CockroachPsycopgAsyncQueueStore,
    CockroachPsycopgSyncQueueStore,
    DuckDBQueueStore,
    MssqlPythonAsyncQueueStore,
    MssqlPythonSyncQueueStore,
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

if TYPE_CHECKING:
    from litestar_queues.backends import BaseQueueBackend
    from tests.integration._backends import BackendCase
    from tests.integration.backends.sqlspec.conftest import SqliteConfigFactory

pytestmark = pytest.mark.anyio


class FakeSQLSpecConfig(SimpleNamespace):
    """Structural config used by SQLSpec store dispatch tests."""

    extension_config: dict[str, object]
    statement_config: SimpleNamespace
    connection_config: dict[str, object]


def _fake_adapter_config(
    adapter_name: str,
    *,
    dialect: str | None = None,
    config_type_name: str | None = None,
    connection_config: dict[str, object] | None = None,
    extension_config: dict[str, object] | None = None,
) -> FakeSQLSpecConfig:
    config_type = cast(
        "type[FakeSQLSpecConfig]",
        type(
            config_type_name or f"Fake{adapter_name.title().replace('_', '')}Config",
            (FakeSQLSpecConfig,),
            {"__module__": f"sqlspec.adapters.{adapter_name}.config"},
        ),
    )
    config = config_type()
    config.extension_config = extension_config or {}
    config.statement_config = SimpleNamespace(dialect=dialect)
    config.connection_config = connection_config or {}
    return config


# ---------------------------------------------------------------------------
# Registry-parametrized contract tests
# ---------------------------------------------------------------------------


async def test_backend_contract_enqueue_claim_complete_cycle(queue_backend: "BaseQueueBackend") -> None:
    """A backend must support the full enqueue → claim → complete cycle."""
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
) -> None:
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


async def test_backend_contract_requeue_stale_running_recovers_every_task(queue_backend: "BaseQueueBackend") -> None:
    """Stale recovery must requeue every stale running task.

    For SQLSpec backends the per-row stale-recovery writes are batched into a
    single ``StatementStack`` / ``execute_stack`` call (pipelined on Oracle
    >=23ai and psycopg, sequential elsewhere). This asserts the batched writes
    actually apply on the real container behind each adapter.
    """
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


async def test_sqlspec_backend_supports_sync_sqlspec_config_via_sync_tools_bridge(tmp_path: "Path") -> None:
    """SQLSpecQueueBackend must support sync SQLSpec configs via sqlspec.utils.sync_tools."""
    from sqlspec.adapters.sqlite import SqliteConfig

    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            sqlspec_config=SqliteConfig(connection_config={"database": str(tmp_path / "queue-sync.db")})
        )
    )
    await backend.open()
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
    finally:
        await backend.close()


async def test_sqlspec_backend_is_registered_without_advanced_alchemy() -> None:
    assert "sqlspec" in list_queue_backends()
    assert get_queue_backend_class("sqlspec") is SQLSpecQueueBackend


def test_top_level_litestar_queues_import_does_not_pull_in_sqlspec() -> None:
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


def test_sqlspec_backend_store_factory_does_not_import_optional_adapter_drivers() -> None:
    code = """
import builtins
from types import SimpleNamespace

blocked_prefixes = (
    "adbc_driver_manager",
    "adbc_driver_bigquery",
    "adbc_driver_duckdb",
    "adbc_driver_flightsql",
    "adbc_driver_postgresql",
    "adbc_driver_snowflake",
    "adbc_driver_sqlite",
    "aiomysql",
    "aiosqlite",
    "arrow_odbc",
    "asyncmy",
    "asyncpg",
    "duckdb",
    "google.cloud.bigquery",
    "google.cloud.spanner",
    "mysql.connector",
    "mssql_python",
    "oracledb",
    "psqlpy",
    "psycopg",
    "pymysql",
    "sqlspec.adapters.adbc",
    "sqlspec.adapters.aiomysql",
    "sqlspec.adapters.aiosqlite",
    "sqlspec.adapters.arrow_odbc",
    "sqlspec.adapters.asyncmy",
    "sqlspec.adapters.asyncpg",
    "sqlspec.adapters.bigquery",
    "sqlspec.adapters.cockroach_asyncpg",
    "sqlspec.adapters.cockroach_psycopg",
    "sqlspec.adapters.duckdb",
    "sqlspec.adapters.mssql_python",
    "sqlspec.adapters.mysqlconnector",
    "sqlspec.adapters.oracledb",
    "sqlspec.adapters.psqlpy",
    "sqlspec.adapters.psycopg",
    "sqlspec.adapters.pymysql",
    "sqlspec.adapters.spanner",
)
blocked_package_prefixes = tuple(f"{name}." for name in blocked_prefixes)
original_import = builtins.__import__

def blocked_import(name, *args, **kwargs):
    if name in blocked_prefixes or name.startswith(blocked_package_prefixes):
        raise ModuleNotFoundError(name)
    return original_import(name, *args, **kwargs)

builtins.__import__ = blocked_import

from litestar_queues.backends.sqlspec.stores import (
    AdbcQueueStore,
    AiomysqlQueueStore,
    AiosqliteQueueStore,
    AsyncmyQueueStore,
    AsyncpgQueueStore,
    BigQueryQueueStore,
    CockroachAsyncpgQueueStore,
    CockroachPsycopgAsyncQueueStore,
    CockroachPsycopgSyncQueueStore,
    DuckDBQueueStore,
    MssqlPythonAsyncQueueStore,
    MssqlPythonQueueStore,
    MssqlPythonSyncQueueStore,
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

def fake_config(adapter_name, dialect, config_type_name):
    config_type = type(config_type_name, (), {"__module__": f"sqlspec.adapters.{adapter_name}.config"})
    config = config_type()
    config.extension_config = {}
    config.statement_config = SimpleNamespace(dialect=dialect)
    config.connection_config = {}
    return config

expected = (
    ("adbc", "adbc", "FakeAdbcConfig", AdbcQueueStore),
    ("aiomysql", "mysql", "AiomysqlConfig", AiomysqlQueueStore),
    ("aiosqlite", "sqlite", "AiosqliteConfig", AiosqliteQueueStore),
    ("asyncmy", "mysql", "AsyncmyConfig", AsyncmyQueueStore),
    ("asyncpg", "postgres", "AsyncpgConfig", AsyncpgQueueStore),
    ("bigquery", "bigquery", "BigQueryConfig", BigQueryQueueStore),
    ("cockroach_asyncpg", "postgres", "CockroachAsyncpgConfig", CockroachAsyncpgQueueStore),
    ("cockroach_psycopg", "postgres", "CockroachPsycopgSyncConfig", CockroachPsycopgSyncQueueStore),
    ("cockroach_psycopg", "postgres", "CockroachPsycopgAsyncConfig", CockroachPsycopgAsyncQueueStore),
    ("duckdb", "duckdb", "DuckDBConfig", DuckDBQueueStore),
    ("mssql_python", "tsql", "MssqlPythonConfig", MssqlPythonSyncQueueStore),
    ("mssql_python", "tsql", "MssqlPythonAsyncConfig", MssqlPythonAsyncQueueStore),
    ("mysqlconnector", "mysql", "MysqlConnectorSyncConfig", MysqlConnectorSyncQueueStore),
    ("mysqlconnector", "mysql", "MysqlConnectorAsyncConfig", MysqlConnectorAsyncQueueStore),
    ("oracledb", "oracle", "OracleSyncConfig", OracledbSyncQueueStore),
    ("oracledb", "oracle", "OracleAsyncConfig", OracledbAsyncQueueStore),
    ("psqlpy", "postgres", "PsqlpyConfig", PsqlpyQueueStore),
    ("psycopg", "postgres", "PsycopgSyncConfig", PsycopgSyncQueueStore),
    ("psycopg", "postgres", "PsycopgAsyncConfig", PsycopgAsyncQueueStore),
    ("pymysql", "mysql", "PyMysqlConfig", PymysqlQueueStore),
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
) -> None:
    backend_config = SQLSpecBackendConfig(table_name="queue_tasks")
    store = create_queue_store(sqlite_config_factory(tmp_path / "queue.db"), table_name=backend_config.table_name)

    assert backend_config.table_name == "queue_tasks"
    assert isinstance(store, AiosqliteQueueStore)
    assert store.table_name == "queue_tasks"
    assert any('"queue_tasks"' in statement for statement in store.create_statements())

    insert_statement = store.insert_task({"id": "task-1", "task_name": "tasks.sync"}).build(dialect="sqlite")
    pending_statement = store.list_pending(now=datetime.now(UTC).isoformat(), limit=10, queue="default").build(
        dialect="sqlite"
    )

    assert 'INSERT INTO "queue_tasks"' in insert_statement.sql
    assert "task-1" in insert_statement.parameters.values()
    assert 'FROM "queue_tasks"' in pending_statement.sql
    assert "queue" in pending_statement.sql


def test_sqlspec_backend_rejects_unsupported_sqlspec_adapter() -> None:
    with pytest.raises(QueueConfigurationError, match="arrow_odbc"):
        create_queue_store(
            _fake_adapter_config("arrow_odbc", dialect="sqlite", config_type_name="ArrowOdbcConfig"),
            table_name="queue_tasks",
        )


def test_sqlspec_backend_store_factory_tracks_sqlspec_event_store_adapters() -> None:
    import sqlspec.adapters

    event_store_adapters = {
        event_store.parent.parent.name
        for adapter_path in (Path(base) for base in sqlspec.adapters.__path__)
        for event_store in adapter_path.glob("*/events/store.py")
    }
    supported_adapters = {
        "adbc",
        "aiomysql",
        "aiosqlite",
        "asyncmy",
        "asyncpg",
        "bigquery",
        "cockroach_asyncpg",
        "cockroach_psycopg",
        "duckdb",
        "mssql_python",
        "mysqlconnector",
        "oracledb",
        "psqlpy",
        "psycopg",
        "pymysql",
        "spanner",
        "sqlite",
    }

    assert event_store_adapters <= supported_adapters
    assert "arrow_odbc" not in event_store_adapters


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
        ("adbc", "duckdb", "FakeAdbcConfig", {"driver_name": "adbc_driver_duckdb"}, AdbcQueueStore, "JSON"),
        (
            "adbc",
            "postgres",
            "FakeAdbcConfig",
            {"driver_name": "adbc_driver_postgresql"},
            AdbcQueueStore,
            'WHERE "status" IN',
        ),
        ("adbc", "bigquery", "FakeAdbcConfig", {"driver_name": "adbc_driver_bigquery"}, AdbcQueueStore, "CLUSTER BY"),
        ("adbc", None, "FakeAdbcConfig", {"driver_name": "adbc_driver_snowflake"}, AdbcQueueStore, "VARIANT"),
        ("aiomysql", "mysql", "AiomysqlConfig", {}, AiomysqlQueueStore, "ENGINE=InnoDB"),
        ("aiosqlite", "sqlite", "AiosqliteConfig", {}, AiosqliteQueueStore, '"queue_tasks"'),
        ("asyncmy", "mysql", "AsyncmyConfig", {}, AsyncmyQueueStore, "ENGINE=InnoDB"),
        ("asyncpg", "postgres", "AsyncpgConfig", {}, AsyncpgQueueStore, 'WHERE "status" IN'),
        ("bigquery", "bigquery", "BigQueryConfig", {}, BigQueryQueueStore, "CREATE TABLE"),
        ("cockroach_asyncpg", "postgres", "CockroachAsyncpgConfig", {}, CockroachAsyncpgQueueStore, "TIMESTAMPTZ"),
        (
            "cockroach_psycopg",
            "postgres",
            "CockroachPsycopgSyncConfig",
            {},
            CockroachPsycopgSyncQueueStore,
            "TIMESTAMPTZ",
        ),
        (
            "cockroach_psycopg",
            "postgres",
            "CockroachPsycopgAsyncConfig",
            {},
            CockroachPsycopgAsyncQueueStore,
            "TIMESTAMPTZ",
        ),
        ("duckdb", "duckdb", "DuckDBConfig", {}, DuckDBQueueStore, "JSON"),
        ("mssql_python", "tsql", "MssqlPythonConfig", {}, MssqlPythonSyncQueueStore, "DATETIME2(6)"),
        ("mssql_python", "tsql", "MssqlPythonAsyncConfig", {}, MssqlPythonAsyncQueueStore, "DATETIME2(6)"),
        ("mysqlconnector", "mysql", "MysqlConnectorSyncConfig", {}, MysqlConnectorSyncQueueStore, "ENGINE=InnoDB"),
        ("mysqlconnector", "mysql", "MysqlConnectorAsyncConfig", {}, MysqlConnectorAsyncQueueStore, "ENGINE=InnoDB"),
        ("oracledb", "oracle", "OracleSyncConfig", {}, OracledbSyncQueueStore, "BLOB CHECK (args_json IS JSON)"),
        ("oracledb", "oracle", "OracleAsyncConfig", {}, OracledbAsyncQueueStore, "BLOB CHECK (args_json IS JSON)"),
        ("psqlpy", "postgres", "PsqlpyConfig", {}, PsqlpyQueueStore, 'WHERE "status" IN'),
        ("psycopg", "postgres", "PsycopgSyncConfig", {}, PsycopgSyncQueueStore, 'WHERE "status" IN'),
        ("psycopg", "postgres", "PsycopgAsyncConfig", {}, PsycopgAsyncQueueStore, 'WHERE "status" IN'),
        ("pymysql", "mysql", "PyMysqlConfig", {}, PymysqlQueueStore, "ENGINE=InnoDB"),
        ("spanner", "spanner", "SpannerConfig", {}, SpannerQueueStore, "PRIMARY KEY"),
        ("sqlite", "sqlite", "SqliteConfig", {}, SqliteQueueStore, '"queue_tasks"'),
    ),
)
async def test_sqlspec_backend_store_factory_covers_sqlspec_adapter_modules(
    adapter_name: str,
    dialect: str | None,
    config_type_name: str,
    connection_config: dict[str, object],
    expected_store_type: type[object],
    expected_sql_fragment: str,
) -> None:
    store = create_queue_store(
        _fake_adapter_config(
            adapter_name, dialect=dialect, config_type_name=config_type_name, connection_config=connection_config
        ),
        table_name="queue_tasks",
    )

    assert isinstance(store, expected_store_type)
    assert store.__class__.__module__.startswith(f"litestar_queues.backends.sqlspec.stores.{adapter_name}.")
    assert expected_sql_fragment in "\n".join(store.create_statements())


def _event_hinted_config(
    adapter_name: str, *, select_for_update: bool, skip_locked: bool, dialect: str | None = None
) -> FakeSQLSpecConfig:
    """Fake adapter config advertising SQLSpec event runtime hints."""
    from sqlspec.extensions.events import EventRuntimeHints

    config = _fake_adapter_config(adapter_name, dialect=dialect)
    config.get_event_runtime_hints = lambda: EventRuntimeHints(  # type: ignore[attr-defined]
        select_for_update=select_for_update, skip_locked=skip_locked
    )
    return config


@pytest.mark.parametrize(
    ("adapter_name", "select_for_update", "skip_locked", "expected"),
    (
        ("asyncpg", True, True, True),
        ("asyncmy", True, True, True),
        ("psqlpy", True, True, True),
        ("aiosqlite", False, False, False),
        ("duckdb", False, False, False),
        # Oracle supports FOR UPDATE SKIP LOCKED, but its config advertises False today
        # (sqlspec FR litestar-org/sqlspec#544); the gate must honour the config, not guess.
        ("oracledb", False, False, False),
    ),
)
def test_sqlspec_store_supports_skip_locked_follows_config_event_hints(
    adapter_name: str, select_for_update: bool, skip_locked: bool, expected: bool
) -> None:
    """``supports_skip_locked`` gates off the adapter config's event runtime hints."""
    store = create_queue_store(
        _event_hinted_config(adapter_name, select_for_update=select_for_update, skip_locked=skip_locked),
        table_name="queue_tasks",
    )

    assert store.supports_skip_locked is expected


def test_sqlspec_store_supports_skip_locked_defaults_false_without_hints() -> None:
    """A config that does not advertise event hints degrades to optimistic CAS."""
    store = create_queue_store(_fake_adapter_config("aiosqlite", dialect="sqlite"), table_name="queue_tasks")

    assert store.supports_skip_locked is False


def test_sqlspec_store_select_claimable_uses_skip_locked_on_supporting_dialect() -> None:
    """``select_claimable`` builds a due-task SELECT that locks rows with SKIP LOCKED."""
    store = create_queue_store(
        _event_hinted_config("asyncpg", select_for_update=True, skip_locked=True, dialect="postgres"),
        table_name="queue_tasks",
    )

    built = store.select_claimable(now="2026-01-01T00:00:00+00:00", limit=1, queue="default").build(dialect="postgres")

    assert "FOR UPDATE SKIP LOCKED" in built.sql
    assert 'FROM "queue_tasks"' in built.sql


@pytest.mark.parametrize(
    ("adapter_name", "config_type_name"),
    (
        ("aiomysql", "AiomysqlConfig"),
        ("asyncmy", "AsyncmyConfig"),
        ("mysqlconnector", "MysqlConnectorSyncConfig"),
        ("mysqlconnector", "MysqlConnectorAsyncConfig"),
        ("pymysql", "PyMysqlConfig"),
    ),
)
async def test_sqlspec_mysql_queue_store_uses_safe_index_prefixes(adapter_name: str, config_type_name: str) -> None:
    store = create_queue_store(
        _fake_adapter_config(adapter_name, dialect="mysql", config_type_name=config_type_name), table_name="queue_tasks"
    )

    ddl = "\n".join(store.create_statements())

    assert "`status`(32), `queue`(191)" in ddl
    assert "`execution_backend`(191), `scheduled_at`" in ddl
    assert "`status`(32), `heartbeat_at`" in ddl
    assert "`task_key` VARCHAR(255) UNIQUE" in ddl


async def test_sqlspec_backend_deduplicates_active_keys_and_replaces_terminal_keys(
    sqlspec_backend: SQLSpecQueueBackend,
) -> None:
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


async def test_sqlspec_backend_claims_due_tasks_by_priority(sqlspec_backend: SQLSpecQueueBackend) -> None:
    later = datetime.now(UTC) + timedelta(minutes=5)

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


async def test_sqlspec_backend_fail_task_retries_then_fails_permanently(sqlspec_backend: SQLSpecQueueBackend) -> None:
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
    sqlspec_backend: SQLSpecQueueBackend,
) -> None:
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

    await sqlspec_backend.touch_heartbeat(claimed.id)
    touched = await sqlspec_backend.get_task(claimed.id)

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


async def test_sqlspec_backend_uses_sqlspec_json_serializer(sqlspec_backend: SQLSpecQueueBackend) -> None:
    encoded_at = datetime.now(UTC)

    record = await sqlspec_backend.enqueue("tasks.metadata", metadata={"encoded_at": encoded_at})
    stored = await sqlspec_backend.get_task(record.id)

    assert stored is not None
    assert stored.metadata["encoded_at"] == encoded_at.isoformat().replace("+00:00", "Z")


def test_sqlspec_backend_does_not_create_sqlspec_litestar_plugin() -> None:
    with pytest.raises(TypeError):
        SQLSpecQueueBackend(register_plugin=True)  # type: ignore[call-arg]


async def test_sqlspec_backend_can_start_with_packaged_migrations(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> None:
    db_path = tmp_path / "migrated.db"

    first = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            sqlspec_config=sqlite_config_factory(db_path), create_schema=False, run_migrations=True
        )
    )
    await first.open()
    await first.close()

    second = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            sqlspec_config=sqlite_config_factory(db_path), create_schema=False, run_migrations=True
        )
    )
    await second.open()
    try:
        record = await second.enqueue("tasks.migrated")
    finally:
        await second.close()

    assert record.task_name == "tasks.migrated"

    with sqlite3.connect(db_path) as connection:
        versions = [row[0] for row in connection.execute("SELECT version_num FROM ddl_migrations")]

    assert versions == ["ext_litestar_queues_0001"]


async def test_sqlspec_backend_uses_configured_table_name(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> None:
    db_path = tmp_path / "custom-table.db"
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(sqlspec_config=sqlite_config_factory(db_path), table_name="queue_tasks")
    )

    await backend.open()
    try:
        record = await backend.enqueue("tasks.custom_table")
    finally:
        await backend.close()

    assert record.task_name == "tasks.custom_table"
    with sqlite3.connect(db_path) as connection:
        table_names = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}

    assert "queue_tasks" in table_names
    assert "litestar_queue_tasks" not in table_names


async def test_sqlspec_backend_uses_structured_extension_config_when_explicit_values_are_absent(
    tmp_path: "Path",
) -> None:
    db_path = tmp_path / "extension-config.db"
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        extension_config={QUEUE_EXTENSION_NAME: {"table_name": "extension_queue_tasks"}},
    )
    backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(sqlspec_config=sqlspec_config))

    await backend.open()
    try:
        record = await backend.enqueue("tasks.extension_config")
    finally:
        await backend.close()

    assert record.task_name == "tasks.extension_config"
    with sqlite3.connect(db_path) as connection:
        table_names = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}

    assert "extension_queue_tasks" in table_names
    assert "litestar_queue_tasks" not in table_names


async def test_sqlspec_backend_explicit_config_values_override_sqlspec_extension_config(tmp_path: "Path") -> None:
    db_path = tmp_path / "explicit-config.db"
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        extension_config={QUEUE_EXTENSION_NAME: {"table_name": "extension_queue_tasks"}},
    )
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(sqlspec_config=sqlspec_config, table_name="explicit_queue_tasks")
    )

    await backend.open()
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
) -> None:
    @task("tasks.lower", retries=1)
    async def lowercase(value: str) -> str:
        return value.lower()

    config = QueueConfig(
        queue_backend=SQLSpecBackendConfig(sqlspec_config=sqlite_config_factory(tmp_path / "service.db")),
        execution_backend="local",
    )

    async with QueueService(config) as service:
        result = await service.enqueue(lowercase, "QUEUE")

        pending_status = result.status
        assert pending_status == "pending"

        record = await service.claim_next()
        assert record is not None
        await service.execute_record(record)
        await result.refresh()

    completed_status = result.status
    assert completed_status == "completed"
    assert result.result == "queue"

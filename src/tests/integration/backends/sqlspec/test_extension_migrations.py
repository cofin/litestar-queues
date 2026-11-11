"""Extension-migration tests for the SQLSpec queue backend."""

import contextlib
import importlib
import sqlite3
from datetime import UTC, datetime
from inspect import isawaitable
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

pytest.importorskip("sqlspec")

from click import Group
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.schema import SchemaTarget

from litestar_queues import QueueConfig, QueuePlugin, WorkerConfig
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend, SQLSpecWorkerWakeupConfig
from litestar_queues.backends.sqlspec.event_log import create_event_log_store
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME, configure_queue_migration_extension
from litestar_queues.backends.sqlspec.migrations.reconcile import (
    down,
    downgrade,
    reconcile,
    reconcile_events_queue_async,
    reconcile_events_queue_sync,
    reconcile_stores_async,
    reconcile_stores_sync,
)
from litestar_queues.backends.sqlspec.schema import (
    event_history_table_name_for,
    maintenance_table_name_for,
    migration_directory,
    task_reservation_table_name_for,
)
from litestar_queues.backends.sqlspec.stores.factory import create_queue_store
from litestar_queues.backends.sqlspec.stores.sqlite import SqliteQueueStore
from litestar_queues.events import EventHistoryConfig, EventHistoryExtraColumn, QueueEvent
from tests.integration._names import table_name_for_test
from tests.integration.backends.sqlspec._schema import run_queue_migrations

_AdbcConfig: Any
try:
    from sqlspec.adapters.adbc import AdbcConfig as _AdbcConfig
except ImportError:  # pragma: no cover
    _AdbcConfig = None

_AiosqliteConfig: Any
try:
    from sqlspec.adapters.aiosqlite import AiosqliteConfig as _AiosqliteConfig
except ImportError:  # pragma: no cover
    _AiosqliteConfig = None

_AsyncmyConfig: Any
try:
    from sqlspec.adapters.asyncmy import AsyncmyConfig as _AsyncmyConfig
except ImportError:  # pragma: no cover
    _AsyncmyConfig = None

_AsyncpgConfig: Any
try:
    from sqlspec.adapters.asyncpg import AsyncpgConfig as _AsyncpgConfig
except ImportError:  # pragma: no cover
    _AsyncpgConfig = None

_CockroachAsyncpgConfig: Any
try:
    from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig as _CockroachAsyncpgConfig
except ImportError:  # pragma: no cover
    _CockroachAsyncpgConfig = None

_MssqlPythonConfig: Any
try:
    from sqlspec.adapters.mssql_python import MssqlPythonConfig as _MssqlPythonConfig
except ImportError:  # pragma: no cover
    _MssqlPythonConfig = None

_PymssqlConfig: Any
try:
    from sqlspec.adapters.pymssql import PymssqlConfig as _PymssqlConfig
except ImportError:  # pragma: no cover
    _PymssqlConfig = None

_SpannerSyncConfig: Any
try:
    from sqlspec.adapters.spanner import SpannerSyncConfig as _SpannerSyncConfig
except ImportError:  # pragma: no cover
    _SpannerSyncConfig = None

if TYPE_CHECKING:
    from pathlib import Path

    from pytest import FixtureRequest

    from litestar_queues.backends.sqlspec.typing import ExtensionMigrationMode
    from tests.integration._backends import PostgresService

pytestmark = pytest.mark.anyio


class FakeSQLSpecConfig(SimpleNamespace):
    """Structural config used by SQLSpec store dispatch tests."""

    is_async: ClassVar[bool] = False
    extension_config: "dict[str, object]"
    statement_config: "SimpleNamespace"
    connection_config: "dict[str, object]"


def _fake_adapter_config(
    adapter_name: "str",
    *,
    dialect: "str | None" = None,
    config_type_name: "str | None" = None,
    is_async: "bool" = False,
    connection_config: "dict[str, object] | None" = None,
    extension_config: "dict[str, object] | None" = None,
) -> "FakeSQLSpecConfig":
    config_type = cast(
        "type[FakeSQLSpecConfig]",
        type(
            config_type_name or f"Fake{adapter_name.title().replace('_', '')}Config",
            (),
            {"__module__": f"sqlspec.adapters.{adapter_name}.config", "is_async": is_async},
        ),
    )
    config = config_type()
    config.extension_config = extension_config or {}
    config.statement_config = SimpleNamespace(dialect=dialect)
    config.connection_config = connection_config or {}
    return config


async def test_sqlspec_backend_migration_uses_adapter_specific_queue_store() -> "None":
    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    context = SimpleNamespace(config=_fake_adapter_config("duckdb", dialect="duckdb"))

    statements = await migration.up(context)

    assert "CREATE TABLE IF NOT EXISTS" in statements[0]
    assert "JSON" in statements[0]
    assert any("queue_maintenance" in statement for statement in statements)
    assert any("queue_task_reservation" in statement for statement in statements)


async def test_sqlspec_backend_migration_creates_coordination_and_reservation_tables() -> "None":
    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    context = SimpleNamespace(config=_fake_adapter_config("duckdb", dialect="duckdb"))

    statements = await migration.up(context)
    assert any("queue_maintenance" in statement for statement in statements)
    assert any(
        "CREATE TABLE IF NOT EXISTS" in statement
        and "queue_task_reservation" in statement
        and "identity_key" in statement
        for statement in statements
    )

    down_statements = await migration.down(context)
    assert any("queue_maintenance" in statement for statement in down_statements)
    assert any("queue_task_reservation" in statement for statement in down_statements)


async def test_sqlspec_backend_migration_orders_coordination_tables_safely() -> "None":
    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    context = SimpleNamespace(config=_fake_adapter_config("duckdb", dialect="duckdb"))

    statements = await migration.up(context)
    maintenance_create = next(index for index, statement in enumerate(statements) if "_maintenance" in statement)
    reservation_create = next(index for index, statement in enumerate(statements) if "_reservation" in statement)
    assert maintenance_create < reservation_create

    down_statements = await migration.down(context)
    reservation_drop = next(index for index, statement in enumerate(down_statements) if "_reservation" in statement)
    maintenance_drop = next(index for index, statement in enumerate(down_statements) if "_maintenance" in statement)
    assert reservation_drop < maintenance_drop


async def test_sqlspec_backend_migration_uses_configured_table_names() -> "None":
    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    context = SimpleNamespace(
        config=_fake_adapter_config(
            "duckdb",
            dialect="duckdb",
            extension_config={
                QUEUE_EXTENSION_NAME: {
                    "queue_table_name": "custom_queue",
                    "maintenance_table_name": "custom_maintenance",
                    "task_reservation_table_name": "custom_reservation",
                }
            },
        )
    )

    statements = await migration.up(context)
    assert any("custom_maintenance" in statement for statement in statements)
    assert any("custom_reservation" in statement for statement in statements)


async def test_queue_plugin_keeps_runtime_and_migration_table_overrides_aligned() -> "None":
    if _AiosqliteConfig is None:
        pytest.skip("aiosqlite adapter is not installed")

    sqlspec_config = _AiosqliteConfig(connection_config={"database": ":memory:"})
    backend_config = SQLSpecBackendConfig(
        sqlspec_config=sqlspec_config,
        queue_table_name="custom_queue",
        maintenance_table_name="custom_maintenance",
        task_reservation_table_name="custom_reservation",
    )
    plugin = QueuePlugin(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend=backend_config, initialize_schedules=False)
    )

    plugin.on_cli_init(Group())

    queue_settings = sqlspec_config.get_migration_commands().extension_configs[QUEUE_EXTENSION_NAME]
    assert queue_settings == {
        "queue_table_name": "custom_queue",
        "maintenance_table_name": "custom_maintenance",
        "task_reservation_table_name": "custom_reservation",
        "column_map": dict(backend_config.column_map),
        "migrations_path": migration_directory(),
        "migration_mode": "reconcile",
    }

    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    migration_config = _fake_adapter_config(
        "aiosqlite", dialect="sqlite", extension_config={QUEUE_EXTENSION_NAME: queue_settings}
    )
    statements = await migration.up(SimpleNamespace(config=migration_config))
    assert any("custom_maintenance" in statement for statement in statements)
    assert any("custom_reservation" in statement for statement in statements)

    backend = SQLSpecQueueBackend(backend_config=backend_config)
    assert backend._maintenance_table_name == queue_settings["maintenance_table_name"]
    assert backend._task_reservation_table_name == queue_settings["task_reservation_table_name"]


async def test_sqlspec_backend_migration_derives_names_from_custom_queue_table() -> "None":
    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    context = SimpleNamespace(
        config=_fake_adapter_config(
            "duckdb", dialect="duckdb", extension_config={QUEUE_EXTENSION_NAME: {"queue_table_name": "custom_queue"}}
        )
    )

    statements = await migration.up(context)
    assert any("custom_queue_maintenance" in statement for statement in statements)
    assert any("custom_queue_reservation" in statement for statement in statements)


async def test_sqlspec_backend_exposes_packaged_migration_assets() -> "None":
    paths = sorted(migration_directory().glob("[0-9]*.py"))

    assert [path.name for path in paths] == ["0001_create_queue_tasks.py"]
    migration_content = paths[0].read_text()
    assert "create_queue_store" in migration_content
    assert "create_maintenance_store" in migration_content
    assert "create_task_reservation_store" in migration_content
    assert "return SQLSpecQueueStore(" not in migration_content
    assert "CREATE TABLE IF NOT EXISTS queue_task" not in migration_content


async def test_sqlspec_backend_initial_migration_includes_expiration() -> "None":
    """The initial migration creates the expiration column on a fresh database."""
    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    context = SimpleNamespace(config=_fake_adapter_config("aiosqlite", dialect="sqlite"))

    statements = await migration.up(context)

    assert "expires_at" in statements[0]

    with contextlib.closing(sqlite3.connect(":memory:")) as connection:
        for statement in statements:
            connection.executescript(statement)
        columns = connection.execute("PRAGMA table_info(queue_task)").fetchall()

    assert [column[1] for column in columns].count("expires_at") == 1


async def test_sqlspec_backend_packaged_migration_down_drops_migrated_postgres_table(
    postgres_service: "PostgresService", request: "FixtureRequest"
) -> "None":
    pytest.importorskip("asyncpg")

    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    from litestar_queues.backends.sqlspec.backend import _bridge_session

    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    table_name = table_name_for_test("lq_migration_down", "asyncpg", request.node.nodeid)
    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        },
        extension_config={QUEUE_EXTENSION_NAME: {"queue_table_name": table_name}},
    )
    context = SimpleNamespace(config=config)
    sqlspec_manager = SQLSpec()

    try:
        async with _bridge_session(sqlspec_manager, config) as driver:
            try:
                for statement in await migration.up(context):
                    await driver.execute_script(statement)
                assert await _postgres_table_exists(driver, table_name)

                for statement in await migration.down(context):
                    await driver.execute_script(statement)
                assert not await _postgres_table_exists(driver, table_name)
            finally:
                await driver.execute_script(f'DROP TABLE IF EXISTS "{table_name}"')
    finally:
        await sqlspec_manager.close_all_pools()


async def _postgres_table_exists(driver: "Any", table_name: "str") -> "bool":
    table_ref = await driver.select_value(f"SELECT to_regclass('public.{table_name}')")
    return table_ref is not None


async def test_sqlspec_psycopg_fresh_migration_serves_query(request: "FixtureRequest") -> "None":
    try:
        postgres_service = request.getfixturevalue("postgres_service")
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"Docker service not available: {e}")

    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig

    from litestar_queues.backends.sqlspec.extension import configure_queue_migration_extension
    from litestar_queues.events import EventHistoryConfig, QueueEventQuery
    from tests.integration._names import table_name_for_test

    table = table_name_for_test("queue_task", "sqlspec_mig", request.node.nodeid)

    config = PsycopgAsyncConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "dbname": postgres_service.database,
        }
    )

    configure_queue_migration_extension(config, queue_table_name=table, event_history_enabled=True)

    settings = config.get_migration_commands().extension_configs[QUEUE_EXTENSION_NAME]
    config.extension_config = {QUEUE_EXTENSION_NAME: settings}

    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    statements = await migration.up(SimpleNamespace(config=config))

    from sqlspec import SQLSpec

    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend
    from litestar_queues.backends.sqlspec.backend import _bridge_session

    sqlspec_manager = SQLSpec()
    try:
        async with _bridge_session(sqlspec_manager, config) as driver:
            for statement in statements:
                await driver.execute_script(statement)
            await driver.commit()

        backend = SQLSpecQueueBackend(
            backend_config=SQLSpecBackendConfig(sqlspec_config=config, queue_table_name=table)
        )
        await backend.open()

        try:
            event_log = backend.get_event_log(EventHistoryConfig(batch_size=1, flush_interval=60))
            assert event_log is not None

            from datetime import datetime

            from litestar_queues.events.models import QueueEvent

            event = QueueEvent(
                id="mig-1",
                occurred_at=datetime.now(UTC),
                type="task.log",
                scope="task",
                scope_key="acme-mig",
                payload={"stage": "start"},
            )
            await event_log.publish_event(event)

            if hasattr(event_log, "flush_events"):
                await event_log.flush_events()

            page = await event_log.query_events(QueueEventQuery(scope_key="acme-mig"))
            assert len(page.items) == 1
            assert page.items[0].event_id == "mig-1"
            assert page.items[0].scope_key == "acme-mig"

        finally:
            await backend.close()
    finally:
        await sqlspec_manager.close_all_pools()


@pytest.mark.xdist_group("mssql_service")
@pytest.mark.parametrize("adapter,autocommit", [("pymssql", False), ("pymssql", True), ("mssql_python", False)])
async def test_sqlserver_native_migration_schema_and_queue_cycle(
    request: "FixtureRequest", adapter: "str", autocommit: "bool"
) -> "None":
    """Native SQL Server configs migrate twice and serve queues in an explicit schema."""
    config_type: Any = _PymssqlConfig if adapter == "pymssql" else _MssqlPythonConfig
    if config_type is None:
        pytest.skip(f"{adapter} adapter is not installed")

    svc = request.getfixturevalue("mssql_service")
    schema = f"queue_native_{uuid4().hex[:12]}"
    connection = {
        "host" if adapter == "pymssql" else "server": svc.host,
        "port": svc.port,
        "user": svc.user,
        "password": svc.password,
        "database": svc.database,
        "autocommit": autocommit,
    }
    if adapter == "mssql_python":
        connection["trust_server_certificate"] = True
    admin_config = config_type(connection_config={**connection, "autocommit": True})
    connection["user"] = schema
    config = config_type(
        connection_config=connection, migration_config={"default_schema": schema, "version_table_schema": schema}
    )
    manager = SQLSpec()
    manager.add_config(config)
    table = f"{schema}.tasks"
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(sqlspec_config=config, queue_table_name=table, worker_wakeups=None)
    )
    try:
        with manager.provide_session(admin_config) as driver:
            driver.execute_script(f"CREATE LOGIN [{schema}] WITH PASSWORD = '{svc.password}'")
            driver.execute_script(f"CREATE USER [{schema}] FOR LOGIN [{schema}]")
            driver.execute_script(f"ALTER ROLE db_owner ADD MEMBER [{schema}]")
            driver.execute_script(f"CREATE SCHEMA [{schema}]")
            driver.commit()
        await run_queue_migrations(config, queue_table_name=table, migration_mode="versioned")
        await run_queue_migrations(config, queue_table_name=table, migration_mode="versioned")
        await backend.open()
        record = await backend.enqueue("tasks.native_sqlserver")
        claimed = await backend.claim_task(record.id)
        assert claimed is not None
        await backend.complete_task(record.id, result={"ok": True})
        stored = await backend.get_task(record.id)
        assert stored is not None and stored.status == "completed"
        if adapter == "pymssql":
            await _assert_sqlserver_failed_batch_is_atomic(backend, manager, config, schema)
        with manager.provide_session(config) as driver:
            assert driver.select_value(f"SELECT COUNT(*) FROM [{schema}].[ddl_migrations]") == 1
            driver.rollback()
    finally:
        await backend.close()
        await manager.close_all_pools()
        _drop_sqlserver_test_schema(manager, admin_config, schema)
        await manager.close_all_pools()


async def _assert_sqlserver_failed_batch_is_atomic(
    backend: "Any", manager: "Any", config: "Any", schema: "str"
) -> "None":
    """A real failing second insert cannot persist the first batch member."""
    from sqlspec.exceptions import IntegrityError

    from litestar_queues import TaskRequest

    with manager.provide_session(config) as driver:
        driver.execute_script(
            f"ALTER TABLE [{schema}].[tasks] ADD CONSTRAINT [reject_task] CHECK (task_name <> 'tasks.reject')"
        )
        driver.commit()
    with pytest.raises(IntegrityError):
        await backend.enqueue_many([TaskRequest(task_name="tasks.first"), TaskRequest(task_name="tasks.reject")])
    with manager.provide_session(config) as driver:
        assert driver.select_value(f"SELECT COUNT(*) FROM [{schema}].[tasks] WHERE task_name = 'tasks.first'") == 0
        driver.rollback()
    record = await backend.enqueue("tasks.after_failure")
    assert await backend.get_task(record.id) is not None


def _drop_sqlserver_test_schema(manager: "Any", config: "Any", schema: "str") -> "None":
    """Drop only this test principal and its schema, including pooled sessions."""
    with manager.provide_session(config) as driver:
        tables = driver.select("SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID(:schema)", {"schema": schema})
        for row in tables:
            name = str(row["name"]).replace("]", "]]")
            driver.execute_script(f"DROP TABLE [{schema}].[{name}]")
        driver.execute_script(f"DROP USER [{schema}]")
        driver.execute_script(f"DROP SCHEMA [{schema}]")
        sessions = driver.select(
            "SELECT session_id FROM sys.dm_exec_sessions WHERE login_name = :login", {"login": schema}
        )
        for session in sessions:
            driver.execute_script(f"KILL {int(session['session_id'])}")
        driver.execute_script(f"DROP LOGIN [{schema}]")
        driver.commit()


async def test_sqlspec_packaged_migration_long_postgres_name_is_idempotent(
    postgres_service: "PostgresService", request: "FixtureRequest"
) -> "None":
    pytest.importorskip("asyncpg")
    from hashlib import sha256

    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend
    from litestar_queues.backends.sqlspec.schema import maintenance_table_name_for, task_reservation_table_name_for
    from tests.integration.backends.sqlspec._schema import run_queue_migrations

    identity = table_name_for_test("lq_long", "asyncpg", request.node.nodeid)
    table_name = (identity + "_" * 62)[:62]
    tracking = f"{identity}_versions"
    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        },
        migration_config={"version_table_name": tracking},
        extension_config={QUEUE_EXTENSION_NAME: {"queue_table_name": table_name}},
    )
    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    context = SimpleNamespace(config=config)
    manager = SQLSpec()
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(sqlspec_config=config, queue_table_name=table_name, worker_wakeups=None)
    )
    legacy_name = f"ix_{table_name}_pending"[:63]
    expected_columns = {
        "dispatch_repair": "(execution_backend, status, dispatch_checked_at, created_at, id)",
        "pending": "(queue, execution_backend, priority DESC, queued_at, created_at)",
        "scheduled": "(scheduled_at)",
        "heartbeat": "(heartbeat_at)",
    }
    expected_indexes = {
        suffix: f"{raw[:54]}_{sha256(raw.encode()).hexdigest()[:8]}"
        for suffix in expected_columns
        for raw in (f"ix_{table_name}_{suffix}",)
    }
    discovered = {
        f"ext_{QUEUE_EXTENSION_NAME}_{path.name.split('_', maxsplit=1)[0]}"
        for path in migration_directory().glob("[0-9]*.py")
    }
    try:
        async with manager.provide_session(config) as driver:
            await driver.execute_script((await migration.up(context))[0])
            await driver.execute_script(f'CREATE INDEX "{legacy_name}" ON "{table_name}" (id)')
        for _ in range(2):
            await run_queue_migrations(config, queue_table_name=table_name, migration_mode="versioned")
            async with manager.provide_session(config) as driver:
                versions = await driver.select(f'SELECT version_num FROM "{tracking}"')
                assert {row["version_num"] for row in versions} == discovered
                for name in (
                    table_name,
                    maintenance_table_name_for(table_name),
                    task_reservation_table_name_for(table_name),
                ):
                    assert len(name.encode()) <= 63
                    assert await _postgres_table_exists(driver, name)
                indexes = await driver.select(
                    "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename = :table",
                    {"table": table_name},
                )
                by_name = {row["indexname"]: row["indexdef"] for row in indexes}
                assert legacy_name in by_name
                assert len(set(expected_indexes.values())) == 4
                for suffix, name in expected_indexes.items():
                    assert len(name.encode()) == 63
                    assert name in by_name
                    assert expected_columns[suffix] in by_name[name]
        await backend.open()
        record = await backend.enqueue("tasks.long_migration", execution_backend="cloudtasks")
        candidates = await backend.list_dispatch_repair_candidates("cloudtasks", limit=1)
        assert [candidate.id for candidate in candidates.records] == [record.id]
        stored = await backend.get_task(record.id)
        assert stored is not None and stored.dispatch_checked_at is not None
    finally:
        await backend.close()
        async with manager.provide_session(config) as driver:
            for statement in await migration.down(context):
                await driver.execute_script(statement)
            await driver.execute_script(f'DROP TABLE IF EXISTS "{tracking}"')
        await manager.close_all_pools()


@pytest.mark.parametrize(
    ("action", "context_factory", "match"),
    [
        (
            "reconcile",
            lambda: SimpleNamespace(config=SqliteConfig(), driver=None, is_async_driver=False, extension_config={}),
            "active database driver",
        ),
        (
            "downgrade",
            lambda: SimpleNamespace(config=None, driver=MagicMock(), is_async_driver=False, extension_config={}),
            None,
        ),
        ("reconcile", lambda: None, None),
        ("downgrade", lambda: None, None),
    ],
)
async def test_reconcile_and_downgrade_validate_context(
    action: "str", context_factory: "Any", match: "str | None"
) -> "None":
    target = reconcile if action == "reconcile" else downgrade
    with pytest.raises(MigrationError, match=match):
        target(context_factory())


@pytest.mark.parametrize("is_async", [False, True])
async def test_reconcile_stores_skips_empty_ddl_without_driver_calls(is_async: "bool") -> "None":
    config = SqliteConfig()
    store = create_queue_store(config, manage_schema=False)
    driver = MagicMock()
    driver.data_dictionary.get_tables.side_effect = AssertionError("data_dictionary should not be queried")
    driver.execute.side_effect = AssertionError("execute should not be called")
    driver.execute_script.side_effect = AssertionError("execute_script should not be called")

    if is_async:
        await reconcile_stores_async(driver, [store], config=config)
    else:
        reconcile_stores_sync(driver, [store], config=config)


async def test_reconcile_and_downgrade_sync_sqlite_with_event_history(tmp_path: "Path") -> "None":
    db_path = tmp_path / "sync_reconcile.db"
    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        extension_config={
            QUEUE_EXTENSION_NAME: {
                "event_history_enabled": True,
                "event_history_extra_columns": [{"name": "tenant_id", "source": "metadata.tenant_id", "indexed": True}],
            }
        },
    )
    manager = SQLSpec()
    expected_tables = {
        "queue_task",
        event_history_table_name_for("queue_task"),
        maintenance_table_name_for("queue_task"),
        task_reservation_table_name_for("queue_task"),
    }
    assert down is downgrade

    try:
        with manager.provide_session(config) as driver:
            context = MigrationContext.from_config(config)
            context.driver = driver
            assert reconcile(context) is None
            driver.commit()

        with contextlib.closing(sqlite3.connect(db_path)) as connection:
            tables = {
                row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            }
            assert expected_tables.issubset(tables)
            assert "ddl_migrations" not in tables
            event_columns = {
                row[1]
                for row in connection.execute(
                    f"PRAGMA table_info({event_history_table_name_for('queue_task')})"
                ).fetchall()
            }
            assert "tenant_id" in event_columns

        with manager.provide_session(config) as driver:
            context = MigrationContext.from_config(config)
            context.driver = driver
            assert downgrade(context) is None
            driver.commit()

        with contextlib.closing(sqlite3.connect(db_path)) as connection:
            tables_after = {
                row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            }
            assert expected_tables.isdisjoint(tables_after)
    finally:
        await manager.close_all_pools()


async def test_reconcile_aiosqlite_runner_migrate_up_and_down(tmp_path: "Path") -> "None":
    if _AiosqliteConfig is None:
        pytest.skip("aiosqlite adapter is not installed")

    db_path = tmp_path / "async_reconcile.db"
    config = _AiosqliteConfig(connection_config={"database": str(db_path)})
    configure_queue_migration_extension(config, event_history_enabled=True, migration_mode="reconcile")

    commands = config.get_migration_commands()
    assert QUEUE_EXTENSION_NAME in {descriptor.name for descriptor in commands.extension_reconcilers}

    expected_tables = {
        "queue_task",
        event_history_table_name_for("queue_task"),
        maintenance_table_name_for("queue_task"),
        task_reservation_table_name_for("queue_task"),
    }

    await config.migrate_up(echo=False)
    await config.migrate_up(echo=False)
    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        tables = {
            row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
        assert expected_tables.issubset(tables)
        versions = [
            row[0]
            for row in connection.execute(
                "SELECT version_num FROM ddl_migrations WHERE version_num LIKE 'ext_litestar_queues_%'"
            ).fetchall()
        ]
        assert versions == []

    await config.migrate_down("base", echo=False)
    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        tables_after = {
            row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
        assert expected_tables.isdisjoint(tables_after)


@pytest.mark.parametrize(
    "create_table_sql",
    [
        "CREATE TABLE queue_task (id VARCHAR(64) PRIMARY KEY)",
        "CREATE TABLE queue_task (id VARCHAR(64) PRIMARY KEY, task_name VARCHAR(255) NOT NULL, priority TEXT NOT NULL)",
    ],
)
async def test_reconcile_refuses_strict_non_additive_table_drift(tmp_path: "Path", create_table_sql: "str") -> "None":
    db_path = tmp_path / "strict_refusal.db"
    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        connection.execute(create_table_sql)
        connection.commit()

    config = SqliteConfig(connection_config={"database": str(db_path)})
    manager = SQLSpec()
    try:
        with manager.provide_session(config) as driver:
            context = MigrationContext.from_config(config)
            context.driver = driver
            with pytest.raises(MigrationError):
                reconcile(context)
            driver.rollback()

        with contextlib.closing(sqlite3.connect(db_path)) as connection:
            columns = [row[1] for row in connection.execute("PRAGMA table_info(queue_task)").fetchall()]
            assert "id" in columns
            assert "expires_at" not in columns
    finally:
        await manager.close_all_pools()


async def test_reconcile_adds_missing_nullable_column_and_honors_context_extension_config(tmp_path: "Path") -> "None":
    db_path = tmp_path / "additive_reconcile.db"
    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        extension_config={QUEUE_EXTENSION_NAME: {"queue_table_name": "base_queue"}},
    )
    initial_store = create_queue_store(config, table_name="custom_ctx_queue", manage_schema=True)
    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        connection.executescript(initial_store._create_initial_table_sql())
        columns_before = {row[1] for row in connection.execute("PRAGMA table_info(custom_ctx_queue)").fetchall()}
        assert "expires_at" not in columns_before
        connection.commit()

    manager = SQLSpec()
    try:
        with manager.provide_session(config) as driver:
            context = MigrationContext.from_config(config)
            context.driver = driver
            context.extension_config = {"queue_table_name": "custom_ctx_queue"}
            assert reconcile(context) is None
            driver.commit()

        with contextlib.closing(sqlite3.connect(db_path)) as connection:
            columns_after = {row[1] for row in connection.execute("PRAGMA table_info(custom_ctx_queue)").fetchall()}
            assert "expires_at" in columns_after
            tables = {
                row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            }
            assert maintenance_table_name_for("custom_ctx_queue") in tables
            assert task_reservation_table_name_for("custom_ctx_queue") in tables
            assert "base_queue" not in tables
    finally:
        await manager.close_all_pools()


@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize(
    ("table_name", "dropped_index"),
    [("queue_task", "ix_queue_task_pending"), ("queue_task_event_history", "ix_queue_task_event_history_occurred_at")],
)
async def test_reconcile_repairs_dropped_sqlite_companion_index(
    tmp_path: "Path", is_async: "bool", table_name: "str", dropped_index: "str"
) -> "None":
    if is_async:
        pytest.importorskip("aiosqlite")
    db_path = tmp_path / "companion_repair.db"
    config_cls: Any = _AiosqliteConfig if is_async else SqliteConfig
    config = config_cls(
        connection_config={"database": str(db_path)},
        extension_config={QUEUE_EXTENSION_NAME: {"event_history_enabled": True}},
    )
    manager = SQLSpec()

    try:
        if is_async:
            async with manager.provide_session(config) as driver:
                context = MigrationContext.from_config(config)
                context.driver = driver
                awaitable = reconcile(context)
                assert awaitable is not None
                await awaitable
                await driver.commit()
        else:
            with manager.provide_session(config) as driver:
                context = MigrationContext.from_config(config)
                context.driver = driver
                assert reconcile(context) is None
                driver.commit()

        with contextlib.closing(sqlite3.connect(db_path)) as connection:
            indexes_before = {
                (row[0], row[1])
                for row in connection.execute(
                    "SELECT name, tbl_name FROM sqlite_master WHERE type = 'index'"
                ).fetchall()
            }
            assert (dropped_index, table_name) in indexes_before
            connection.execute(f'DROP INDEX "{dropped_index}"')
            connection.commit()
            indexes_dropped = {
                row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'index'").fetchall()
            }
            assert dropped_index not in indexes_dropped

        if is_async:
            async with manager.provide_session(config) as driver:
                context = MigrationContext.from_config(config)
                context.driver = driver
                awaitable = reconcile(context)
                assert awaitable is not None
                await awaitable
                await driver.commit()
        else:
            with manager.provide_session(config) as driver:
                context = MigrationContext.from_config(config)
                context.driver = driver
                assert reconcile(context) is None
                driver.commit()

        with contextlib.closing(sqlite3.connect(db_path)) as connection:
            indexes_after = {
                (row[0], row[1])
                for row in connection.execute(
                    "SELECT name, tbl_name FROM sqlite_master WHERE type = 'index'"
                ).fetchall()
            }
            assert (dropped_index, table_name) in indexes_after
    finally:
        await manager.close_all_pools()


@pytest.mark.parametrize("precreate_queue_table", [False, True])
async def test_reconcile_raises_migration_error_on_foreign_table_index_collision(
    tmp_path: "Path", precreate_queue_table: "bool"
) -> "None":
    db_path = tmp_path / "collision.db"
    config = SqliteConfig(connection_config={"database": str(db_path)})
    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        if precreate_queue_table:
            connection.executescript(create_queue_store(config).create_table_statement())
        connection.execute("CREATE TABLE other_table (x TEXT)")
        connection.execute("CREATE INDEX ix_queue_task_pending ON other_table (x)")
        connection.commit()

    manager = SQLSpec()
    try:
        with manager.provide_session(config) as driver:
            context = MigrationContext.from_config(config)
            context.driver = driver
            with pytest.raises(MigrationError) as caught:
                reconcile(context)
            assert "ix_queue_task_pending" in str(caught.value)
            assert "queue_task" in str(caught.value)
            driver.rollback()
    finally:
        await manager.close_all_pools()


_EVENT_QUEUE_RECONCILE_DDL = (
    "CREATE TABLE IF NOT EXISTS sqlspec_event_queue ("
    "event_id VARCHAR(64) PRIMARY KEY, "
    "channel VARCHAR(128) NOT NULL, "
    "payload_json TEXT NOT NULL, "
    "metadata_json TEXT, "
    "status VARCHAR(32) NOT NULL DEFAULT 'pending', "
    "available_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "lease_expires_at TIMESTAMP, "
    "attempts INTEGER NOT NULL DEFAULT 0, "
    "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "acknowledged_at TIMESTAMP"
    ")"
)


class _RecordingEventQueueStore:
    """Custom events queue store double exercising _schema_management_flags()."""

    def __init__(self, flags: "tuple[bool, bool]") -> "None":
        self._flags = flags
        self.calls: "list[tuple[Any, ...]]" = []

    def _schema_management_flags(self) -> "tuple[bool, bool]":
        return self._flags

    def prepare_schema_sync(self, driver: "Any") -> "None":
        self.calls.append(("prepare", driver))

    async def prepare_schema_async(self, driver: "Any") -> "None":
        self.calls.append(("prepare", driver))

    def get_schema_targets(self) -> "list[SchemaTarget]":
        self.calls.append(("targets",))
        return [SchemaTarget.from_ddl("sqlspec_event_queue", _EVENT_QUEUE_RECONCILE_DDL, dialect="sqlite")]

    def reconcile_companion_objects_sync(self, driver: "Any") -> "None":
        self.calls.append(("companions", driver))

    async def reconcile_companion_objects_async(self, driver: "Any") -> "None":
        self.calls.append(("companions", driver))

    def native_backend_targets(self, backend_name: "str") -> "tuple[str, ...]":
        self.calls.append(("native_targets", backend_name))
        return ("native_queue_obj",)

    def reconcile_native_backend_sync(
        self, driver: "Any", backend_name: "str", targets: "tuple[str, ...]", *, create_schema: "bool"
    ) -> "None":
        self.calls.append(("native", backend_name, targets, create_schema))

    async def reconcile_native_backend_async(
        self, driver: "Any", backend_name: "str", targets: "tuple[str, ...]", *, create_schema: "bool"
    ) -> "None":
        self.calls.append(("native", backend_name, targets, create_schema))


class _FallbackEventQueueStore:
    """Custom events queue store double lacking get_schema_targets."""

    def __init__(self, flags: "tuple[bool, bool]") -> "None":
        self._flags = flags

    def _schema_management_flags(self) -> "tuple[bool, bool]":
        return self._flags

    def create_statements(self) -> "list[str]":
        return [_EVENT_QUEUE_RECONCILE_DDL]


def _seed_legacy_queue_task(db_path: "Path", sqlspec_config: "Any", task_id: "UUID", task_name: "str") -> "None":
    initial_store = create_queue_store(sqlspec_config, manage_schema=True)
    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        connection.executescript(initial_store._create_initial_table_sql())
        connection.execute('CREATE INDEX "ix_queue_task_heartbeat" ON "queue_task"("status", "heartbeat_at")')
        connection.execute(
            """
            INSERT INTO queue_task (
                id, task_name, task_args, task_kwargs, queue, execution_backend,
                status, priority, max_retries, retry_count, created_at, queued_at, result, metadata
            ) VALUES (?, ?, '[]', '{}', 'default', 'local', 'pending', 0, 1, 0,
                      '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00', 'null', '{}')
            """,
            (str(task_id), task_name),
        )
        connection.commit()


def _seed_legacy_events_queue(db_path: "Path") -> "None":
    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        connection.execute(
            """
            CREATE TABLE sqlspec_event_queue (
                event_id VARCHAR(64) PRIMARY KEY,
                channel VARCHAR(128) NOT NULL,
                payload_json TEXT NOT NULL,
                metadata_json TEXT,
                status VARCHAR(32) NOT NULL DEFAULT 'pending',
                available_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                lease_expires_at TIMESTAMP,
                attempts INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            "INSERT INTO sqlspec_event_queue (event_id, channel, payload_json) VALUES ('evt-1', 'default', '{}')"
        )
        connection.commit()


@pytest.mark.parametrize("is_async", [False, True])
async def test_backend_create_schema_reconcile_converges_sync_and_async(tmp_path: "Path", is_async: "bool") -> "None":
    if is_async:
        pytest.importorskip("aiosqlite")
    db_path = tmp_path / "backend_reconcile.db"
    config_cls: Any = _AiosqliteConfig if is_async else SqliteConfig
    sqlspec_config = config_cls(connection_config={"database": str(db_path)})
    existing_id = UUID("00000000-0000-0000-0000-000000000042")
    _seed_legacy_queue_task(db_path, sqlspec_config, existing_id, "tasks.legacy")

    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            sqlspec_config=sqlspec_config, migration_mode="reconcile", worker_wakeups=None
        )
    )
    await backend.open()
    try:
        await backend.create_schema()
        await backend.create_schema()
        preserved = await backend.get_task(existing_id)
        assert preserved is not None
        assert preserved.task_name == "tasks.legacy"
    finally:
        await backend.close()

    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(queue_task)").fetchall()}
        indexes = {
            row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'index'").fetchall()
        }
        tables = {
            row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
    assert "expires_at" in columns
    assert {"ix_queue_task_dispatch_repair", "ix_queue_task_pending", "ix_queue_task_heartbeat"}.issubset(indexes)
    assert {"queue_task", "queue_maintenance", "queue_task_reservation"}.issubset(tables)


async def test_backend_create_schema_versioned_keeps_direct_ddl(tmp_path: "Path") -> "None":
    db_path = tmp_path / "backend_versioned.db"
    sqlspec_config = SqliteConfig(connection_config={"database": str(db_path)})
    existing_id = UUID("00000000-0000-0000-0000-000000000043")
    _seed_legacy_queue_task(db_path, sqlspec_config, existing_id, "tasks.versioned")

    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            sqlspec_config=sqlspec_config, migration_mode="versioned", worker_wakeups=None
        )
    )
    await backend.open()
    try:
        await backend.create_schema()
    finally:
        await backend.close()

    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(queue_task)").fetchall()}
        row = connection.execute("SELECT task_name FROM queue_task WHERE id = ?", (str(existing_id),)).fetchone()
    assert "expires_at" not in columns
    assert row == ("tasks.versioned",)


@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize("precreate_legacy_events", [False, True])
async def test_backend_create_schema_reconciles_events_queue_when_enabled(
    tmp_path: "Path", is_async: "bool", precreate_legacy_events: "bool"
) -> "None":
    if is_async:
        pytest.importorskip("aiosqlite")
    db_path = tmp_path / "backend_events_reconcile.db"
    if precreate_legacy_events:
        _seed_legacy_events_queue(db_path)

    config_cls: Any = _AiosqliteConfig if is_async else SqliteConfig
    sqlspec_config = config_cls(connection_config={"database": str(db_path)})
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            sqlspec_config=sqlspec_config,
            migration_mode="reconcile",
            worker_wakeups=SQLSpecWorkerWakeupConfig(transport="poll_queue"),
        )
    )
    await backend.open()
    try:
        await backend.create_schema()
    finally:
        await backend.close()

    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        tables = {
            row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
        event_columns = {row[1] for row in connection.execute("PRAGMA table_info(sqlspec_event_queue)").fetchall()}
        indexes = {
            row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'index'").fetchall()
        }
        rows = connection.execute("SELECT event_id FROM sqlspec_event_queue").fetchall()
    assert {"queue_task", "queue_maintenance", "queue_task_reservation", "sqlspec_event_queue"}.issubset(tables)
    assert "acknowledged_at" in event_columns
    assert "idx_sqlspec_event_queue_channel_status" in indexes
    if precreate_legacy_events:
        assert rows == [("evt-1",)]


@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize("flags", [(False, True), (True, False)])
async def test_reconcile_events_queue_honors_schema_management_flags(
    tmp_path: "Path", is_async: "bool", flags: "tuple[bool, bool]"
) -> "None":
    if is_async:
        pytest.importorskip("aiosqlite")
    db_path = tmp_path / "events_flags.db"
    config_cls: Any = _AiosqliteConfig if is_async else SqliteConfig
    sqlspec_config = config_cls(connection_config={"database": str(db_path)})
    store = _RecordingEventQueueStore(flags)
    manager = SQLSpec()

    try:
        if flags == (False, True):
            if is_async:
                async with manager.provide_session(sqlspec_config) as driver:
                    await reconcile_events_queue_async(driver, store, "poll_queue")
                    await driver.commit()
            else:
                with manager.provide_session(sqlspec_config) as driver:
                    reconcile_events_queue_sync(driver, store, "poll_queue")
                    driver.commit()
            assert store.calls == []
            with contextlib.closing(sqlite3.connect(db_path)) as connection:
                tables = {
                    row[0]
                    for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
                }
            assert tables == set()
        else:
            if is_async:
                async with manager.provide_session(sqlspec_config) as driver:
                    with pytest.raises(MigrationError, match="create_schema=False"):
                        await reconcile_events_queue_async(driver, store, "poll_queue")
                    await driver.rollback()
            else:
                with manager.provide_session(sqlspec_config) as driver:
                    with pytest.raises(MigrationError, match="create_schema=False"):
                        reconcile_events_queue_sync(driver, store, "poll_queue")
                    driver.rollback()

            _seed_legacy_events_queue(db_path)
            store.calls.clear()
            if is_async:
                async with manager.provide_session(sqlspec_config) as driver:
                    await reconcile_events_queue_async(driver, store, "poll_queue")
                    await driver.commit()
            else:
                with manager.provide_session(sqlspec_config) as driver:
                    reconcile_events_queue_sync(driver, store, "poll_queue")
                    driver.commit()

            with contextlib.closing(sqlite3.connect(db_path)) as connection:
                columns = {row[1] for row in connection.execute("PRAGMA table_info(sqlspec_event_queue)").fetchall()}
            assert "acknowledged_at" in columns
            assert ("native", "poll_queue", ("native_queue_obj",), False) in store.calls
    finally:
        await manager.close_all_pools()


@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize(
    ("flags", "expect_created"), [((True, True), True), ((True, False), False), ((False, True), False)]
)
async def test_reconcile_events_queue_fallback_without_schema_targets(
    tmp_path: "Path", is_async: "bool", flags: "tuple[bool, bool]", expect_created: "bool"
) -> "None":
    if is_async:
        pytest.importorskip("aiosqlite")
    db_path = tmp_path / "events_fallback.db"
    config_cls: Any = _AiosqliteConfig if is_async else SqliteConfig
    sqlspec_config = config_cls(connection_config={"database": str(db_path)})
    store = _FallbackEventQueueStore(flags)
    manager = SQLSpec()

    try:
        if is_async:
            async with manager.provide_session(sqlspec_config) as driver:
                await reconcile_events_queue_async(driver, store, "poll_queue")
                await driver.commit()
        else:
            with manager.provide_session(sqlspec_config) as driver:
                reconcile_events_queue_sync(driver, store, "poll_queue")
                driver.commit()
    finally:
        await manager.close_all_pools()

    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        tables = {
            row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
    assert ("sqlspec_event_queue" in tables) is expect_created


async def test_backend_create_schema_spanner_preserves_create_schema_for_config() -> "None":
    executed_ddl: "list[str]" = []

    def _update_ddl(statements: "list[str]") -> "Any":
        executed_ddl.extend(statements)
        return SimpleNamespace(result=lambda: None)

    spanner_config = _fake_adapter_config("spanner", dialect="spanner", is_async=False)
    spanner_config.get_database = lambda: SimpleNamespace(update_ddl=_update_ddl)

    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            sqlspec_config=cast("Any", spanner_config), migration_mode="reconcile", worker_wakeups=None
        )
    )
    await backend.open()
    try:
        await backend.create_schema()
    finally:
        await backend.close()

    assert any("CREATE TABLE `queue_task`" in statement for statement in executed_ddl)
    assert any("`queue_maintenance`" in statement for statement in executed_ddl)
    assert any("`queue_task_reservation`" in statement for statement in executed_ddl)


class _CustomThirdPartySqliteStore(SqliteQueueStore):
    """Third-party SQLite store subclass for reconcile registration tests."""


async def _run_configured_migrations(
    sqlspec_config: "Any",
    *,
    direction: "Literal['up', 'down']" = "up",
    revision: "str" = "head",
    migration_mode: "ExtensionMigrationMode" = "reconcile",
    queue_table_name: "str" = "queue_task",
    event_history_enabled: "bool" = False,
    event_history_extra_columns: "tuple[EventHistoryExtraColumn, ...]" = (),
    terminal_ttl: "float | None" = None,
) -> "None":
    configure_queue_migration_extension(
        sqlspec_config,
        queue_table_name=queue_table_name,
        event_history_enabled=event_history_enabled,
        event_history_extra_columns=event_history_extra_columns,
        terminal_ttl=terminal_ttl,
        migration_mode=migration_mode,
    )
    if direction == "down":
        result = sqlspec_config.migrate_down(revision="base", echo=False)
    else:
        result = sqlspec_config.migrate_up(revision=revision, echo=False)
    if isawaitable(result):
        await result


def _drop_postgres_tables_sql(queue_table: "str", tracking_table: "str | None" = None) -> "str":
    tables = [
        task_reservation_table_name_for(queue_table),
        maintenance_table_name_for(queue_table),
        event_history_table_name_for(queue_table),
        queue_table,
    ]
    if tracking_table is not None:
        tables.append(tracking_table)
    return "; ".join(f'DROP TABLE IF EXISTS "{name}"' for name in tables)


def _drop_mysql_tables_sql(queue_table: "str", tracking_table: "str | None" = None) -> "list[str]":
    tables = [
        task_reservation_table_name_for(queue_table),
        maintenance_table_name_for(queue_table),
        event_history_table_name_for(queue_table),
        queue_table,
    ]
    if tracking_table is not None:
        tables.append(tracking_table)
    return [f"DROP TABLE IF EXISTS `{name}`" for name in tables]


async def _execute_all(driver: "Any", statements: "list[str]") -> "None":
    for statement in statements:
        await driver.execute_script(statement)


@pytest.mark.parametrize("adapter", ["sqlite", "aiosqlite"])
async def test_reconcile_default_and_repeat_migrates_and_preserves_rows(tmp_path: "Path", adapter: "str") -> "None":
    if adapter == "aiosqlite" and _AiosqliteConfig is None:
        pytest.skip("aiosqlite adapter is not installed")

    db_path = tmp_path / f"repeat_{adapter}.db"
    config_cls: Any = _AiosqliteConfig if adapter == "aiosqlite" else SqliteConfig
    config = config_cls(connection_config={"database": str(db_path)})
    expected_tables = {
        "queue_task",
        event_history_table_name_for("queue_task"),
        maintenance_table_name_for("queue_task"),
        task_reservation_table_name_for("queue_task"),
    }

    await _run_configured_migrations(config, event_history_enabled=True)

    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(sqlspec_config=config, migration_mode="reconcile", worker_wakeups=None)
    )
    await backend.open()
    try:
        task_record = await backend.enqueue("tasks.reconcile.repeat", kwargs={"step": 1})
        assert (
            await backend.reserve_identity("inv-forever-1", task_id=task_record.id, task_name=task_record.task_name)
            is None
        )
        event_log = backend.get_event_log(EventHistoryConfig(batch_size=1, flush_interval=60))
        assert event_log is not None
        await event_log.publish_event(
            QueueEvent(
                id=f"evt-{adapter}",
                occurred_at=datetime.now(UTC),
                type="task.enqueued",
                scope="task",
                scope_key=str(task_record.id),
                payload={"step": 1},
            )
        )
        await event_log.flush_events()
    finally:
        await backend.close()

    await _run_configured_migrations(config, event_history_enabled=True)

    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        tables = {
            row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
        queue_indexes = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'queue_task'"
            ).fetchall()
        }
        ext_versions = connection.execute(
            "SELECT version_num FROM ddl_migrations WHERE version_num LIKE 'ext_litestar_queues_%'"
        ).fetchall()
        task_rows = connection.execute("SELECT id, task_name FROM queue_task").fetchall()
        reservation_rows = connection.execute("SELECT identity_key FROM queue_task_reservation").fetchall()
        event_rows = connection.execute("SELECT event_id FROM queue_task_event_history").fetchall()

    assert expected_tables.issubset(tables)
    assert any("pending" in index_name for index_name in queue_indexes)
    assert len(queue_indexes) >= 3
    assert ext_versions == []
    assert task_rows == [(str(task_record.id), "tasks.reconcile.repeat")]
    assert reservation_rows == [("inv-forever-1",)]
    assert event_rows == [(f"evt-{adapter}",)]

    await _run_configured_migrations(config, direction="down", revision="base", event_history_enabled=True)
    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        remaining_tables = {
            row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
    assert expected_tables.isdisjoint(remaining_tables)
    close_result = config.close_pool()
    if isawaitable(close_result):
        await close_result


@pytest.mark.xdist_group("postgres_service")
async def test_reconcile_default_and_repeat_postgres_preserves_rows(request: "FixtureRequest") -> "None":
    if _AsyncpgConfig is None:
        pytest.skip("asyncpg adapter is not installed")
    try:
        postgres_service = request.getfixturevalue("postgres_service")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Docker service not available: {exc}")

    queue_table = table_name_for_test("lq_rec_pg", "asyncpg", request.node.nodeid)
    tracking_table = f"{queue_table[:48]}_mig"
    config = _AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        },
        migration_config={"version_table_name": tracking_table},
    )
    expected_tables = {
        queue_table,
        event_history_table_name_for(queue_table),
        maintenance_table_name_for(queue_table),
        task_reservation_table_name_for(queue_table),
    }
    manager = SQLSpec()
    try:
        await _run_configured_migrations(config, queue_table_name=queue_table, event_history_enabled=True)

        backend = SQLSpecQueueBackend(
            backend_config=SQLSpecBackendConfig(
                sqlspec_config=config, queue_table_name=queue_table, migration_mode="reconcile", worker_wakeups=None
            )
        )
        await backend.open()
        try:
            task_record = await backend.enqueue("tasks.pg.reconcile")
            assert (
                await backend.reserve_identity("pg-forever-1", task_id=task_record.id, task_name=task_record.task_name)
                is None
            )
        finally:
            await backend.close()

        await _run_configured_migrations(config, queue_table_name=queue_table, event_history_enabled=True)

        async with manager.provide_session(config) as driver:
            tables = await driver.select("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            assert expected_tables.issubset({str(row["tablename"]) for row in tables})
            indexes = await driver.select(
                "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND tablename = :table",
                {"table": queue_table},
            )
            companion_names = {str(row["indexname"]) for row in indexes if not str(row["indexname"]).endswith("_pkey")}
            assert any("pending" in name for name in companion_names)
            assert len(companion_names) >= 3
            ext_count = await driver.select_value(
                f"SELECT COUNT(*) FROM \"{tracking_table}\" WHERE version_num LIKE 'ext_litestar_queues_%'"
            )
            assert int(ext_count or 0) == 0
            stored_count = await driver.select_value(f'SELECT COUNT(*) FROM "{queue_table}"')
            assert int(stored_count or 0) == 1

        await _run_configured_migrations(
            config, direction="down", revision="base", queue_table_name=queue_table, event_history_enabled=True
        )
        async with manager.provide_session(config) as driver:
            remaining = await driver.select("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            assert expected_tables.isdisjoint({str(row["tablename"]) for row in remaining})
    finally:
        async with manager.provide_session(config) as driver:
            await driver.execute_script(_drop_postgres_tables_sql(queue_table, tracking_table))
            await driver.commit()
        await config.close_pool()
        await manager.close_all_pools()


async def test_versioned_opt_out_records_extension_migration_revision(tmp_path: "Path") -> "None":
    if _AiosqliteConfig is None:
        pytest.skip("aiosqlite adapter is not installed")

    db_path = tmp_path / "versioned_opt_out.db"
    config = _AiosqliteConfig(connection_config={"database": str(db_path)})
    try:
        await _run_configured_migrations(config, migration_mode="versioned", event_history_enabled=True)
        with contextlib.closing(sqlite3.connect(db_path)) as connection:
            versions = [
                row[0]
                for row in connection.execute(
                    "SELECT version_num FROM ddl_migrations WHERE version_num LIKE 'ext_litestar_queues_%'"
                ).fetchall()
            ]
            tables = {
                row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            }
        assert versions == ["ext_litestar_queues_0001"]
        assert {"queue_task", "queue_task_event_history", "queue_maintenance", "queue_task_reservation"}.issubset(
            tables
        )
    finally:
        await config.close_pool()


async def test_reconcile_converges_nullable_extra_event_column_and_index(tmp_path: "Path") -> "None":
    db_path = tmp_path / "nullable_converge.db"
    config = SqliteConfig(connection_config={"database": str(db_path)})
    existing_task_id = UUID("00000000-0000-0000-0000-000000000099")
    _seed_legacy_queue_task(db_path, config, existing_task_id, "tasks.legacy.converge")

    base_event_store = create_event_log_store(config, queue_table_name="queue_task", manage_schema=True)
    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        connection.executescript(base_event_store.create_table_statement())
        connection.execute(
            """
            INSERT INTO queue_task_event_history (
                event_id, event_type, scope, scope_key, detail, sequence, occurred_at, created_at
            ) VALUES ('evt-legacy-1', 'task.log', 'task', 'scope-1', '{}', 1,
                      '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')
            """
        )
        event_cols_before = {
            row[1] for row in connection.execute("PRAGMA table_info(queue_task_event_history)").fetchall()
        }
        assert "tenant_id" not in event_cols_before
        connection.commit()

    extra_column = EventHistoryExtraColumn(name="tenant_id", source="tenant_id", indexed=True)
    expected_extra_index = create_event_log_store(
        config, queue_table_name="queue_task", manage_schema=True, extra_columns=(extra_column,)
    )._index_name("tenant_id")
    try:
        await _run_configured_migrations(
            config, event_history_enabled=True, event_history_extra_columns=(extra_column,)
        )
        with contextlib.closing(sqlite3.connect(db_path)) as connection:
            queue_cols = {row[1] for row in connection.execute("PRAGMA table_info(queue_task)").fetchall()}
            event_cols = {
                row[1] for row in connection.execute("PRAGMA table_info(queue_task_event_history)").fetchall()
            }
            event_indexes = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'queue_task_event_history'"
                ).fetchall()
            }
            extra_index_cols = [
                row[2] for row in connection.execute(f'PRAGMA index_info("{expected_extra_index}")').fetchall()
            ]
            preserved_task = connection.execute(
                "SELECT task_name FROM queue_task WHERE id = ?", (str(existing_task_id),)
            ).fetchone()
            preserved_event = connection.execute(
                "SELECT event_id, tenant_id FROM queue_task_event_history WHERE event_id = 'evt-legacy-1'"
            ).fetchone()

        assert "expires_at" in queue_cols
        assert "tenant_id" in event_cols
        assert expected_extra_index in event_indexes
        assert extra_index_cols == ["tenant_id", "occurred_at"]
        assert preserved_task == ("tasks.legacy.converge",)
        assert preserved_event == ("evt-legacy-1", None)
    finally:
        config.close_pool()


async def test_reconcile_migrate_up_refuses_missing_required_column(tmp_path: "Path") -> "None":
    db_path = tmp_path / "runner_strict_refusal.db"
    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        connection.execute("CREATE TABLE queue_task (id VARCHAR(64) PRIMARY KEY, status VARCHAR(32) NOT NULL)")
        connection.commit()

    config = SqliteConfig(connection_config={"database": str(db_path)})
    try:
        with pytest.raises(MigrationError):
            await _run_configured_migrations(config)
        with contextlib.closing(sqlite3.connect(db_path)) as connection:
            columns = [row[1] for row in connection.execute("PRAGMA table_info(queue_task)").fetchall()]
        assert columns == ["id", "status"]
    finally:
        config.close_pool()


def _build_index_repair_target(request: "FixtureRequest", dialect: "str") -> "tuple[Any, str, str, str]":
    if dialect == "postgres":
        if _AsyncpgConfig is None:
            pytest.skip("asyncpg adapter is not installed")
        try:
            svc = request.getfixturevalue("postgres_service")
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"Docker service not available: {exc}")
        table = table_name_for_test("lq_idx_rep", "asyncpg", request.node.nodeid)
        tracking = f"{table[:48]}_mig"
        config: Any = _AsyncpgConfig(
            connection_config={
                "host": svc.host,
                "port": svc.port,
                "user": svc.user,
                "password": svc.password,
                "database": svc.database,
            },
            migration_config={"version_table_name": tracking},
        )
        return config, table, tracking, "public"

    if _AsyncmyConfig is None:
        pytest.skip("asyncmy adapter is not installed")
    try:
        svc = request.getfixturevalue("mysql_service")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Docker service not available: {exc}")
    table = table_name_for_test("lq_idx_rep", "asyncmy", request.node.nodeid)
    tracking = f"{table[:48]}_mig"
    config = _AsyncmyConfig(
        connection_config={
            "host": svc.host,
            "port": svc.port,
            "user": svc.user,
            "password": svc.password,
            "database": svc.db,
        },
        migration_config={"version_table_name": tracking},
    )
    return config, table, tracking, str(svc.db)


@pytest.mark.parametrize(
    "dialect",
    [
        pytest.param("postgres", marks=pytest.mark.xdist_group("postgres_service")),
        pytest.param("mysql", marks=pytest.mark.xdist_group("mysql_service")),
    ],
)
async def test_reconcile_repairs_dropped_companion_index_postgres_and_mysql(
    request: "FixtureRequest", dialect: "str"
) -> "None":
    config, table, tracking, schema_name = _build_index_repair_target(request, dialect)
    pending_index = create_queue_store(config, table_name=table)._index_name("pending")
    drop_index_sql = (
        f'DROP INDEX "{pending_index}"' if dialect == "postgres" else f"DROP INDEX `{pending_index}` ON `{table}`"
    )
    manager = SQLSpec()
    try:
        await _run_configured_migrations(config, queue_table_name=table)
        async with manager.provide_session(config) as driver:
            await driver.execute_script(drop_index_sql)
            await driver.commit()

        await _run_configured_migrations(config, queue_table_name=table)
        async with manager.provide_session(config) as driver:
            if dialect == "postgres":
                rows = await driver.select(
                    "SELECT indexname AS idx FROM pg_indexes WHERE schemaname = :schema AND tablename = :table",
                    {"schema": schema_name, "table": table},
                )
            else:
                rows = await driver.select(
                    "SELECT DISTINCT index_name AS idx FROM information_schema.statistics "
                    "WHERE table_schema = :schema AND table_name = :table",
                    {"schema": schema_name, "table": table},
                )
            companion_names = {
                str(row["idx"])
                for row in rows
                if str(row["idx"]) != "PRIMARY" and not str(row["idx"]).endswith("_pkey")
            }
            assert pending_index in companion_names
            assert any("pending" in name for name in companion_names)
            assert len(companion_names) >= 3
    finally:
        async with manager.provide_session(config) as driver:
            if dialect == "postgres":
                await driver.execute_script(_drop_postgres_tables_sql(table, tracking))
            else:
                await _execute_all(driver, _drop_mysql_tables_sql(table, tracking))
            await driver.commit()
        await config.close_pool()
        await manager.close_all_pools()


async def _verify_mysql_foreign_same_name_index(request: "FixtureRequest") -> "None":
    if _AsyncmyConfig is None:
        pytest.skip("asyncmy adapter is not installed")
    try:
        mysql_svc = request.getfixturevalue("mysql_service")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Docker service not available: {exc}")
    table = table_name_for_test("lq_fidx", "asyncmy", request.node.nodeid)
    foreign_table = f"{table[:44]}_other"
    tracking = f"{table[:44]}_mig"
    config = _AsyncmyConfig(
        connection_config={
            "host": mysql_svc.host,
            "port": mysql_svc.port,
            "user": mysql_svc.user,
            "password": mysql_svc.password,
            "database": mysql_svc.db,
        },
        migration_config={"version_table_name": tracking},
    )
    pending_index = create_queue_store(config, table_name=table)._index_name("pending")
    manager = SQLSpec()
    try:
        async with manager.provide_session(config) as driver:
            await driver.execute_script(f"CREATE TABLE `{foreign_table}` (`x` VARCHAR(32))")
            await driver.execute_script(f"CREATE INDEX `{pending_index}` ON `{foreign_table}` (`x`)")
            await driver.commit()

        await _run_configured_migrations(config, queue_table_name=table)
        async with manager.provide_session(config) as driver:
            await driver.execute_script(f"DROP INDEX `{pending_index}` ON `{table}`")
            await driver.commit()

        await _run_configured_migrations(config, queue_table_name=table)
        async with manager.provide_session(config) as driver:
            rows = await driver.select(
                "SELECT DISTINCT table_name AS tbl FROM information_schema.statistics "
                "WHERE table_schema = :schema AND index_name = :idx",
                {"schema": mysql_svc.db, "idx": pending_index},
            )
            assert {str(row["tbl"]) for row in rows} == {table, foreign_table}
    finally:
        async with manager.provide_session(config) as driver:
            await _execute_all(
                driver, [*_drop_mysql_tables_sql(table, tracking), f"DROP TABLE IF EXISTS `{foreign_table}`"]
            )
            await driver.commit()
        await config.close_pool()
        await manager.close_all_pools()


async def _verify_postgres_foreign_same_name_index(request: "FixtureRequest", *, same_schema: "bool") -> "None":
    if _AsyncpgConfig is None:
        pytest.skip("asyncpg adapter is not installed")
    try:
        pg_svc = request.getfixturevalue("postgres_service")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Docker service not available: {exc}")

    base = table_name_for_test("lq_pg_fidx", "asyncpg", request.node.nodeid)[:36]
    target_schema = f"{base}_app"
    foreign_schema = target_schema if same_schema else f"{base}_other"
    qualified_table = f"{target_schema}.queue_task"
    pg_config = _AsyncpgConfig(
        connection_config={
            "host": pg_svc.host,
            "port": pg_svc.port,
            "user": pg_svc.user,
            "password": pg_svc.password,
            "database": pg_svc.database,
        },
        migration_config={"version_table_name": f"{target_schema}.ddl_migrations"},
    )
    pending_index = create_queue_store(pg_config, table_name=qualified_table)._index_name("pending")
    manager = SQLSpec()
    try:
        async with manager.provide_session(pg_config) as driver:
            await driver.execute_script(f'CREATE SCHEMA IF NOT EXISTS "{target_schema}"')
            await driver.execute_script(f'CREATE SCHEMA IF NOT EXISTS "{foreign_schema}"')
            await driver.execute_script(f'CREATE TABLE "{foreign_schema}"."other_tbl" (x TEXT)')
            await driver.execute_script(f'CREATE INDEX "{pending_index}" ON "{foreign_schema}"."other_tbl" (x)')
            await driver.commit()

        if same_schema:
            with pytest.raises(MigrationError):
                await _run_configured_migrations(pg_config, queue_table_name=qualified_table)
        else:
            await _run_configured_migrations(pg_config, queue_table_name=qualified_table)
            async with manager.provide_session(pg_config) as driver:
                await driver.execute_script(f'DROP INDEX "{target_schema}"."{pending_index}"')
                await driver.commit()
            await _run_configured_migrations(pg_config, queue_table_name=qualified_table)
            async with manager.provide_session(pg_config) as driver:
                rows = await driver.select(
                    "SELECT schemaname FROM pg_indexes WHERE indexname = :idx", {"idx": pending_index}
                )
                assert {str(row["schemaname"]) for row in rows} == {target_schema, foreign_schema}
    finally:
        async with manager.provide_session(pg_config) as driver:
            await driver.execute_script(f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE')
            await driver.execute_script(f'DROP SCHEMA IF EXISTS "{foreign_schema}" CASCADE')
            await driver.commit()
        await pg_config.close_pool()
        await manager.close_all_pools()


@pytest.mark.parametrize(
    "scenario",
    [
        pytest.param("mysql_same_schema", marks=pytest.mark.xdist_group("mysql_service")),
        pytest.param("postgres_cross_schema", marks=pytest.mark.xdist_group("postgres_service")),
        pytest.param("postgres_same_schema", marks=pytest.mark.xdist_group("postgres_service")),
    ],
)
async def test_reconcile_foreign_same_name_index_mysql_and_postgres(
    request: "FixtureRequest", scenario: "str"
) -> "None":
    if scenario == "mysql_same_schema":
        await _verify_mysql_foreign_same_name_index(request)
    else:
        await _verify_postgres_foreign_same_name_index(request, same_schema=(scenario == "postgres_same_schema"))


@pytest.mark.xdist_group("cockroachdb_service")
async def test_reconcile_cockroachdb_terminal_ttl_convergence(request: "FixtureRequest") -> "None":
    if _CockroachAsyncpgConfig is None:
        pytest.skip("cockroach_asyncpg adapter is not installed")
    try:
        cockroach_service = request.getfixturevalue("cockroachdb_service")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Docker service not available: {exc}")

    table = table_name_for_test("lq_crdb_ttl", "crdb", request.node.nodeid)
    tracking = f"{table[:48]}_mig"
    config = _CockroachAsyncpgConfig(
        connection_config={
            "host": cockroach_service.host,
            "port": cockroach_service.port,
            "user": "root",
            "password": "",
            "database": cockroach_service.database,
            "ssl": False,
        },
        migration_config={"version_table_name": tracking},
    )
    manager = SQLSpec()

    async def _create_statement_text() -> "str":
        async with manager.provide_session(config) as driver:
            row = await driver.select_one_or_none(f'SHOW CREATE TABLE "{table}"')
            assert row is not None
            return str(row.get("create_statement") or next(reversed(tuple(row.values()))))

    try:
        await _run_configured_migrations(config, queue_table_name=table, terminal_ttl=None)
        ddl_none = await _create_statement_text()
        assert "ttl_expiration_expression" not in ddl_none

        await _run_configured_migrations(config, queue_table_name=table, terminal_ttl=3600.0)
        ddl_3600 = await _create_statement_text()
        assert "ttl_expiration_expression" in ddl_3600
        assert "3600" in ddl_3600 or "01:00:00" in ddl_3600

        await _run_configured_migrations(config, queue_table_name=table, terminal_ttl=7200.0)
        ddl_7200 = await _create_statement_text()
        assert "ttl_expiration_expression" in ddl_7200
        assert "7200" in ddl_7200 or "02:00:00" in ddl_7200

        await _run_configured_migrations(config, queue_table_name=table, terminal_ttl=None)
        ddl_reset = await _create_statement_text()
        assert "ttl_expiration_expression" not in ddl_reset
    finally:
        async with manager.provide_session(config) as driver:
            await driver.execute_script(_drop_postgres_tables_sql(table, tracking))
            await driver.commit()
        await config.close_pool()
        await manager.close_all_pools()


async def test_reconcile_supports_third_party_queue_store_class(tmp_path: "Path") -> "None":
    if _AiosqliteConfig is None:
        pytest.skip("aiosqlite adapter is not installed")

    custom_config_cls = cast(
        "Any",
        type(
            "CustomThirdPartySqliteConfig",
            (_AiosqliteConfig,),
            {
                "__module__": "external_vendor.sqlite_adapter",
                "adapter_name": "custom_sqlite",
                "queue_store_class": _CustomThirdPartySqliteStore,
            },
        ),
    )
    db_path = tmp_path / "third_party_store.db"
    config = custom_config_cls(connection_config={"database": str(db_path)})
    expected_tables = {
        "queue_task",
        event_history_table_name_for("queue_task"),
        maintenance_table_name_for("queue_task"),
        task_reservation_table_name_for("queue_task"),
    }
    try:
        assert isinstance(create_queue_store(config), _CustomThirdPartySqliteStore)
        await _run_configured_migrations(config, event_history_enabled=True)

        with contextlib.closing(sqlite3.connect(db_path)) as connection:
            tables = {
                row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            }
            indexes = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'queue_task'"
                ).fetchall()
            }
            ext_rows = connection.execute(
                "SELECT version_num FROM ddl_migrations WHERE version_num LIKE 'ext_litestar_queues_%'"
            ).fetchall()

        assert expected_tables.issubset(tables)
        assert any("pending" in name for name in indexes)
        assert len(indexes) >= 3
        assert ext_rows == []

        await _run_configured_migrations(config, direction="down", revision="base", event_history_enabled=True)
        with contextlib.closing(sqlite3.connect(db_path)) as connection:
            tables_after = {
                row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            }
        assert expected_tables.isdisjoint(tables_after)
    finally:
        await config.close_pool()


def _unsupported_reconcile_config(case_id: "str") -> "Any":
    if case_id == "spanner":
        if _SpannerSyncConfig is not None:
            return _SpannerSyncConfig(connection_config={"project": "demo", "instance_id": "inst", "database_id": "db"})
        return _fake_adapter_config("spanner", dialect="spanner")
    if case_id == "db2":
        return _fake_adapter_config("db2", dialect="db2")
    if case_id == "adbc_flightsql":
        if _AdbcConfig is not None:
            return _AdbcConfig(connection_config={"driver_name": "adbc_driver_flightsql"})
        return _fake_adapter_config(
            "adbc", config_type_name="AdbcConfig", connection_config={"driver_name": "adbc_driver_flightsql"}
        )
    if case_id == "adbc_postgresql":
        if _AdbcConfig is not None:
            return _AdbcConfig(
                connection_config={"driver_name": "adbc_driver_postgresql", "uri": "postgresql://localhost/app"}
            )
        return _fake_adapter_config(
            "adbc",
            dialect="postgres",
            config_type_name="AdbcConfig",
            connection_config={"driver_name": "adbc_driver_postgresql"},
        )
    if case_id == "adbc_duckdb":
        if _AdbcConfig is not None:
            return _AdbcConfig(connection_config={"driver_name": "duckdb"})
        return _fake_adapter_config(
            "adbc", dialect="duckdb", config_type_name="AdbcConfig", connection_config={"driver_name": "duckdb"}
        )
    if case_id == "arrow_odbc_db2":
        return _fake_adapter_config("arrow_odbc", dialect="db2", config_type_name="ArrowOdbcConfig")
    return _fake_adapter_config("arrow_odbc", dialect="postgres", config_type_name="ArrowOdbcConfig")


@pytest.mark.parametrize("action", ["reconcile", "downgrade"])
@pytest.mark.parametrize(
    "case_id",
    ["spanner", "db2", "adbc_flightsql", "adbc_postgresql", "adbc_duckdb", "arrow_odbc_db2", "arrow_odbc_postgres"],
)
async def test_reconcile_and_downgrade_refuse_unsupported_adapters(action: "str", case_id: "str") -> "None":
    config = _unsupported_reconcile_config(case_id)
    driver = MagicMock()
    driver.execute.side_effect = AssertionError("DDL execute must not be called for refused adapters")
    driver.execute_script.side_effect = AssertionError("DDL execute_script must not be called for refused adapters")
    context = cast(
        "MigrationContext", SimpleNamespace(config=config, driver=driver, is_async_driver=False, extension_config={})
    )
    target_fn = reconcile if action == "reconcile" else downgrade

    with pytest.raises(MigrationError):
        target_fn(context)

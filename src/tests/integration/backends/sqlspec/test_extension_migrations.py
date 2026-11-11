"""Extension-migration tests for the SQLSpec queue backend."""

import contextlib
import importlib
import sqlite3
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, ClassVar, cast

import pytest

pytest.importorskip("sqlspec")

from datetime import UTC
from unittest.mock import MagicMock
from uuid import UUID

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.schema import SchemaTarget

from litestar_queues import WorkerConfig
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend, SQLSpecWorkerWakeupConfig
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
from tests.integration._names import table_name_for_test

_AiosqliteConfig: Any
try:
    from sqlspec.adapters.aiosqlite import AiosqliteConfig as _AiosqliteConfig
except ImportError:  # pragma: no cover
    _AiosqliteConfig = None

if TYPE_CHECKING:
    from pathlib import Path

    from pytest import FixtureRequest

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
    pytest.importorskip("aiosqlite")
    from click import Group
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    from litestar_queues import QueueConfig, QueuePlugin
    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend

    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
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


@pytest.mark.parametrize("adapter,autocommit", [("pymssql", False), ("pymssql", True), ("mssql_python", False)])
async def test_sqlserver_native_migration_schema_and_queue_cycle(
    request: "FixtureRequest", adapter: "str", autocommit: "bool"
) -> "None":
    """Native SQL Server configs migrate twice and serve queues in an explicit schema."""
    from uuid import uuid4

    from sqlspec import SQLSpec
    from sqlspec.adapters.mssql_python import MssqlPythonConfig
    from sqlspec.adapters.pymssql import PymssqlConfig

    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend
    from tests.integration.backends.sqlspec._schema import run_queue_migrations

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
    config_type = PymssqlConfig if adapter == "pymssql" else MssqlPythonConfig
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
    pytest.importorskip("aiosqlite")
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    db_path = tmp_path / "async_reconcile.db"
    config = AiosqliteConfig(connection_config={"database": str(db_path)})
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

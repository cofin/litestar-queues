"""Extension-migration tests for the SQLSpec queue backend."""

import contextlib
import importlib
import sqlite3
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, ClassVar, cast

import pytest

pytest.importorskip("sqlspec")

from litestar_queues import WorkerConfig
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.schema import migration_directory
from tests.integration._names import table_name_for_test

if TYPE_CHECKING:
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

        backend = SQLSpecQueueBackend(
            backend_config=SQLSpecBackendConfig(sqlspec_config=config, queue_table_name=table)
        )
        await backend.open()

        try:
            event_log = backend.get_event_log(EventHistoryConfig(batch_size=1, flush_interval=60))
            assert event_log is not None

            from datetime import datetime, timezone

            from litestar_queues.events.models import QueueEvent

            event = QueueEvent(
                id="mig-1",
                occurred_at=datetime.now(timezone.utc),
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
        await run_queue_migrations(config, queue_table_name=table)
        await run_queue_migrations(config, queue_table_name=table)
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
            # A deployment may already have one PostgreSQL-truncated index.
            await driver.execute_script((await migration.up(context))[0])
            await driver.execute_script(f'CREATE INDEX "{legacy_name}" ON "{table_name}" (id)')
        for _ in range(2):
            await run_queue_migrations(config, queue_table_name=table_name)
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

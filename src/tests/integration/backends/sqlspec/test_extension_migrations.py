"""Extension-migration tests for the SQLSpec queue backend."""

import contextlib
import importlib
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

pytest.importorskip("sqlspec")

from litestar_queues import WorkerConfig
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.schema import migration_paths
from tests.integration._names import table_name_for_test

if TYPE_CHECKING:
    from pytest import FixtureRequest

    from tests.integration._backends import PostgresService

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
) -> "FakeSQLSpecConfig":
    config_type = cast(
        "type[FakeSQLSpecConfig]",
        type(
            config_type_name or f"Fake{adapter_name.title().replace('_', '')}Config",
            (),
            {"__module__": f"sqlspec.adapters.{adapter_name}.config"},
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
    paths = tuple(Path(path) for path in migration_paths())

    assert [path.name for path in paths] == ["0001_create_queue_tasks.py", "0002_add_dispatch_checked_at.py"]
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


async def test_dispatch_checked_migration_fresh_mapped_and_downgrade(tmp_path: "Path") -> "None":
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.migrations.context import MigrationContext

    from litestar_queues.backends.sqlspec.extension import configure_queue_migration_extension

    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "mapped.db")})
    configure_queue_migration_extension(
        config,
        queue_table_name="MappedTasks",
        column_map={
            "dispatch_checked_at": "ScanTime",
            "execution_backend": "ExecutionBackend",
            "status": "Lifecycle",
            "created_at": "Created",
            "id": "RecordId",
        },
    )
    initial = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    additive = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0002_add_dispatch_checked_at")
    async with config.provide_session() as driver:
        context = MigrationContext(config=config, driver=driver)
        for statement in await initial.up(context):
            await driver.execute_script(statement)
        await driver.commit()
        assert await additive.up(context) == []
        down = await additive.down(context)
        assert len(down) == 2 and "scantime" in down[1]
        for statement in down:
            await driver.execute_script(statement)
        await driver.commit()
        assert await additive.down(context) == []
        for statement in await additive.up(context):
            await driver.execute_script(statement)
        await driver.commit()
        assert await additive.up(context) == []


async def test_dispatch_checked_migration_upgrades_frozen_schema(tmp_path: "Path") -> "None":
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.migrations.context import MigrationContext

    config = SqliteConfig(connection_config={"database": str(tmp_path / "legacy.db")})
    additive = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0002_add_dispatch_checked_at")
    # Frozen pre-upgrade shape: independent of the current store's CREATE TABLE.
    legacy = """CREATE TABLE queue_task (
        id VARCHAR(64) PRIMARY KEY, task_name VARCHAR(255) NOT NULL,
        task_args TEXT NOT NULL, task_kwargs TEXT NOT NULL, queue VARCHAR(255) NOT NULL,
        execution_backend VARCHAR(255) NOT NULL, execution_profile VARCHAR(255), execution_ref VARCHAR(255),
        worker_id VARCHAR(255), status VARCHAR(255) NOT NULL, priority INTEGER NOT NULL,
        max_retries INTEGER NOT NULL, retry_count INTEGER NOT NULL, scheduled_at TEXT, expires_at TEXT,
        created_at TEXT NOT NULL, queued_at TEXT NOT NULL, started_at TEXT, completed_at TEXT,
        heartbeat_at TEXT, result TEXT NOT NULL, error TEXT, task_key VARCHAR(255) UNIQUE, metadata TEXT NOT NULL
    ); CREATE INDEX legacy_status ON queue_task(status);
    INSERT INTO queue_task(id, task_name, task_args, task_kwargs, queue, execution_backend, status,
        priority, max_retries, retry_count, created_at, queued_at, result, metadata)
        VALUES ('original', 'recover', '[]', '{}', 'default', 'cloudtasks', 'pending', 0, 3, 0,
        '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00', 'null', '{"kept": true}');"""
    with config.provide_session() as driver:
        driver.execute_script(legacy)
        driver.commit()
        context = MigrationContext(config=config, driver=driver)
        assert len(await additive.up(context)) == 2
        for statement in await additive.up(context):
            driver.execute_script(statement)
        driver.commit()
        row = driver.select_one("SELECT id, metadata, dispatch_checked_at FROM queue_task")
        assert row == {"id": "original", "metadata": '{"kept": true}', "dispatch_checked_at": None}
        assert await additive.up(context) == []
        for statement in await additive.down(context):
            driver.execute_script(statement)
        driver.commit()
        assert driver.select_value("SELECT COUNT(*) FROM queue_task") == 1
        assert driver.select_value("SELECT COUNT(*) FROM sqlite_master WHERE name = 'legacy_status'") == 1


async def test_dispatch_checked_migration_requires_driver() -> "None":
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.exceptions import SQLSpecError
    from sqlspec.migrations.context import MigrationContext

    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0002_add_dispatch_checked_at")
    context = MigrationContext(config=SqliteConfig(connection_config={"database": ":memory:"}))
    for function in (migration.up, migration.down):
        with pytest.raises(SQLSpecError, match="active migration driver"):
            await function(context)


async def test_dispatch_checked_spanner_native_upgrade(spanner_service: "Any", request: "FixtureRequest") -> "None":
    pytest.importorskip("google.cloud.spanner")
    from sqlspec.adapters.spanner import SpannerSyncConfig
    from sqlspec.migrations.context import MigrationContext

    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend
    from tests.integration.backends.sqlspec.test_spanner_contract import (
        _ensure_spanner_emulator_database,
        _spanner_emulator_connection_config,
    )

    _ensure_spanner_emulator_database(spanner_service)
    table = table_name_for_test("dispatch_upgrade", "spanner", request.node.nodeid)
    config = SpannerSyncConfig(
        connection_config=_spanner_emulator_connection_config(spanner_service),
        extension_config={QUEUE_EXTENSION_NAME: {"queue_table_name": table}},
    )
    backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(sqlspec_config=config, queue_table_name=table))
    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0002_add_dispatch_checked_at")
    await backend.open()
    try:
        await backend.create_schema()
        record = await backend.enqueue("native.upgrade", execution_backend="cloudtasks")
        database = cast("Any", config.get_database())
        with config.provide_session() as driver:
            context = MigrationContext(config=config, driver=driver)
            assert await migration.up(context) == []
            statements = await migration.down(context)
        database.update_ddl(statements).result(120)
        with config.provide_session() as driver:
            statements = await migration.up(MigrationContext(config=config, driver=driver))
        database.update_ddl(statements).result(120)
        page = await backend.list_dispatch_repair_candidates("cloudtasks", limit=1)
        assert page.records[0].id == record.id
        with config.provide_session() as driver:
            statements = await migration.down(MigrationContext(config=config, driver=driver))
        database.update_ddl(statements).result(120)
        initial = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
        database.update_ddl(await initial.down(MigrationContext(config=config))).result(120)
    finally:
        await backend.close()


async def test_dispatch_checked_long_postgres_table_is_idempotent(postgres_service: "PostgresService") -> "None":
    from uuid import uuid4

    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.migrations.context import MigrationContext

    table = "dispatch_" + uuid4().hex + "_" + "x" * 20
    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        },
        extension_config={QUEUE_EXTENSION_NAME: {"queue_table_name": table}},
    )
    initial = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    additive = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0002_add_dispatch_checked_at")
    async with config.provide_session() as driver:
        context = MigrationContext(config=config, driver=driver)
        try:
            for statement in await initial.up(context):
                await driver.execute_script(statement)
            await driver.commit()
            assert await additive.up(context) == []
            down = await additive.down(context)
            assert len(down) == 2
            for statement in down:
                await driver.execute_script(statement)
            await driver.commit()
            for statement in await additive.up(context):
                await driver.execute_script(statement)
            await driver.commit()
            assert await additive.up(context) == []
        finally:
            for statement in await initial.down(context):
                await driver.execute_script(statement)
            await driver.commit()

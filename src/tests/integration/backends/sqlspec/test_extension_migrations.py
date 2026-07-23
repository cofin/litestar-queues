"""Extension-migration tests for the SQLSpec queue backend."""

import importlib
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

pytest.importorskip("sqlspec")

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
                    "table_name": "custom_queue",
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
    plugin = QueuePlugin(QueueConfig(queue_backend=backend_config, initialize_schedules=False))

    plugin.on_cli_init(Group())

    queue_settings = sqlspec_config.get_migration_commands().extension_configs[QUEUE_EXTENSION_NAME]
    assert queue_settings == {
        "table_name": "custom_queue",
        "maintenance_table_name": "custom_maintenance",
        "task_reservation_table_name": "custom_reservation",
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
            "duckdb", dialect="duckdb", extension_config={QUEUE_EXTENSION_NAME: {"table_name": "custom_queue"}}
        )
    )

    statements = await migration.up(context)
    assert any("custom_queue_maintenance" in statement for statement in statements)
    assert any("custom_queue_reservation" in statement for statement in statements)


async def test_sqlspec_backend_exposes_packaged_migration_assets() -> "None":
    paths = tuple(Path(path) for path in migration_paths())

    assert [path.name for path in paths] == ["0001_create_queue_tasks.py"]
    migration_content = paths[0].read_text()
    assert "create_queue_store" in migration_content
    assert "create_maintenance_store" in migration_content
    assert "create_task_reservation_store" in migration_content
    assert "return SQLSpecQueueStore(" not in migration_content
    assert "CREATE TABLE IF NOT EXISTS queue_task" not in migration_content


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
        extension_config={QUEUE_EXTENSION_NAME: {"table_name": table_name}},
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

"""SQLSpec schema-qualified table quoting regression tests."""

import re
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, ClassVar, cast

import pytest

pytest.importorskip("sqlspec")

from sqlspec import SQLSpec
from sqlspec.migrations.schema import SchemaTarget

from litestar_queues.backends.sqlspec.extension import configure_queue_migration_extension
from litestar_queues.backends.sqlspec.schema import (
    event_history_table_name_for,
    maintenance_table_name_for,
    task_reservation_table_name_for,
)
from litestar_queues.backends.sqlspec.stores import create_queue_store
from tests.integration._names import table_name_for_test

_AsyncpgConfig: Any
try:
    from sqlspec.adapters.asyncpg import AsyncpgConfig as _AsyncpgConfig
except ImportError:  # pragma: no cover
    _AsyncpgConfig = None

if TYPE_CHECKING:
    from pytest import FixtureRequest


class FakeSQLSpecConfig(SimpleNamespace):
    """Structural SQLSpec config used by the quoting regression test."""

    is_async: ClassVar[bool] = True
    extension_config: "dict[str, object]"
    statement_config: "SimpleNamespace"
    connection_config: "dict[str, object]"


class _RecordingAsyncDriver:
    def __init__(self) -> "None":
        self.queries: "list[str]" = []
        self.executed: "list[str]" = []

    async def select_one_or_none(self, sql_text: "str") -> "None":
        self.queries.append(sql_text)
        return None

    async def execute(self, sql_text: "str") -> "None":
        self.executed.append(sql_text)


def test_sqlspec_queue_store_create_statements_split_schema_qualified_table_name() -> "None":
    """CREATE TABLE must quote schema and table as separate identifiers."""
    store = create_queue_store(_fake_adapter_config("asyncpg", "postgres"), table_name="app.queue_task")

    ddl = "\n".join(store.create_statements())

    assert 'CREATE TABLE IF NOT EXISTS "app"."queue_task"' in ddl


@pytest.mark.parametrize(
    ("adapter", "dialect", "expected_table_quote", "expected_index_table"),
    [
        ("asyncpg", "postgres", '"app"."queue_task"', '"app"."queue_task"'),
        ("cockroach_asyncpg", "postgres", '"app"."queue_task"', '"app"."queue_task"'),
        ("asyncmy", "mysql", "`app`.`queue_task`", "`app`.`queue_task`"),
        ("pymysql", "mysql", "`app`.`queue_task`", "`app`.`queue_task`"),
        ("pymssql", "mssql", "[app].[queue_task]", "[app].[queue_task]"),
        ("arrow_odbc", "tsql", "[app].[queue_task]", "[app].[queue_task]"),
    ],
)
def test_schema_qualified_create_table_statement_and_companion_indexes(
    adapter: "str", dialect: "str", expected_table_quote: "str", expected_index_table: "str"
) -> "None":
    """Table DDL, companion indexes, and drop statements quote schema and table separately."""
    store = create_queue_store(
        _fake_adapter_config(adapter, dialect, is_async=(adapter not in {"pymysql", "pymssql", "arrow_odbc"})),
        table_name="app.queue_task",
        terminal_ttl=3600.0 if adapter == "cockroach_asyncpg" else None,
    )

    table_ddl = store.create_table_statement()
    assert expected_table_quote in table_ddl
    target = SchemaTarget.from_ddl(store.table_name, table_ddl, dialect=store.sqlglot_dialect())
    assert target.schema == "app"
    assert target.table_name == "queue_task"
    assert len(target.create_table.columns) == 25

    indexes = store.companion_indexes()
    companion_names = {name for name, _ in indexes}
    assert len(companion_names) >= 3
    assert any("pending" in name for name in companion_names)
    assert all(expected_index_table in stmt for _, stmt in indexes)

    drops = store.drop_statements()
    assert drops
    assert expected_table_quote in drops[-1]
    assert all("app.queue_task" not in re.sub(r"N?'[^']*'", "", stmt) for stmt in drops)


@pytest.mark.anyio
async def test_cockroach_schema_qualified_reconcile_storage_options() -> "None":
    """CockroachDB TTL reconciliation quotes schema-qualified table names separately."""
    store = create_queue_store(
        _fake_adapter_config("cockroach_asyncpg", "postgres"), table_name="app.queue_task", terminal_ttl=3600.0
    )
    driver = _RecordingAsyncDriver()

    await store.reconcile_storage_options_async(driver)

    assert driver.queries == ['SHOW CREATE TABLE "app"."queue_task"']
    assert len(driver.executed) == 1
    assert driver.executed[0].startswith('ALTER TABLE "app"."queue_task" SET (ttl = \'on\'')


@pytest.mark.anyio
@pytest.mark.xdist_group("postgres_service")
async def test_postgres_schema_qualified_reconcile_repair_and_downgrade(request: "FixtureRequest") -> "None":
    """Live PostgreSQL reconciles, repairs indexes, and downgrades schema-qualified queue tables."""
    if _AsyncpgConfig is None:
        pytest.skip("asyncpg adapter is not installed")
    try:
        postgres_service = request.getfixturevalue("postgres_service")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Docker service not available: {exc}")

    schema_name = table_name_for_test("lq_schema", "asyncpg", request.node.nodeid)[:40]
    qualified_queue = f"{schema_name}.queue_task"
    version_table = f"{schema_name}.ddl_migrations"
    config = _AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        },
        migration_config={"version_table_name": version_table},
    )
    configure_queue_migration_extension(
        config, queue_table_name=qualified_queue, event_history_enabled=True, migration_mode="reconcile"
    )
    store = create_queue_store(config, table_name=qualified_queue)
    pending_index = store._index_name("pending")
    expected_tables = {
        "queue_task",
        event_history_table_name_for(qualified_queue).split(".", maxsplit=1)[1],
        maintenance_table_name_for(qualified_queue).split(".", maxsplit=1)[1],
        task_reservation_table_name_for(qualified_queue).split(".", maxsplit=1)[1],
    }
    manager = SQLSpec()
    try:
        async with manager.provide_session(config) as driver:
            await driver.execute_script(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
            await driver.commit()

        await config.migrate_up(echo=False)
        await config.migrate_up(echo=False)

        async with manager.provide_session(config) as driver:
            tables = await driver.select(
                "SELECT tablename FROM pg_tables WHERE schemaname = :schema", {"schema": schema_name}
            )
            assert expected_tables.issubset({str(row["tablename"]) for row in tables})
            ext_rows = await driver.select_value(
                f'SELECT COUNT(*) FROM "{schema_name}"."ddl_migrations" '
                "WHERE version_num LIKE 'ext_litestar_queues_%'"
            )
            assert int(ext_rows or 0) == 0
            await driver.execute_script(f'DROP INDEX "{schema_name}"."{pending_index}"')
            await driver.commit()

        await config.migrate_up(echo=False)
        async with manager.provide_session(config) as driver:
            indexes = await driver.select(
                "SELECT indexname FROM pg_indexes WHERE schemaname = :schema AND tablename = 'queue_task'",
                {"schema": schema_name},
            )
            index_names = {str(row["indexname"]) for row in indexes}
            assert pending_index in index_names
            assert len(index_names) >= 3

        await config.migrate_down("base", echo=False)
        async with manager.provide_session(config) as driver:
            remaining = await driver.select(
                "SELECT tablename FROM pg_tables WHERE schemaname = :schema", {"schema": schema_name}
            )
            assert expected_tables.isdisjoint({str(row["tablename"]) for row in remaining})
    finally:
        async with manager.provide_session(config) as driver:
            await driver.execute_script(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
            await driver.commit()
        await manager.close_all_pools()


def _fake_adapter_config(adapter: "str", dialect: "str", *, is_async: "bool" = True) -> "FakeSQLSpecConfig":
    config_type = cast(
        "type[FakeSQLSpecConfig]",
        type(
            f"Fake{adapter.title()}Config",
            (FakeSQLSpecConfig,),
            {"__module__": f"sqlspec.adapters.{adapter}.config", "is_async": is_async},
        ),
    )
    config = config_type()
    config.extension_config = {}
    config.statement_config = SimpleNamespace(dialect=dialect)
    config.connection_config = {}
    return config

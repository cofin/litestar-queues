"""SQLSpec schema-qualified table quoting regression test."""

from types import SimpleNamespace
from typing import ClassVar, cast

import pytest

pytest.importorskip("sqlspec")

from sqlspec.migrations.schema import SchemaTarget

from litestar_queues.backends.sqlspec.stores import create_queue_store


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
        ("pymysql", "mysql", "`app`.`queue_task`", "`app`.`queue_task`"),
        ("pymssql", "mssql", "[app].[queue_task]", "[app].[queue_task]"),
        ("arrow_odbc", "tsql", "[app].[queue_task]", "[app].[queue_task]"),
    ],
)
def test_schema_qualified_create_table_statement_and_companion_indexes(
    adapter: "str", dialect: "str", expected_table_quote: "str", expected_index_table: "str"
) -> "None":
    """Table DDL and companion indexes quote schema and table separately and parse via SchemaTarget."""
    store = create_queue_store(
        _fake_adapter_config(adapter, dialect, is_async=(adapter not in {"pymssql", "arrow_odbc"})),
        table_name="app.queue_task",
        terminal_ttl=3600.0 if adapter == "cockroach_asyncpg" else None,
    )

    table_ddl = store.create_table_statement()
    assert expected_table_quote in table_ddl
    target = SchemaTarget.from_ddl(store.table_name, table_ddl, dialect=store.sqlglot_dialect())
    assert len(target.create_table.columns) == 25

    indexes = store.companion_indexes()
    assert indexes
    assert all(expected_index_table in stmt for _, stmt in indexes)


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

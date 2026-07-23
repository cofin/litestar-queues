"""SQLSpec schema-qualified table quoting regression test."""

from types import SimpleNamespace
from typing import cast

import pytest

pytest.importorskip("sqlspec")

from litestar_queues.backends.sqlspec.stores import create_queue_store


class FakeSQLSpecConfig(SimpleNamespace):
    """Structural SQLSpec config used by the quoting regression test."""

    extension_config: "dict[str, object]"
    statement_config: "SimpleNamespace"
    connection_config: "dict[str, object]"


def test_sqlspec_queue_store_create_statements_split_schema_qualified_table_name() -> "None":
    """CREATE TABLE must quote schema and table as separate identifiers."""
    store = create_queue_store(_fake_postgres_config(), table_name="app.queue_task")

    ddl = "\n".join(store.create_statements())

    assert 'CREATE TABLE IF NOT EXISTS "app"."queue_task"' in ddl


def _fake_postgres_config() -> "FakeSQLSpecConfig":
    config_type = cast(
        "type[FakeSQLSpecConfig]",
        type("FakeAsyncpgConfig", (FakeSQLSpecConfig,), {"__module__": "sqlspec.adapters.asyncpg.config"}),
    )
    config = config_type()
    config.extension_config = {}
    config.statement_config = SimpleNamespace(dialect="postgres")
    config.connection_config = {}
    return config

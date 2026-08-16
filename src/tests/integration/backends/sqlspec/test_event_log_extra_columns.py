"""Adopter-declared scoping columns on the SQLSpec event-history table."""

import importlib
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

pytest.importorskip("sqlspec")

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig
from litestar_queues.backends.sqlspec.event_log import create_event_log_store
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME, configure_queue_migration_extension
from litestar_queues.events import EventHistoryConfig, EventHistoryExtraColumn, QueueEventsConfig, publish_task_log
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.task import clear_task_registry
from tests.integration._names import table_name_for_test
from tests.integration.backends.sqlspec._schema import bootstrap_queue_schema

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_databases.docker.postgres import PostgresService

    from litestar_queues.backends.sqlspec.event_log import SQLSpecQueueEventLog

pytestmark = pytest.mark.anyio

_TENANT_COLUMN = EventHistoryExtraColumn(name="tenant_id", source="tenant_id", indexed=True)


@pytest.fixture
def aiosqlite_history_config(tmp_path: "Path") -> "Any":
    """Return a name-binding SQLSpec adapter config."""
    pytest.importorskip("aiosqlite")
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    return AiosqliteConfig(connection_config={"database": str(tmp_path / "extra-columns.db")})


@pytest.fixture
def psycopg_history_config(postgres_service: "PostgresService", request: "pytest.FixtureRequest") -> "Any":
    """Return a positional-binding SQLSpec adapter config backed by a real PostgreSQL service."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig

    return PsycopgAsyncConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "dbname": postgres_service.database,
        },
        extension_config={
            QUEUE_EXTENSION_NAME: {"table_name": table_name_for_test("lq_extra_cols", "psycopg", request.node.nodeid)}
        },
    )


@pytest.fixture(params=["aiosqlite", "psycopg"])
def event_history_config(request: "pytest.FixtureRequest") -> "Any":
    """Return a SQLSpec adapter config for a name-binding and a positional-binding engine."""
    return request.getfixturevalue(f"{request.param}_history_config")


async def _run_scoped_task(config: "Any", *, tenants: "tuple[str, ...]") -> "list[Any]":
    clear_task_registry()

    @task("tasks.tenant_scoped")
    async def tenant_scoped(*, tenant: "str") -> "str":
        await publish_task_log("scoped", payload={"tenant_id": tenant, "stage": "load", "note": "kept"})
        return tenant

    history = EventHistoryConfig(batch_size=1, flush_interval=60, extra_columns=(_TENANT_COLUMN,))
    backend_config = SQLSpecBackendConfig(sqlspec_config=config, event_history_extra_columns=(_TENANT_COLUMN,))
    await bootstrap_queue_schema(backend_config, event_history_enabled=True)
    queue_config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend=backend_config,
        execution_backend="immediate",
        events=QueueEventsConfig(history=history),
    )

    async with QueueService(queue_config) as service:
        for tenant in tenants:
            await service.enqueue(tenant_scoped, tenant=tenant)

        event_log = cast("SQLSpecQueueEventLog", service.get_queue_backend().get_event_log(history))
        await event_log.flush_events()

        scoped = await event_log.list_events(extra={"tenant_id": tenants[0]})
        everything = await event_log.list_events()

        with pytest.raises(QueueConfigurationError):
            await event_log.list_events(extra={"unknown": "x"})

    assert len(everything) > len(scoped)
    return scoped


async def test_extra_column_is_created_written_and_filterable(event_history_config: "Any") -> "None":
    """A declared extra column is provisioned, populated from the payload, and filterable."""
    scoped = await _run_scoped_task(event_history_config, tenants=("t-1", "t-2"))

    assert scoped, "expected at least one event for tenant t-1"
    assert {record.detail["tenant_id"] for record in scoped} == {"t-1"}
    # ``detail`` remains the complete payload; the column is an indexing/filtering copy.
    assert all(record.detail["note"] == "kept" for record in scoped)


@pytest.mark.parametrize("adapter", ["aiosqlite", "duckdb", "psycopg"])
async def test_packaged_migration_ddl_matches_managed_schema(
    adapter: "str", request: "pytest.FixtureRequest", tmp_path: "Path"
) -> "None":
    """The packaged migration emits the same event-history DDL as the managed store."""
    if adapter == "duckdb":
        pytest.importorskip("duckdb")
        from sqlspec.adapters.duckdb import DuckDBConfig

        config: "Any" = DuckDBConfig(connection_config={"database": str(tmp_path / "extra.duckdb")})
    else:
        config = request.getfixturevalue(f"{adapter}_history_config")

    configure_queue_migration_extension(
        config, queue_table_name="queue_task", event_history_enabled=True, event_history_extra_columns=(_TENANT_COLUMN,)
    )
    settings = config.get_migration_commands().extension_configs[QUEUE_EXTENSION_NAME]
    config.extension_config = {QUEUE_EXTENSION_NAME: settings}

    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    statements = await migration.up(SimpleNamespace(config=config))

    expected = create_event_log_store(
        config, queue_table_name="queue_task", extra_columns=(_TENANT_COLUMN,)
    ).create_statements()

    assert settings["event_history_extra_columns"] == ({"name": "tenant_id", "source": "tenant_id", "indexed": True},)
    assert any("tenant_id" in statement for statement in expected)
    for statement in expected:
        assert statement in statements

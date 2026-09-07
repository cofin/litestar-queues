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
from litestar_queues.events.query import QueueEventQuery
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
            QUEUE_EXTENSION_NAME: {
                "queue_table_name": table_name_for_test("lq_extra_cols", "psycopg", request.node.nodeid)
            }
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

        scoped = (await event_log.query_events(QueueEventQuery(), extra={"tenant_id": tenants[0]})).items
        everything = (await event_log.query_events(QueueEventQuery())).items

        with pytest.raises(QueueConfigurationError):
            await event_log.query_events(QueueEventQuery(), extra={"unknown": "x"})

    assert len(everything) > len(scoped)
    return list(scoped)


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


@pytest.mark.parametrize("adapter", ["aiosqlite", "duckdb", "psycopg"])
async def test_packaged_migration_ddl_includes_dimensions(
    adapter: "str", request: "pytest.FixtureRequest", tmp_path: "Path"
) -> "None":
    """The packaged migration DDL includes the package-owned dimensions."""
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

    create_table = next(s for s in statements if s.startswith("CREATE TABLE") and "event_history" in s)
    for dimension in ("scope", "scope_key", "actor", "entity"):
        assert dimension in create_table
    assert any("scope_key" in s and s.startswith("CREATE INDEX") for s in statements)
    assert any("entity" in s and s.startswith("CREATE INDEX") for s in statements)


@pytest.mark.parametrize("descending", [False, True])
@pytest.mark.parametrize("offset,limit", [(0, 2), (2, 2), (8, 2), (0, None)])
async def test_filtered_history_total_precedes_page_window(
    aiosqlite_history_config: "Any", *, descending: bool, offset: int, limit: "int | None"
) -> None:
    from datetime import datetime, timezone

    from msgspec.structs import replace as replace_event

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend
    from litestar_queues.events import QueueEvent, QueueEventActor, QueueEventEntityRef
    from litestar_queues.events._log_records import event_entity_key

    backend_config = SQLSpecBackendConfig(
        sqlspec_config=aiosqlite_history_config, event_history_extra_columns=(_TENANT_COLUMN,)
    )
    await bootstrap_queue_schema(backend_config, event_history_enabled=True)
    backend = SQLSpecQueueBackend(backend_config=backend_config)
    await backend.open()
    try:
        log = backend.get_event_log(EventHistoryConfig(batch_size=20, extra_columns=(_TENANT_COLUMN,), strict=True))
        assert log is not None
        entity = QueueEventEntityRef(type="document", id="1")
        base = QueueEvent(
            type="task.log",
            scope="task",
            task_id="task",
            task_name="demo",
            scope_key="scope",
            level="info",
            entity=entity,
            actor=QueueEventActor(type="user", id="actor"),
            payload={"tenant_id": "tenant"},
            occurred_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            sequence=1,
        )
        for index in range(5):
            await log.publish_event(replace_event(base, id=f"match-{index}"))
        mismatches: list[dict[str, Any]] = [
            {"task_id": "other"},
            {"task_name": "other"},
            {"type": "task.progress"},
            {"scope": "queue"},
            {"scope_key": "other"},
            {"level": "error"},
            {"entity": QueueEventEntityRef(type="document", id="2")},
            {"actor": QueueEventActor(type="other", id="actor")},
            {"actor": QueueEventActor(type="user", id="other")},
            {"payload": {"tenant_id": "other"}},
        ]
        for index, changes in enumerate(mismatches):
            await log.publish_event(replace_event(base, id=f"other-{index}", **changes))
        query = QueueEventQuery(
            task_id="task",
            task_name="demo",
            event_type="task.log",
            scope="task",
            scope_key="scope",
            level="info",
            entity=event_entity_key(entity),
            order="desc" if descending else "asc",
            limit=limit,
            offset=offset,
        )
        extra = {"tenant_id": "tenant", "actor_id": "actor", "actor_type": "user"}
        page = await log.query_events(query, extra=extra)
        expected = [f"match-{index}" for index in range(5)]
        if descending:
            expected.reverse()
        expected = expected[offset:] if limit is None else expected[offset : offset + limit]
        assert [record.event_id for record in page.items] == expected
        assert page.total == 5
        assert page.offset == offset
        assert page.limit == (5 if limit is None else limit)
        empty = await log.query_events(query, extra={**extra, "tenant_id": "absent"})
        assert empty.total == 0
        assert empty.items == []
        with pytest.raises(QueueConfigurationError):
            await log.query_events(query, extra={"undeclared": "value"})
    finally:
        await backend.close()

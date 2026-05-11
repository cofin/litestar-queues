import asyncio
import contextlib
import importlib
import sqlite3
import sys
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from subprocess import run
from types import SimpleNamespace
from typing import Any, cast

import pytest
from litestar import Litestar

pytest.importorskip("aiosqlite")
pytest.importorskip("sqlspec")

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.extensions.litestar import SQLSpecPlugin

from litestar_queues import QueueConfig, QueuePlugin, QueueService, task
from litestar_queues.backends import get_queue_backend_class, list_queue_backends
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.schema import migration_paths
from litestar_queues.backends.sqlspec.store import (
    AdbcQueueStore,
    AiomysqlQueueStore,
    AiosqliteQueueStore,
    AsyncmyQueueStore,
    AsyncpgQueueStore,
    BigQueryQueueStore,
    CockroachAsyncpgQueueStore,
    CockroachPsycopgAsyncQueueStore,
    CockroachPsycopgSyncQueueStore,
    DuckDBQueueStore,
    MysqlConnectorAsyncQueueStore,
    MysqlConnectorSyncQueueStore,
    OracledbAsyncQueueStore,
    OracledbSyncQueueStore,
    PsqlpyQueueStore,
    PsycopgAsyncQueueStore,
    PsycopgSyncQueueStore,
    PymysqlQueueStore,
    SpannerQueueStore,
    SqliteQueueStore,
    create_queue_store,
)
from litestar_queues.task import clear_task_registry

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def clean_task_registry() -> None:
    clear_task_registry()


@pytest.fixture
async def sqlspec_backend(tmp_path: Path) -> AsyncIterator[SQLSpecQueueBackend]:
    backend = SQLSpecQueueBackend(sqlspec_config=_sqlite_config(tmp_path / "queue.db"))
    await backend.open()
    try:
        yield backend
    finally:
        await backend.close()


def _sqlite_config(path: Path) -> AiosqliteConfig:
    return AiosqliteConfig(connection_config={"database": str(path)})


def _fake_adapter_config(
    adapter_name: str,
    *,
    dialect: str | None = None,
    config_type_name: str | None = None,
    connection_config: dict[str, Any] | None = None,
    extension_config: dict[str, Any] | None = None,
) -> Any:
    config_type = type(
        config_type_name or f"Fake{adapter_name.title().replace('_', '')}Config",
        (),
        {"__module__": f"sqlspec.adapters.{adapter_name}.config"},
    )
    config = config_type()
    config.extension_config = extension_config or {}
    config.statement_config = SimpleNamespace(dialect=dialect)
    config.connection_config = connection_config or {}
    return config


@dataclass(slots=True)
class StubEvent:
    event_id: str
    payload: dict[str, Any]
    metadata: dict[str, Any] | None = None


class StubAsyncEventChannel:
    __slots__ = ("_backend_name", "_events", "acked", "published")

    def __init__(self, backend_name: str = "table_queue") -> None:
        self._backend_name = backend_name
        self.acked: list[str] = []
        self.published: list[tuple[str, dict[str, Any], dict[str, Any] | None]] = []
        self._events: asyncio.Queue[StubEvent] = asyncio.Queue()

    async def publish(
        self,
        channel: str,
        payload: dict[str, Any],
        metadata: dict[str, Any] | None = None,
    ) -> str:
        event_id = f"event-{len(self.published) + 1}"
        self.published.append((channel, payload, metadata))
        await self._events.put(StubEvent(event_id, payload, metadata))
        return event_id

    async def iter_events(self, channel: str, *, poll_interval: float | None = None) -> AsyncIterator[StubEvent]:
        while True:
            if poll_interval is None:
                event = await self._events.get()
            else:
                try:
                    event = await asyncio.wait_for(self._events.get(), timeout=poll_interval)
                except TimeoutError:
                    continue
            if channel == self.published[-1][0]:
                yield event

    async def ack(self, event_id: str) -> None:
        self.acked.append(event_id)

    async def shutdown(self) -> None:
        return None


async def test_sqlspec_backend_is_registered_without_advanced_alchemy() -> None:
    assert "sqlspec" in list_queue_backends()
    assert get_queue_backend_class("sqlspec") is SQLSpecQueueBackend


def test_sqlspec_backend_package_import_does_not_import_sqlspec() -> None:
    code = """
import builtins

original_import = builtins.__import__

def blocked_import(name, *args, **kwargs):
    if name == "sqlspec" or name.startswith("sqlspec."):
        raise ModuleNotFoundError(name)
    return original_import(name, *args, **kwargs)

builtins.__import__ = blocked_import
import litestar_queues
from litestar_queues.backends.sqlspec import SQLSpecQueueBackend
assert "SQLSpecQueueBackend" in litestar_queues.__all__
assert SQLSpecQueueBackend is not None
"""
    result = run([sys.executable, "-c", code], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr


def test_sqlspec_backend_store_factory_does_not_import_optional_adapter_drivers() -> None:
    code = """
import builtins
from types import SimpleNamespace

blocked_prefixes = (
    "adbc_driver_manager",
    "adbc_driver_bigquery",
    "adbc_driver_duckdb",
    "adbc_driver_flightsql",
    "adbc_driver_postgresql",
    "adbc_driver_snowflake",
    "adbc_driver_sqlite",
    "aiomysql",
    "aiosqlite",
    "asyncmy",
    "asyncpg",
    "duckdb",
    "google.cloud.bigquery",
    "google.cloud.spanner",
    "mysql.connector",
    "oracledb",
    "psqlpy",
    "psycopg",
    "pymysql",
    "sqlspec.adapters.adbc",
    "sqlspec.adapters.aiomysql",
    "sqlspec.adapters.aiosqlite",
    "sqlspec.adapters.asyncmy",
    "sqlspec.adapters.asyncpg",
    "sqlspec.adapters.bigquery",
    "sqlspec.adapters.cockroach_asyncpg",
    "sqlspec.adapters.cockroach_psycopg",
    "sqlspec.adapters.duckdb",
    "sqlspec.adapters.mysqlconnector",
    "sqlspec.adapters.oracledb",
    "sqlspec.adapters.psqlpy",
    "sqlspec.adapters.psycopg",
    "sqlspec.adapters.pymysql",
    "sqlspec.adapters.spanner",
)
blocked_package_prefixes = tuple(f"{name}." for name in blocked_prefixes)
original_import = builtins.__import__

def blocked_import(name, *args, **kwargs):
    if name in blocked_prefixes or name.startswith(blocked_package_prefixes):
        raise ModuleNotFoundError(name)
    return original_import(name, *args, **kwargs)

builtins.__import__ = blocked_import

from litestar_queues.backends.sqlspec.store import (
    AdbcQueueStore,
    AiomysqlQueueStore,
    AiosqliteQueueStore,
    AsyncmyQueueStore,
    AsyncpgQueueStore,
    BigQueryQueueStore,
    CockroachAsyncpgQueueStore,
    CockroachPsycopgAsyncQueueStore,
    CockroachPsycopgSyncQueueStore,
    DuckDBQueueStore,
    MysqlConnectorAsyncQueueStore,
    MysqlConnectorSyncQueueStore,
    OracledbAsyncQueueStore,
    OracledbSyncQueueStore,
    PsqlpyQueueStore,
    PsycopgAsyncQueueStore,
    PsycopgSyncQueueStore,
    PymysqlQueueStore,
    SqliteQueueStore,
    SpannerQueueStore,
    create_queue_store,
)

def fake_config(adapter_name, dialect, config_type_name):
    config_type = type(config_type_name, (), {"__module__": f"sqlspec.adapters.{adapter_name}.config"})
    config = config_type()
    config.extension_config = {}
    config.statement_config = SimpleNamespace(dialect=dialect)
    config.connection_config = {}
    return config

expected = (
    ("adbc", "adbc", "FakeAdbcConfig", AdbcQueueStore),
    ("aiomysql", "mysql", "AiomysqlConfig", AiomysqlQueueStore),
    ("aiosqlite", "sqlite", "AiosqliteConfig", AiosqliteQueueStore),
    ("asyncmy", "mysql", "AsyncmyConfig", AsyncmyQueueStore),
    ("asyncpg", "postgres", "AsyncpgConfig", AsyncpgQueueStore),
    ("bigquery", "bigquery", "BigQueryConfig", BigQueryQueueStore),
    ("cockroach_asyncpg", "postgres", "CockroachAsyncpgConfig", CockroachAsyncpgQueueStore),
    ("cockroach_psycopg", "postgres", "CockroachPsycopgSyncConfig", CockroachPsycopgSyncQueueStore),
    ("cockroach_psycopg", "postgres", "CockroachPsycopgAsyncConfig", CockroachPsycopgAsyncQueueStore),
    ("duckdb", "duckdb", "DuckDBConfig", DuckDBQueueStore),
    ("mysqlconnector", "mysql", "MysqlConnectorSyncConfig", MysqlConnectorSyncQueueStore),
    ("mysqlconnector", "mysql", "MysqlConnectorAsyncConfig", MysqlConnectorAsyncQueueStore),
    ("oracledb", "oracle", "OracleSyncConfig", OracledbSyncQueueStore),
    ("oracledb", "oracle", "OracleAsyncConfig", OracledbAsyncQueueStore),
    ("psqlpy", "postgres", "PsqlpyConfig", PsqlpyQueueStore),
    ("psycopg", "postgres", "PsycopgSyncConfig", PsycopgSyncQueueStore),
    ("psycopg", "postgres", "PsycopgAsyncConfig", PsycopgAsyncQueueStore),
    ("pymysql", "mysql", "PyMysqlConfig", PymysqlQueueStore),
    ("spanner", "spanner", "SpannerConfig", SpannerQueueStore),
    ("sqlite", "sqlite", "SqliteConfig", SqliteQueueStore),
)

for adapter_name, dialect, config_type_name, expected_store in expected:
    store = create_queue_store(fake_config(adapter_name, dialect, config_type_name), table_name="queue_tasks")
    assert isinstance(store, expected_store), adapter_name
"""
    result = run([sys.executable, "-c", code], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr


async def test_sqlspec_backend_exposes_config_type_and_builder_store(tmp_path: Path) -> None:
    backend_config = SQLSpecBackendConfig(table_name="queue_tasks")
    store = create_queue_store(_sqlite_config(tmp_path / "queue.db"), table_name=backend_config.table_name)

    assert backend_config.table_name == "queue_tasks"
    assert isinstance(store, AiosqliteQueueStore)
    assert store.table_name == "queue_tasks"
    assert any('"queue_tasks"' in statement for statement in store.create_statements())

    insert_statement = store.insert_task({"id": "task-1", "task_name": "tasks.sync"}).build(dialect="sqlite")
    pending_statement = store.list_pending(now=datetime.now(UTC).isoformat(), limit=10, queue="default").build(
        dialect="sqlite"
    )

    assert 'INSERT INTO "queue_tasks"' in insert_statement.sql
    assert "task-1" in insert_statement.parameters.values()
    assert 'FROM "queue_tasks"' in pending_statement.sql
    assert "queue" in pending_statement.sql


@pytest.mark.parametrize(
    (
        "adapter_name",
        "dialect",
        "config_type_name",
        "connection_config",
        "expected_store_type",
        "expected_sql_fragment",
    ),
    (
        ("adbc", "duckdb", "FakeAdbcConfig", {"driver_name": "adbc_driver_duckdb"}, AdbcQueueStore, "JSON"),
        (
            "adbc",
            "postgres",
            "FakeAdbcConfig",
            {"driver_name": "adbc_driver_postgresql"},
            AdbcQueueStore,
            "WHERE status IN",
        ),
        ("adbc", "bigquery", "FakeAdbcConfig", {"driver_name": "adbc_driver_bigquery"}, AdbcQueueStore, "CLUSTER BY"),
        ("adbc", None, "FakeAdbcConfig", {"driver_name": "adbc_driver_snowflake"}, AdbcQueueStore, "VARIANT"),
        ("aiomysql", "mysql", "AiomysqlConfig", {}, AiomysqlQueueStore, "ENGINE=InnoDB"),
        ("aiosqlite", "sqlite", "AiosqliteConfig", {}, AiosqliteQueueStore, '"queue_tasks"'),
        ("asyncmy", "mysql", "AsyncmyConfig", {}, AsyncmyQueueStore, "ENGINE=InnoDB"),
        ("asyncpg", "postgres", "AsyncpgConfig", {}, AsyncpgQueueStore, "WHERE status IN"),
        ("bigquery", "bigquery", "BigQueryConfig", {}, BigQueryQueueStore, "CLUSTER BY"),
        (
            "cockroach_asyncpg",
            "postgres",
            "CockroachAsyncpgConfig",
            {},
            CockroachAsyncpgQueueStore,
            "TIMESTAMPTZ",
        ),
        (
            "cockroach_psycopg",
            "postgres",
            "CockroachPsycopgSyncConfig",
            {},
            CockroachPsycopgSyncQueueStore,
            "TIMESTAMPTZ",
        ),
        (
            "cockroach_psycopg",
            "postgres",
            "CockroachPsycopgAsyncConfig",
            {},
            CockroachPsycopgAsyncQueueStore,
            "TIMESTAMPTZ",
        ),
        ("duckdb", "duckdb", "DuckDBConfig", {}, DuckDBQueueStore, "JSON"),
        ("mysqlconnector", "mysql", "MysqlConnectorSyncConfig", {}, MysqlConnectorSyncQueueStore, "ENGINE=InnoDB"),
        ("mysqlconnector", "mysql", "MysqlConnectorAsyncConfig", {}, MysqlConnectorAsyncQueueStore, "ENGINE=InnoDB"),
        ("oracledb", "oracle", "OracleSyncConfig", {}, OracledbSyncQueueStore, "BLOB CHECK (args_json IS JSON)"),
        ("oracledb", "oracle", "OracleAsyncConfig", {}, OracledbAsyncQueueStore, "BLOB CHECK (args_json IS JSON)"),
        ("psqlpy", "postgres", "PsqlpyConfig", {}, PsqlpyQueueStore, "WHERE status IN"),
        ("psycopg", "postgres", "PsycopgSyncConfig", {}, PsycopgSyncQueueStore, "WHERE status IN"),
        ("psycopg", "postgres", "PsycopgAsyncConfig", {}, PsycopgAsyncQueueStore, "WHERE status IN"),
        ("pymysql", "mysql", "PyMysqlConfig", {}, PymysqlQueueStore, "ENGINE=InnoDB"),
        ("spanner", "spanner", "SpannerConfig", {}, SpannerQueueStore, "PRIMARY KEY"),
        ("sqlite", "sqlite", "SqliteConfig", {}, SqliteQueueStore, '"queue_tasks"'),
    ),
)
async def test_sqlspec_backend_store_factory_covers_sqlspec_adapter_modules(
    adapter_name: str,
    dialect: str | None,
    config_type_name: str,
    connection_config: dict[str, Any],
    expected_store_type: type[Any],
    expected_sql_fragment: str,
) -> None:
    store = create_queue_store(
        _fake_adapter_config(
            adapter_name,
            dialect=dialect,
            config_type_name=config_type_name,
            connection_config=connection_config,
        ),
        table_name="queue_tasks",
    )

    assert isinstance(store, expected_store_type)
    assert store.__class__.__module__.startswith(f"litestar_queues.backends.sqlspec.stores.{adapter_name}.")
    assert expected_sql_fragment in "\n".join(store.create_statements())


@pytest.mark.parametrize(
    ("config_type_name", "expected_store_type"),
    (
        ("OracleSyncConfig", OracledbSyncQueueStore),
        ("OracleAsyncConfig", OracledbAsyncQueueStore),
    ),
)
@pytest.mark.parametrize(
    ("queue_settings", "expected_json_fragment", "expected_serialized_type"),
    (
        ({}, "BLOB CHECK ({column} IS JSON)", bytes),
        ({"json_storage": "json", "in_memory": True}, "JSON", str),
        ({"json_storage": "blob"}, "BLOB", bytes),
        ({"json_storage": "blob_plain"}, "BLOB", bytes),
    ),
)
async def test_sqlspec_backend_oracledb_json_storage_avoids_clob_and_honors_settings(
    config_type_name: str,
    expected_store_type: type[Any],
    queue_settings: dict[str, Any],
    expected_json_fragment: str,
    expected_serialized_type: type[Any],
) -> None:
    store = create_queue_store(
        _fake_adapter_config(
            "oracledb",
            dialect="oracle",
            config_type_name=config_type_name,
            extension_config={QUEUE_EXTENSION_NAME: queue_settings},
        ),
        table_name="queue_tasks",
    )

    ddl = "\n".join(store.create_statements())

    assert isinstance(store, expected_store_type)
    assert "CLOB" not in ddl
    for column_name in ("args_json", "kwargs_json", "result_json", "metadata_json"):
        expected_column_type = expected_json_fragment.format(column=column_name)
        assert f"{column_name} {expected_column_type} NOT NULL" in ddl
    assert ("INMEMORY PRIORITY HIGH" in ddl) is bool(queue_settings.get("in_memory"))
    serialized = store.serialize_payload_json({"ok": True})
    assert isinstance(serialized, expected_serialized_type)
    assert store.deserialize_json(serialized) == {"ok": True}


async def test_sqlspec_backend_migration_uses_adapter_specific_queue_store() -> None:
    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    context = SimpleNamespace(config=_fake_adapter_config("bigquery", dialect="bigquery"))

    statements = await migration.up(context)

    assert "CLUSTER BY" in statements[0]
    assert not any("CREATE INDEX" in statement for statement in statements)


async def test_sqlspec_backend_exposes_packaged_migration_assets() -> None:
    paths = tuple(Path(path) for path in migration_paths())

    assert [path.name for path in paths] == ["0001_create_queue_tasks.py"]
    content = paths[0].read_text()
    assert "create_queue_store" in content
    assert "return SQLSpecQueueStore(" not in content
    assert "CREATE TABLE IF NOT EXISTS litestar_queue_tasks" not in content


async def test_sqlspec_backend_deduplicates_active_keys_and_replaces_terminal_keys(
    sqlspec_backend: SQLSpecQueueBackend,
) -> None:
    first = await sqlspec_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-1"}, key="sync:acct-1")
    duplicate = await sqlspec_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")

    assert duplicate.id == first.id
    assert duplicate.kwargs == {"account_id": "acct-1"}

    await sqlspec_backend.complete_task(first.id, result={"ok": True})
    replacement = await sqlspec_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")

    assert replacement.id != first.id
    assert replacement.kwargs == {"account_id": "acct-2"}
    keyed = await sqlspec_backend.get_task_by_key("sync:acct-1")
    assert keyed is not None
    assert keyed.id == replacement.id


async def test_sqlspec_backend_claims_due_tasks_by_priority(sqlspec_backend: SQLSpecQueueBackend) -> None:
    later = datetime.now(UTC) + timedelta(minutes=5)

    low = await sqlspec_backend.enqueue("tasks.low", priority=1)
    scheduled = await sqlspec_backend.enqueue("tasks.later", priority=100, scheduled_at=later)
    high = await sqlspec_backend.enqueue("tasks.high", priority=10)

    claimed = await sqlspec_backend.claim_next()

    assert claimed is not None
    assert claimed.id == high.id
    assert claimed.status == "running"
    assert claimed.started_at is not None
    stored_low = await sqlspec_backend.get_task(low.id)
    stored_scheduled = await sqlspec_backend.get_task(scheduled.id)
    assert stored_low is not None
    assert stored_scheduled is not None
    assert stored_low.status == "pending"
    assert stored_scheduled.status == "scheduled"


async def test_sqlspec_backend_fail_task_retries_then_fails_permanently(
    sqlspec_backend: SQLSpecQueueBackend,
) -> None:
    record = await sqlspec_backend.enqueue("tasks.flaky", max_retries=1)

    await sqlspec_backend.claim_task(record.id)
    retried = await sqlspec_backend.fail_task(record.id, "first failure")

    assert retried is not None
    assert retried.status == "pending"
    assert retried.retry_count == 1

    await sqlspec_backend.claim_task(record.id)
    failed = await sqlspec_backend.fail_task(record.id, "second failure")

    assert failed is not None
    assert failed.status == "failed"
    assert failed.error == "second failure"
    assert failed.completed_at is not None


async def test_sqlspec_backend_cancels_heartbeats_and_requeues_stale_running(
    sqlspec_backend: SQLSpecQueueBackend,
) -> None:
    pending = await sqlspec_backend.enqueue("tasks.cancel")

    assert await sqlspec_backend.cancel_task(pending.id)
    assert not await sqlspec_backend.cancel_task(pending.id)

    cancelled = await sqlspec_backend.get_task(pending.id)
    assert cancelled is not None
    assert cancelled.status == "cancelled"

    running = await sqlspec_backend.enqueue("tasks.heartbeat")
    claimed = await sqlspec_backend.claim_task(running.id)

    assert claimed is not None
    assert claimed.heartbeat_at is not None

    await sqlspec_backend.touch_heartbeat(claimed.id)
    touched = await sqlspec_backend.get_task(claimed.id)

    assert touched is not None
    assert touched.heartbeat_at is not None
    assert touched.heartbeat_at >= claimed.heartbeat_at

    assert await sqlspec_backend.requeue_stale_running(stale_after=timedelta(seconds=0)) == 1
    requeued = await sqlspec_backend.get_task(claimed.id)

    assert requeued is not None
    assert requeued.status == "pending"
    assert requeued.retry_count == 1


async def test_sqlspec_backend_uses_sqlspec_json_serializer(sqlspec_backend: SQLSpecQueueBackend) -> None:
    encoded_at = datetime.now(UTC)

    record = await sqlspec_backend.enqueue("tasks.metadata", metadata={"encoded_at": encoded_at})
    stored = await sqlspec_backend.get_task(record.id)

    assert stored is not None
    assert stored.metadata["encoded_at"] == encoded_at.isoformat().replace("+00:00", "Z")


def test_sqlspec_backend_does_not_create_sqlspec_litestar_plugin() -> None:
    with pytest.raises(TypeError):
        SQLSpecQueueBackend(register_plugin=True)  # type: ignore[call-arg]


async def test_sqlspec_backend_can_start_with_packaged_migrations(tmp_path: Path) -> None:
    db_path = tmp_path / "migrated.db"

    first = SQLSpecQueueBackend(
        sqlspec_config=_sqlite_config(db_path),
        create_schema=False,
        run_migrations=True,
    )
    await first.open()
    await first.close()

    second = SQLSpecQueueBackend(
        sqlspec_config=_sqlite_config(db_path),
        create_schema=False,
        run_migrations=True,
    )
    await second.open()
    try:
        record = await second.enqueue("tasks.migrated")
    finally:
        await second.close()

    assert record.task_name == "tasks.migrated"

    with sqlite3.connect(db_path) as connection:
        versions = [row[0] for row in connection.execute("SELECT version_num FROM ddl_migrations")]

    assert versions == ["ext_litestar_queues_0001"]


async def test_sqlspec_backend_uses_configured_table_name(tmp_path: Path) -> None:
    db_path = tmp_path / "custom-table.db"
    backend = SQLSpecQueueBackend(
        sqlspec_config=_sqlite_config(db_path),
        table_name="queue_tasks",
    )

    await backend.open()
    try:
        record = await backend.enqueue("tasks.custom_table")
    finally:
        await backend.close()

    assert record.task_name == "tasks.custom_table"
    with sqlite3.connect(db_path) as connection:
        table_names = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}

    assert "queue_tasks" in table_names
    assert "litestar_queue_tasks" not in table_names


async def test_sqlspec_backend_uses_structured_extension_config_when_explicit_values_are_absent(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "extension-config.db"
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        extension_config={
            QUEUE_EXTENSION_NAME: {
                "table_name": "extension_queue_tasks",
            },
        },
    )
    backend = SQLSpecQueueBackend(sqlspec_config=sqlspec_config)

    await backend.open()
    try:
        record = await backend.enqueue("tasks.extension_config")
    finally:
        await backend.close()

    assert record.task_name == "tasks.extension_config"
    with sqlite3.connect(db_path) as connection:
        table_names = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}

    assert "extension_queue_tasks" in table_names
    assert "litestar_queue_tasks" not in table_names


async def test_sqlspec_backend_explicit_config_values_override_sqlspec_extension_config(tmp_path: Path) -> None:
    db_path = tmp_path / "explicit-config.db"
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        extension_config={
            QUEUE_EXTENSION_NAME: {
                "table_name": "extension_queue_tasks",
            },
        },
    )
    backend = SQLSpecQueueBackend(sqlspec_config=sqlspec_config, table_name="explicit_queue_tasks")

    await backend.open()
    try:
        record = await backend.enqueue("tasks.explicit_config")
    finally:
        await backend.close()

    assert record.task_name == "tasks.explicit_config"
    with sqlite3.connect(db_path) as connection:
        table_names = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}

    assert "explicit_queue_tasks" in table_names
    assert "extension_queue_tasks" not in table_names


async def test_sqlspec_backend_event_channel_notifications_wake_waiters(tmp_path: Path) -> None:
    event_channel = StubAsyncEventChannel()
    backend = SQLSpecQueueBackend(
        sqlspec_config=_sqlite_config(tmp_path / "notifications.db"),
        event_channel=cast("Any", event_channel),
        notification_channel="queue_notifications",
    )

    await backend.open()
    try:
        waiter = asyncio.create_task(backend.wait_for_notifications(timeout=1))
        record = await backend.enqueue("tasks.notified", queue="critical", execution_backend="local")

        assert await waiter is True
        assert backend.capabilities.supports_notifications is True
        assert backend.capabilities.notification_backend == "table_queue"
        assert backend.capabilities.notifications_durable is True
        assert event_channel.published == [
            (
                "queue_notifications",
                {
                    "task_id": str(record.id),
                    "task_name": "tasks.notified",
                    "queue": "critical",
                    "execution_backend": "local",
                },
                {"event_type": "litestar_queues.task_available"},
            )
        ]
        assert event_channel.acked == ["event-1"]
    finally:
        await backend.close()


async def test_sqlspec_backend_derives_sqlspec_event_channel_from_config(tmp_path: Path) -> None:
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "derived-notifications.db")},
        extension_config={
            "events": {
                "backend": "table_queue",
                "poll_interval": 0.01,
                "queue_table": "queue_events",
            },
        },
    )
    backend = SQLSpecQueueBackend(
        sqlspec_config=sqlspec_config,
        create_schema=False,
        run_migrations=True,
        notifications=True,
        notification_channel="derived_notifications",
    )

    await backend.open()
    try:
        waiter = asyncio.create_task(backend.wait_for_notifications(timeout=1))
        await backend.enqueue("tasks.derived_notified")

        assert await waiter is True
        assert backend.capabilities.supports_notifications is True
        assert backend.capabilities.notification_backend == "table_queue"
        assert backend.capabilities.notifications_durable is True
    finally:
        await backend.close()


async def test_sqlspec_backend_notification_channel_uses_extension_config_with_explicit_override(
    tmp_path: Path,
) -> None:
    extension_channel = StubAsyncEventChannel()
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "extension-notifications.db")},
        extension_config={
            QUEUE_EXTENSION_NAME: {
                "notification_channel": "extension_notifications",
            },
        },
    )
    extension_backend = SQLSpecQueueBackend(sqlspec_config=sqlspec_config, event_channel=cast("Any", extension_channel))
    await extension_backend.open()
    try:
        await extension_backend.enqueue("tasks.extension_notified")
    finally:
        await extension_backend.close()

    explicit_channel = StubAsyncEventChannel()
    explicit_backend = SQLSpecQueueBackend(
        sqlspec_config=sqlspec_config,
        event_channel=cast("Any", explicit_channel),
        notification_channel="explicit_notifications",
        table_name="explicit_notification_queue",
    )
    await explicit_backend.open()
    try:
        await explicit_backend.enqueue("tasks.explicit_notified")
    finally:
        await explicit_backend.close()

    assert extension_channel.published[0][0] == "extension_notifications"
    assert explicit_channel.published[0][0] == "explicit_notifications"


async def test_sqlspec_backend_uses_user_registered_litestar_sqlspec_plugin(tmp_path: Path) -> None:
    sqlspec = SQLSpec()
    sqlspec_config = _sqlite_config(tmp_path / "litestar.db")
    sqlspec.add_config(sqlspec_config)

    plugin = QueuePlugin(
        QueueConfig(
            queue_backend="sqlspec",
            queue_backend_config={
                "sqlspec": sqlspec,
                "create_schema": False,
            },
            initialize_schedules=False,
        )
    )

    app = Litestar(plugins=[SQLSpecPlugin(sqlspec), plugin])

    assert "db_session" in app.dependencies
    assert "AiosqliteDriver" in app.signature_namespace


async def test_queue_service_uses_sqlspec_backend_from_config(tmp_path: Path) -> None:
    @task("tasks.lower", retries=1)
    async def lowercase(value: str) -> str:
        return value.lower()

    config = QueueConfig(
        queue_backend="sqlspec",
        queue_backend_config={"sqlspec_config": _sqlite_config(tmp_path / "service.db")},
        execution_backend="local",
    )
    async with QueueService(config) as service:
        result = await service.enqueue(lowercase, "QUEUE")

        pending_status = result.status
        assert pending_status == "pending"

        record = await service.claim_next()
        assert record is not None
        await service.execute_record(record)
        await result.refresh()

    completed_status = result.status
    assert completed_status == "completed"
    assert result.result == "queue"


async def test_sqlspec_backend_default_heartbeat_uses_main_pool(
    sqlspec_backend: SQLSpecQueueBackend,
) -> None:
    """When heartbeat_pool_config is None, heartbeat writes use the main pool."""
    record = await sqlspec_backend.enqueue("tasks.heartbeat")
    claimed = await sqlspec_backend.claim_task(record.id)

    assert claimed is not None
    assert sqlspec_backend._heartbeat_pool_enabled is False
    assert sqlspec_backend._heartbeat_pool_registered is False

    await sqlspec_backend.touch_heartbeat(claimed.id)
    touched = await sqlspec_backend.get_task(claimed.id)
    assert touched is not None
    assert touched.heartbeat_at is not None


async def test_sqlspec_backend_dedicated_heartbeat_pool_isolates_heartbeat_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """touch_heartbeat / null_heartbeats hit the dedicated pool only."""
    queue_path = tmp_path / "queue.db"
    main_config = _sqlite_config(queue_path)
    heartbeat_config = _sqlite_config(queue_path)
    sqlspec = SQLSpec()
    sqlspec.add_config(main_config)

    backend = SQLSpecQueueBackend(
        sqlspec=sqlspec,
        sqlspec_config=main_config,
        heartbeat_pool_config=heartbeat_config,
    )
    await backend.open()
    try:
        assert id(heartbeat_config) in sqlspec.configs
        assert backend._heartbeat_pool_registered is True

        record = await backend.enqueue("tasks.isolated_heartbeat")
        claimed = await backend.claim_task(record.id)
        assert claimed is not None

        original_session = SQLSpecQueueBackend._session
        original_heartbeat = SQLSpecQueueBackend._heartbeat_session
        main_calls = 0
        heartbeat_calls = 0

        @contextlib.asynccontextmanager
        async def counting_session(self: SQLSpecQueueBackend) -> AsyncIterator[Any]:
            nonlocal main_calls
            main_calls += 1
            async with original_session(self) as driver:
                yield driver

        @contextlib.asynccontextmanager
        async def counting_heartbeat(self: SQLSpecQueueBackend) -> AsyncIterator[Any]:
            nonlocal heartbeat_calls
            heartbeat_calls += 1
            async with original_heartbeat(self) as driver:
                yield driver

        monkeypatch.setattr(SQLSpecQueueBackend, "_session", counting_session)
        monkeypatch.setattr(SQLSpecQueueBackend, "_heartbeat_session", counting_heartbeat)

        await backend.touch_heartbeat(claimed.id)
        await backend.null_heartbeats([claimed.id])

        assert heartbeat_calls == 2
        assert main_calls == 0
    finally:
        await backend.close()


async def test_sqlspec_backend_heartbeat_pool_failure_falls_back_to_main(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Dedicated pool registration failure does not prevent backend.open()."""
    main_config = _sqlite_config(tmp_path / "main.db")
    bad_heartbeat_config = _sqlite_config(tmp_path / "main.db")

    real_add_config = SQLSpec.add_config

    def failing_add_config(self: SQLSpec, config: Any) -> Any:
        if config is bad_heartbeat_config:
            msg = "simulated heartbeat pool registration failure"
            raise RuntimeError(msg)
        return real_add_config(self, config)

    monkeypatch.setattr(SQLSpec, "add_config", failing_add_config)

    backend = SQLSpecQueueBackend(
        sqlspec_config=main_config,
        heartbeat_pool_config=bad_heartbeat_config,
    )
    with caplog.at_level("WARNING", logger="litestar_queues"):
        await backend.open()
    try:
        assert backend._heartbeat_pool_registered is False
        assert backend._heartbeat_pool_enabled is False
        assert any("heartbeat pool registration failed" in entry.getMessage() for entry in caplog.records)
        record = await backend.enqueue("tasks.fallback_heartbeat")
        claimed = await backend.claim_task(record.id)
        assert claimed is not None
        await backend.touch_heartbeat(claimed.id)
        touched = await backend.get_task(claimed.id)
        assert touched is not None
        assert touched.heartbeat_at is not None
    finally:
        await backend.close()


async def test_sqlspec_backend_dedicated_heartbeat_pool_handles_concurrent_heartbeats(
    tmp_path: Path,
) -> None:
    """Many concurrent heartbeats on the dedicated pool must not deadlock."""
    queue_path = tmp_path / "queue.db"
    main_config = _sqlite_config(queue_path)
    heartbeat_config = _sqlite_config(queue_path)
    backend = SQLSpecQueueBackend(
        sqlspec_config=main_config,
        heartbeat_pool_config=heartbeat_config,
    )
    await backend.open()
    try:
        records = [await backend.enqueue(f"tasks.bulk-{i}") for i in range(16)]
        claimed = []
        for record in records:
            c = await backend.claim_task(record.id)
            assert c is not None
            claimed.append(c)

        await asyncio.wait_for(
            asyncio.gather(*(backend.touch_heartbeat(record.id) for _ in range(4) for record in claimed)),
            timeout=10.0,
        )
        for record in claimed:
            stored = await backend.get_task(record.id)
            assert stored is not None
            assert stored.heartbeat_at is not None
    finally:
        await backend.close()

"""Unit tests for SQLSpec maintenance table naming and ownership fencing."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import Mock

import pytest

pytest.importorskip("sqlspec")

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig
from sqlspec.adapters.oracledb import OracleAsyncConfig
from sqlspec.adapters.pymssql import PymssqlConfig
from sqlspec.adapters.pymysql import PyMysqlConfig
from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.migrations import SchemaTarget

from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.maintenance import (
    SpannerMaintenanceStore,
    create_maintenance_store,
    resolve_maintenance_table_name,
)
from litestar_queues.backends.sqlspec.reservation import (
    SpannerQueueReservationStore,
    create_task_reservation_store,
    resolve_task_reservation_table_name,
)
from litestar_queues.backends.sqlspec.schema import maintenance_table_name_for, task_reservation_table_name_for

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable
    from pathlib import Path

    from pytest import MonkeyPatch

_MAX_IDENTIFIER_LENGTH = 63


def test_derives_suffixed_name_for_short_table() -> None:
    assert maintenance_table_name_for("queue_task") == "queue_maintenance"


def test_preserves_schema_qualifier() -> None:
    assert maintenance_table_name_for("app.queue_task") == "app.queue_maintenance"


def test_long_table_name_is_bounded_and_deterministic() -> None:
    long_table = "queue_task_" + "x" * 60
    first = maintenance_table_name_for(long_table)
    second = maintenance_table_name_for(long_table)

    assert first == second
    assert len(first) <= _MAX_IDENTIFIER_LENGTH
    assert first.endswith("_maintenance")
    other = maintenance_table_name_for("queue_task_" + "y" * 60)
    assert other != first
    assert len(other) <= _MAX_IDENTIFIER_LENGTH


@pytest.mark.parametrize("schema", (None, "app"))
def test_long_task_reservation_table_name_is_bounded_and_shared_by_runtime_and_migrations(schema: "str | None") -> None:
    table_part = "queue_task_" + "x" * 60
    queue_table = f"{schema}.{table_part}" if schema is not None else table_part

    first = task_reservation_table_name_for(queue_table)
    second = task_reservation_table_name_for(queue_table)
    resolved_runtime_name = resolve_task_reservation_table_name(queue_table)
    derived_part = first.rsplit(".", maxsplit=1)[-1]

    assert first == second == resolved_runtime_name
    assert len(derived_part) <= _MAX_IDENTIFIER_LENGTH
    assert derived_part.endswith("_reservation")
    if schema is not None:
        assert first.startswith(f"{schema}.")


def test_explicit_override_wins() -> None:
    assert (
        resolve_maintenance_table_name("queue_task", maintenance_table_name="custom_maintenance")
        == "custom_maintenance"
    )


@pytest.mark.anyio
async def test_release_reports_false_when_successor_replaces_token_before_delete(monkeypatch: "MonkeyPatch") -> None:
    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    class MaintenanceStore:
        @staticmethod
        def count_coordination(*, name: "str", token: "str") -> "tuple[str, str, str]":
            return "count", name, token

        @staticmethod
        def delete_coordination(*, name: "str", token: "str") -> "tuple[str, str, str]":
            return "delete", name, token

        @staticmethod
        def select_coordination_token(*, name: "str") -> "tuple[str, str]":
            return "select", name

    class Driver:
        current_token = "token-a"

        async def begin(self) -> "None":
            return None

        async def execute(self, statement: "tuple[str, str, str]") -> "None":
            assert statement == ("delete", "maintenance", "token-a")
            self.current_token = "token-b"

        async def commit(self) -> "None":
            return None

        async def rollback(self) -> "None":
            return None

    driver = Driver()

    @asynccontextmanager
    async def session(_backend: "SQLSpecQueueBackend") -> "AsyncIterator[Driver]":
        yield driver

    async def select_one_row(
        _backend: "SQLSpecQueueBackend", _driver: "Driver", statement: "tuple[str, ...]"
    ) -> "dict[str, Any]":
        if statement[0] == "count":
            return {"coordination_count": 1}
        assert statement == ("select", "maintenance")
        return {"token": driver.current_token}

    monkeypatch.setattr(SQLSpecQueueBackend, "_session", session)
    monkeypatch.setattr(SQLSpecQueueBackend, "_select_one_row", select_one_row)
    monkeypatch.setattr(SQLSpecQueueBackend, "_get_maintenance_store", lambda _backend: MaintenanceStore())

    backend = SQLSpecQueueBackend()

    assert await backend.release_maintenance("maintenance", "token-a") is False


@pytest.mark.anyio
async def test_dispatch_repair_zero_budget_does_not_open_storage() -> None:
    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend
    from litestar_queues.exceptions import QueueConfigurationError

    backend = SQLSpecQueueBackend()
    result = await backend.list_dispatch_repair_candidates("cloudtasks", limit=0)
    assert result.records == ()
    assert result.examined == 0
    assert result.limit_reached is False
    with pytest.raises(QueueConfigurationError, match="non-negative"):
        await backend.list_dispatch_repair_candidates("cloudtasks", limit=-1)


@pytest.mark.anyio
async def test_dispatch_repair_discarded_candidate_consumes_budget(
    tmp_path: "Path", monkeypatch: "MonkeyPatch"
) -> None:
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend

    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            sqlspec_config=AiosqliteConfig(connection_config={"database": str(tmp_path / "repair.db")})
        )
    )
    await backend.open()
    await backend.create_schema()
    try:
        selected = await backend.enqueue("selected", execution_backend="cloudtasks")
        deferred = await backend.enqueue("deferred", execution_backend="cloudtasks")
        original_session = SQLSpecQueueBackend._session
        page_sizes: list[int] = []

        @asynccontextmanager
        async def session(instance: "SQLSpecQueueBackend") -> "AsyncIterator[Any]":
            async with original_session(instance) as driver:

                async def select(statement: Any) -> Any:
                    rows = await driver.select(statement)
                    page_sizes.append(len(rows))
                    await driver.execute(
                        "UPDATE queue_task SET status = 'cancelled' WHERE id = :task_id", task_id=str(selected.id)
                    )
                    return rows

                yield SimpleNamespace(
                    begin=driver.begin,
                    commit=driver.commit,
                    rollback=driver.rollback,
                    execute=driver.execute,
                    select=select,
                    select_one_or_none=driver.select_one_or_none,
                )

        with monkeypatch.context() as patch:
            patch.setattr(SQLSpecQueueBackend, "_session", session)
            result = await backend.list_dispatch_repair_candidates("cloudtasks", limit=1)
        assert result.records == ()
        assert result.examined == 1
        assert result.limit_reached is True
        assert page_sizes == [1]
        stored = await backend.get_task(deferred.id)
        assert stored is not None and stored.dispatch_checked_at is None
        following = await backend.list_dispatch_repair_candidates("cloudtasks", limit=1)
        assert [record.id for record in following.records] == [deferred.id]
    finally:
        await backend.close()


@pytest.mark.anyio
async def test_scheduled_execution_ref_unknown_rowcount_does_not_accept_existing_reference(
    tmp_path: "Path", monkeypatch: "MonkeyPatch"
) -> None:
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend

    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            sqlspec_config=AiosqliteConfig(connection_config={"database": str(tmp_path / "cas.db")})
        )
    )
    await backend.open()
    await backend.create_schema()
    try:
        record = await backend.enqueue("existing_reference", execution_backend="cloudtasks")
        await backend.set_execution_ref(record.id, "cloudtasks", "already-reserved")
        monkeypatch.setattr(SQLSpecQueueBackend, "_resolve_rows_affected", lambda _self, _result: -1)
        rejected = await backend.reserve_scheduled_execution_ref(
            record.id, "cloudtasks", "already-reserved", expected_retry_count=0, expected_execution_ref=None
        )
        assert rejected is None
    finally:
        await backend.close()


def _sqlite_config() -> "SqliteConfig":
    return SqliteConfig(connection_config={"database": ":memory:"})


def _asyncpg_config() -> "AsyncpgConfig":
    return AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/db"},
        extension_config={QUEUE_EXTENSION_NAME: {"table_storage_parameters": True}},
    )


def _cockroach_config() -> "CockroachAsyncpgConfig":
    return CockroachAsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/db"},
        extension_config={QUEUE_EXTENSION_NAME: {"terminal_ttl": 3600}},
    )


def _pymysql_config() -> "PyMysqlConfig":
    return PyMysqlConfig(connection_config={"host": "localhost", "database": "db"})


def _pymssql_config() -> "PymssqlConfig":
    return PymssqlConfig(connection_config={"host": "localhost", "database": "db"})


def _oracle_config() -> "OracleAsyncConfig":
    return OracleAsyncConfig(connection_config={"dsn": "localhost/db"})


def _spanner_config() -> "SpannerSyncConfig":
    return SpannerSyncConfig(connection_config={"project": "p", "instance": "i", "database": "d"})


_STANDARD_ADAPTER_CONFIGS = (
    pytest.param(_sqlite_config, id="sqlite"),
    pytest.param(_asyncpg_config, id="asyncpg"),
    pytest.param(_cockroach_config, id="cockroach_asyncpg"),
    pytest.param(_pymysql_config, id="pymysql"),
    pytest.param(_pymssql_config, id="pymssql"),
    pytest.param(_oracle_config, id="oracledb"),
)

_ALL_ADAPTER_CONFIGS = (*_STANDARD_ADAPTER_CONFIGS, pytest.param(_spanner_config, id="spanner"))


@pytest.mark.parametrize("config_factory", _STANDARD_ADAPTER_CONFIGS)
def test_maintenance_and_reservation_schema_target(config_factory: "Callable[[], Any]") -> "None":
    config = config_factory()
    maintenance = create_maintenance_store(config, queue_table_name="queue_task")
    reservation = create_task_reservation_store(config, queue_table_name="queue_task")

    m_ddl = maintenance.create_table_statement()
    r_ddl = reservation.create_table_statement()
    m_target = SchemaTarget.from_ddl(maintenance.table_name, m_ddl, dialect=maintenance.sqlglot_dialect())
    r_target = SchemaTarget.from_ddl(reservation.table_name, r_ddl, dialect=reservation.sqlglot_dialect())

    assert len(m_target.create_table.columns) == 3
    assert maintenance.companion_indexes() == ()
    assert maintenance.create_statements() == [m_ddl]
    assert len(r_target.create_table.columns) == 4
    assert reservation.companion_indexes() == ()
    assert reservation.create_statements() == [r_ddl]


def test_spanner_maintenance_and_reservation_ddl_split() -> "None":
    config = _spanner_config()
    maintenance = create_maintenance_store(config, queue_table_name="queue_task")
    reservation = create_task_reservation_store(config, queue_table_name="queue_task")

    m_ddl = maintenance.create_table_statement()
    r_ddl = reservation.create_table_statement()

    assert isinstance(maintenance, SpannerMaintenanceStore)
    assert isinstance(reservation, SpannerQueueReservationStore)
    assert m_ddl.startswith("CREATE TABLE `queue_maintenance`")
    assert "PRIMARY KEY (`name`)" in m_ddl
    assert maintenance.companion_indexes() == ()
    assert maintenance.create_statements() == [m_ddl]
    assert r_ddl.startswith("CREATE TABLE `queue_task_reservation`")
    assert "PRIMARY KEY (`identity_key`)" in r_ddl
    assert reservation.companion_indexes() == ()
    assert reservation.create_statements() == [r_ddl]


@pytest.mark.anyio
@pytest.mark.parametrize("config_factory", _ALL_ADAPTER_CONFIGS)
async def test_maintenance_mro_never_leaks_queue_hooks(config_factory: "Callable[[], Any]") -> "None":
    maintenance = create_maintenance_store(config_factory(), queue_table_name="queue_task")
    spy = Mock()

    assert maintenance.companion_indexes() == ()
    maintenance.prepare_schema_sync(spy)
    await maintenance.prepare_schema_async(spy)
    maintenance.reconcile_storage_options_sync(spy)
    await maintenance.reconcile_storage_options_async(spy)

    assert spy.mock_calls == []


@pytest.mark.anyio
@pytest.mark.parametrize("config_factory", _ALL_ADAPTER_CONFIGS)
async def test_reservation_hooks_are_noops(config_factory: "Callable[[], Any]") -> "None":
    reservation = create_task_reservation_store(config_factory(), queue_table_name="queue_task")
    spy = Mock()

    assert reservation.companion_indexes() == ()
    reservation.prepare_schema_sync(spy)
    await reservation.prepare_schema_async(spy)
    reservation.reconcile_storage_options_sync(spy)
    await reservation.reconcile_storage_options_async(spy)

    assert spy.mock_calls == []


@pytest.mark.parametrize("store_kind", ["maintenance", "reservation"])
@pytest.mark.parametrize("config_factory", _ALL_ADAPTER_CONFIGS)
def test_companion_stores_manage_schema_false(store_kind: "str", config_factory: "Callable[[], Any]") -> "None":
    config = config_factory()
    store = (
        create_maintenance_store(config, queue_table_name="queue_task", manage_schema=False)
        if store_kind == "maintenance"
        else create_task_reservation_store(config, queue_table_name="queue_task", manage_schema=False)
    )

    assert store.create_table_statement() == ""
    assert store.companion_indexes() == ()
    assert store.create_statements() == []

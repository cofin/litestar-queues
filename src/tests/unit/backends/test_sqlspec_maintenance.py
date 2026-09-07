"""Unit tests for SQLSpec maintenance table naming and ownership fencing."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

pytest.importorskip("sqlspec")

from litestar_queues.backends.sqlspec.maintenance import resolve_maintenance_table_name
from litestar_queues.backends.sqlspec.reservation import resolve_task_reservation_table_name
from litestar_queues.backends.sqlspec.schema import maintenance_table_name_for, task_reservation_table_name_for

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
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

    assert first == second  # deterministic for the same input
    assert len(first) <= _MAX_IDENTIFIER_LENGTH
    assert first.endswith("_maintenance")
    # A different long table produces a distinct bounded name.
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
                    # A selected record can cease to be eligible before its guarded mark.
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
        # Some adapters cannot distinguish zero affected rows from unknown rowcount.
        monkeypatch.setattr(SQLSpecQueueBackend, "_resolve_rows_affected", lambda _self, _result: -1)
        rejected = await backend.reserve_scheduled_execution_ref(
            record.id, "cloudtasks", "already-reserved", expected_retry_count=0, expected_execution_ref=None
        )
        assert rejected is None
    finally:
        await backend.close()

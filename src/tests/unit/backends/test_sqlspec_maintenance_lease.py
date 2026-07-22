"""Unit tests for SQLSpec maintenance-lease table-name derivation."""

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import pytest

pytest.importorskip("sqlspec")

from litestar_queues.backends.sqlspec.maintenance_lease import resolve_maintenance_lease_table_name
from litestar_queues.backends.sqlspec.schema import maintenance_lease_table_name_for, uniqueness_table_name_for
from litestar_queues.backends.sqlspec.uniqueness import resolve_uniqueness_table_name

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pytest import MonkeyPatch

_MAX_IDENTIFIER_LENGTH = 63


def test_derives_suffixed_name_for_short_table() -> None:
    assert maintenance_lease_table_name_for("litestar_queue_task") == "litestar_queue_task_maintenance_lease"


def test_preserves_schema_qualifier() -> None:
    assert maintenance_lease_table_name_for("app.litestar_queue_task") == "app.litestar_queue_task_maintenance_lease"


def test_long_table_name_is_bounded_and_deterministic() -> None:
    long_table = "litestar_queue_task_" + "x" * 60
    first = maintenance_lease_table_name_for(long_table)
    second = maintenance_lease_table_name_for(long_table)

    assert first == second  # deterministic for the same input
    assert len(first) <= _MAX_IDENTIFIER_LENGTH
    assert first.endswith("_maintenance_lease")
    # A different long table produces a distinct bounded name.
    other = maintenance_lease_table_name_for("litestar_queue_task_" + "y" * 60)
    assert other != first
    assert len(other) <= _MAX_IDENTIFIER_LENGTH


@pytest.mark.parametrize("schema", (None, "app"))
def test_long_uniqueness_table_name_is_bounded_and_shared_by_runtime_and_migrations(schema: "str | None") -> None:
    table_part = "litestar_queue_task_" + "x" * 60
    queue_table = f"{schema}.{table_part}" if schema is not None else table_part

    first = uniqueness_table_name_for(queue_table)
    second = uniqueness_table_name_for(queue_table)
    resolved_runtime_name = resolve_uniqueness_table_name(queue_table)
    derived_part = first.rsplit(".", maxsplit=1)[-1]

    assert first == second == resolved_runtime_name
    assert len(derived_part) <= _MAX_IDENTIFIER_LENGTH
    assert derived_part.endswith("_uniqueness")
    if schema is not None:
        assert first.startswith(f"{schema}.")


def test_explicit_override_wins() -> None:
    assert (
        resolve_maintenance_lease_table_name("litestar_queue_task", maintenance_lease_table_name="custom_lease")
        == "custom_lease"
    )


@pytest.mark.anyio
async def test_release_reports_false_when_successor_replaces_token_before_delete(monkeypatch: "MonkeyPatch") -> None:
    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    class LeaseStore:
        @staticmethod
        def count_lease(*, name: "str", token: "str") -> "tuple[str, str, str]":
            return "count", name, token

        @staticmethod
        def release_delete(*, name: "str", token: "str") -> "tuple[str, str, str]":
            return "delete", name, token

        @staticmethod
        def select_lease_token(*, name: "str") -> "tuple[str, str]":
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
            return {"lease_count": 1}
        assert statement == ("select", "maintenance")
        return {"token": driver.current_token}

    monkeypatch.setattr(SQLSpecQueueBackend, "_session", session)
    monkeypatch.setattr(SQLSpecQueueBackend, "_select_one_row", select_one_row)
    monkeypatch.setattr(SQLSpecQueueBackend, "_get_maintenance_lease_store", lambda _backend: LeaseStore())

    backend = SQLSpecQueueBackend()

    assert await backend.release_maintenance_lease("maintenance", "token-a") is False

"""Unit tests for SQLSpec maintenance-lease table-name derivation."""

import pytest

pytest.importorskip("sqlspec")

from litestar_queues.backends.sqlspec.maintenance_lease import resolve_maintenance_lease_table_name
from litestar_queues.backends.sqlspec.schema import maintenance_lease_table_name_for

_MAX_IDENTIFIER_LENGTH = 63


def test_derives_suffixed_name_for_short_table() -> None:
    assert maintenance_lease_table_name_for("litestar_queue_task") == "litestar_queue_task_maintenance_lease"


def test_preserves_schema_qualifier() -> None:
    assert (
        maintenance_lease_table_name_for("app.litestar_queue_task") == "app.litestar_queue_task_maintenance_lease"
    )


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


def test_explicit_override_wins() -> None:
    assert (
        resolve_maintenance_lease_table_name("litestar_queue_task", maintenance_lease_table_name="custom_lease")
        == "custom_lease"
    )

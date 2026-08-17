"""Package-owned scoping dimensions on the SQLSpec event-history table."""

import pytest

pytest.importorskip("sqlspec")
pytest.importorskip("aiosqlite")

from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues.backends.sqlspec.event_log import create_event_log_store
from litestar_queues.backends.sqlspec.schema import EVENT_HISTORY_COLUMNS

DIMENSIONS = ("scope", "scope_key", "actor", "entity")


def _store() -> "object":
    return create_event_log_store(
        AiosqliteConfig(connection_config={"database": ":memory:"}), queue_table_name="queue_task"
    )


def test_dimensions_are_package_owned_columns() -> "None":
    assert EVENT_HISTORY_COLUMNS[-4:] == DIMENSIONS
    assert len(EVENT_HISTORY_COLUMNS) == 25


def test_dimensions_appear_in_ddl_insert_and_indexes() -> "None":
    store = _store()
    statements = store.create_statements()  # type: ignore[attr-defined]
    template = store.insert_events_template()  # type: ignore[attr-defined]

    create_table = next(s for s in statements if s.startswith("CREATE TABLE"))
    assert all(name in create_table for name in DIMENSIONS)
    assert all(f":{name}" in template for name in DIMENSIONS)
    assert any("scope_key" in s and s.startswith("CREATE INDEX") for s in statements)
    assert any("entity" in s and s.startswith("CREATE INDEX") for s in statements)

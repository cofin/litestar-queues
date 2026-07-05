"""Integration tests for the SQLSpec native bulk-enqueue fast path.

Runs against the aiosqlite-pinned ``sqlspec_backend`` fixture. aiosqlite reports
``supports_native_arrow_import`` so the native ``load_from_records`` tier is
exercised by default; the ``force_fallback`` parameter disables the capability to
exercise the universal ``execute_many`` tier and prove both produce identical
results.
"""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import pytest

from litestar_queues import EnqueueSpec
from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.anyio


@pytest.fixture(params=["native", "fallback"])
def bulk_tier(
    request: "pytest.FixtureRequest", sqlspec_backend: "SQLSpecQueueBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "Iterator[str]":
    """Select the bulk tier by toggling the store's native-ingest capability.

    Yields:
        Selected bulk tier name.
    """
    store = sqlspec_backend._get_store()
    if request.param == "fallback":
        monkeypatch.setattr(type(store), "supports_native_bulk_ingest", property(lambda _store: False))
        yield request.param
    else:
        assert store.supports_native_bulk_ingest is True
        yield request.param


async def test_enqueue_many_persists_and_roundtrips(sqlspec_backend: "SQLSpecQueueBackend", bulk_tier: "str") -> "None":
    specs = [
        EnqueueSpec(task_name="tasks.a", args=(1, "x"), kwargs={"k": 1}, metadata={"m": [1]}, priority=3),
        EnqueueSpec(task_name="tasks.b", kwargs={"k": 2}, queue="reports"),
        EnqueueSpec(task_name="tasks.c", metadata={"nested": {"deep": True}}),
    ]

    records = await sqlspec_backend.enqueue_many(specs)

    assert [r.task_name for r in records] == ["tasks.a", "tasks.b", "tasks.c"]
    stats = await sqlspec_backend.get_statistics()
    assert stats.total == 3

    fetched = await sqlspec_backend.get_task(records[0].id)
    assert fetched is not None
    assert fetched.args == (1, "x")
    assert fetched.kwargs == {"k": 1}
    assert fetched.metadata == {"m": [1]}
    assert fetched.priority == 3
    assert fetched.status == "pending"

    nested = await sqlspec_backend.get_task(records[2].id)
    assert nested is not None
    assert nested.metadata == {"nested": {"deep": True}}


async def test_enqueue_many_matches_single_enqueue(sqlspec_backend: "SQLSpecQueueBackend", bulk_tier: "str") -> "None":
    single = await sqlspec_backend.enqueue("tasks.parity", args=(7,), kwargs={"a": "b"}, metadata={"z": 9}, priority=4)
    (bulk,) = await sqlspec_backend.enqueue_many([
        EnqueueSpec(task_name="tasks.parity", args=(7,), kwargs={"a": "b"}, metadata={"z": 9}, priority=4)
    ])

    single_fetched = await sqlspec_backend.get_task(single.id)
    bulk_fetched = await sqlspec_backend.get_task(bulk.id)
    assert single_fetched is not None
    assert bulk_fetched is not None
    assert bulk_fetched.args == single_fetched.args
    assert bulk_fetched.kwargs == single_fetched.kwargs
    assert bulk_fetched.metadata == single_fetched.metadata
    assert bulk_fetched.priority == single_fetched.priority
    assert bulk_fetched.status == single_fetched.status


async def test_enqueue_many_honors_scheduled_status(sqlspec_backend: "SQLSpecQueueBackend", bulk_tier: "str") -> "None":
    later = datetime.now(timezone.utc) + timedelta(minutes=5)

    records = await sqlspec_backend.enqueue_many([
        EnqueueSpec(task_name="tasks.now"),
        EnqueueSpec(task_name="tasks.later", scheduled_at=later),
    ])

    assert records[0].status == "pending"
    assert records[1].status == "scheduled"
    fetched = await sqlspec_backend.get_task(records[1].id)
    assert fetched is not None
    assert fetched.status == "scheduled"


async def test_enqueue_many_notifies_only_immediately_due_records(
    sqlspec_backend: "SQLSpecQueueBackend", bulk_tier: "str", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    notified_ids = []
    later = datetime.now(timezone.utc) + timedelta(minutes=5)

    async def notify(self: "SQLSpecQueueBackend", record: "Any") -> "None":
        del self
        notified_ids.append(record.id)

    monkeypatch.setattr(SQLSpecQueueBackend, "notify_new_task", notify)

    records = await sqlspec_backend.enqueue_many([
        EnqueueSpec(task_name="tasks.now"),
        EnqueueSpec(task_name="tasks.later", scheduled_at=later),
    ])

    assert notified_ids == [records[0].id]


async def test_enqueue_many_deduplicates_active_keys(
    sqlspec_backend: "SQLSpecQueueBackend", bulk_tier: "str"
) -> "None":
    first = await sqlspec_backend.enqueue("tasks.sync", key="sync:1", kwargs={"v": 1})

    records = await sqlspec_backend.enqueue_many([
        EnqueueSpec(task_name="tasks.sync", key="sync:1", kwargs={"v": 2}),
        EnqueueSpec(task_name="tasks.fresh", key="sync:2"),
    ])

    assert records[0].id == first.id
    assert records[0].kwargs == {"v": 1}
    assert records[1].task_name == "tasks.fresh"

    stats = await sqlspec_backend.get_statistics()
    assert stats.total == 2  # the duplicate key did not create a new row


async def test_enqueue_many_replaces_terminal_keys(sqlspec_backend: "SQLSpecQueueBackend", bulk_tier: "str") -> "None":
    first = await sqlspec_backend.enqueue("tasks.sync", key="sync:term", kwargs={"v": 1})
    await sqlspec_backend.complete_task(first.id, result={"ok": True})

    (replacement,) = await sqlspec_backend.enqueue_many([
        EnqueueSpec(task_name="tasks.sync", key="sync:term", kwargs={"v": 2})
    ])

    assert replacement.id != first.id
    assert replacement.kwargs == {"v": 2}
    refetched = await sqlspec_backend.get_task_by_key("sync:term")
    assert refetched is not None
    assert refetched.id == replacement.id


async def test_enqueue_many_empty_returns_empty(sqlspec_backend: "SQLSpecQueueBackend", bulk_tier: "str") -> "None":
    assert await sqlspec_backend.enqueue_many([]) == []


def test_bulk_values_orders_columns_to_match_create_table() -> "None":
    """``bulk_values`` must yield physical columns in CREATE TABLE order.

    The native Arrow ingest path inserts positionally on some adapters
    (DuckDB runs ``INSERT INTO t SELECT * FROM arrow``), so any mismatch
    between bulk column order and the table DDL drops each value into the
    wrong column. This pins the contract without needing a database; the
    DuckDB roundtrip below proves the order matches the *real* DDL.
    """
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    from litestar_queues.backends.sqlspec.stores.aiosqlite.store import AiosqliteQueueStore
    from litestar_queues.backends.sqlspec.stores.base import _TASK_COLUMNS

    store = AiosqliteQueueStore(AiosqliteConfig(connection_config={"database": ":memory:"}))
    # Feed keys in alphabetical order — exactly what the backend's record
    # serializer emits — and require they come back in DDL order.
    alphabetical = {column: index for index, column in enumerate(sorted(_TASK_COLUMNS))}

    (mapped,) = store.bulk_values([alphabetical])

    assert list(mapped) == list(_TASK_COLUMNS)
    assert mapped["kwargs_json"] == alphabetical["kwargs_json"]


async def test_enqueue_many_native_positional_roundtrip(duckdb_backend: "SQLSpecQueueBackend") -> "None":
    """The native Arrow ingest path must place every column in its DDL slot.

    Regression guard for positional ingest adapters: the bulk path emitted
    columns alphabetically, so DuckDB's positional insert dropped the
    ``kwargs_json`` string into the ``priority`` INT column and raised a
    conversion error. Name-binding adapters (aiosqlite, asyncpg) masked it.
    """
    pytest.importorskip("pyarrow")
    store = duckdb_backend._get_store()
    assert store.supports_native_bulk_ingest is True

    later = datetime.now(timezone.utc) + timedelta(minutes=5)
    records = await duckdb_backend.enqueue_many([
        EnqueueSpec(task_name="tasks.a", args=(1, "x"), kwargs={"n": 0}, metadata={"m": [1]}, priority=7),
        EnqueueSpec(task_name="tasks.later", scheduled_at=later, priority=2),
    ])

    assert [r.task_name for r in records] == ["tasks.a", "tasks.later"]

    first = await duckdb_backend.get_task(records[0].id)
    assert first is not None
    assert first.task_name == "tasks.a"
    assert first.args == (1, "x")
    assert first.kwargs == {"n": 0}
    assert first.metadata == {"m": [1]}
    assert first.priority == 7
    assert first.status == "pending"

    later_task = await duckdb_backend.get_task(records[1].id)
    assert later_task is not None
    assert later_task.priority == 2
    assert later_task.status == "scheduled"

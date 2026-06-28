"""Integration tests for the SQLSpec native bulk-enqueue fast path.

Runs against the aiosqlite-pinned ``sqlspec_backend`` fixture. aiosqlite reports
``supports_native_arrow_import`` so the native ``load_from_records`` tier is
exercised by default; the ``force_fallback`` parameter disables the capability to
exercise the universal ``execute_many`` tier and prove both produce identical
results.
"""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest

from litestar_queues import EnqueueSpec
from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

pytestmark = pytest.mark.anyio


@pytest.fixture(params=["native", "fallback"])
def bulk_tier(request: pytest.FixtureRequest, sqlspec_backend: SQLSpecQueueBackend) -> Iterator[str]:
    """Select the bulk tier by toggling the store's native-ingest capability."""
    store = sqlspec_backend._get_store()
    if request.param == "fallback":
        original = type(store).supports_native_bulk_ingest
        type(store).supports_native_bulk_ingest = False
        try:
            yield request.param
        finally:
            type(store).supports_native_bulk_ingest = original
    else:
        assert store.supports_native_bulk_ingest is True
        yield request.param


async def test_enqueue_many_persists_and_roundtrips(sqlspec_backend: SQLSpecQueueBackend, bulk_tier: str) -> None:
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


async def test_enqueue_many_matches_single_enqueue(sqlspec_backend: SQLSpecQueueBackend, bulk_tier: str) -> None:
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


async def test_enqueue_many_honors_scheduled_status(sqlspec_backend: SQLSpecQueueBackend, bulk_tier: str) -> None:
    later = datetime.now(UTC) + timedelta(minutes=5)

    records = await sqlspec_backend.enqueue_many([
        EnqueueSpec(task_name="tasks.now"),
        EnqueueSpec(task_name="tasks.later", scheduled_at=later),
    ])

    assert records[0].status == "pending"
    assert records[1].status == "scheduled"
    fetched = await sqlspec_backend.get_task(records[1].id)
    assert fetched is not None
    assert fetched.status == "scheduled"


async def test_enqueue_many_deduplicates_active_keys(sqlspec_backend: SQLSpecQueueBackend, bulk_tier: str) -> None:
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


async def test_enqueue_many_replaces_terminal_keys(sqlspec_backend: SQLSpecQueueBackend, bulk_tier: str) -> None:
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


async def test_enqueue_many_empty_returns_empty(sqlspec_backend: SQLSpecQueueBackend, bulk_tier: str) -> None:
    assert await sqlspec_backend.enqueue_many([]) == []

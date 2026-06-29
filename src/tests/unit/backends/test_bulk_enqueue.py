"""Unit tests for the backend-neutral bulk enqueue API.

These exercise ``EnqueueSpec`` and the ``BaseQueueBackend.enqueue_many`` default
implementation through the in-memory backend, which inherits the naive
per-item loop. Adapter-specific fast paths are covered in the SQLSpec
integration suite.
"""

from datetime import datetime, timedelta, timezone

import pytest

from litestar_queues import EnqueueSpec
from litestar_queues.backends import InMemoryQueueBackend

pytestmark = pytest.mark.anyio


async def test_enqueue_many_persists_all_specs_in_order() -> "None":
    backend = InMemoryQueueBackend()

    records = await backend.enqueue_many([
        EnqueueSpec(task_name="tasks.a", args=(1,), metadata={"i": 0}),
        EnqueueSpec(task_name="tasks.b", kwargs={"x": 2}, priority=5),
        EnqueueSpec(task_name="tasks.c", queue="reports"),
    ])

    assert [record.task_name for record in records] == ["tasks.a", "tasks.b", "tasks.c"]
    assert records[0].args == (1,)
    assert records[0].metadata == {"i": 0}
    assert records[1].kwargs == {"x": 2}
    assert records[1].priority == 5
    assert records[2].queue == "reports"
    for record in records:
        assert await backend.get_task(record.id) is not None


async def test_enqueue_many_empty_returns_empty_list() -> "None":
    backend = InMemoryQueueBackend()

    assert await backend.enqueue_many([]) == []


async def test_enqueue_many_honors_scheduled_status() -> "None":
    backend = InMemoryQueueBackend()
    later = datetime.now(timezone.utc) + timedelta(minutes=5)

    (record,) = await backend.enqueue_many([EnqueueSpec(task_name="tasks.later", scheduled_at=later)])

    assert record.status == "scheduled"


async def test_enqueue_many_deduplicates_active_keys() -> "None":
    backend = InMemoryQueueBackend()
    first = await backend.enqueue("tasks.sync", key="sync:1", kwargs={"v": 1})

    records = await backend.enqueue_many([
        EnqueueSpec(task_name="tasks.sync", key="sync:1", kwargs={"v": 2}),
        EnqueueSpec(task_name="tasks.other"),
    ])

    assert records[0].id == first.id
    assert records[0].kwargs == {"v": 1}
    assert records[1].task_name == "tasks.other"

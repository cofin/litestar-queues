"""Unit tests for the backend-neutral bulk enqueue API.

These exercise ``EnqueueSpec`` and the ``BaseQueueBackend.enqueue_many`` default
implementation through the in-memory backend, which inherits the naive
per-item loop. Adapter-specific fast paths are covered in the SQLSpec
integration suite.
"""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import pytest

from litestar_queues import EnqueueSpec
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.backends.base import BaseQueueBackend
from litestar_queues.models import QueuedTaskRecord

if TYPE_CHECKING:
    from uuid import UUID

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


async def test_enqueue_many_normalizes_naive_scheduled_at_to_utc() -> "None":
    backend = InMemoryQueueBackend()
    naive_later = (datetime.now(timezone.utc) + timedelta(minutes=5)).replace(tzinfo=None)

    (record,) = await backend.enqueue_many([EnqueueSpec(task_name="tasks.later", scheduled_at=naive_later)])

    assert record.status == "scheduled"
    assert record.scheduled_at == naive_later.replace(tzinfo=timezone.utc)


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


async def test_base_enqueue_many_calls_batch_notification_once_for_due_records() -> "None":
    backend = _BatchNotifyingBackend()
    later = datetime.now(timezone.utc) + timedelta(minutes=5)

    records = await backend.enqueue_many([
        EnqueueSpec(task_name="tasks.one"),
        EnqueueSpec(task_name="tasks.two", scheduled_at=later),
        EnqueueSpec(task_name="tasks.three"),
    ])

    assert [record.task_name for record in records] == ["tasks.one", "tasks.two", "tasks.three"]
    assert [record.task_name for record in backend.notified_records] == ["tasks.one"]


async def test_base_enqueue_many_skips_batch_notification_when_no_records_are_due() -> "None":
    backend = _BatchNotifyingBackend()
    later = datetime.now(timezone.utc) + timedelta(minutes=5)

    records = await backend.enqueue_many([EnqueueSpec(task_name="tasks.later", scheduled_at=later)])

    assert [record.status for record in records] == ["scheduled"]
    assert backend.notified_records == []


class _BatchNotifyingBackend(BaseQueueBackend):
    __slots__ = ("notified_records", "records")

    def __init__(self) -> "None":
        super().__init__()
        self.records: "list[QueuedTaskRecord]" = []
        self.notified_records: "list[QueuedTaskRecord]" = []

    async def enqueue(
        self,
        task_name: "str",
        *,
        args: "tuple[Any, ...]" = (),
        kwargs: "dict[str, Any] | None" = None,
        queue: "str" = "default",
        priority: "int" = 0,
        max_retries: "int" = 0,
        scheduled_at: "datetime | None" = None,
        key: "str | None" = None,
        execution_backend: "str" = "local",
        execution_profile: "str | None" = None,
        metadata: "dict[str, Any] | None" = None,
        id: "UUID | None" = None,  # noqa: A002
    ) -> "QueuedTaskRecord":
        record = QueuedTaskRecord(
            task_name=task_name,
            args=args,
            kwargs=dict(kwargs or {}),
            queue=queue,
            priority=priority,
            max_retries=max_retries,
            scheduled_at=scheduled_at,
            key=key,
            execution_backend=execution_backend,
            execution_profile=execution_profile,
            metadata=dict(metadata or {}),
            status="scheduled" if scheduled_at is not None and scheduled_at > datetime.now(timezone.utc) else "pending",
        )
        if id is not None:
            record.id = id
        self.records.append(record)
        return record

    async def notify_new_task(self, record: "QueuedTaskRecord") -> "None":
        self.notified_records.append(record)

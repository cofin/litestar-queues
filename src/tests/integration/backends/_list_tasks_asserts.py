"""Shared ``list_tasks`` assertions for Redis and Valkey contract suites."""

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

from litestar_queues.models import TaskListPage
from tests.helpers._timing import MutableClock

if TYPE_CHECKING:
    import pytest

    from litestar_queues.backends import BaseQueueBackend


async def assert_list_tasks_filters_tie_breaks_and_paginates(
    backend: "BaseQueueBackend", monkeypatch: "pytest.MonkeyPatch", clock_module: "Any"
) -> "None":
    """Verify multi-status/queue/key filtering, tie-breaking, and pagination."""
    clock = MutableClock(datetime(2026, 2, 1, 12, 0, 0, tzinfo=UTC))
    monkeypatch.setattr(clock_module, "_utc_now", clock)

    tie_low = await backend.enqueue("tasks.email", queue="emails", id=UUID(int=1))
    tie_high = await backend.enqueue("tasks.email", queue="emails", id=UUID(int=2))

    clock.advance(timedelta(seconds=1))
    first_keyed = await backend.enqueue("tasks.sync", queue="emails", key="sync:acct-1")
    assert await backend.claim_task(first_keyed.id) is not None
    await backend.complete_task(first_keyed.id, result={"ok": True})

    clock.advance(timedelta(seconds=1))
    second_keyed = await backend.enqueue("tasks.sync", queue="emails", key="sync:acct-1")

    clock.advance(timedelta(seconds=1))
    failed = await backend.enqueue("tasks.email", queue="emails", max_retries=0)
    assert await backend.claim_task(failed.id) is not None
    await backend.fail_task(failed.id, "boom")

    clock.advance(timedelta(seconds=1))
    other_queue = await backend.enqueue("tasks.email", queue="reports")

    tie_page = await backend.list_tasks(status="pending", queue="emails", task_name="tasks.email")
    assert [record.id for record in tie_page.items] == [tie_high.id, tie_low.id]
    assert tie_page.total == 2
    assert tie_page.has_more is False

    reused_key_page = await backend.list_tasks(key="sync:acct-1")
    assert [record.id for record in reused_key_page.items] == [second_keyed.id, first_keyed.id]
    assert reused_key_page.total == 2

    page = await backend.list_tasks(status=("completed", "failed", "pending"), queue="emails", limit=2, offset=1)
    assert page.total == 5
    assert [record.id for record in page.items] == [second_keyed.id, first_keyed.id]
    assert page.has_more is True

    tail = await backend.list_tasks(status=("completed", "failed", "pending"), queue="emails", limit=2, offset=4)
    assert [record.id for record in tail.items] == [tie_low.id]
    assert tail.total == 5
    assert tail.has_more is False

    reports_page = await backend.list_tasks(queue="reports")
    assert [record.id for record in reports_page.items] == [other_queue.id]

    assert await backend.list_tasks(status=(), limit=5, offset=0) == TaskListPage(items=[], limit=5, offset=0, total=0)
    assert await backend.list_tasks(limit=2, offset=10) == TaskListPage(items=[], limit=2, offset=10, total=6)

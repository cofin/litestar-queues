from datetime import UTC, datetime, timedelta, timezone

import pytest

import litestar_queues
from litestar_queues import TaskListPage as RootTaskListPage
from litestar_queues.models import TERMINAL_STATUSES, QueuedTaskRecord, QueueStatistics, TaskListPage


def test_expired_is_terminal() -> "None":
    assert "expired" in TERMINAL_STATUSES


def test_is_expired_true_when_deadline_passed() -> "None":
    record = QueuedTaskRecord(task_name="tasks.expired", expires_at=datetime.now(UTC) - timedelta(seconds=1))

    assert record.is_expired is True


def test_is_expired_false_when_deadline_future_or_none() -> "None":
    future = QueuedTaskRecord(task_name="tasks.future", expires_at=datetime.now(UTC) + timedelta(minutes=1))
    without_deadline = QueuedTaskRecord(task_name="tasks.no_deadline")

    assert future.is_expired is False
    assert without_deadline.is_expired is False


def test_queue_statistics_counts_expired_in_total() -> "None":
    statistics = QueueStatistics(pending=2, expired=3)

    assert statistics.total == 5


def test_queued_task_record_coerces_naive_expires_at_to_utc() -> "None":
    record = QueuedTaskRecord(
        task_name="tasks.naive",
        expires_at=datetime(2026, 7, 25, 12, 0),  # noqa: DTZ001 - explicitly exercise naive coercion
    )

    assert record.expires_at == datetime(2026, 7, 25, 12, 0, tzinfo=UTC)


def test_dispatch_checked_at_normalizes_to_utc() -> "None":
    naive = datetime(2026, 9, 7, 12, 0)  # noqa: DTZ001 - exercise naive coercion
    offset = timezone(timedelta(hours=2))
    expected = naive.replace(tzinfo=UTC)
    assert QueuedTaskRecord("task").dispatch_checked_at is None
    assert QueuedTaskRecord("task", dispatch_checked_at=naive).dispatch_checked_at == expected
    assert QueuedTaskRecord(
        "task", dispatch_checked_at=naive.replace(tzinfo=offset)
    ).dispatch_checked_at == expected - timedelta(hours=2)


@pytest.mark.parametrize(
    ("items", "limit", "offset", "total", "expected_has_more"),
    [
        ([QueuedTaskRecord(task_name="tasks.one"), QueuedTaskRecord(task_name="tasks.two")], 2, 0, 5, True),
        ([QueuedTaskRecord(task_name="tasks.five")], 2, 4, 5, False),
        ([], 2, 6, 5, False),
        ([], 2, 0, 0, False),
    ],
)
def test_task_list_page_has_more(
    items: "list[QueuedTaskRecord]", limit: "int", offset: "int", total: "int", expected_has_more: "bool"
) -> "None":
    page = TaskListPage(items=items, limit=limit, offset=offset, total=total)

    assert page.has_more is expected_has_more


def test_task_list_page_reexported_at_package_root() -> "None":
    assert RootTaskListPage is TaskListPage
    assert "TaskListPage" in litestar_queues.__all__

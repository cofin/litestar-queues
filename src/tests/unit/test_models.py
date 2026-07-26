from datetime import datetime, timedelta, timezone

from litestar_queues.models import TERMINAL_STATUSES, QueuedTaskRecord, QueueStatistics


def test_expired_is_terminal() -> "None":
    assert "expired" in TERMINAL_STATUSES


def test_is_expired_true_when_deadline_passed() -> "None":
    record = QueuedTaskRecord(task_name="tasks.expired", expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))

    assert record.is_expired is True


def test_is_expired_false_when_deadline_future_or_none() -> "None":
    future = QueuedTaskRecord(task_name="tasks.future", expires_at=datetime.now(timezone.utc) + timedelta(minutes=1))
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

    assert record.expires_at == datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)

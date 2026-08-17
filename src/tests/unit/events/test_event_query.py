"""Public event-history query, page, and retention models."""

import pytest

from litestar_queues.events import QueueEventEntityRef, QueueEventQuery
from litestar_queues.events.typing import OffsetPagination
from litestar_queues.exceptions import QueueConfigurationError


def test_default_query_is_unconstrained() -> "None":
    query = QueueEventQuery()

    assert query.filters() == ()
    assert query.order == "asc"
    assert query.is_paginated is False


def test_entity_reference_is_canonicalized() -> "None":
    query = QueueEventQuery(entity=QueueEventEntityRef(type="invoice", id="42"))  # type: ignore[arg-type]

    assert query.entity == "invoice:42"
    assert query.filters() == (("entity", "invoice:42"),)


def test_filters_are_declaration_ordered_and_skip_none() -> "None":
    query = QueueEventQuery(task_name="tasks.demo", level="error", scope_key="acme")

    assert query.filters() == (("task_name", "tasks.demo"), ("scope_key", "acme"), ("level", "error"))


@pytest.mark.parametrize("kwargs", [{"order": "ASC"}, {"limit": 0}, {"limit": -1}, {"limit": True}, {"offset": -1}])
def test_invalid_query_raises(kwargs: "dict[str, object]") -> "None":
    with pytest.raises(QueueConfigurationError):
        QueueEventQuery(**kwargs)  # type: ignore[arg-type]


def test_empty_page_reports_a_zero_total() -> "None":
    from litestar_queues.events.query import paginate_event_records

    page = paginate_event_records((), None)

    assert page.items == []
    assert page.total == 0


def test_total_counts_matches_before_the_limit_is_applied() -> "None":
    from datetime import datetime, timezone

    from litestar_queues.events import QueueEventLogRecord
    from litestar_queues.events.query import paginate_event_records

    base = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def _record(sequence: int) -> QueueEventLogRecord:
        return QueueEventLogRecord(
            event_id=str(sequence),
            event_type="task.log",
            task_id="t-1",
            task_name="tasks.demo",
            queue="default",
            worker_id=None,
            execution_backend="local",
            execution_profile=None,
            stage=None,
            level=None,
            message=None,
            detail={},
            progress_current=None,
            progress_total=None,
            progress_percent=None,
            duration_ms=None,
            sequence=sequence,
            occurred_at=base,
            created_at=base,
            actor_type=None,
            actor_id=None,
            scope=None,
            scope_key=None,
            entity=None,
        )

    records = tuple(_record(sequence=n) for n in range(5))

    page = paginate_event_records(records, QueueEventQuery(limit=2))

    assert [r.sequence for r in page.items] == [0, 1]
    assert page.total == 5
    assert isinstance(page, OffsetPagination)


def test_level_ranks_are_total_and_never_drop_unknown_levels() -> "None":
    from litestar_queues.events import event_level_rank

    assert event_level_rank(None) == 0
    assert event_level_rank("") == 0
    assert event_level_rank("trace") == 1  # unknown, still ranked
    assert event_level_rank("DEBUG") == 10  # case-insensitive
    assert event_level_rank("critical") > event_level_rank("error") > event_level_rank("warning")


def test_stage_summary_carries_latest_and_worst_fields() -> "None":
    from litestar_queues.events import QueueEventStageSummary

    summary = QueueEventStageSummary(
        stage="load", event_count=1, total_duration_ms=0.0, first_event_at=None, last_event_at=None
    )

    assert (summary.latest_sequence, summary.latest_message, summary.worst_level) == (None, None, None)


def test_retention_rule_defaults_to_matching_everything() -> "None":
    from litestar_queues.events import QueueEventRetentionRule

    rule = QueueEventRetentionRule(max_age=60.0)

    assert rule.match.filters() == ()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_age": 0},
        {"max_age": -1.0},
        {"max_age": float("inf")},
        {"max_age": 60.0, "match": QueueEventQuery(limit=5)},
        {"max_age": 60.0, "match": QueueEventQuery(offset=5)},
        {"max_age": 60.0, "match": QueueEventQuery(order="desc")},
    ],
)
def test_invalid_retention_rule_raises(kwargs: "dict[str, object]") -> "None":
    from litestar_queues.events import QueueEventRetentionRule

    with pytest.raises(QueueConfigurationError):
        QueueEventRetentionRule(**kwargs)  # type: ignore[arg-type]

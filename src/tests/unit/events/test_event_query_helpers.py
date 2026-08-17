"""Reference semantics for the cross-backend event query contract."""

from datetime import datetime, timedelta, timezone

from litestar_queues.events import QueueEventLogRecord, QueueEventQuery
from litestar_queues.events.query import (
    match_event_record,
    paginate_event_records,
    sort_event_records,
    summarize_event_records,
)

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _record(event_id: str, *, seq: "int | None" = None, offset: int = 0, **kwargs: object) -> QueueEventLogRecord:
    defaults: "dict[str, object]" = {
        "event_type": "task.log",
        "task_id": "t-1",
        "task_name": "tasks.demo",
        "queue": "default",
        "worker_id": None,
        "execution_backend": "local",
        "execution_profile": None,
        "stage": None,
        "level": None,
        "message": None,
        "detail": {},
        "progress_current": None,
        "progress_total": None,
        "progress_percent": None,
        "duration_ms": None,
        "sequence": seq,
        "occurred_at": BASE + timedelta(seconds=offset),
        "created_at": BASE,
        "actor_type": None,
        "actor_id": None,
        "scope": None,
        "scope_key": None,
        "entity": None,
    }
    defaults.update(kwargs)
    return QueueEventLogRecord(event_id=event_id, **defaults)  # type: ignore[arg-type]


def test_sequence_ties_break_on_event_id() -> "None":
    records = [_record("b", seq=1), _record("a", seq=1)]

    assert [r.event_id for r in sort_event_records(records)] == ["a", "b"]
    assert [r.event_id for r in sort_event_records(records, order="desc")] == ["b", "a"]


def test_match_ands_every_set_filter() -> "None":
    record = _record("a", scope_key="acme", level="error")

    assert match_event_record(record, QueueEventQuery(scope_key="acme", level="error")) is True
    assert match_event_record(record, QueueEventQuery(scope_key="acme", level="info")) is False
    assert match_event_record(record, None) is True


def test_pagination_reports_total_and_empty_pages() -> "None":
    records = sort_event_records([_record(str(i), offset=i) for i in range(5)])

    first = paginate_event_records(records, QueueEventQuery(limit=2))
    last = paginate_event_records(records, QueueEventQuery(limit=2, offset=4))
    past_end = paginate_event_records(records, QueueEventQuery(limit=2, offset=99))

    assert [r.event_id for r in first.items] == ["0", "1"]
    assert [r.event_id for r in last.items] == ["4"]
    assert past_end.items == []
    # total is the match count, independent of the window - including past the end.
    assert (first.total, last.total, past_end.total) == (5, 5, 5)


def test_summary_latest_and_worst_level() -> "None":
    records = [
        _record("a", offset=0, stage="load", level="info", message="one", seq=1, duration_ms=10.0),
        _record("b", offset=1, stage="load", level="error", message="two", seq=2, duration_ms=5.0),
        _record("c", offset=2, stage="load", level="info", message="three", seq=3),
        _record("z", offset=0, stage=None, level=None, message="unstaged"),
    ]

    summaries = summarize_event_records(records)

    assert [s.stage for s in summaries] == [None, "load"]
    load = summaries[1]
    assert (load.event_count, load.total_duration_ms) == (3, 15.0)
    assert (load.latest_sequence, load.latest_message) == (3, "three")
    assert load.worst_level == "error"
    assert summaries[0].worst_level is None

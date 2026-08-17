from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from litestar_queues.backends.ephemeral.backend import EphemeralQueueBackend
from litestar_queues.events import EventHistoryConfig, QueueEvent
from litestar_queues.events.query import QueueEventQuery
from litestar_queues.exceptions import QueueConfigurationError
from tests.unit.backends.test_ephemeral import backend, server_context

__all__ = ["backend", "server_context"]

pytestmark = pytest.mark.anyio

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _event(event_id: str, *, offset: int, **kwargs: object) -> QueueEvent:
    return QueueEvent(
        id=event_id,
        type=kwargs.pop("type", "task.log"),  # type: ignore[arg-type]
        scope=kwargs.pop("scope", "task"),  # type: ignore[arg-type]
        task_id="t-1",
        task_name="tasks.demo",
        occurred_at=BASE + timedelta(seconds=offset),
        **kwargs,  # type: ignore[arg-type]
    )


async def _log(backend: EphemeralQueueBackend, *events: QueueEvent) -> "Any":
    log = backend.get_event_log(EventHistoryConfig())
    assert log is not None
    for event in events:
        await log.publish_event(event)
    return log


async def test_event_table_has_dimension_columns(backend: "EphemeralQueueBackend") -> None:
    """Reuse the existing `backend` fixture in src/tests/unit/backends/test_ephemeral.py."""
    columns = {
        row["name"]
        for row in await backend._run(
            lambda connection: connection.execute("PRAGMA table_info(queue_event)").fetchall()
        )
    }

    assert {"event_type", "level", "scope", "scope_key", "actor", "entity"} <= columns


async def test_query_filters_on_every_dimension(backend: EphemeralQueueBackend) -> None:
    log = await _log(
        backend,
        _event("a", offset=0, scope_key="acme", level="error"),
        _event("b", offset=1, scope_key="other", level="error"),
        _event("c", offset=2, scope_key="acme", level="info"),
    )

    page = await log.query_events(QueueEventQuery(scope_key="acme", level="error"))

    assert [record.event_id for record in page.items] == ["a"]


async def test_query_orders_and_pages_stably(backend: EphemeralQueueBackend) -> None:
    log = await _log(backend, *[_event(str(index), offset=index) for index in range(5)])

    first = await log.query_events(QueueEventQuery(limit=2))
    second = await log.query_events(QueueEventQuery(limit=2, offset=2))
    descending = await log.query_events(QueueEventQuery(order="desc", limit=2))
    empty = await log.query_events(QueueEventQuery(limit=2, offset=50))

    assert [r.event_id for r in first.items] == ["0", "1"] and first.total == 5
    assert [r.event_id for r in second.items] == ["2", "3"]
    assert [r.event_id for r in descending.items] == ["4", "3"]
    assert empty.items == [] and empty.total == 5


async def test_summaries_are_scoped_and_rank_levels(backend: EphemeralQueueBackend) -> None:
    log = await _log(
        backend,
        _event("a", offset=0, scope_key="acme", level="info", message="one", payload={"stage": "load"}),
        _event("b", offset=1, scope_key="acme", level="error", message="two", payload={"stage": "load"}),
        _event("c", offset=2, scope_key="other", level="critical", payload={"stage": "load"}),
    )

    summaries = await log.summarize_stages(QueueEventQuery(scope_key="acme"))

    assert len(summaries) == 1
    assert summaries[0].stage == "load"
    assert summaries[0].event_count == 2
    assert summaries[0].latest_message == "two"
    assert summaries[0].worst_level == "error"


async def test_summary_rejects_pagination(backend: EphemeralQueueBackend) -> None:
    log = await _log(backend, _event("a", offset=0))

    with pytest.raises(QueueConfigurationError):
        await log.summarize_stages(QueueEventQuery(limit=1))


async def test_filtered_bounded_cleanup_converges_and_spares_unmatched(backend: EphemeralQueueBackend) -> None:
    log = await _log(
        backend,
        *[_event(f"m{index}", offset=index, scope_key="acme") for index in range(5)],
        *[_event(f"u{index}", offset=index, scope_key="other") for index in range(3)],
    )
    cutoff = BASE + timedelta(hours=1)
    match = QueueEventQuery(scope_key="acme")

    deleted = [await log.cleanup_events(before=cutoff, match=match, limit=2) for _ in range(4)]
    remaining = await log.query_events()

    assert deleted == [2, 2, 1, 0]
    assert {record.scope_key for record in remaining.items} == {"other"}
    assert len(remaining.items) == 3


async def test_exclude_implements_first_match_wins(backend: EphemeralQueueBackend) -> None:
    log = await _log(
        backend,
        _event("a", offset=0, scope_key="acme", level="error"),
        _event("b", offset=1, scope_key="acme", level="info"),
    )
    cutoff = BASE + timedelta(hours=1)

    deleted = await log.cleanup_events(
        before=cutoff, match=QueueEventQuery(scope_key="acme"), exclude=(QueueEventQuery(level="error"),)
    )
    remaining = await log.query_events()

    assert deleted == 1
    assert [record.event_id for record in remaining.items] == ["a"]

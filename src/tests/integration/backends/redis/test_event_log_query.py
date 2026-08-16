"""Redis event-history dimensions, query, summary, and filtered retention."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest

from litestar_queues.backends.redis.event_log import RedisQueueEventLog
from litestar_queues.events import EventHistoryConfig, QueueEvent, QueueEventQuery

if TYPE_CHECKING:
    from litestar_queues.backends.redis import RedisQueueBackend

pytestmark = pytest.mark.anyio

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _event(event_id: str, *, offset: int = 0, **kwargs: object) -> QueueEvent:
    return QueueEvent(
        id=event_id,
        type=kwargs.pop("type", "task.log"),  # type: ignore[arg-type]
        scope=kwargs.pop("scope", "task"),  # type: ignore[arg-type]
        task_id="t-1",
        task_name="tasks.demo",
        occurred_at=BASE + timedelta(seconds=offset),
        **kwargs,  # type: ignore[arg-type]
    )


async def test_record_hash_carries_dimensions(redis_backend: "RedisQueueBackend") -> "None":
    log = redis_backend.get_event_log(EventHistoryConfig(batch_size=1))
    assert isinstance(log, RedisQueueEventLog)
    await log.publish_event(_event("a", scope_key="acme", entity={"type": "invoice", "id": "42"}))
    await log.flush_events()

    client = await redis_backend._get_client()
    mapping = await client.hgetall(redis_backend._event_log_event_key("a"))
    decoded = {k.decode() if isinstance(k, bytes) else k: v for k, v in mapping.items()}

    assert decoded["scope_key"] in {"acme", b"acme"}
    assert decoded["entity"] in {"invoice:42", b"invoice:42"}


async def test_dimension_indexes_are_registered_and_cleaned(redis_backend: "RedisQueueBackend") -> "None":
    log = redis_backend.get_event_log(EventHistoryConfig(batch_size=1))
    assert isinstance(log, RedisQueueEventLog)
    await log.publish_event(_event("a", scope_key="acme", entity={"type": "invoice", "id": "42"}))
    await log.flush_events()

    from typing import Any, cast

    client = cast("Any", await redis_backend._get_client())  # Assert index cards
    from litestar_queues.backends.redis.event_log import hashed_index_value

    scope_index = f"{redis_backend._key_prefix}:events:scope_key:{hashed_index_value('acme')}"
    entity_index = f"{redis_backend._key_prefix}:events:entity:{hashed_index_value('invoice:42')}"

    assert await client.zcard(scope_index) == 1
    assert await client.zcard(entity_index) == 1

    # Cleanup past its occurred_at
    cutoff = BASE + timedelta(hours=1)
    # Note: cleanup_events replaces cleanup_before
    await log.cleanup_events(before=cutoff)

    # Assert both index keys are gone
    assert await client.zcard(scope_index) == 0
    assert await client.zcard(entity_index) == 0


async def _log(redis_backend: "RedisQueueBackend", *events: QueueEvent) -> "RedisQueueEventLog":
    log = redis_backend.get_event_log(EventHistoryConfig(batch_size=1))
    assert isinstance(log, RedisQueueEventLog)
    for event in events:
        await log.publish_event(event)
    await log.flush_events()
    return log


async def test_query_filters_on_every_dimension(redis_backend: "RedisQueueBackend") -> None:
    log = await _log(
        redis_backend,
        _event("a", offset=0, scope_key="acme", level="error"),
        _event("b", offset=1, scope_key="other", level="error"),
        _event("c", offset=2, scope_key="acme", level="info"),
    )

    page = await log.query_events(QueueEventQuery(scope_key="acme", level="error"))

    assert [record.event_id for record in page.items] == ["a"]


async def test_query_orders_and_pages_stably(redis_backend: "RedisQueueBackend") -> None:
    log = await _log(redis_backend, *[_event(str(index), offset=index) for index in range(5)])

    first = await log.query_events(QueueEventQuery(limit=2))
    second = await log.query_events(QueueEventQuery(limit=2, offset=2))
    descending = await log.query_events(QueueEventQuery(order="desc", limit=2))
    empty = await log.query_events(QueueEventQuery(limit=2, offset=50))

    assert [r.event_id for r in first.items] == ["0", "1"] and first.total == 5
    assert [r.event_id for r in second.items] == ["2", "3"]
    assert [r.event_id for r in descending.items] == ["4", "3"]
    assert empty.items == [] and empty.total == 5


async def test_summaries_are_scoped_and_rank_levels(redis_backend: "RedisQueueBackend") -> None:
    log = await _log(
        redis_backend,
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


async def test_summary_rejects_pagination(redis_backend: "RedisQueueBackend") -> None:
    log = await _log(redis_backend, _event("a", offset=0))

    from litestar_queues.exceptions import QueueConfigurationError

    with pytest.raises(QueueConfigurationError):
        await log.summarize_stages(QueueEventQuery(limit=1))


async def test_filtered_bounded_cleanup_converges_and_spares_unmatched(redis_backend: "RedisQueueBackend") -> None:
    log = await _log(
        redis_backend,
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


async def test_exclude_implements_first_match_wins(redis_backend: "RedisQueueBackend") -> None:
    log = await _log(
        redis_backend,
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

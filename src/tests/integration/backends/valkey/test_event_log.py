"""Valkey backend-managed queue event history tests."""

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

import pytest

pytest.importorskip("valkey")

from litestar_queues import EventLogConfig
from litestar_queues.backends.redis.event_log import RedisQueueEventLog
from litestar_queues.events import QueueEvent

if TYPE_CHECKING:
    from litestar_queues.backends.valkey import ValkeyQueueBackend

pytestmark = pytest.mark.anyio


async def test_valkey_event_log_reuses_redis_protocol_implementation(valkey_backend: "ValkeyQueueBackend") -> "None":
    event_log_config = EventLogConfig(buffer_size=10, flush_interval=60)
    event_log = valkey_backend.get_event_log(event_log_config)
    assert isinstance(event_log, RedisQueueEventLog)

    await event_log.publish_event(
        QueueEvent(
            id="valkey-event-1",
            type="task.event",
            scope="task",
            task_id="task-valkey-1",
            task_name="tasks.valkey.history",
            sequence=1,
            payload={"stage": "load", "duration_ms": 7},
            occurred_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
        )
    )
    await event_log.flush_events()

    records = await event_log.list_events(task_name="tasks.valkey.history")

    assert [record.event_id for record in records] == ["valkey-event-1"]
    assert records[0].detail == {"stage": "load", "duration_ms": 7}


async def test_valkey_event_cleanup_always_removes_the_global_index(valkey_backend: "ValkeyQueueBackend") -> "None":
    """Valkey must share Redis' explicit global-index cleanup invariant."""
    event_log = valkey_backend.get_event_log(EventLogConfig(buffer_size=1))
    assert event_log is not None
    event = QueueEvent(
        id="valkey-event-global-cleanup",
        type="task.event",
        scope="task",
        task_id="task-valkey-cleanup",
        task_name="tasks.valkey.cleanup",
        sequence=1,
        payload={},
        occurred_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
    )
    await event_log.publish_event(event)

    client = cast("Any", await valkey_backend._get_client())
    event_key = valkey_backend._event_log_event_key(event.id)
    mapping = await client.hgetall(event_key)
    secondary_indexes = [
        str(index_key)
        for index_key in json.loads(str(mapping["index_keys"]))
        if str(index_key) != valkey_backend._event_log_global_key()
    ]
    await client.hset(event_key, mapping={"index_keys": json.dumps(secondary_indexes)})

    assert await event_log.cleanup_before(datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc), limit=1) == 1
    assert await client.zrange(valkey_backend._event_log_global_key(), 0, -1) == []


async def test_valkey_event_cleanup_continues_in_exact_bounded_batches(valkey_backend: "ValkeyQueueBackend") -> "None":
    """Valkey shares Redis' deterministic bounded cleanup continuation."""
    event_log = valkey_backend.get_event_log(EventLogConfig(buffer_size=1))
    assert event_log is not None
    events = [
        QueueEvent(
            id=f"valkey-bounded-{second}",
            type="task.event",
            scope="task",
            task_id="task-valkey-bounded",
            task_name="tasks.valkey.bounded",
            sequence=second,
            payload={},
            occurred_at=datetime(2026, 1, 1, 0, 0, second, tzinfo=timezone.utc),
        )
        for second in range(1, 6)
    ]
    for event in events:
        await event_log.publish_event(event)

    cutoff = datetime(2026, 1, 1, 0, 0, 6, tzinfo=timezone.utc)
    assert await event_log.cleanup_before(cutoff, limit=2) == 2
    assert [record.event_id for record in await event_log.list_events()] == [event.id for event in events[2:]]
    assert await event_log.cleanup_before(cutoff, limit=2) == 2
    assert [record.event_id for record in await event_log.list_events()] == [events[4].id]
    assert await event_log.cleanup_before(cutoff, limit=2) == 1
    assert await event_log.cleanup_before(cutoff, limit=2) == 0
    assert await event_log.list_events() == []

"""Redis backend-managed queue event history tests."""

import json
from datetime import datetime, timezone
from typing import Any, cast

import pytest

pytest.importorskip("redis")

from litestar_queues import EventLogConfig, QueueConfig, QueueService
from litestar_queues.backends.redis import RedisBackendConfig, RedisQueueBackend
from litestar_queues.backends.redis.event_log import RedisQueueEventLog
from litestar_queues.events import QueueEvent

pytestmark = pytest.mark.anyio


async def test_redis_event_log_records_queries_and_cleans_up(redis_backend: "RedisQueueBackend") -> "None":
    event_log_config = EventLogConfig(buffer_size=10, flush_interval=60)
    event_log = redis_backend.get_event_log(event_log_config)
    assert event_log is not None

    task_id = "task-redis-1"
    await event_log.publish_event(
        _event("redis-event-2", task_id=task_id, task_name="tasks.redis.history", sequence=2, second=2)
    )
    await event_log.publish_event(
        _event(
            "redis-event-1",
            task_id=task_id,
            task_name="tasks.redis.history",
            sequence=1,
            second=1,
            detail={"stage": "load", "duration_ms": 7},
        )
    )
    await event_log.publish_event(
        _event("redis-event-other", task_id="task-redis-2", task_name="tasks.other", sequence=1, second=3)
    )
    await event_log.flush_events()

    records = await event_log.list_events(task_id=task_id)
    limited = await event_log.list_events(task_name="tasks.redis.history", limit=1)
    deleted = await event_log.cleanup_before(datetime(2026, 1, 1, 0, 0, 3, tzinfo=timezone.utc))
    remaining = await event_log.list_events(task_name="tasks.redis.history")
    client = cast("Any", await redis_backend._get_client())
    global_remaining = await client.zrange(redis_backend._event_log_global_key(), 0, -1)

    assert [record.event_id for record in records] == ["redis-event-1", "redis-event-2"]
    assert records[0].detail == {"stage": "load", "duration_ms": 7}
    assert records[0].stage == "load"
    assert records[0].duration_ms == 7
    assert [record.event_id for record in limited] == ["redis-event-1"]
    assert deleted == 2
    assert remaining == []
    assert global_remaining == ["redis-event-other"]


async def test_redis_queue_service_accepts_event_log_config(redis_backend: "RedisQueueBackend") -> "None":
    async with QueueService(QueueConfig(event_log=EventLogConfig()), queue_backend=redis_backend) as service:
        assert service.get_queue_backend().get_event_log(EventLogConfig()) is not None


async def test_redis_event_cleanup_always_removes_the_global_index(redis_backend: "RedisQueueBackend") -> "None":
    """Cleanup must not rely on a hash's secondary-index metadata for the global ZREM."""
    event_log = redis_backend.get_event_log(EventLogConfig(buffer_size=1))
    assert event_log is not None
    event = _event("redis-event-global-cleanup")
    await event_log.publish_event(event)

    client = cast("Any", await redis_backend._get_client())
    event_key = redis_backend._event_log_event_key(event.id)
    mapping = await client.hgetall(event_key)
    secondary_indexes = [
        str(index_key)
        for index_key in json.loads(str(mapping["index_keys"]))
        if str(index_key) != redis_backend._event_log_global_key()
    ]
    await client.hset(event_key, mapping={"index_keys": json.dumps(secondary_indexes)})

    assert await event_log.cleanup_before(datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc), limit=1) == 1
    assert await client.zrange(redis_backend._event_log_global_key(), 0, -1) == []


async def test_redis_event_cleanup_continues_in_exact_bounded_batches(redis_backend: "RedisQueueBackend") -> "None":
    """Bounded cleanup deletes oldest events first and reaches a stable no-op."""
    event_log = redis_backend.get_event_log(EventLogConfig(buffer_size=1))
    assert event_log is not None
    events = [_event(f"redis-bounded-{second}", second=second) for second in range(1, 6)]
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


async def test_redis_event_log_non_strict_flush_preserves_failed_batch() -> "None":
    backend = RedisQueueBackend(
        backend_config=RedisBackendConfig(
            client=cast("Any", _FailingRedisClient()),
            key_prefix="litestar_queues:test:failing-event-log",
            notifications=False,
        )
    )
    await backend.open()
    event_log = backend.get_event_log(EventLogConfig(buffer_size=1, strict=False))
    assert isinstance(event_log, RedisQueueEventLog)

    await event_log.publish_event(_event("redis-failing-event"))

    assert len(event_log._pending) == 1


def _event(
    event_id: "str",
    *,
    task_id: "str" = "task-redis",
    task_name: "str" = "tasks.redis.history",
    sequence: "int" = 1,
    second: "int" = 1,
    detail: "dict[str, Any] | None" = None,
) -> "QueueEvent":
    return QueueEvent(
        id=event_id,
        type="task.event",
        scope="task",
        task_id=task_id,
        task_name=task_name,
        queue="default",
        sequence=sequence,
        payload=dict(detail or {}),
        occurred_at=datetime(2026, 1, 1, 0, 0, second, tzinfo=timezone.utc),
    )


class _FailingPipeline:
    def hset(self, *_args: "Any", **_kwargs: "Any") -> "None":
        return None

    def zadd(self, *_args: "Any", **_kwargs: "Any") -> "None":
        return None

    def execute(self) -> "None":
        msg = "controlled pipeline failure"
        raise RuntimeError(msg)


class _FailingRedisClient:
    def pipeline(self, *, transaction: "bool" = False) -> "_FailingPipeline":
        del transaction
        return _FailingPipeline()

    async def zrangebyscore(
        self,
        _name: "str",
        _minimum: "float | str",
        _maximum: "float | str",
        *,
        start: "int | None" = None,
        num: "int | None" = None,
    ) -> "list[Any]":
        del start, num
        return []

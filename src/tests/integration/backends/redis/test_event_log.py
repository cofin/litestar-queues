"""Redis backend-managed queue event history tests."""

import json
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any, cast

import pytest

pytest.importorskip("redis")

from litestar_queues import EventHistoryConfig, QueueConfig, QueueService, WorkerConfig
from litestar_queues.backends.redis import RedisQueueBackend
from litestar_queues.backends.redis.event_log import RedisQueueEventLog
from litestar_queues.events import QueueEvent, QueueEventsConfig
from litestar_queues.events.query import QueueEventQuery

pytestmark = pytest.mark.anyio


async def test_redis_event_log_records_queries_and_cleans_up(redis_backend: "RedisQueueBackend") -> "None":
    event_log_config = EventHistoryConfig(batch_size=10, flush_interval=60)
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

    records = (await event_log.query_events(QueueEventQuery(task_id=task_id))).items
    limited = (await event_log.query_events(QueueEventQuery(task_name="tasks.redis.history", limit=1))).items
    deleted = await event_log.cleanup_events(before=datetime(2026, 1, 1, 0, 0, 3, tzinfo=timezone.utc))
    remaining = (await event_log.query_events(QueueEventQuery(task_name="tasks.redis.history"))).items
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
    async with QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(history=EventHistoryConfig()),
        ),
        queue_backend=redis_backend,
    ) as service:
        assert service.get_queue_backend().get_event_log(EventHistoryConfig()) is not None


async def test_redis_event_cleanup_always_removes_the_global_index(redis_backend: "RedisQueueBackend") -> "None":
    """Cleanup must not rely on a hash's secondary-index metadata for the global ZREM."""
    event_log = redis_backend.get_event_log(EventHistoryConfig(batch_size=1))
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

    assert await event_log.cleanup_events(before=datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc), limit=1) == 1
    assert await client.zrange(redis_backend._event_log_global_key(), 0, -1) == []


async def test_redis_event_cleanup_continues_in_exact_bounded_batches(redis_backend: "RedisQueueBackend") -> "None":
    """Bounded cleanup deletes oldest events first and reaches a stable no-op."""
    event_log = redis_backend.get_event_log(EventHistoryConfig(batch_size=1))
    assert event_log is not None
    events = [_event(f"redis-bounded-{second}", second=second) for second in range(1, 6)]
    for event in events:
        await event_log.publish_event(event)

    cutoff = datetime(2026, 1, 1, 0, 0, 6, tzinfo=timezone.utc)
    assert await event_log.cleanup_events(before=cutoff, limit=2) == 2
    assert [record.event_id for record in (await event_log.query_events(QueueEventQuery())).items] == [
        event.id for event in events[2:]
    ]
    assert await event_log.cleanup_events(before=cutoff, limit=2) == 2
    assert [record.event_id for record in (await event_log.query_events(QueueEventQuery())).items] == [events[4].id]
    assert await event_log.cleanup_events(before=cutoff, limit=2) == 1
    assert await event_log.cleanup_events(before=cutoff, limit=2) == 0
    assert (await event_log.query_events(QueueEventQuery())).items == []


async def test_redis_event_log_non_strict_flush_preserves_failed_batch(redis_backend: "RedisQueueBackend") -> "None":
    log = redis_backend.get_event_log(EventHistoryConfig(batch_size=1, flush_interval=60, strict=False))
    client = cast("Any", await redis_backend._get_client())
    event = _event("redis-failing-event")
    assert event.task_id is not None
    key = redis_backend._event_log_task_key(event.task_id)
    await client.set(key, "wrong type")
    try:
        await log.publish_event(event)
        assert await client.zrange(redis_backend._event_log_global_key(), 0, -1) == [event.id]
        await client.delete(key)
        await log.flush_events()
        assert await client.zrange(key, 0, -1) == [event.id]
    finally:
        await client.delete(key)


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


async def _assert_sparse_history_commits(backend: Any) -> None:
    import asyncio

    log = backend.get_event_log(EventHistoryConfig(batch_size=20, flush_interval=0.02, strict=True))
    client = await backend._get_client()
    event = _event("sparse-history")
    await log.publish_event(event)
    deadline = asyncio.get_running_loop().time() + 1
    while asyncio.get_running_loop().time() < deadline:
        if await client.zrange(backend._event_log_global_key(), 0, -1):
            break
        await asyncio.sleep(0.01)
    assert await client.zrange(backend._event_log_global_key(), 0, -1) == [event.id]


async def test_redis_sparse_history_commits(redis_backend: RedisQueueBackend) -> None:
    await _assert_sparse_history_commits(redis_backend)


async def _assert_duplicate_preserves_first_record(backend: Any) -> None:
    from litestar_queues.exceptions import QueueConfigurationError

    log = backend.get_event_log(EventHistoryConfig(batch_size=1, strict=True))
    event = _event("immutable-history", detail={"value": "first"})
    await log.publish_event(event)
    client = await backend._get_client()
    original = await client.hgetall(backend._event_log_event_key(event.id))
    try:
        with pytest.raises(QueueConfigurationError, match="Conflicting immutable"):
            await log.publish_event(_event(event.id, task_id="different-task", detail={"value": "second"}))
        assert await client.hgetall(backend._event_log_event_key(event.id)) == original
    finally:
        # A conflicting retained batch makes close fail as well; consume that
        # expected failure here so the backend fixture can close idempotently.
        with suppress(QueueConfigurationError):
            await backend.close()


async def test_redis_duplicate_preserves_first_record(redis_backend: RedisQueueBackend) -> None:
    await _assert_duplicate_preserves_first_record(redis_backend)


async def _assert_partial_history_replay(
    backend: Any, monkeypatch: pytest.MonkeyPatch, pipeline: bool, returned_errors: bool = False
) -> None:
    import litestar_queues.backends.redis.event_log as history_module

    log = backend.get_event_log(EventHistoryConfig(batch_size=1, flush_interval=60, strict=True))
    client = await backend._get_client()
    event = _event("partial-history", detail={"nested": {"stable": True}})
    broken_key = backend._event_log_task_key(event.task_id)
    # Wrong key type makes a real Redis command fail after hash initialization.
    await client.set(broken_key, "controlled wrong index type")
    if not pipeline:
        monkeypatch.setattr(history_module, "_create_pipeline", lambda _client: None)
    elif returned_errors:

        class ReturnedErrorsPipeline:
            def __init__(self) -> None:
                self.pipeline = client.pipeline(transaction=False)

            def __getattr__(self, name: str) -> Any:
                return getattr(self.pipeline, name)

            async def execute(self) -> Any:
                return await self.pipeline.execute(raise_on_error=False)

        monkeypatch.setattr(history_module, "_create_pipeline", lambda _client: ReturnedErrorsPipeline())
    released = []

    async def release() -> None:
        stored = await client.hgetall(backend._event_log_event_key(event.id))
        for key in json.loads(stored["index_keys"]):
            assert event.id in await client.zrange(key, 0, -1)
        released.append(stored)

    try:
        with pytest.raises(Exception, match="WRONGTYPE"):
            await log.publish_event_after_commit(event, release=release, barrier=True)
        original = await client.hgetall(backend._event_log_event_key(event.id))
        assert original and released == []
        await client.delete(broken_key)
        await log.flush_events()
        assert len(released) == 1 and released[0] == original
        await log.publish_event(event)
        assert await client.hgetall(backend._event_log_event_key(event.id)) == original
    finally:
        await client.delete(broken_key)


@pytest.mark.parametrize("pipeline", [False, True])
async def test_redis_partial_history_replay(
    redis_backend: RedisQueueBackend, monkeypatch: pytest.MonkeyPatch, pipeline: bool
) -> None:
    await _assert_partial_history_replay(redis_backend, monkeypatch, pipeline)


async def _assert_concurrent_history_initializers(backend: Any, conflict: bool) -> None:
    import asyncio

    from litestar_queues.exceptions import QueueConfigurationError

    config = EventHistoryConfig(batch_size=1, flush_interval=60, strict=True)
    logs = [backend.get_event_log(config), RedisQueueEventLog(backend=backend, config=config)]
    first = _event("concurrent-history", detail={"winner": 1})
    second = _event(first.id, task_id="other-task", detail={"winner": 2}) if conflict else first
    releases = []

    async def publish(index: int, event: QueueEvent) -> None:
        async def release() -> None:
            releases.append(index)

        await logs[index].publish_event_after_commit(event, release=release, barrier=True)

    try:
        results = await asyncio.gather(publish(0, first), publish(1, second), return_exceptions=True)
        assert sum(isinstance(result, QueueConfigurationError) for result in results) == int(conflict)
        assert all(result is None or isinstance(result, QueueConfigurationError) for result in results)
        assert len(releases) == (1 if conflict else 2)
        client = await backend._get_client()
        stored = await client.hgetall(backend._event_log_event_key(first.id))
        assert json.loads(stored["detail"]) == [first, second][releases[0]].payload
        assert await client.zrange(backend._event_log_global_key(), 0, -1) == [first.id]
        if conflict:
            loser = [first, second][1 - releases[0]]
            assert await client.zrange(backend._event_log_task_key(loser.task_id), 0, -1) == []
    finally:
        for log in logs:
            with suppress(QueueConfigurationError):
                await log.aclose()


@pytest.mark.parametrize("conflict", [False, True])
async def test_redis_concurrent_history_initializers(redis_backend: RedisQueueBackend, conflict: bool) -> None:
    await _assert_concurrent_history_initializers(redis_backend, conflict)


async def _assert_history_failed_close_reopens(backend: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from litestar_queues.exceptions import QueueConfigurationError

    config = EventHistoryConfig(batch_size=20, flush_interval=60, strict=True)
    old = backend.get_event_log(config)
    client = await backend._get_client()
    event = _event("failed-close-history")
    await old.publish_event(event)
    real_write = RedisQueueEventLog._write_history_batch

    async def fail_write(self: RedisQueueEventLog, records: Any) -> None:
        message = "controlled history close failure"
        raise ConnectionError(message)

    monkeypatch.setattr(RedisQueueEventLog, "_write_history_batch", fail_write)
    # Existing bound callback belongs to the old instance; replace its retained writer.
    monkeypatch.setattr(old._buffer, "_write_batch", lambda records: fail_write(old, records))
    with pytest.raises(ConnectionError, match="controlled history close failure"):
        await backend.close()
    assert backend._client is None
    monkeypatch.setattr(RedisQueueEventLog, "_write_history_batch", real_write)
    await backend.open()
    fresh = backend.get_event_log(config)
    assert fresh is not old and await backend._get_client() is not client
    with pytest.raises(QueueConfigurationError, match="closing or closed"):
        await old.publish_event(_event("closed-direct"))
    await fresh.publish_event(_event("fresh-history"))
    await fresh.flush_events()
    assert (await fresh.query_events()).items[0].event_id == "fresh-history"


async def test_redis_history_failed_close_reopens(
    redis_backend: RedisQueueBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _assert_history_failed_close_reopens(redis_backend, monkeypatch)


async def test_redis_returned_pipeline_error_retains_history(
    redis_backend: RedisQueueBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _assert_partial_history_replay(redis_backend, monkeypatch, True, returned_errors=True)


async def _assert_conflicting_replay_repairs_stored_indices(backend: Any) -> None:
    from litestar_queues.exceptions import QueueConfigurationError

    log = backend.get_event_log(EventHistoryConfig(batch_size=1, strict=True))
    event = _event("repair-conflict", detail={"winner": "first"})
    await log.publish_event(event)
    client = await backend._get_client()
    original = await client.hgetall(backend._event_log_event_key(event.id))
    for key in json.loads(original["index_keys"]):
        await client.zrem(key, event.id)
    try:
        with pytest.raises(QueueConfigurationError, match="Conflicting immutable"):
            await log.publish_event(_event(event.id, task_id="losing-task", detail={"winner": "second"}))
        assert await client.hgetall(backend._event_log_event_key(event.id)) == original
        for key in json.loads(original["index_keys"]):
            assert await client.zrange(key, 0, -1) == [event.id]
        assert await client.zrange(backend._event_log_task_key("losing-task"), 0, -1) == []
    finally:
        with suppress(QueueConfigurationError):
            await backend.close()


async def test_redis_conflicting_replay_repairs_stored_indices(redis_backend: RedisQueueBackend) -> None:
    await _assert_conflicting_replay_repairs_stored_indices(redis_backend)


async def _assert_close_continues_after_later_cancellation(backend: Any) -> None:
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    waiting = asyncio.Event()
    closed = []

    async def unsubscribe(_channel: str) -> None:
        waiting.set()
        await asyncio.Event().wait()

    async def close_pubsub() -> None:
        closed.append("pubsub")

    backend._event_log = SimpleNamespace(aclose=AsyncMock(side_effect=RuntimeError("history failed")))
    backend._pubsub = SimpleNamespace(unsubscribe=unsubscribe, aclose=close_pubsub)
    backend._control_pubsub = SimpleNamespace(unsubscribe=AsyncMock(), aclose=AsyncMock())
    control = backend._control_pubsub
    closing = asyncio.create_task(backend.close())
    try:
        await asyncio.wait_for(waiting.wait(), timeout=1)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(closing, timeout=1)
        assert closed == ["pubsub"]
        control.aclose.assert_awaited_once()
        assert backend._event_log is backend._pubsub is backend._control_pubsub is backend._client is None
        await backend.close()
    finally:
        if not closing.done():
            closing.cancel()
        await asyncio.gather(closing, return_exceptions=True)


async def test_redis_close_continues_after_later_cancellation(redis_backend: RedisQueueBackend) -> None:
    await _assert_close_continues_after_later_cancellation(redis_backend)


async def test_close_preserves_caller_cancellation_while_completion_reader_stops(
    redis_backend: RedisQueueBackend,
) -> None:
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    running = asyncio.Event()
    stopping = asyncio.Event()
    finish = asyncio.Event()

    async def reader() -> None:
        running.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopping.set()
            await finish.wait()

    backend = cast("Any", redis_backend)
    reader_task = asyncio.create_task(reader())
    await running.wait()
    subscriber = SimpleNamespace(unsubscribe=AsyncMock(), aclose=AsyncMock())
    backend._completion_reader_task = reader_task
    backend._completion_pubsub = subscriber
    closing = asyncio.create_task(backend.close())
    try:
        await asyncio.wait_for(stopping.wait(), timeout=1)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(closing, timeout=1)
        subscriber.aclose.assert_awaited_once()
        assert backend._completion_reader_task is backend._completion_pubsub is backend._client is None
    finally:
        finish.set()
        if not closing.done():
            closing.cancel()
        await asyncio.gather(closing, reader_task, return_exceptions=True)


@pytest.mark.parametrize("control", [False, True])
@pytest.mark.parametrize("cancel", [False, True])
async def test_receive_failure_survives_reset_cleanup(control: bool, cancel: bool) -> None:
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    backend = RedisQueueBackend()
    backend._notifications = True
    original = ConnectionError("original receive failure")
    waiting = asyncio.Event()

    async def unsubscribe(_channel: str) -> None:
        if cancel:
            waiting.set()
            await asyncio.Event().wait()
        message = "secondary unsubscribe failure"
        raise RuntimeError(message)

    pubsub = SimpleNamespace(get_message=AsyncMock(side_effect=original), unsubscribe=unsubscribe, aclose=AsyncMock())
    if control:
        backend._control_pubsub = pubsub
        operation = backend.wait_for_worker_control(worker_id="worker", timeout=1)
    else:
        backend._pubsub = pubsub
        operation = backend.wait_for_wakeups(timeout=1)
    task = asyncio.create_task(operation)
    try:
        if cancel:
            await asyncio.wait_for(waiting.wait(), timeout=1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(ConnectionError) as raised:
                await task
            assert raised.value is original
        pubsub.aclose.assert_awaited_once()
        assert backend._pubsub is backend._control_pubsub is None
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await backend.close()

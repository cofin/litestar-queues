import asyncio
import builtins
import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID, uuid4

import pytest

from litestar_queues import EnqueueSpec, HeartbeatTouch
from litestar_queues.backends.redis import RedisBackendConfig, RedisQueueBackend
from litestar_queues.models import QueuedTaskRecord

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

pytestmark = pytest.mark.anyio


async def test_redis_records_from_ids_batches_hgetall_with_pipeline() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client)))
    records = [QueuedTaskRecord(task_name=f"tasks.batch.{index}") for index in range(3)]
    for record in records:
        client.hashes[backend._task_key(record.id)] = backend._record_to_mapping(record)

    fetched = await backend._records_from_ids([str(record.id) for record in records])

    assert [record.id for record in fetched] == [record.id for record in records]
    assert client.pipeline_calls == 1
    assert client.pipeline_execute_calls == 1
    assert client.hgetall_calls == 0


async def test_redis_statistics_use_status_indexes_without_task_scan() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client)))
    client.sets[backend._status_key("pending")] = {"pending-1", "pending-2"}
    client.sets[backend._status_key("failed")] = {"failed-1"}

    statistics = await backend.get_statistics()

    assert statistics.pending == 2
    assert statistics.failed == 1
    assert client.scard_calls == 6
    assert client.smembers_calls == 0


async def test_redis_backend_does_not_advertise_native_batch_claim() -> "None":
    """Redis stays on the correctness fallback: its ZSET orders by due time only.

    A bounded atomic ``claim_many`` needs a ready-by-priority index migration
    (out of scope), so the backend must not advertise ``supports_batch_claim``
    and the worker keeps looping the exclusive ``claim_next`` primitive.
    """
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client)))

    assert backend.capabilities.supports_batch_claim is False


async def test_redis_wait_for_notifications_reuses_pubsub_subscription() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client), notifications=True))

    assert await backend.wait_for_notifications(timeout=0.001) is False
    assert await backend.wait_for_notifications(timeout=0.001) is False
    await backend.close()

    assert client.pubsub_calls == 1
    assert client.pubsubs[0].subscribe_calls == 1
    # Ten empty waits must not create ten receive reads on the subscription.
    assert client.pubsubs[0].get_message_calls == 1
    assert client.pubsubs[0].unsubscribe_calls == 1
    assert client.pubsubs[0].close_calls == 1


async def test_redis_wait_for_notifications_wakes_after_prior_timeout() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client), notifications=True))

    assert await backend.wait_for_notifications(timeout=0.001) is False
    pubsub = client.pubsubs[0]
    pubsub.deliver()

    # The retained receive resumes on the same subscription without re-subscribing.
    assert await backend.wait_for_notifications(timeout=1.0) is True
    assert pubsub.subscribe_calls == 1
    assert pubsub.get_message_calls == 1
    assert backend._pending_read.has_pending is False

    # The consumed message does not linger for the next waiter.
    assert await backend.wait_for_notifications(timeout=0.001) is False
    assert pubsub.get_message_calls == 2
    await backend.close()


async def test_redis_enqueue_many_persists_unkeyed_batch_with_one_pipeline_and_one_notification() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client), notifications=True))

    records = await backend.enqueue_many([
        EnqueueSpec(task_name="tasks.redis.batch", kwargs={"index": 0}),
        EnqueueSpec(task_name="tasks.redis.batch", kwargs={"index": 1}),
        EnqueueSpec(task_name="tasks.redis.batch", kwargs={"index": 2}),
    ])

    assert [record.kwargs["index"] for record in records] == [0, 1, 2]
    assert client.pipeline_calls == 1
    assert client.pipeline_execute_calls == 1
    assert client.publish_calls == 1
    assert json.loads(client.published[0][1]) == {"event": "task_available"}


async def test_redis_enqueue_many_future_batch_does_not_publish_notification() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client), notifications=True))
    later = datetime.now(timezone.utc) + timedelta(minutes=5)

    records = await backend.enqueue_many([EnqueueSpec(task_name="tasks.redis.later", scheduled_at=later)])

    assert records[0].status == "scheduled"
    assert client.publish_calls == 0


async def test_redis_backend_touch_heartbeats_fences_and_merges_metadata() -> "None":
    backend = _RedisHeartbeatBackend()
    running = QueuedTaskRecord(
        task_name="tasks.redis.heartbeat", status="running", retry_count=2, metadata={"existing": "kept"}
    )
    backend.records[running.id] = running
    missing_id = uuid4()

    result = await backend.touch_heartbeats([
        HeartbeatTouch(task_id=running.id, expected_retry_count=3),
        HeartbeatTouch(task_id=running.id, expected_retry_count=2, metadata_patch={"progress_detail": "row 500"}),
        HeartbeatTouch(task_id=missing_id, expected_retry_count=None),
    ])

    assert result.touched_task_ids == {running.id}
    assert result.missed_task_ids == {running.id, missing_id}
    assert result.failed_task_ids == set()
    assert running.heartbeat_at is not None
    assert running.metadata == {"existing": "kept", "progress_detail": "row 500"}
    assert backend.lock_names == [f"task:{running.id}", f"task:{running.id}", f"task:{missing_id}"]
    assert backend.saved_records == [running.id]


async def test_redis_backend_touch_heartbeats_batches_records_with_one_pipeline() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client)))
    first = QueuedTaskRecord(
        task_name="tasks.redis.bulk_heartbeat", status="running", retry_count=2, metadata={"existing": "kept"}
    )
    second = QueuedTaskRecord(task_name="tasks.redis.bulk_heartbeat", status="running", retry_count=0)
    missing_id = uuid4()
    client.hashes[backend._task_key(first.id)] = backend._record_to_mapping(first)
    client.hashes[backend._task_key(second.id)] = backend._record_to_mapping(second)

    result = await backend.touch_heartbeats([
        HeartbeatTouch(task_id=first.id, expected_retry_count=3),
        HeartbeatTouch(task_id=first.id, expected_retry_count=2, metadata_patch={"progress_detail": "row 500"}),
        HeartbeatTouch(task_id=second.id, expected_retry_count=0, metadata_patch={"progress_detail": "row 501"}),
        HeartbeatTouch(task_id=missing_id, expected_retry_count=None),
    ])

    assert result.touched_task_ids == {first.id, second.id}
    assert result.missed_task_ids == {first.id, missing_id}
    assert client.pipeline_calls == 1
    assert client.pipeline_execute_calls == 1
    assert client.hgetall_calls == 0
    stored_first = backend._record_from_mapping(client.hashes[backend._task_key(first.id)])
    stored_second = backend._record_from_mapping(client.hashes[backend._task_key(second.id)])
    assert stored_first.heartbeat_at is not None
    assert stored_first.metadata == {"existing": "kept", "progress_detail": "row 500"}
    assert stored_second.heartbeat_at is not None
    assert stored_second.metadata == {"progress_detail": "row 501"}


async def test_redis_backend_touch_heartbeats_rechecks_status_inside_pipeline_write() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client)))
    record = QueuedTaskRecord(
        task_name="tasks.redis.bulk_heartbeat", status="running", retry_count=0, metadata={"existing": "kept"}
    )
    task_key = backend._task_key(record.id)
    client.hashes[task_key] = backend._record_to_mapping(record)

    def complete_before_write() -> "None":
        terminal = backend._record_from_mapping(client.hashes[task_key])
        terminal.status = "completed"
        terminal.heartbeat_at = None
        terminal.metadata = {"existing": "kept"}
        client.hashes[task_key] = backend._record_to_mapping(terminal)

    client.after_hgetall_execute = complete_before_write
    client.before_touch_eval = complete_before_write

    result = await backend.touch_heartbeats([
        HeartbeatTouch(task_id=record.id, expected_retry_count=0, metadata_patch={"progress_detail": "stale"})
    ])
    stored = backend._record_from_mapping(client.hashes[task_key])

    assert result.touched_task_ids == set()
    assert result.missed_task_ids == {record.id}
    assert stored.status == "completed"
    assert stored.heartbeat_at is None
    assert stored.metadata == {"existing": "kept"}


class _CountingRedisClient:
    def __init__(self) -> "None":
        self.hashes: "dict[str, dict[str, str]]" = {}
        self.sets: "dict[str, builtins.set[str]]" = {}
        self.strings: "dict[str, str]" = {}
        self.after_hgetall_execute: "Callable[[], None] | None" = None
        self.before_touch_eval: "Callable[[], None] | None" = None
        self.hgetall_calls = 0
        self.pipeline_calls = 0
        self.pipeline_execute_calls = 0
        self.publish_calls = 0
        self.published: "list[tuple[str, str]]" = []
        self.pubsub_calls = 0
        self.scard_calls = 0
        self.smembers_calls = 0
        self.pubsubs: "list[_CountingPubSub]" = []

    async def hgetall(self, key: "str") -> "dict[str, str]":
        self.hgetall_calls += 1
        return self.hashes.get(key, {})

    async def publish(self, channel: "str", payload: "str") -> "int":
        self.publish_calls += 1
        self.published.append((channel, payload))
        return 1

    async def get(self, key: "str") -> "str | None":
        return self.strings.get(key)

    async def set(self, key: "str", value: "str", *, nx: "bool" = False, px: "int | None" = None) -> "bool":
        del px
        if nx and key in self.strings:
            return False
        self.strings[key] = value
        return True

    async def delete(self, *keys: "str") -> "int":
        deleted = 0
        for key in keys:
            if key in self.strings:
                deleted += 1
                del self.strings[key]
            if key in self.hashes:
                deleted += 1
                del self.hashes[key]
        return deleted

    async def scard(self, key: "str") -> "int":
        self.scard_calls += 1
        return len(self.sets.get(key, set()))

    async def smembers(self, key: "str") -> "builtins.set[str]":
        self.smembers_calls += 1
        return self.sets.get(key, builtins.set())

    def pipeline(self, *, transaction: "bool" = False) -> "_CountingPipeline":
        del transaction
        self.pipeline_calls += 1
        return _CountingPipeline(self)

    def pubsub(self) -> "_CountingPubSub":
        self.pubsub_calls += 1
        pubsub = _CountingPubSub()
        self.pubsubs.append(pubsub)
        return pubsub

    async def aclose(self) -> "None":
        return None


class _RedisHeartbeatBackend(RedisQueueBackend):
    __slots__ = ("lock_names", "records", "saved_records")

    def __init__(self) -> "None":
        super().__init__(backend_config=RedisBackendConfig(client=cast("Any", _NoPipelineLockClient())))
        self.records: "dict[UUID, QueuedTaskRecord]" = {}
        self.lock_names: "list[str]" = []
        self.saved_records: "list[UUID]" = []

    async def get_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        return self.records.get(task_id)

    async def _save_record(self, record: "QueuedTaskRecord") -> "None":
        self.saved_records.append(record.id)

    @asynccontextmanager
    async def _lock(self, lock_name: "str", *, wait: "bool") -> "AsyncIterator[bool]":
        del wait
        self.lock_names.append(lock_name)
        yield True


class _NoPipelineLockClient:
    def __init__(self) -> "None":
        self.strings: "dict[str, str]" = {}

    async def get(self, key: "str") -> "str | None":
        return self.strings.get(key)

    async def set(self, key: "str", value: "str", *, nx: "bool" = False, px: "int | None" = None) -> "bool":
        del px
        if nx and key in self.strings:
            return False
        self.strings[key] = value
        return True

    async def delete(self, *keys: "str") -> "int":
        deleted = 0
        for key in keys:
            if key in self.strings:
                deleted += 1
                del self.strings[key]
        return deleted


class _CountingPipeline:
    def __init__(self, client: "_CountingRedisClient") -> "None":
        self.client = client
        self.operations: "list[tuple[str, tuple[Any, ...], dict[str, Any]]]" = []

    def hgetall(self, key: "str") -> "_CountingPipeline":
        self.operations.append(("hgetall", (key,), {}))
        return self

    def eval(self, script: "str", numkeys: "int", *keys_and_args: "str") -> "_CountingPipeline":
        self.operations.append(("eval", (script, numkeys, *keys_and_args), {}))
        return self

    def hset(
        self,
        name: "str",
        key: "str | None" = None,
        value: "Any | None" = None,
        *,
        mapping: "dict[str, str] | None" = None,
    ) -> "_CountingPipeline":
        self.operations.append(("hset", (name, key, value), {"mapping": mapping}))
        return self

    def sadd(self, name: "str", *values: "str") -> "_CountingPipeline":
        self.operations.append(("sadd", (name, *values), {}))
        return self

    def srem(self, name: "str", *values: "str") -> "_CountingPipeline":
        self.operations.append(("srem", (name, *values), {}))
        return self

    def zadd(self, name: "str", mapping: "dict[str, float]") -> "_CountingPipeline":
        self.operations.append(("zadd", (name,), {"mapping": mapping}))
        return self

    def zrem(self, name: "str", *values: "str") -> "_CountingPipeline":
        self.operations.append(("zrem", (name, *values), {}))
        return self

    def delete(self, *keys: "str") -> "_CountingPipeline":
        self.operations.append(("delete", keys, {}))
        return self

    def scard(self, key: "str") -> "_CountingPipeline":
        self.operations.append(("scard", (key,), {}))
        return self

    async def execute(self) -> "list[Any]":
        self.client.pipeline_execute_calls += 1
        results: "list[Any]" = []
        operations = list(self.operations)
        self.operations.clear()
        for operation, args, kwargs in operations:
            results.append(self._execute_operation(operation, args, kwargs))
        if operations and all(operation == "hgetall" for operation, _, _ in operations):
            hook = self.client.after_hgetall_execute
            self.client.after_hgetall_execute = None
            if hook is not None:
                hook()
        return results

    def _execute_operation(self, operation: "str", args: "tuple[Any, ...]", kwargs: "dict[str, Any]") -> "Any":
        if operation == "hgetall":
            return self.client.hashes.get(cast("str", args[0]), {})
        if operation == "eval":
            return self._execute_eval(args)
        if operation == "hset":
            return self._execute_hset(args, kwargs)
        if operation == "sadd":
            return self._execute_sadd(args)
        if operation == "srem":
            return self._execute_srem(args)
        if operation == "zadd":
            return len(cast("dict[str, float]", kwargs["mapping"]))
        if operation == "zrem":
            return len(args) - 1
        if operation == "delete":
            return self._execute_delete(args)
        if operation == "scard":
            self.client.scard_calls += 1
            return len(self.client.sets.get(cast("str", args[0]), set()))
        return None

    def _execute_eval(self, args: "tuple[Any, ...]") -> "int":
        hook = self.client.before_touch_eval
        self.client.before_touch_eval = None
        if hook is not None:
            hook()
        key = cast("str", args[2])
        expected_retry_count = cast("str", args[3])
        heartbeat_at = cast("str", args[4])
        metadata_patch = cast("str", args[5])
        mapping = self.client.hashes.get(key)
        if not mapping or mapping.get("status") != "running":
            return 0
        if expected_retry_count and mapping.get("retry_count") != expected_retry_count:
            return 0
        mapping["heartbeat_at"] = heartbeat_at
        if metadata_patch:
            metadata = json.loads(mapping.get("metadata") or "{}")
            metadata.update(json.loads(metadata_patch))
            mapping["metadata"] = json.dumps(metadata, separators=(",", ":"), sort_keys=True)
        return 1

    def _execute_hset(self, args: "tuple[Any, ...]", kwargs: "dict[str, Any]") -> "int":
        name = cast("str", args[0])
        hash_field = cast("str | None", args[1])
        value = args[2]
        mapping = cast("dict[str, str] | None", kwargs["mapping"])
        hash_target = self.client.hashes.setdefault(name, {})
        if mapping is not None:
            hash_target.update(mapping)
            return len(mapping)
        if hash_field is not None:
            hash_target[hash_field] = str(value)
            return 1
        return 0

    def _execute_sadd(self, args: "tuple[Any, ...]") -> "int":
        set_target = self.client.sets.setdefault(cast("str", args[0]), builtins.set())
        before = len(set_target)
        set_target.update(str(value) for value in args[1:])
        return len(set_target) - before

    def _execute_srem(self, args: "tuple[Any, ...]") -> "int":
        set_target = self.client.sets.setdefault(cast("str", args[0]), builtins.set())
        before = len(set_target)
        set_target.difference_update(str(value) for value in args[1:])
        return before - len(set_target)

    def _execute_delete(self, keys: "tuple[Any, ...]") -> "int":
        deleted = 0
        for key in keys:
            key = cast("str", key)
            if key in self.client.hashes:
                deleted += 1
                del self.client.hashes[key]
            if key in self.client.strings:
                deleted += 1
                del self.client.strings[key]
        return deleted


class _CountingPubSub:
    def __init__(self) -> "None":
        self.close_calls = 0
        self.subscribe_calls = 0
        self.unsubscribe_calls = 0
        self.get_message_calls = 0
        self._message: "asyncio.Queue[object]" = asyncio.Queue()

    async def subscribe(self, channel: "str") -> "None":
        del channel
        self.subscribe_calls += 1

    async def unsubscribe(self, channel: "str") -> "None":
        del channel
        self.unsubscribe_calls += 1

    async def get_message(self, *, ignore_subscribe_messages: "bool", timeout: "float | None") -> "object | None":
        del ignore_subscribe_messages
        self.get_message_calls += 1
        # Model the real client: ``timeout=None`` blocks until a message lands.
        if timeout is None:
            return await self._message.get()
        try:
            return await asyncio.wait_for(self._message.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    def deliver(self) -> "None":
        self._message.put_nowait({"type": "message", "data": "task_available"})

    async def aclose(self) -> "None":
        self.close_calls += 1

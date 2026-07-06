from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID, uuid4

import pytest

from litestar_queues import HeartbeatTouch
from litestar_queues.backends.redis import RedisBackendConfig, RedisQueueBackend
from litestar_queues.models import QueuedTaskRecord

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

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


async def test_redis_wait_for_notifications_reuses_pubsub_subscription() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client), notifications=True))

    assert await backend.wait_for_notifications(timeout=0.001) is False
    assert await backend.wait_for_notifications(timeout=0.001) is False
    await backend.close()

    assert client.pubsub_calls == 1
    assert client.pubsubs[0].subscribe_calls == 1
    assert client.pubsubs[0].unsubscribe_calls == 1
    assert client.pubsubs[0].close_calls == 1


async def test_redis_backend_touch_heartbeats_fences_and_merges_metadata() -> "None":
    backend = _RedisHeartbeatBackend()
    running = QueuedTaskRecord(
        task_name="tasks.redis.heartbeat",
        status="running",
        retry_count=2,
        metadata={"existing": "kept"},
    )
    backend.records[running.id] = running
    missing_id = uuid4()

    result = await backend.touch_heartbeats(
        [
            HeartbeatTouch(task_id=running.id, expected_retry_count=3),
            HeartbeatTouch(
                task_id=running.id,
                expected_retry_count=2,
                metadata_patch={"progress_detail": "row 500"},
            ),
            HeartbeatTouch(task_id=missing_id, expected_retry_count=None),
        ]
    )

    assert result.touched_task_ids == {running.id}
    assert result.missed_task_ids == {running.id, missing_id}
    assert result.failed_task_ids == set()
    assert running.heartbeat_at is not None
    assert running.metadata == {"existing": "kept", "progress_detail": "row 500"}
    assert backend.lock_names == [f"task:{running.id}", f"task:{running.id}", f"task:{missing_id}"]
    assert backend.saved_records == [running.id]


class _CountingRedisClient:
    def __init__(self) -> "None":
        self.hashes: "dict[str, dict[str, str]]" = {}
        self.sets: "dict[str, set[str]]" = {}
        self.hgetall_calls = 0
        self.pipeline_calls = 0
        self.pubsub_calls = 0
        self.scard_calls = 0
        self.smembers_calls = 0
        self.pubsubs: "list[_CountingPubSub]" = []

    async def hgetall(self, key: "str") -> "dict[str, str]":
        self.hgetall_calls += 1
        return self.hashes.get(key, {})

    async def scard(self, key: "str") -> "int":
        self.scard_calls += 1
        return len(self.sets.get(key, set()))

    async def smembers(self, key: "str") -> "set[str]":
        self.smembers_calls += 1
        return self.sets.get(key, set())

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
        super().__init__(backend_config=RedisBackendConfig(client=cast("Any", _CountingRedisClient())))
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


class _CountingPipeline:
    def __init__(self, client: "_CountingRedisClient") -> "None":
        self.client = client
        self.operations: "list[tuple[str, str]]" = []

    def hgetall(self, key: "str") -> "_CountingPipeline":
        self.operations.append(("hgetall", key))
        return self

    def scard(self, key: "str") -> "_CountingPipeline":
        self.operations.append(("scard", key))
        return self

    async def execute(self) -> "list[Any]":
        results: "list[Any]" = []
        for operation, key in self.operations:
            if operation == "hgetall":
                results.append(self.client.hashes.get(key, {}))
            elif operation == "scard":
                self.client.scard_calls += 1
                results.append(len(self.client.sets.get(key, set())))
        return results


class _CountingPubSub:
    def __init__(self) -> "None":
        self.close_calls = 0
        self.subscribe_calls = 0
        self.unsubscribe_calls = 0

    async def subscribe(self, channel: "str") -> "None":
        del channel
        self.subscribe_calls += 1

    async def unsubscribe(self, channel: "str") -> "None":
        del channel
        self.unsubscribe_calls += 1

    async def get_message(self, *, ignore_subscribe_messages: "bool", timeout: "float | None") -> "None":
        del ignore_subscribe_messages, timeout
        return None

    async def aclose(self) -> "None":
        self.close_calls += 1

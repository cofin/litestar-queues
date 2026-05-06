from __future__ import annotations

import asyncio
import importlib
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest

from litestar_queues.backends import get_queue_backend_class, list_queue_backends

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = pytest.mark.anyio


class FakePubSub:
    __slots__ = ("_channels", "_client", "_queues")

    def __init__(self, client: "FakeRedisLikeClient") -> None:
        self._client = client
        self._channels: set[str] = set()
        self._queues: dict[str, asyncio.Queue[dict[str, Any]]] = {}

    async def subscribe(self, *channels: str) -> None:
        for channel in channels:
            queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
            self._channels.add(channel)
            self._queues[channel] = queue
            self._client._subscribe(channel, queue)

    async def unsubscribe(self, *channels: str) -> None:
        selected_channels = channels or tuple(self._channels)
        for channel in selected_channels:
            queue = self._queues.pop(channel, None)
            if queue is not None:
                self._client._unsubscribe(channel, queue)
            self._channels.discard(channel)

    async def get_message(self, *, ignore_subscribe_messages: bool = True, timeout: float | None = None) -> Any:
        if not self._queues:
            if timeout:
                await asyncio.sleep(timeout)
            return None
        waiters = [asyncio.create_task(queue.get()) for queue in self._queues.values()]
        try:
            done, _pending = await asyncio.wait(waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
            if not done:
                return None
            return done.pop().result()
        finally:
            for waiter in waiters:
                if not waiter.done():
                    waiter.cancel()

    async def aclose(self) -> None:
        await self.unsubscribe()

    async def close(self) -> None:
        await self.aclose()


class FakeRedisLikeClient:
    __slots__ = ("closed", "hashes", "published", "sets", "strings", "subscribers", "zsets")

    def __init__(self) -> None:
        self.closed = False
        self.hashes: dict[str, dict[str, Any]] = {}
        self.published: list[tuple[str, str]] = []
        self.sets: dict[str, set[str]] = {}
        self.strings: dict[str, str] = {}
        self.subscribers: dict[str, list[asyncio.Queue[dict[str, Any]]]] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    async def set(self, name: str, value: str, *, nx: bool = False, px: int | None = None) -> bool:
        if nx and name in self.strings:
            return False
        self.strings[name] = value
        return True

    async def get(self, name: str) -> str | None:
        return self.strings.get(name)

    async def delete(self, *names: str) -> int:
        deleted = 0
        for name in names:
            deleted += int(self.strings.pop(name, None) is not None)
            deleted += int(self.hashes.pop(name, None) is not None)
            deleted += int(self.sets.pop(name, None) is not None)
            deleted += int(self.zsets.pop(name, None) is not None)
        return deleted

    async def hget(self, name: str, key: str) -> Any:
        return self.hashes.get(name, {}).get(key)

    async def hgetall(self, name: str) -> dict[str, Any]:
        return dict(self.hashes.get(name, {}))

    async def hset(
        self,
        name: str,
        key: str | None = None,
        value: Any | None = None,
        *,
        mapping: dict[str, Any] | None = None,
    ) -> int:
        hash_value = self.hashes.setdefault(name, {})
        before = len(hash_value)
        if mapping is not None:
            hash_value.update(mapping)
        elif key is not None:
            hash_value[key] = value
        return len(hash_value) - before

    async def hdel(self, name: str, *keys: str) -> int:
        hash_value = self.hashes.get(name)
        if hash_value is None:
            return 0
        deleted = 0
        for key in keys:
            deleted += int(hash_value.pop(key, None) is not None)
        return deleted

    async def sadd(self, name: str, *values: str) -> int:
        values_set = self.sets.setdefault(name, set())
        before = len(values_set)
        values_set.update(values)
        return len(values_set) - before

    async def srem(self, name: str, *values: str) -> int:
        values_set = self.sets.get(name)
        if values_set is None:
            return 0
        removed = 0
        for value in values:
            if value in values_set:
                values_set.remove(value)
                removed += 1
        return removed

    async def smembers(self, name: str) -> set[str]:
        return set(self.sets.get(name, set()))

    async def zadd(self, name: str, mapping: dict[str, float]) -> int:
        values = self.zsets.setdefault(name, {})
        before = len(values)
        values.update(mapping)
        return len(values) - before

    async def zrem(self, name: str, *values: str) -> int:
        zset = self.zsets.get(name)
        if zset is None:
            return 0
        removed = 0
        for value in values:
            if value in zset:
                del zset[value]
                removed += 1
        return removed

    async def zrangebyscore(self, name: str, min: float | str, max: float | str) -> list[str]:  # noqa: A002
        lower = float("-inf") if min == "-inf" else float(min)
        upper = float("inf") if max == "+inf" else float(max)
        values = self.zsets.get(name, {})
        due = [value for value, score in values.items() if lower <= score <= upper]
        return sorted(due, key=lambda value: (values[value], value))

    def pubsub(self) -> FakePubSub:
        return FakePubSub(self)

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        payload = {"type": "message", "channel": channel, "data": message}
        subscribers = list(self.subscribers.get(channel, ()))
        for subscriber in subscribers:
            await subscriber.put(payload)
        return len(subscribers)

    async def aclose(self) -> None:
        self.closed = True

    def _subscribe(self, channel: str, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self.subscribers.setdefault(channel, []).append(queue)

    def _unsubscribe(self, channel: str, queue: asyncio.Queue[dict[str, Any]]) -> None:
        subscribers = self.subscribers.get(channel)
        if subscribers is None:
            return
        if queue in subscribers:
            subscribers.remove(queue)


@pytest.fixture(params=("redis", "valkey"))
async def backend(request: pytest.FixtureRequest) -> AsyncIterator[Any]:
    backend_name = str(request.param)
    module = importlib.import_module(f"litestar_queues.backends.{backend_name}")
    class_name = "RedisQueueBackend" if backend_name == "redis" else "ValkeyQueueBackend"
    backend_cls = getattr(module, class_name)
    client = FakeRedisLikeClient()
    instance = backend_cls(
        client=client,
        key_prefix=f"litestar_queues:test:{backend_name}",
        notifications=True,
        notification_channel=f"litestar_queues:test:{backend_name}:notifications",
        lock_timeout=0.1,
    )
    await instance.open()
    try:
        yield instance
    finally:
        await instance.close()


def test_redis_valkey_packages_do_not_import_optional_clients() -> None:
    code = """
import builtins
import sys

original_import = builtins.__import__

def blocked_import(name, *args, **kwargs):
    if name in {"redis", "valkey"} or name.startswith(("redis.", "valkey.")):
        raise ModuleNotFoundError(name)
    return original_import(name, *args, **kwargs)

builtins.__import__ = blocked_import
import litestar_queues
from litestar_queues.backends.redis import RedisBackendConfig, RedisQueueBackend
from litestar_queues.backends.valkey import ValkeyBackendConfig, ValkeyQueueBackend

assert "RedisQueueBackend" in litestar_queues.__all__
assert "ValkeyQueueBackend" in litestar_queues.__all__
assert RedisBackendConfig is not None
assert RedisQueueBackend is not None
assert ValkeyBackendConfig is not None
assert ValkeyQueueBackend is not None
assert "redis" not in sys.modules
assert "valkey" not in sys.modules
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr


def test_redis_valkey_backends_are_registered() -> None:
    from litestar_queues.backends.redis import RedisQueueBackend
    from litestar_queues.backends.valkey import ValkeyQueueBackend

    assert get_queue_backend_class("redis") is RedisQueueBackend
    assert get_queue_backend_class("valkey") is ValkeyQueueBackend
    assert "redis" in list_queue_backends()
    assert "valkey" in list_queue_backends()


async def test_redis_valkey_backend_deduplicates_active_keys_and_replaces_terminal_keys(backend: Any) -> None:
    first = await backend.enqueue("tasks.sync", kwargs={"account_id": "acct-1"}, key="sync:acct-1")
    duplicate = await backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")

    assert duplicate.id == first.id
    assert duplicate.kwargs == {"account_id": "acct-1"}

    await backend.complete_task(first.id, result={"ok": True})
    replacement = await backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")
    keyed = await backend.get_task_by_key("sync:acct-1")

    assert replacement.id != first.id
    assert replacement.kwargs == {"account_id": "acct-2"}
    assert keyed is not None
    assert keyed.id == replacement.id


async def test_redis_valkey_backend_claims_due_tasks_once_by_priority_and_filters_execution(backend: Any) -> None:
    later = datetime.now(UTC) + timedelta(minutes=5)

    low = await backend.enqueue("tasks.low", priority=1, execution_backend="local")
    await backend.enqueue("tasks.later", priority=100, scheduled_at=later, execution_backend="local")
    high = await backend.enqueue("tasks.high", priority=10, execution_backend="cloudrun")

    local_pending = await backend.list_pending(limit=10, execution_backend="local")
    cloud_pending = await backend.list_pending(limit=10, execution_backend="cloudrun")

    assert [record.id for record in local_pending] == [low.id]
    assert [record.id for record in cloud_pending] == [high.id]

    claimed_results = await asyncio.gather(backend.claim_task(high.id), backend.claim_task(high.id))
    claimed = [record for record in claimed_results if record is not None]

    assert len(claimed) == 1
    assert claimed[0].id == high.id
    assert claimed[0].status == "running"
    assert claimed[0].started_at is not None
    assert (await backend.get_task(low.id)).status == "pending"


async def test_redis_valkey_backend_retries_cancels_heartbeats_and_cleans_up(backend: Any) -> None:
    flaky = await backend.enqueue("tasks.flaky", max_retries=1)

    await backend.claim_task(flaky.id)
    retried = await backend.fail_task(flaky.id, "first failure")
    assert retried is not None
    assert retried.status == "pending"
    assert retried.retry_count == 1

    await backend.claim_task(flaky.id)
    failed = await backend.fail_task(flaky.id, "second failure")
    assert failed is not None
    assert failed.status == "failed"
    assert failed.error == "second failure"
    assert failed.completed_at is not None

    cancellable = await backend.enqueue("tasks.cancel")
    assert await backend.cancel_task(cancellable.id) is True
    assert await backend.cancel_task(cancellable.id) is False

    running = await backend.enqueue("tasks.running", execution_backend="cloudrun")
    claimed = await backend.claim_task(running.id)
    assert claimed is not None

    await backend.set_execution_ref(claimed.id, "cloudrun", "jobs/abc-123", execution_profile="batch-small")
    await backend.null_heartbeats([claimed.id])
    running_external = await backend.list_running_external()
    stale_count = await backend.requeue_stale_running(stale_after=timedelta(seconds=0))

    assert [record.id for record in running_external] == [claimed.id]
    assert running_external[0].execution_ref == "jobs/abc-123"
    assert stale_count == 1
    requeued = await backend.get_task(claimed.id)
    assert requeued is not None
    assert requeued.status == "pending"
    assert requeued.retry_count == 1

    completed = await backend.enqueue("tasks.completed")
    await backend.claim_task(completed.id)
    await backend.complete_task(completed.id, result={"ok": True})
    statistics = await backend.get_statistics()
    completed_records = await backend.list_completed_by_task("tasks.completed")
    cleanup_count = await backend.cleanup_terminal(datetime.now(UTC) + timedelta(seconds=1))

    assert statistics.failed == 1
    assert statistics.cancelled == 1
    assert statistics.completed == 1
    assert [record.id for record in completed_records] == [completed.id]
    assert cleanup_count >= 3
    assert await backend.get_task(completed.id) is None


async def test_redis_valkey_backend_pubsub_notifications_wake_waiters(backend: Any) -> None:
    waiter = asyncio.create_task(backend.wait_for_notifications(timeout=1))
    await asyncio.sleep(0)

    record = await backend.enqueue("tasks.notified", queue="critical", execution_backend="local")

    assert await waiter is True
    assert backend.capabilities.supports_notifications is True
    assert backend.capabilities.notifications_durable is False
    assert backend.capabilities.notification_backend in {"redis-pubsub", "valkey-pubsub"}
    assert await backend.wait_for_notifications(timeout=0.01) is False
    assert record.status == "pending"

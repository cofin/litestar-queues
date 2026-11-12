import asyncio
import builtins
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID, uuid4

import pytest

from litestar_queues import HeartbeatTouch, TaskRequest
from litestar_queues.backends.redis import RedisBackendConfig, RedisQueueBackend
from litestar_queues.backends.valkey import ValkeyBackendConfig
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.models import QueuedTaskRecord, TaskListPage, TaskStatus

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

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
    assert statistics.expired == 0
    assert client.scard_calls == 7
    assert client.smembers_calls == 0


async def test_redis_wait_for_wakeups_reuses_pubsub_subscription() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client), worker_wakeups=True))

    assert await backend.wait_for_wakeups(timeout=0.001) is False
    assert await backend.wait_for_wakeups(timeout=0.001) is False
    await backend.close()

    assert client.pubsub_calls == 1
    assert client.pubsubs[0].subscribe_calls == 1
    assert client.pubsubs[0].get_message_calls == 1
    assert client.pubsubs[0].unsubscribe_calls == 1
    assert client.pubsubs[0].close_calls == 1


async def test_redis_wait_for_wakeups_wakes_after_prior_timeout() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client), worker_wakeups=True))

    assert await backend.wait_for_wakeups(timeout=0.001) is False
    pubsub = client.pubsubs[0]
    pubsub.deliver()

    assert await backend.wait_for_wakeups(timeout=1.0) is True
    assert pubsub.subscribe_calls == 1
    assert pubsub.get_message_calls == 1
    assert backend._pending_read.has_pending is False

    assert await backend.wait_for_wakeups(timeout=0.001) is False
    assert pubsub.get_message_calls == 2
    await backend.close()


async def test_redis_notify_worker_control_publishes_to_control_channel() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client), worker_wakeups=True))

    await backend.notify_worker_control("worker-a")

    assert len(client.published) == 1
    channel, payload = client.published[0]
    assert channel == "litestar_queues:worker_control"
    assert json.loads(payload) == {"event": "worker_control", "worker_id": "worker-a"}


async def test_redis_worker_control_wait_retains_one_read_across_timeouts() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client), worker_wakeups=True))

    assert await backend.wait_for_worker_control(worker_id="worker-a", timeout=0.001) is False
    assert await backend.wait_for_worker_control(worker_id="worker-a", timeout=0.001) is False
    control_pubsub = client.pubsubs[0]
    control_pubsub.deliver()
    assert await backend.wait_for_worker_control(worker_id="worker-a", timeout=1.0) is True

    assert control_pubsub.channels == ["litestar_queues:worker_control"]
    assert control_pubsub.subscribe_calls == 1
    assert control_pubsub.get_message_calls == 1

    assert await backend.wait_for_wakeups(timeout=0.001) is False
    assert client.pubsubs[1].channels == ["litestar_queues:worker_wakeups"]

    await backend.close()
    assert control_pubsub.unsubscribe_calls == 1
    assert control_pubsub.close_calls == 1


async def test_redis_worker_control_falls_back_to_polling_without_notifications() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client), worker_wakeups=False))

    await backend.notify_worker_control("worker-a")

    assert await backend.wait_for_worker_control(worker_id="worker-a", timeout=0) is False
    assert client.publish_calls == 0
    assert client.pubsub_calls == 0


async def test_redis_enqueue_many_persists_unkeyed_batch_with_one_pipeline_and_one_notification() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client), worker_wakeups=True))

    records = await backend.enqueue_many([
        TaskRequest(task_name="tasks.redis.batch", kwargs={"index": 0}),
        TaskRequest(task_name="tasks.redis.batch", kwargs={"index": 1}),
        TaskRequest(task_name="tasks.redis.batch", kwargs={"index": 2}),
    ])

    assert [record.kwargs["index"] for record in records] == [0, 1, 2]
    assert client.pipeline_calls == 1
    assert client.pipeline_execute_calls == 1
    assert client.publish_calls == 1
    assert json.loads(client.published[0][1]) == {"event": "task_available"}


async def test_redis_enqueue_many_future_batch_does_not_publish_notification() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client), worker_wakeups=True))
    later = datetime.now(UTC) + timedelta(minutes=5)

    records = await backend.enqueue_many([TaskRequest(task_name="tasks.redis.later", scheduled_at=later)])

    assert records[0].status == "scheduled"
    assert client.publish_calls == 0


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


@pytest.mark.parametrize("config_cls", [RedisBackendConfig, ValkeyBackendConfig])
@pytest.mark.parametrize("invalid_ttl", [0.0, -10.0])
async def test_redis_and_valkey_config_reject_non_positive_terminal_ttl(
    config_cls: "type[RedisBackendConfig]", invalid_ttl: "float"
) -> "None":
    with pytest.raises(QueueConfigurationError, match="terminal_ttl must be greater than 0"):
        config_cls(terminal_ttl=invalid_ttl)


async def test_redis_statistics_prunes_expired_terminal_entries_when_terminal_ttl_enabled() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client), terminal_ttl=600.0))
    client.sets[backend._status_key("completed")] = {"completed-1"}

    statistics = await backend.get_statistics()

    assert statistics.completed == 1
    assert len(client.eval_calls) == 1
    script, numkeys, *args = client.eval_calls[0]
    assert "ZRANGEBYSCORE" in script
    assert numkeys == 0
    assert args[0] == "litestar_queues"


async def test_redis_cleanup_terminal_sweeps_orphaned_ids_after_hash_expiry() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client), terminal_ttl=60.0))
    client.strings[backend._maintenance_index_version_key] = "3"
    orphan_id = str(uuid4())
    client.zsets[backend._maintenance_terminal_key] = {orphan_id: 1000.0}
    client.sets[backend._status_key("completed")] = {orphan_id}
    client.sets[f"{backend._key_prefix}:tasks"] = {orphan_id}
    client.hashes[backend._terminal_meta_key] = {
        orphan_id: json.dumps({
            "status": "completed",
            "queue_index_key": backend._queue_index_key("default"),
            "key": "dedup-1",
        })
    }
    client.sets[backend._queue_index_key("default")] = {orphan_id}
    client.hashes[backend._keys_key] = {"dedup-1": orphan_id}

    cutoff = datetime.now(UTC) + timedelta(seconds=10)
    deleted = await backend.cleanup_terminal(cutoff, limit=10)

    assert deleted == 1
    assert orphan_id not in client.sets.get(backend._status_key("completed"), set())
    assert orphan_id not in client.sets.get(f"{backend._key_prefix}:tasks", set())
    assert orphan_id not in client.sets.get(backend._queue_index_key("default"), set())
    assert orphan_id not in client.zsets.get(backend._maintenance_terminal_key, {})
    assert orphan_id not in client.hashes.get(backend._terminal_meta_key, {})
    assert "dedup-1" not in client.hashes.get(backend._keys_key, {})


async def test_redis_rebuild_maintenance_indexes_populates_terminal_meta() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client), terminal_ttl=300.0))
    now = datetime.now(UTC)
    record = QueuedTaskRecord(task_name="tasks.redis.terminal", status="completed", completed_at=now, key="order-42")
    task_id = str(record.id)
    client.hashes[backend._task_key(record.id)] = backend._record_to_mapping(record)
    client.sets[backend._status_key("completed")] = {task_id}

    indexed = await backend.rebuild_maintenance_indexes()

    assert indexed == 1
    raw_meta = client.hashes.get(backend._terminal_meta_key, {}).get(task_id)
    assert raw_meta is not None
    assert json.loads(raw_meta) == {
        "key": "order-42",
        "queue_index_key": backend._queue_index_key("default"),
        "status": "completed",
    }


def _seed_listing_tasks(
    client: "_CountingRedisClient",
    backend: "RedisQueueBackend",
    records: "Sequence[QueuedTaskRecord]",
    *,
    drop_created_score: "bool" = False,
) -> "None":
    for record in records:
        task_id = str(record.id)
        mapping = backend._record_to_mapping(record)
        if drop_created_score:
            mapping.pop("created_score", None)
        client.hashes[backend._task_key(record.id)] = mapping
        client.sets.setdefault(backend._tasks_key, set()).add(task_id)
        client.sets.setdefault(backend._status_key(record.status), set()).add(task_id)
        client.sets.setdefault(backend._queue_index_key(record.queue), set()).add(task_id)


@pytest.mark.parametrize(
    "query",
    [{"limit": 0}, {"limit": True}, {"offset": -1}, {"offset": False}, {"status": "bogus"}, {"status": b"pending"}],
)
async def test_redis_list_tasks_validates_query_before_opening_client(
    monkeypatch: "pytest.MonkeyPatch", query: "dict[str, Any]"
) -> "None":
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(url="redis://unused:6379/0"))

    async def fail_get_client(_self: "RedisQueueBackend") -> "Any":
        msg = "_get_client must not be called for invalid list_tasks queries"
        raise AssertionError(msg)

    monkeypatch.setattr(RedisQueueBackend, "_get_client", fail_get_client)

    with pytest.raises(QueueConfigurationError):
        await backend.list_tasks(**query)


@pytest.mark.parametrize("empty_status", [(), []])
async def test_redis_list_tasks_empty_status_short_circuits_before_opening_client(
    monkeypatch: "pytest.MonkeyPatch", empty_status: "Sequence[TaskStatus]"
) -> "None":
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(url="redis://unused:6379/0"))

    async def fail_get_client(_self: "RedisQueueBackend") -> "Any":
        msg = "_get_client must not be called for empty status filter"
        raise AssertionError(msg)

    monkeypatch.setattr(RedisQueueBackend, "_get_client", fail_get_client)

    page = await backend.list_tasks(status=empty_status, limit=10, offset=2)

    assert page == TaskListPage(items=[], limit=10, offset=2, total=0)
    assert page.has_more is False


async def test_redis_list_tasks_intersects_sets_orders_desc_and_hydrates_page_only() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client)))
    base = datetime(2026, 1, 1, tzinfo=UTC)
    t1 = QueuedTaskRecord(
        task_name="tasks.email", status="completed", queue="emails", key="dup-k", created_at=base + timedelta(seconds=1)
    )
    t2 = QueuedTaskRecord(
        task_name="tasks.email", status="failed", queue="emails", key="dup-k", created_at=base + timedelta(seconds=2)
    )
    t3 = QueuedTaskRecord(
        task_name="tasks.other", status="completed", queue="emails", key=None, created_at=base + timedelta(seconds=3)
    )
    t4 = QueuedTaskRecord(
        task_name="tasks.email", status="pending", queue="emails", key=None, created_at=base + timedelta(seconds=4)
    )
    t5 = QueuedTaskRecord(
        task_name="tasks.email",
        status="completed",
        queue="reports",
        key="dup-k",
        created_at=base + timedelta(seconds=5),
    )
    _seed_listing_tasks(client, backend, (t1, t2, t3, t4, t5))

    page = await backend.list_tasks(status=("completed", "failed"), queue="emails", limit=2, offset=1)

    assert page.total == 3
    assert [item.id for item in page.items] == [t2.id, t1.id]
    assert page.limit == 2
    assert page.offset == 1
    assert page.has_more is False
    assert len(client.pipeline_hmget_keys) == 3
    assert len(client.pipeline_hgetall_keys) == 2
    assert client.hgetall_calls == 0

    filtered = await backend.list_tasks(task_name="tasks.email", key="dup-k")
    assert [item.id for item in filtered.items] == [t5.id, t2.id, t1.id]
    assert filtered.total == 3


@pytest.mark.parametrize(
    ("status", "queue", "expected_ids", "expected_counts"),
    [
        (None, None, ("t1", "t2", "t3"), (1, 0, 0)),
        (None, "emails", ("t1", "t2"), (1, 0, 0)),
        ("pending", None, ("t1",), (1, 0, 0)),
        (("pending", "pending"), None, ("t1",), (1, 0, 0)),
        (("pending", "pending"), "emails", ("t1",), (0, 1, 0)),
        (("completed", "failed"), None, ("t2", "t3"), (0, 0, 1)),
        (("completed", "failed"), "emails", ("t2",), (0, 2, 0)),
    ],
)
async def test_redis_list_tasks_selects_minimal_index_set_operation(
    status: "TaskStatus | Sequence[TaskStatus] | None",
    queue: "str | None",
    expected_ids: "tuple[str, ...]",
    expected_counts: "tuple[int, int, int]",
) -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client)))
    base = datetime(2026, 1, 1, tzinfo=UTC)
    records = {
        "t1": QueuedTaskRecord(
            id=UUID(int=1),
            task_name="tasks.a",
            status="pending",
            queue="emails",
            created_at=base + timedelta(seconds=1),
        ),
        "t2": QueuedTaskRecord(
            id=UUID(int=2),
            task_name="tasks.a",
            status="completed",
            queue="emails",
            created_at=base + timedelta(seconds=2),
        ),
        "t3": QueuedTaskRecord(
            id=UUID(int=3),
            task_name="tasks.a",
            status="failed",
            queue="reports",
            created_at=base + timedelta(seconds=3),
        ),
    }
    _seed_listing_tasks(client, backend, tuple(records.values()))

    page = await backend.list_tasks(status=status, queue=queue)

    expected_uuids = [records[name].id for name in reversed(expected_ids)]
    assert [item.id for item in page.items] == expected_uuids
    assert page.total == len(expected_uuids)
    assert (client.smembers_calls, client.sinter_calls, client.sunion_calls) == expected_counts


async def test_redis_list_tasks_excludes_expired_terminal_ttl_hashes_without_short_page() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client)))
    base = datetime(2026, 1, 1, tzinfo=UTC)
    live = [
        QueuedTaskRecord(task_name="tasks.done", status="completed", created_at=base + timedelta(seconds=index))
        for index in range(1, 4)
    ]
    _seed_listing_tasks(client, backend, live)
    expired_id = str(uuid4())
    client.sets[backend._status_key("completed")].add(expired_id)
    client.sets[backend._tasks_key].add(expired_id)

    page = await backend.list_tasks(status="completed", limit=2, offset=0)

    assert page.total == 3
    assert [item.id for item in page.items] == [live[2].id, live[1].id]
    assert page.has_more is True
    assert len(client.pipeline_hmget_keys) == 4
    assert len(client.pipeline_hgetall_keys) == 2


async def test_redis_list_tasks_out_of_range_offset_skips_hgetall() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client)))
    base = datetime(2026, 1, 1, tzinfo=UTC)
    _seed_listing_tasks(
        client,
        backend,
        [
            QueuedTaskRecord(task_name="tasks.page", status="pending", created_at=base + timedelta(seconds=index))
            for index in range(3)
        ],
    )

    page = await backend.list_tasks(limit=2, offset=5)

    assert page == TaskListPage(items=[], limit=2, offset=5, total=3)
    assert len(client.pipeline_hmget_keys) == 3
    assert client.pipeline_hgetall_keys == []


async def test_redis_list_tasks_falls_back_to_created_at_and_handles_mid_call_expiry() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client)))
    same_time = datetime(2026, 1, 1, tzinfo=UTC)
    low = QueuedTaskRecord(id=UUID(int=1), task_name="tasks.tie", status="completed", created_at=same_time)
    high = QueuedTaskRecord(id=UUID(int=2), task_name="tasks.tie", status="completed", created_at=same_time)
    newer = QueuedTaskRecord(
        id=UUID(int=3), task_name="tasks.tie", status="completed", created_at=same_time + timedelta(seconds=5)
    )
    _seed_listing_tasks(client, backend, (low, high, newer), drop_created_score=True)

    page = await backend.list_tasks(status="completed")
    assert [item.id for item in page.items] == [newer.id, high.id, low.id]

    def expire_newer_after_hmget() -> "None":
        client.hashes.pop(backend._task_key(newer.id), None)

    client.after_hmget_execute = expire_newer_after_hmget
    raced_page = await backend.list_tasks(status="completed", limit=2, offset=0)

    assert raced_page.total == 3
    assert [item.id for item in raced_page.items] == [high.id]


async def test_redis_list_tasks_batches_hmget_over_500_candidates() -> "None":
    client = _CountingRedisClient()
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client)))
    base = datetime(2026, 1, 1, tzinfo=UTC)
    records = [
        QueuedTaskRecord(task_name="tasks.bulk", status="pending", created_at=base + timedelta(seconds=index))
        for index in range(505)
    ]
    _seed_listing_tasks(client, backend, records)

    page = await backend.list_tasks(status="pending", limit=2, offset=0)

    assert page.total == 505
    assert [item.id for item in page.items] == [records[-1].id, records[-2].id]
    assert page.has_more is True
    assert len(client.pipeline_hmget_keys) == 505
    assert len(client.pipeline_hgetall_keys) == 2
    assert client.pipeline_execute_calls == 3


@pytest.mark.parametrize(
    ("status", "queue", "expected_ids"),
    [
        ("completed", None, ("done-2", "done-1")),
        ("completed", "emails", ("done-1",)),
        (("completed", "failed"), None, ("fail-1", "done-2", "done-1")),
        (("completed", "failed"), "emails", ("fail-1", "done-1")),
        (("completed", "cancelled"), "emails", ("done-1",)),
    ],
)
async def test_redis_list_tasks_handles_resp2_list_and_none_replies(
    status: "TaskStatus | Sequence[TaskStatus]", queue: "str | None", expected_ids: "tuple[str, ...]"
) -> "None":
    client = _CountingRedisClient(resp2_list_replies=True, simulate_missing_hmget_none=True)
    backend = RedisQueueBackend(backend_config=RedisBackendConfig(client=cast("Any", client)))
    base = datetime(2026, 1, 1, tzinfo=UTC)
    records = {
        "done-1": QueuedTaskRecord(
            id=UUID(int=1),
            task_name="tasks.resp2",
            status="completed",
            queue="emails",
            created_at=base + timedelta(seconds=1),
        ),
        "done-2": QueuedTaskRecord(
            id=UUID(int=2),
            task_name="tasks.resp2",
            status="completed",
            queue="reports",
            created_at=base + timedelta(seconds=2),
        ),
        "fail-1": QueuedTaskRecord(
            id=UUID(int=3),
            task_name="tasks.resp2",
            status="failed",
            queue="emails",
            created_at=base + timedelta(seconds=3),
        ),
    }
    _seed_listing_tasks(client, backend, tuple(records.values()))
    ghost_id = str(uuid4())
    for status_name in ("completed", "failed"):
        client.sets[backend._status_key(status_name)].add(ghost_id)
    client.sets[backend._queue_index_key("emails")].add(ghost_id)
    client.sets[backend._tasks_key].add(ghost_id)

    page = await backend.list_tasks(status=status, queue=queue)

    assert [item.id for item in page.items] == [records[name].id for name in expected_ids]
    assert page.total == len(expected_ids)


class _CountingRedisClient:
    def __init__(self, *, resp2_list_replies: "bool" = False, simulate_missing_hmget_none: "bool" = False) -> "None":
        self.resp2_list_replies = resp2_list_replies
        self.simulate_missing_hmget_none = simulate_missing_hmget_none
        self.hashes: "dict[str, dict[str, str]]" = {}
        self.sets: "dict[str, builtins.set[str]]" = {}
        self.strings: "dict[str, str]" = {}
        self.zsets: "dict[str, dict[str, float]]" = {}
        self.after_hmget_execute: "Callable[[], None] | None" = None
        self.after_hgetall_execute: "Callable[[], None] | None" = None
        self.before_touch_eval: "Callable[[], None] | None" = None
        self.eval_calls: "list[tuple[Any, ...]]" = []
        self.hgetall_calls = 0
        self.pipeline_calls = 0
        self.pipeline_execute_calls = 0
        self.pipeline_hgetall_keys: "list[str]" = []
        self.pipeline_hmget_keys: "list[str]" = []
        self.publish_calls = 0
        self.published: "list[tuple[str, str]]" = []
        self.pubsub_calls = 0
        self.scard_calls = 0
        self.sinter_calls = 0
        self.smembers_calls = 0
        self.sunion_calls = 0
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

    async def smembers(self, key: "str") -> "builtins.set[str] | list[str]":
        self.smembers_calls += 1
        members = self.sets.get(key, builtins.set())
        return sorted(members) if self.resp2_list_replies else members

    async def sinter(self, *keys: "Any") -> "builtins.set[str] | list[str]":
        members = self._sinter_sets(keys)
        return sorted(members) if self.resp2_list_replies else members

    async def sunion(self, *keys: "Any") -> "builtins.set[str] | list[str]":
        members = self._sunion_sets(keys)
        return sorted(members) if self.resp2_list_replies else members

    def _sinter_sets(self, keys: "tuple[Any, ...]") -> "builtins.set[str]":
        self.sinter_calls += 1
        target_keys = self._resolve_set_keys(keys)
        if not target_keys:
            return builtins.set()
        result = builtins.set(self.sets.get(target_keys[0], builtins.set()))
        for key in target_keys[1:]:
            result.intersection_update(self.sets.get(key, builtins.set()))
        return result

    def _sunion_sets(self, keys: "tuple[Any, ...]") -> "builtins.set[str]":
        self.sunion_calls += 1
        result: "builtins.set[str]" = builtins.set()
        for key in self._resolve_set_keys(keys):
            result.update(self.sets.get(key, builtins.set()))
        return result

    def _hmget_values(self, name: "str", fields: "Sequence[str]") -> "list[str | None] | None":
        mapping = self.hashes.get(name)
        if mapping is None:
            return None if self.simulate_missing_hmget_none else [None for _ in fields]
        return [mapping.get(field) for field in fields]

    @staticmethod
    def _resolve_set_keys(keys: "tuple[Any, ...]") -> "list[str]":
        resolved: "list[str]" = []
        for item in keys:
            if isinstance(item, str):
                resolved.append(item)
            else:
                resolved.extend(str(entry) for entry in item)
        return resolved

    async def zrangebyscore(
        self,
        name: "str",
        minimum: "float | str",
        maximum: "float | str",
        *,
        start: "int | None" = None,
        num: "int | None" = None,
    ) -> "list[str]":
        del minimum
        zset = self.zsets.get(name, {})
        max_str = str(maximum)
        exclusive = max_str.startswith("(")
        max_val = float("inf") if max_str in {"+inf", "inf"} else float(max_str[1:] if exclusive else max_str)
        members = [
            member
            for member, score in sorted(zset.items(), key=lambda item: (item[1], item[0]))
            if (score < max_val if exclusive else score <= max_val)
        ]
        if start is not None and num is not None:
            return members[start : start + num]
        return members

    async def eval(self, script: "str", numkeys: "int", *keys_and_args: "str") -> "list[Any]":
        self.eval_calls.append((script, numkeys, *keys_and_args))
        keys = keys_and_args[:numkeys]
        args = keys_and_args[numkeys:]
        if "SCARD" in script and len(keys) == 2:
            version = self.strings.get(keys[0])
            if version:
                return [version]
            if not self.sets.get(keys[1]):
                return [args[0]]
            return [""]
        if "return {delete_terminal_entry(ARGV[1], ARGV[2])}" in script:
            return [self._delete_terminal_entry(args[0], args[1])]
        return [0]

    def _delete_terminal_entry(self, prefix: "str", task_id: "str") -> "int":
        task_key = f"{prefix}:task:{task_id}"
        meta_key = f"{prefix}:terminal_meta"
        task_hash = self.hashes.pop(task_key, {})
        meta_raw = self.hashes.get(meta_key, {}).pop(task_id, None)
        meta = json.loads(meta_raw) if meta_raw else {}
        status = task_hash.get("status") or meta.get("status")
        queue_index_key = task_hash.get("queue_index_key") or meta.get("queue_index_key")
        dedup_key = task_hash.get("key") or meta.get("key")
        self.sets.get(f"{prefix}:tasks", set()).discard(task_id)
        if status:
            self.sets.get(f"{prefix}:status:{status}", set()).discard(task_id)
        if queue_index_key:
            self.sets.get(queue_index_key, set()).discard(task_id)
        if dedup_key and self.hashes.get(f"{prefix}:keys", {}).get(dedup_key) == task_id:
            self.hashes[f"{prefix}:keys"].pop(dedup_key, None)
        self.zsets.get(f"{prefix}:maintenance:terminal", {}).pop(task_id, None)
        return 1

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


class _CountingPipeline:
    def __init__(self, client: "_CountingRedisClient") -> "None":
        self.client = client
        self.operations: "list[tuple[str, tuple[Any, ...], dict[str, Any]]]" = []

    def hgetall(self, key: "str") -> "_CountingPipeline":
        self.operations.append(("hgetall", (key,), {}))
        return self

    def hmget(self, name: "str", keys: "str | Sequence[str]", *args: "str") -> "_CountingPipeline":
        fields = [keys, *args] if isinstance(keys, str) else [*keys, *args]
        self.operations.append(("hmget", (name, fields), {}))
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

    def smembers(self, key: "str") -> "_CountingPipeline":
        self.operations.append(("smembers", (key,), {}))
        return self

    def sinter(self, *keys: "Any") -> "_CountingPipeline":
        self.operations.append(("sinter", keys, {}))
        return self

    async def execute(self) -> "list[Any]":
        self.client.pipeline_execute_calls += 1
        results: "list[Any]" = []
        operations = list(self.operations)
        self.operations.clear()
        for operation, args, kwargs in operations:
            results.append(self._execute_operation(operation, args, kwargs))
        if operations and all(operation == "hmget" for operation, _, _ in operations):
            hook = self.client.after_hmget_execute
            self.client.after_hmget_execute = None
            if hook is not None:
                hook()
        if operations and all(operation == "hgetall" for operation, _, _ in operations):
            hook = self.client.after_hgetall_execute
            self.client.after_hgetall_execute = None
            if hook is not None:
                hook()
        return results

    def _execute_operation(self, operation: "str", args: "tuple[Any, ...]", kwargs: "dict[str, Any]") -> "Any":
        if operation == "hgetall":
            key = cast("str", args[0])
            self.client.pipeline_hgetall_keys.append(key)
            return self.client.hashes.get(key, {})
        if operation == "hmget":
            key = cast("str", args[0])
            self.client.pipeline_hmget_keys.append(key)
            return self.client._hmget_values(key, cast("list[str]", args[1]))
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
        if operation == "smembers":
            self.client.smembers_calls += 1
            members = self.client.sets.get(cast("str", args[0]), builtins.set())
            return sorted(members) if self.client.resp2_list_replies else members
        if operation == "sinter":
            members = self.client._sinter_sets(args)
            if self.client.simulate_missing_hmget_none and not members:
                return None
            return sorted(members) if self.client.resp2_list_replies else members
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
        self.channels: "list[str]" = []
        self._message: "asyncio.Queue[object]" = asyncio.Queue()

    async def subscribe(self, channel: "str") -> "None":
        self.channels.append(channel)
        self.subscribe_calls += 1

    async def unsubscribe(self, channel: "str") -> "None":
        del channel
        self.unsubscribe_calls += 1

    async def get_message(self, *, ignore_subscribe_messages: "bool", timeout: "float | None") -> "object | None":
        del ignore_subscribe_messages
        self.get_message_calls += 1
        if timeout is None:
            return await self._message.get()
        try:
            return await asyncio.wait_for(self._message.get(), timeout=timeout)
        except TimeoutError:
            return None

    def deliver(self) -> "None":
        self._message.put_nowait({"type": "message", "data": "task_available"})

    async def aclose(self) -> "None":
        self.close_calls += 1

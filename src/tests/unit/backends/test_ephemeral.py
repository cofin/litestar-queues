"""Contract tests for the server-local ephemeral SQLite queue backend."""

import asyncio
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from litestar_queues import QueueConfig, WorkerConfig
from litestar_queues.backends.ephemeral import NONCE_ENV_VAR, EphemeralQueueBackend
from litestar_queues.backends.ephemeral import backend as ephemeral_backend_module
from litestar_queues.backends.ephemeral.codec import MAGIC, encode_payload, record_from_payload, record_to_payload
from litestar_queues.backends.ephemeral.schema import EphemeralDatabaseError
from litestar_queues.backends.ephemeral.server import EphemeralServerContext
from litestar_queues.events import EventHistoryConfig, QueueEvent
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.models import HeartbeatTouch, QueuedTaskRecord, TaskRequest
from tests.helpers._timing import MutableClock
from tests.integration._expiry_contract import assert_claim_many_preserves_expired_dispatch_reservation

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from litestar_queues.events import QueueEventLog

pytestmark = pytest.mark.anyio


@pytest.fixture
def server_context() -> "Iterator[EphemeralServerContext]":
    with EphemeralServerContext(nonce="test-nonce") as context:
        yield context


@pytest.fixture
async def backend(server_context: "EphemeralServerContext") -> "AsyncIterator[EphemeralQueueBackend]":
    del server_context
    instance = EphemeralQueueBackend(QueueConfig(queue_backend="ephemeral", worker=WorkerConfig(poll_interval=0.01)))
    await instance.open()
    try:
        yield instance
    finally:
        await instance.close()


async def _second_backend() -> "EphemeralQueueBackend":
    instance = EphemeralQueueBackend(QueueConfig(queue_backend="ephemeral"))
    await instance.open()
    return instance


async def test_open_requires_a_server_owned_database() -> "None":
    backend = EphemeralQueueBackend(QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory"))

    with pytest.raises(QueueConfigurationError, match="litestar run"):
        await backend.open()


async def test_open_rejects_a_nonce_from_another_invocation(server_context: "EphemeralServerContext") -> "None":
    del server_context
    os.environ[NONCE_ENV_VAR] = "a-different-invocation"
    backend = EphemeralQueueBackend(QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory"))

    with pytest.raises(QueueConfigurationError, match="does not belong to this server invocation"):
        await backend.open()


async def test_capabilities_report_sqlite_poll(backend: "EphemeralQueueBackend") -> "None":
    capabilities = backend.capabilities

    assert capabilities.supports_worker_wakeups is True
    assert capabilities.wakeup_backend == "sqlite-poll"
    assert capabilities.wakeups_durable is False
    assert capabilities.supports_maintenance is True


async def test_enqueue_and_get_round_trip_every_field(backend: "EphemeralQueueBackend") -> "None":
    scheduled = datetime.now(timezone.utc) + timedelta(hours=1)
    expires = scheduled + timedelta(hours=2)
    record = await backend.enqueue(
        "tasks.render",
        args=(1, "two"),
        kwargs={"three": [4]},
        queue="reports",
        priority=7,
        max_retries=3,
        scheduled_at=scheduled,
        expires_at=expires,
        key="report:1",
        execution_backend="local",
        metadata={"requested_by": "user-1"},
    )

    stored = await backend.get_task(record.id)

    assert stored is not None
    assert stored.id == record.id
    assert stored.task_name == "tasks.render"
    assert stored.args == (1, "two")
    assert stored.kwargs == {"three": [4]}
    assert stored.queue == "reports"
    assert stored.priority == 7
    assert stored.max_retries == 3
    assert stored.status == "scheduled"
    assert stored.scheduled_at == scheduled
    assert stored.expires_at == expires
    assert stored.key == "report:1"
    assert stored.metadata == {"requested_by": "user-1"}


async def test_enqueue_returns_the_active_record_for_a_duplicate_key(backend: "EphemeralQueueBackend") -> "None":
    first = await backend.enqueue("tasks.unique", key="only-one")
    second = await backend.enqueue("tasks.unique", key="only-one")

    assert second.id == first.id

    await backend.claim_task(first.id)
    await backend.complete_task(first.id)
    third = await backend.enqueue("tasks.unique", key="only-one")

    assert third.id != first.id


async def test_enqueue_many_preserves_order_and_reuses_active_keys(backend: "EphemeralQueueBackend") -> "None":
    existing = await backend.enqueue("tasks.batch", key="shared")
    expires = datetime.now(timezone.utc) + timedelta(hours=1)

    records = await backend.enqueue_many([
        TaskRequest(task_name="tasks.batch", key="shared"),
        TaskRequest(task_name="tasks.batch", priority=2, expires_at=expires),
        TaskRequest(task_name="tasks.batch", priority=1),
    ])

    assert [record.task_name for record in records] == ["tasks.batch"] * 3
    assert records[0].id == existing.id
    assert records[1].priority == 2
    assert records[1].expires_at == expires

    stored = await backend.get_task(records[1].id)
    assert stored is not None
    assert stored.expires_at == expires


async def test_claim_orders_by_priority_then_creation(backend: "EphemeralQueueBackend") -> "None":
    low = await backend.enqueue("tasks.low", priority=0)
    high = await backend.enqueue("tasks.high", priority=10)

    claimed = await backend.claim_many(limit=2)

    assert [record.id for record in claimed] == [high.id, low.id]
    assert all(record.status == "running" for record in claimed)
    assert all(record.heartbeat_at is not None for record in claimed)


async def test_claim_task_skips_records_that_are_not_due(backend: "EphemeralQueueBackend") -> "None":
    future = await backend.enqueue("tasks.future", scheduled_at=datetime.now(timezone.utc) + timedelta(hours=1))

    assert await backend.claim_task(future.id) is None


async def test_expired_record_is_hidden_and_fenced_at_claim(backend: "EphemeralQueueBackend") -> "None":
    expired = await backend.enqueue("tasks.expired", expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
    available = await backend.enqueue("tasks.available")

    pending = await backend.list_pending(limit=10)
    claimed = await backend.claim_task(expired.id)
    stored = await backend.get_task(expired.id)

    assert [record.id for record in pending] == [available.id]
    assert claimed is None
    assert stored is not None
    assert stored.status == "expired"
    assert stored.completed_at is not None


async def test_claim_many_transitions_expired_records(backend: "EphemeralQueueBackend") -> "None":
    expired = await backend.enqueue("tasks.expired_batch", expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
    available = await backend.enqueue("tasks.available_batch")

    claimed = await backend.claim_many(limit=10)
    stored = await backend.get_task(expired.id)

    assert [record.id for record in claimed] == [available.id]
    assert stored is not None
    assert stored.status == "expired"


async def test_claim_many_preserves_expired_dispatch_reservation(
    backend: "EphemeralQueueBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    clock = MutableClock()
    monkeypatch.setattr(ephemeral_backend_module, "_utc_now", clock)

    await assert_claim_many_preserves_expired_dispatch_reservation(backend, clock)


async def test_expire_overdue_is_bounded_idempotent_and_terminal(backend: "EphemeralQueueBackend") -> "None":
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    records = [await backend.enqueue(f"tasks.expire_{index}", expires_at=past) for index in range(3)]

    first = await backend.expire_overdue(limit=2)
    second = await backend.expire_overdue(limit=2)
    third = await backend.expire_overdue(limit=2)
    statistics = await backend.get_statistics()
    removed = await backend.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1))

    assert [record.id for record in first] == [records[0].id, records[1].id]
    assert [record.id for record in second] == [records[2].id]
    assert third == []
    assert statistics.expired == 3
    assert removed == 3


async def test_complete_and_fail_respect_retry_fencing(backend: "EphemeralQueueBackend") -> "None":
    record = await backend.enqueue("tasks.fence", max_retries=1)
    claimed = await backend.claim_task(record.id)
    assert claimed is not None

    assert await backend.complete_task(record.id, expected_retry_count=99) is None

    failed = await backend.fail_task(record.id, "boom", expected_retry_count=claimed.retry_count)
    assert failed is not None
    assert failed.status == "pending"
    assert failed.retry_count == 1

    reclaimed = await backend.claim_task(record.id)
    assert reclaimed is not None
    terminal = await backend.fail_task(record.id, "boom again", expected_retry_count=reclaimed.retry_count)
    assert terminal is not None
    assert terminal.status == "failed"


async def test_complete_task_stores_the_result(backend: "EphemeralQueueBackend") -> "None":
    record = await backend.enqueue("tasks.result")
    await backend.claim_task(record.id)

    completed = await backend.complete_task(record.id, result={"rows": 3})

    assert completed is not None
    assert completed.status == "completed"
    assert completed.result == {"rows": 3}
    assert completed.heartbeat_at is None


async def test_cancel_task_and_cancel_tasks_filter_by_status(backend: "EphemeralQueueBackend") -> "None":
    pending = await backend.enqueue("tasks.cancel", queue="q1")
    running = await backend.enqueue("tasks.cancel", queue="q1")
    await backend.claim_task(running.id)

    assert await backend.cancel_task(pending.id) is True
    assert await backend.cancel_task(running.id) is False
    assert await backend.cancel_task(running.id, include_running=True) is True

    await backend.enqueue("tasks.cancel", queue="q2")
    assert await backend.cancel_tasks(queue="q2") == 1


async def test_touch_and_null_heartbeats(backend: "EphemeralQueueBackend") -> "None":
    record = await backend.enqueue("tasks.beat")
    claimed = await backend.claim_task(record.id)
    assert claimed is not None

    result = await backend.touch_heartbeats([
        HeartbeatTouch(task_id=record.id, expected_retry_count=0, metadata_patch={"progress": 50})
    ])
    assert result.touched_task_ids == {record.id}

    stale = await backend.touch_heartbeats([HeartbeatTouch(task_id=record.id, expected_retry_count=9)])
    assert stale.missed_task_ids == {record.id}

    stored = await backend.get_task(record.id)
    assert stored is not None
    assert stored.metadata["progress"] == 50

    await backend.null_heartbeats([record.id])
    cleared = await backend.get_task(record.id)
    assert cleared is not None
    assert cleared.heartbeat_at is None


async def test_requeue_stale_running_requeues_then_fails(backend: "EphemeralQueueBackend") -> "None":
    record = await backend.enqueue("tasks.stale", max_retries=1)
    await backend.claim_task(record.id)
    await backend.null_heartbeats([record.id])

    first = await backend.requeue_stale_running(stale_after=timedelta(seconds=1))
    assert first.requeued == 1

    await backend.claim_task(record.id)
    await backend.null_heartbeats([record.id])
    second = await backend.requeue_stale_running(stale_after=timedelta(seconds=1))

    assert second.failed == 1
    assert second.failed_task_ids == [record.id]


async def test_requeue_stale_running_honours_requeue_on_stale_false(backend: "EphemeralQueueBackend") -> "None":
    record = await backend.enqueue("tasks.no_requeue", max_retries=5, metadata={"requeue_on_stale": False})
    await backend.claim_task(record.id)
    await backend.null_heartbeats([record.id])

    result = await backend.requeue_stale_running(stale_after=timedelta(seconds=1))

    assert result.failed == 1
    assert result.handler_needed == 1
    assert result.handler_needed_task_ids == [record.id]


async def test_execution_reference_and_external_listing(backend: "EphemeralQueueBackend") -> "None":
    record = await backend.enqueue("tasks.remote")
    await backend.claim_task(record.id)

    await backend.set_execution_ref(record.id, "cloudrun", "jobs/1")
    running = await backend.list_running_external()
    assert [item.id for item in running] == [record.id]

    reassigned = await backend.set_execution_backend(record.id, "local")
    assert reassigned is not None
    assert reassigned.execution_ref is None
    assert await backend.list_running_external() == []


async def test_statistics_and_completed_listing(backend: "EphemeralQueueBackend") -> "None":
    done = await backend.enqueue("tasks.stats")
    await backend.claim_task(done.id)
    await backend.complete_task(done.id)
    await backend.enqueue("tasks.stats")

    statistics = await backend.get_statistics()
    assert statistics.completed == 1
    assert statistics.pending == 1

    completed = await backend.list_completed_by_task("tasks.stats")
    assert [record.id for record in completed] == [done.id]


async def test_cleanup_terminal_removes_only_old_terminal_records(backend: "EphemeralQueueBackend") -> "None":
    record = await backend.enqueue("tasks.cleanup")
    await backend.claim_task(record.id)
    await backend.complete_task(record.id)
    await backend.enqueue("tasks.cleanup")

    removed = await backend.cleanup_terminal(datetime.now(timezone.utc) + timedelta(minutes=1))

    assert removed == 1
    assert await backend.get_task(record.id) is None


async def test_maintenance_ownership_is_token_fenced(backend: "EphemeralQueueBackend") -> "None":
    assert await backend.acquire_maintenance("sweep", "token-a", ttl=timedelta(minutes=5)) is True
    assert await backend.acquire_maintenance("sweep", "token-b", ttl=timedelta(minutes=5)) is False
    assert await backend.acquire_maintenance("sweep", "token-a", ttl=timedelta(minutes=5)) is True

    assert await backend.release_maintenance("sweep", "token-b") is False
    assert await backend.release_maintenance("sweep", "token-a") is True
    assert await backend.acquire_maintenance("sweep", "token-b", ttl=timedelta(minutes=5)) is True


async def test_reservations_have_one_winner(backend: "EphemeralQueueBackend") -> "None":
    task_id = uuid4()

    assert await backend.reserve_identity("forever", task_id=task_id, task_name="tasks.once") is None
    existing = await backend.reserve_identity("forever", task_id=uuid4(), task_name="tasks.once")
    assert existing is not None
    assert existing.task_id == task_id

    assert await backend.has_identity("forever") is not None
    assert await backend.reset_identity("forever", expected_task_id=uuid4()) is False
    assert await backend.reset_identity("forever", expected_task_id=task_id) is True
    assert await backend.has_identity("forever") is None


async def test_time_until_next_due_uses_the_earliest_scheduled_record(backend: "EphemeralQueueBackend") -> "None":
    assert await backend.time_until_next_due() is None

    await backend.enqueue("tasks.later", scheduled_at=datetime.now(timezone.utc) + timedelta(seconds=120))
    remaining = await backend.time_until_next_due()

    assert remaining is not None
    assert 0 < remaining <= 120


async def test_clear_removes_every_row(backend: "EphemeralQueueBackend") -> "None":
    record = await backend.enqueue("tasks.clear")
    await backend.reserve_identity("k", task_id=record.id, task_name="tasks.clear")

    await backend.clear()

    assert await backend.get_task(record.id) is None
    assert await backend.has_identity("k") is None


async def test_wait_for_wakeups_returns_false_when_no_work_arrives(backend: "EphemeralQueueBackend") -> "None":
    assert await backend.wait_for_wakeups(timeout=0.05) is False


async def test_wait_for_wakeups_notices_another_instance_commit(backend: "EphemeralQueueBackend") -> "None":
    producer = await _second_backend()
    try:
        waiter = asyncio.ensure_future(backend.wait_for_wakeups(timeout=30.0))
        await producer.enqueue("tasks.cross_process")

        assert await asyncio.wait_for(waiter, timeout=2.0) is True
    finally:
        await producer.close()


async def test_wait_for_wakeups_cancellation_leaves_no_pending_read(backend: "EphemeralQueueBackend") -> "None":
    waiter = asyncio.ensure_future(backend.wait_for_wakeups(timeout=30.0))
    await asyncio.sleep(0.05)
    waiter.cancel()

    with pytest.raises(asyncio.CancelledError):
        await waiter

    await backend.close()


async def test_two_instances_share_one_keyed_winner(backend: "EphemeralQueueBackend") -> "None":
    other = await _second_backend()
    try:
        first, second = await asyncio.gather(
            backend.enqueue("tasks.race", key="one"), other.enqueue("tasks.race", key="one")
        )

        assert first.id == second.id
    finally:
        await other.close()


async def test_simultaneous_claim_many_returns_disjoint_rows(backend: "EphemeralQueueBackend") -> "None":
    other = await _second_backend()
    try:
        for index in range(6):
            await backend.enqueue("tasks.disjoint", priority=index)

        left, right = await asyncio.gather(backend.claim_many(limit=3), other.claim_many(limit=3))
        ids = [record.id for record in (*left, *right)]

        assert len(ids) == len(set(ids)) == 6
    finally:
        await other.close()


async def test_only_one_instance_wins_a_forever_reservation(backend: "EphemeralQueueBackend") -> "None":
    other = await _second_backend()
    try:
        results = await asyncio.gather(
            backend.reserve_identity("shared", task_id=uuid4(), task_name="tasks.once"),
            other.reserve_identity("shared", task_id=uuid4(), task_name="tasks.once"),
        )

        assert sum(1 for result in results if result is None) == 1
    finally:
        await other.close()


async def test_only_one_instance_wins_maintenance(backend: "EphemeralQueueBackend") -> "None":
    other = await _second_backend()
    try:
        results = await asyncio.gather(
            backend.acquire_maintenance("sweep", "token-a", ttl=timedelta(minutes=5)),
            other.acquire_maintenance("sweep", "token-b", ttl=timedelta(minutes=5)),
        )

        assert sum(1 for granted in results if granted) == 1
    finally:
        await other.close()


def test_codec_rejects_non_json_arguments() -> "None":
    with pytest.raises(QueueConfigurationError, match="JSON-serializable"):
        encode_payload({"value": object()})


def test_codec_error_never_leaks_the_rejected_value() -> "None":
    secret = "s3cret-token"

    with pytest.raises(QueueConfigurationError) as caught:
        encode_payload({"value": {secret: object()}})

    assert secret not in str(caught.value)


def test_codec_rejects_a_missing_magic_prefix() -> "None":
    with pytest.raises(EphemeralDatabaseError, match="unreadable"):
        record_from_payload(b'{"schema_version": 1}')


def test_codec_round_trips_a_record_with_every_field() -> "None":
    now = datetime.now(timezone.utc)
    record = QueuedTaskRecord(
        task_name="tasks.round_trip",
        args=(1, None, "x"),
        kwargs={"a": {"b": [1, 2]}},
        queue="q",
        execution_backend="local",
        execution_profile="p",
        execution_ref="ref",
        status="running",
        priority=3,
        max_retries=2,
        retry_count=1,
        scheduled_at=now,
        expires_at=now,
        started_at=now,
        completed_at=now,
        heartbeat_at=now,
        result={"ok": True},
        error="err",
        key="k",
        metadata={"m": 1},
    )

    restored = record_from_payload(record_to_payload(record))

    assert restored == record


def test_payload_uses_the_private_magic_prefix() -> "None":
    assert record_to_payload(QueuedTaskRecord(task_name="tasks.magic")).startswith(MAGIC)


def _event_log(backend: "EphemeralQueueBackend", config: "EventHistoryConfig") -> "QueueEventLog":
    log = backend.get_event_log(config)
    assert log is not None
    return log


def _event(index: "int", *, second: "int | None" = None, task_name: "str" = "tasks.bounded") -> "QueueEvent":
    return QueueEvent(
        type="task.log",
        scope="task",
        task_id=f"task-{index}",
        task_name=task_name,
        sequence=index,
        payload={"index": index},
        occurred_at=datetime(2026, 1, 1, 0, 0, index if second is None else second, tzinfo=timezone.utc),
    )


async def test_event_log_prunes_the_oldest_records_beyond_capacity(backend: "EphemeralQueueBackend") -> "None":
    log = _event_log(backend, EventHistoryConfig(memory_capacity=3))

    for index in range(5):
        await log.publish_event(_event(index))

    records = await log.list_events(task_name="tasks.bounded")

    assert [record.detail["index"] for record in records] == [2, 3, 4]


async def test_event_log_prunes_by_sequence_when_timestamps_tie(backend: "EphemeralQueueBackend") -> "None":
    log = _event_log(backend, EventHistoryConfig(memory_capacity=2))

    for index in range(4):
        await log.publish_event(_event(index, second=0))

    records = await log.list_events(task_name="tasks.bounded")

    assert [record.detail["index"] for record in records] == [2, 3]


async def test_event_log_filters_by_task_id_task_name_and_limit(backend: "EphemeralQueueBackend") -> "None":
    log = _event_log(backend, EventHistoryConfig())
    for index in range(3):
        await log.publish_event(_event(index))
    await log.publish_event(_event(9, task_name="tasks.other"))

    by_id = await log.list_events(task_id="task-1")
    by_name = await log.list_events(task_name="tasks.bounded")
    limited = await log.list_events(task_name="tasks.bounded", limit=2)
    other = await log.list_events(task_name="tasks.other")

    assert [record.detail["index"] for record in by_id] == [1]
    assert [record.detail["index"] for record in by_name] == [0, 1, 2]
    assert [record.detail["index"] for record in limited] == [0, 1]
    assert [record.detail["index"] for record in other] == [9]


async def test_event_log_round_trips_every_history_field(backend: "EphemeralQueueBackend") -> "None":
    log = _event_log(backend, EventHistoryConfig())
    occurred = datetime(2026, 2, 3, 4, 5, 6, tzinfo=timezone.utc)

    await log.publish_event(
        QueueEvent(
            type="task.progress",
            scope="task",
            id="event-1",
            task_id="task-1",
            task_name="tasks.detailed",
            queue="reports",
            worker_id="worker-1",
            execution_backend="local",
            execution_profile="profile",
            sequence=7,
            level="info",
            message="halfway",
            progress_current=2,
            progress_total=4,
            progress_percent=50.0,
            payload={"stage": "load", "duration_ms": 11},
            occurred_at=occurred,
        )
    )
    record = (await log.list_events(task_name="tasks.detailed"))[0]

    assert record.event_id == "event-1"
    assert record.event_type == "task.progress"
    assert record.queue == "reports"
    assert record.worker_id == "worker-1"
    assert record.execution_backend == "local"
    assert record.execution_profile == "profile"
    assert record.stage == "load"
    assert record.level == "info"
    assert record.message == "halfway"
    assert record.detail == {"stage": "load", "duration_ms": 11}
    assert record.progress_current == 2.0
    assert record.progress_total == 4.0
    assert record.progress_percent == 50.0
    assert record.duration_ms == 11.0
    assert record.sequence == 7
    assert record.occurred_at == occurred


async def test_event_log_summarize_stages_returns_no_aggregates(backend: "EphemeralQueueBackend") -> "None":
    log = _event_log(backend, EventHistoryConfig())
    await log.publish_event(_event(0))

    assert await log.summarize_stages() == []
    assert await log.summarize_stages(task_name="tasks.bounded") == []


async def test_event_log_cleanup_before_is_bounded_and_idempotent(backend: "EphemeralQueueBackend") -> "None":
    log = _event_log(backend, EventHistoryConfig(memory_capacity=3))
    for index in range(5):
        await log.publish_event(_event(index))
    cutoff = datetime(2026, 1, 1, 0, 0, 4, tzinfo=timezone.utc)

    first_deleted = await log.cleanup_before(cutoff, limit=1)
    after_first = await log.list_events(task_name="tasks.bounded")
    second_deleted = await log.cleanup_before(cutoff, limit=1)
    after_second = await log.list_events(task_name="tasks.bounded")
    final_deleted = await log.cleanup_before(cutoff, limit=1)
    after_final = await log.list_events(task_name="tasks.bounded")

    assert first_deleted == 1
    assert [record.detail["index"] for record in after_first] == [3, 4]
    assert second_deleted == 1
    assert [record.detail["index"] for record in after_second] == [4]
    assert final_deleted == 0
    assert [record.detail["index"] for record in after_final] == [4]


async def test_backend_clear_clears_the_event_log(backend: "EphemeralQueueBackend") -> "None":
    log = _event_log(backend, EventHistoryConfig())
    await log.publish_event(_event(0))

    await backend.clear()

    assert await log.list_events(task_name="tasks.bounded") == []


async def test_event_log_flush_is_a_no_op(backend: "EphemeralQueueBackend") -> "None":
    log = _event_log(backend, EventHistoryConfig())
    await log.publish_event(_event(0))

    await log.flush_events()

    assert len(await log.list_events(task_name="tasks.bounded")) == 1


async def test_two_instances_publish_events_concurrently(backend: "EphemeralQueueBackend") -> "None":
    config = EventHistoryConfig(memory_capacity=100)
    other = await _second_backend()
    try:
        first = _event_log(backend, config)
        second = _event_log(other, config)

        await asyncio.gather(
            *(first.publish_event(_event(index)) for index in range(0, 20, 2)),
            *(second.publish_event(_event(index)) for index in range(1, 20, 2)),
        )
        records = await first.list_events(task_name="tasks.bounded")

        assert sorted(record.detail["index"] for record in records) == list(range(20))
    finally:
        await other.close()


async def test_enqueue_rejects_metadata_that_is_not_json_serializable(backend: "EphemeralQueueBackend") -> "None":
    with pytest.raises(QueueConfigurationError, match="JSON-serializable"):
        await backend.enqueue("tasks.metadata", metadata={"handle": object()})


async def test_complete_task_rejects_a_result_that_is_not_json_serializable(backend: "EphemeralQueueBackend") -> "None":
    record = await backend.enqueue("tasks.result")
    await backend.claim_task(record.id)

    with pytest.raises(QueueConfigurationError, match="JSON-serializable"):
        await backend.complete_task(record.id, result=object())


async def test_publish_event_rejects_detail_that_is_not_json_serializable(backend: "EphemeralQueueBackend") -> "None":
    log = _event_log(backend, EventHistoryConfig())

    with pytest.raises(QueueConfigurationError, match="JSON-serializable"):
        await log.publish_event(QueueEvent(type="task.log", scope="task", payload={"handle": object()}))


async def test_a_corrupt_payload_raises_the_database_error(backend: "EphemeralQueueBackend") -> "None":
    record = await backend.enqueue("tasks.corrupt")
    _write_raw_payload(backend.path, record.id, b'{"schema_version": 1}')

    with pytest.raises(EphemeralDatabaseError, match="unreadable"):
        await backend.get_task(record.id)


async def test_an_unknown_payload_schema_version_raises_the_database_error(backend: "EphemeralQueueBackend") -> "None":
    record = await backend.enqueue("tasks.version")
    _write_raw_payload(backend.path, record.id, encode_payload({"schema_version": 99}))

    with pytest.raises(EphemeralDatabaseError, match="unreadable"):
        await backend.get_task(record.id)


async def test_a_corrupt_payload_never_leaks_arguments_or_the_database_path(backend: "EphemeralQueueBackend") -> "None":
    credential = "postgres://user:s3cret-token@example.invalid/db"
    record = await backend.enqueue("tasks.secret", args=(credential,), metadata={"token": credential})
    _write_raw_payload(backend.path, record.id, b"not-a-payload")

    with pytest.raises(EphemeralDatabaseError) as caught:
        await backend.get_task(record.id)

    message = str(caught.value)
    assert credential not in message
    assert "s3cret-token" not in message
    assert backend.path not in message


async def test_a_locked_database_raises_the_database_error_after_the_busy_timeout(
    backend: "EphemeralQueueBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    from litestar_queues.backends.ephemeral import schema

    monkeypatch.setattr(schema, "BUSY_TIMEOUT_MS", 25)
    blocker = sqlite3.connect(backend.path, isolation_level=None)
    blocker.execute("BEGIN EXCLUSIVE")
    try:
        with pytest.raises(EphemeralDatabaseError, match="busy"):
            await backend.enqueue("tasks.locked")
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()


def _delete_database(path: "str") -> "None":
    database = Path(path)
    for target in (database, database.with_name(f"{database.name}-wal"), database.with_name(f"{database.name}-shm")):
        target.unlink(missing_ok=True)


async def test_a_deleted_database_raises_the_database_error(backend: "EphemeralQueueBackend") -> "None":
    record = await backend.enqueue("tasks.missing")
    _delete_database(backend.path)

    with pytest.raises(EphemeralDatabaseError, match="no longer available"):
        await backend.enqueue("tasks.missing")

    with pytest.raises(EphemeralDatabaseError, match="no longer available"):
        await backend.get_task(record.id)


async def test_database_errors_never_leak_the_database_path(backend: "EphemeralQueueBackend") -> "None":
    _delete_database(backend.path)

    with pytest.raises(EphemeralDatabaseError) as caught:
        await backend.enqueue("tasks.missing")

    assert backend.path not in str(caught.value)


def _write_raw_payload(path: "str", task_id: "object", payload: "bytes") -> "None":
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        connection.execute("UPDATE queue_task SET payload = ? WHERE id = ?", (payload, str(task_id)))
    finally:
        connection.close()


def test_the_ephemeral_backend_imports_no_optional_database_driver() -> "None":
    """Importing the backend must not load SQLSpec or any SQLite driver package."""
    code = """
import sys

from litestar_queues.backends.ephemeral import EphemeralQueueBackend

forbidden = [name for name in ("sqlspec", "aiosqlite", "psycopg", "asyncpg") if name in sys.modules]
assert not forbidden, forbidden
assert EphemeralQueueBackend.__name__ == "EphemeralQueueBackend"
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, check=False, text=True)

    assert result.returncode == 0, result.stderr


def test_the_ephemeral_sources_never_import_pickle_or_an_optional_database_package() -> "None":
    package = Path(__file__).resolve().parents[3] / "litestar_queues" / "backends" / "ephemeral"
    sources = {module.name: module.read_text(encoding="utf-8") for module in sorted(package.glob("*.py"))}

    offenders = {
        name: line.strip()
        for name, text in sources.items()
        for line in text.splitlines()
        if line.startswith(("import ", "from "))
        and any(banned in line for banned in ("pickle", "sqlspec", "aiosqlite", "sqlalchemy", "advanced_alchemy"))
    }

    assert offenders == {}
    assert set(sources) == {"__init__.py", "backend.py", "codec.py", "event_log.py", "schema.py", "server.py"}

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, cast

import pytest

from litestar_queues import EnqueueSpec, EventLogConfig, HeartbeatTouch, QueueConfig, QueueService, task
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.events import QueueEvent, publish_task_event, publish_task_log, publish_task_progress
from litestar_queues.task import clear_task_registry

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def clean_task_registry() -> "None":
    clear_task_registry()


async def test_memory_backend_deduplicates_active_keys_and_replaces_terminal_keys() -> "None":
    backend = InMemoryQueueBackend()

    first = await backend.enqueue("tasks.sync", kwargs={"account_id": "acct-1"}, key="sync:acct-1")
    duplicate = await backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")

    assert duplicate.id == first.id
    assert duplicate.kwargs == {"account_id": "acct-1"}

    await backend.complete_task(first.id, result={"ok": True})
    replacement = await backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")

    assert replacement.id != first.id
    assert replacement.kwargs == {"account_id": "acct-2"}


async def test_memory_backend_event_log_records_task_history_with_custom_detail() -> "None":
    @task("tasks.memory_event_history")
    async def memory_event_history() -> "str":
        await publish_task_log("loaded", payload={"stage": "load", "duration_ms": 7})
        await publish_task_progress(current=2, total=4, payload={"stage": "load", "duration_ms": 5})
        await publish_task_event("task.event", message="custom", payload={"stage": "store", "duration_ms": 11})
        return "ok"

    event_log_config = EventLogConfig(buffer_size=100, flush_interval=60)
    async with QueueService(QueueConfig(execution_backend="immediate", event_log=event_log_config)) as service:
        result = await service.enqueue(memory_event_history)
        event_log = service.get_queue_backend().get_event_log(event_log_config)
        assert event_log is not None
        records = await event_log.list_events(task_id=str(result.id))
        task_name_records = await event_log.list_events(task_name=memory_event_history.name, limit=2)

    assert [record.event_type for record in records] == [
        "task.started",
        "task.log",
        "task.progress",
        "task.event",
        "task.completed",
    ]
    assert [record.sequence for record in task_name_records] == [1, 2]
    custom = next(record for record in records if record.event_type == "task.event")
    assert custom.message == "custom"
    assert custom.detail == {"stage": "store", "duration_ms": 11}


async def test_memory_backend_event_log_is_bounded_and_cleanup_is_queryable() -> "None":
    event_log_config = EventLogConfig(max_records=3)
    backend = InMemoryQueueBackend(QueueConfig(event_log=event_log_config))
    event_log = backend.get_event_log(event_log_config)
    assert event_log is not None

    for index in range(5):
        await event_log.publish_event(
            QueueEvent(
                type="task.log",
                scope="task",
                task_id=f"task-{index}",
                task_name="tasks.bounded",
                sequence=index,
                payload={"index": index},
                occurred_at=datetime(2026, 1, 1, 0, 0, index, tzinfo=timezone.utc),
            )
        )

    records = await event_log.list_events(task_name="tasks.bounded")
    deleted = await event_log.cleanup_before(datetime(2026, 1, 1, 0, 0, 4, tzinfo=timezone.utc))
    remaining = await event_log.list_events(task_name="tasks.bounded")

    assert [record.detail["index"] for record in records] == [2, 3, 4]
    assert deleted == 2
    assert [record.detail["index"] for record in remaining] == [4]


async def test_memory_backend_clear_clears_event_log() -> "None":
    event_log_config = EventLogConfig()
    backend = InMemoryQueueBackend(QueueConfig(event_log=event_log_config))
    event_log = backend.get_event_log(event_log_config)
    assert event_log is not None

    await event_log.publish_event(QueueEvent(type="task.log", scope="task", task_name="tasks.clear"))
    await backend.clear()

    assert await event_log.list_events(task_name="tasks.clear") == []


async def test_memory_backend_claims_due_tasks_by_priority_and_marks_lifecycle() -> "None":
    backend = InMemoryQueueBackend()
    later = datetime.now(timezone.utc) + timedelta(minutes=5)

    low = await backend.enqueue("tasks.low", priority=1)
    scheduled = await backend.enqueue("tasks.later", priority=100, scheduled_at=later)
    high = await backend.enqueue("tasks.high", priority=10)

    claimed = await backend.claim_next()

    assert claimed is not None
    assert claimed.id == high.id
    assert claimed.status == "running"
    assert claimed.started_at is not None
    assert await backend.get_task(low.id) is low
    assert (await backend.get_task(scheduled.id)) is scheduled


async def test_memory_backend_fail_task_retries_then_fails_permanently() -> "None":
    backend = InMemoryQueueBackend()
    record = await backend.enqueue("tasks.flaky", max_retries=1)

    await backend.claim_task(record.id)
    retried = await backend.fail_task(record.id, "first failure")

    assert retried is record
    assert retried.status == "pending"
    assert retried.retry_count == 1

    await backend.claim_task(record.id)
    failed = await backend.fail_task(record.id, "second failure")

    assert failed is record
    assert failed.status == "failed"
    assert failed.error == "second failure"
    assert failed.completed_at is not None


async def test_memory_backend_only_cancels_running_tasks_when_explicitly_allowed() -> "None":
    backend = InMemoryQueueBackend()
    record = await backend.enqueue("tasks.running_cancel")
    claimed = await backend.claim_task(record.id)
    assert claimed is not None

    assert await backend.cancel_task(record.id) is False
    assert await backend.cancel_task(record.id, include_running=True) is True

    stored = await backend.get_task(record.id)
    assert stored is record
    assert stored.status == "cancelled"
    assert stored.completed_at is not None
    assert stored.heartbeat_at is None


async def test_memory_backend_bulk_cancels_matching_domain_predicate() -> "None":
    backend = InMemoryQueueBackend()
    first = await backend.enqueue(
        "tasks.bulk_cancel", kwargs={"workspace_id": "workspace-1", "other": "kept"}, metadata={"kind": "refresh"}
    )
    running = await backend.enqueue("tasks.bulk_cancel", kwargs={"workspace_id": "workspace-1"})
    wrong_workspace = await backend.enqueue("tasks.bulk_cancel", kwargs={"workspace_id": "workspace-2"})
    wrong_task = await backend.enqueue("tasks.other", kwargs={"workspace_id": "workspace-1"})
    claimed = await backend.claim_task(running.id)
    assert claimed is not None

    cancelled = await backend.cancel_tasks(
        task_name="tasks.bulk_cancel", kwargs={"workspace_id": "workspace-1"}, include_running=True
    )

    assert cancelled == 2
    stored_first = await backend.get_task(first.id)
    stored_running = await backend.get_task(running.id)
    stored_wrong_workspace = await backend.get_task(wrong_workspace.id)
    stored_wrong_task = await backend.get_task(wrong_task.id)

    assert stored_first is not None
    assert stored_running is not None
    assert stored_wrong_workspace is not None
    assert stored_wrong_task is not None
    assert stored_first.status == "cancelled"
    assert stored_running.status == "cancelled"
    assert stored_wrong_workspace.status == "pending"
    assert stored_wrong_task.status == "pending"


async def test_memory_backend_requeues_stale_running_task_when_policy_allows() -> "None":
    backend = InMemoryQueueBackend()
    record = await backend.enqueue("tasks.stale", max_retries=1, metadata={"requeue_on_stale": True})
    claimed = await backend.claim_task(record.id)
    assert claimed is not None
    claimed.heartbeat_at = datetime.now(timezone.utc) - timedelta(minutes=10)

    result = await backend.requeue_stale_running(stale_after=timedelta(seconds=1))
    stored = await backend.get_task(record.id)

    assert result.requeued == 1
    assert result.failed == 0
    assert result.skipped == 0
    assert stored is record
    assert stored.status == "pending"
    assert stored.retry_count == 1
    assert stored.started_at is None
    assert stored.heartbeat_at is None


async def test_memory_backend_demotes_stale_requeue_priority_and_preserves_prior_error() -> "None":
    backend = InMemoryQueueBackend()
    record = await backend.enqueue(
        "tasks.stale_demote", priority=10, max_retries=2, metadata={"requeue_on_stale": True}
    )
    first_claim = await backend.claim_task(record.id)
    assert first_claim is not None
    retried = await backend.fail_task(record.id, "first failure")
    assert retried is record
    second_claim = await backend.claim_task(record.id)
    assert second_claim is not None
    second_claim.heartbeat_at = datetime.now(timezone.utc) - timedelta(minutes=10)

    result = await backend.requeue_stale_running(stale_after=timedelta(seconds=1))
    stored = await backend.get_task(record.id)

    assert result.requeued == 1
    assert stored is record
    assert stored.status == "pending"
    assert stored.retry_count == 2
    assert stored.priority == 4
    assert stored.error == "first failure"


async def test_memory_backend_fails_stale_running_task_when_retries_are_exhausted() -> "None":
    backend = InMemoryQueueBackend()
    record = await backend.enqueue("tasks.stale_exhausted", max_retries=0, metadata={"requeue_on_stale": True})
    claimed = await backend.claim_task(record.id)
    assert claimed is not None
    claimed.heartbeat_at = datetime.now(timezone.utc) - timedelta(minutes=10)

    result = await backend.requeue_stale_running(stale_after=timedelta(seconds=1))
    stored = await backend.get_task(record.id)

    assert result.requeued == 0
    assert result.failed == 1
    assert result.handler_needed == 0
    assert stored is record
    assert stored.status == "failed"
    assert stored.error == "Task heartbeat stale"
    assert stored.completed_at is not None
    assert stored.heartbeat_at is None


async def test_memory_backend_fails_stale_running_task_when_requeue_policy_is_disabled() -> "None":
    backend = InMemoryQueueBackend()
    record = await backend.enqueue("tasks.stale_no_requeue", max_retries=3, metadata={"requeue_on_stale": False})
    claimed = await backend.claim_task(record.id)
    assert claimed is not None
    claimed.heartbeat_at = datetime.now(timezone.utc) - timedelta(minutes=10)

    result = await backend.requeue_stale_running(stale_after=timedelta(seconds=1))
    stored = await backend.get_task(record.id)

    assert result.requeued == 0
    assert result.failed == 1
    assert result.handler_needed == 1
    assert stored is record
    assert stored.status == "failed"
    assert stored.retry_count == 0
    assert stored.error == "Task heartbeat stale"


async def test_memory_backend_heartbeat_is_fenced_by_status_and_retry_count() -> "None":
    backend = InMemoryQueueBackend()
    record = await backend.enqueue("tasks.heartbeat", max_retries=1)

    claimed = await backend.claim_task(record.id)
    assert claimed is not None
    result = await backend.touch_heartbeats([
        HeartbeatTouch(task_id=record.id, expected_retry_count=claimed.retry_count + 1),
        HeartbeatTouch(
            task_id=record.id, expected_retry_count=claimed.retry_count, metadata_patch={"progress_detail": "row 200"}
        ),
    ])
    stored = await backend.get_task(record.id)

    assert result.touched_task_ids == {record.id}
    assert result.missed_task_ids == {record.id}
    assert stored is record
    assert stored.metadata["progress_detail"] == "row 200"


async def test_memory_backend_touch_heartbeats_acquires_lock_once() -> "None":
    backend = InMemoryQueueBackend()
    first = await backend.enqueue("tasks.heartbeat.first")
    second = await backend.enqueue("tasks.heartbeat.second")
    first_claimed = await backend.claim_task(first.id)
    second_claimed = await backend.claim_task(second.id)
    lock = _CountingAsyncLock()
    backend._lock = cast("Any", lock)

    assert first_claimed is not None
    assert second_claimed is not None

    result = await backend.touch_heartbeats([
        HeartbeatTouch(task_id=first.id, expected_retry_count=first_claimed.retry_count),
        HeartbeatTouch(task_id=second.id, expected_retry_count=second_claimed.retry_count),
    ])

    assert lock.entries == 1
    assert result.touched_task_ids == {first.id, second.id}
    assert result.missed_task_ids == set()


async def test_memory_backend_enqueue_many_acquires_lock_once_and_sets_event_once() -> "None":
    backend = InMemoryQueueBackend()
    lock = _CountingAsyncLock()
    event = _CountingEvent()
    backend._lock = cast("Any", lock)
    backend._notification_event = cast("Any", event)
    later = datetime.now(timezone.utc) + timedelta(minutes=5)

    records = await backend.enqueue_many([
        EnqueueSpec(task_name="tasks.bulk.first", key="bulk:first"),
        EnqueueSpec(task_name="tasks.bulk.later", scheduled_at=later),
        EnqueueSpec(task_name="tasks.bulk.second", key="bulk:second"),
    ])

    assert lock.entries == 1
    assert event.sets == 1
    assert [record.task_name for record in records] == ["tasks.bulk.first", "tasks.bulk.later", "tasks.bulk.second"]
    assert [record.id for record in await backend.list_pending(limit=10)] == [records[0].id, records[2].id]


async def test_memory_backend_enqueue_many_future_records_do_not_wake_workers() -> "None":
    backend = InMemoryQueueBackend()
    lock = _CountingAsyncLock()
    event = _CountingEvent()
    backend._lock = cast("Any", lock)
    backend._notification_event = cast("Any", event)
    later = datetime.now(timezone.utc) + timedelta(minutes=5)

    records = await backend.enqueue_many([EnqueueSpec(task_name="tasks.bulk.later", scheduled_at=later)])

    assert lock.entries == 1
    assert event.sets == 0
    assert records[0].status == "scheduled"
    assert await backend.list_pending(limit=10) == []


async def test_memory_backend_complete_and_fail_are_fenced_by_claim_ownership() -> "None":
    backend = InMemoryQueueBackend()
    record = await backend.enqueue("tasks.fenced", max_retries=1)
    claimed = await backend.claim_task(record.id)
    assert claimed is not None
    claimed_retry_count = claimed.retry_count
    claimed.heartbeat_at = datetime.now(timezone.utc) - timedelta(minutes=10)

    stale_result = await backend.requeue_stale_running(stale_after=timedelta(seconds=1))
    assert stale_result.requeued == 1

    assert await backend.complete_task(record.id, result="late", expected_retry_count=claimed_retry_count) is None
    assert await backend.fail_task(record.id, "late failure", expected_retry_count=claimed_retry_count) is None

    stored = await backend.get_task(record.id)
    assert stored is record
    assert stored.status == "pending"
    assert stored.retry_count == 1


async def test_memory_backend_null_heartbeats_is_idempotent() -> "None":
    backend = InMemoryQueueBackend()
    record = await backend.enqueue("tasks.cleanup")
    claimed = await backend.claim_task(record.id)
    assert claimed is not None

    await backend.null_heartbeats([record.id])
    await backend.null_heartbeats([record.id])
    stored = await backend.get_task(record.id)

    assert stored is record
    assert stored.heartbeat_at is None


async def test_memory_backend_null_heartbeats_is_fenced_by_retry_count() -> "None":
    backend = InMemoryQueueBackend()
    record = await backend.enqueue("tasks.cleanup_fenced", max_retries=1)
    first_claim = await backend.claim_task(record.id)
    assert first_claim is not None
    first_retry_count = first_claim.retry_count
    first_claim.heartbeat_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    result = await backend.requeue_stale_running(stale_after=timedelta(seconds=1))
    assert result.requeued == 1
    second_claim = await backend.claim_task(record.id)
    assert second_claim is not None
    second_heartbeat = second_claim.heartbeat_at

    await backend.null_heartbeats([record.id], expected_retry_count=first_retry_count)
    stored = await backend.get_task(record.id)

    assert stored is record
    assert stored.heartbeat_at == second_heartbeat


async def test_memory_backend_repeated_timeouts_reuse_one_notification_read() -> "None":
    backend = InMemoryQueueBackend()
    event = _CountingEvent()
    backend._notification_event = cast("Any", event)

    assert await backend.wait_for_notifications(timeout=0.01) is False
    assert await backend.wait_for_notifications(timeout=0.01) is False
    assert await backend.wait_for_notifications(timeout=0.01) is False

    assert event.waits == 1
    assert backend._pending_read.has_pending is True
    await backend.close()


async def test_memory_backend_notification_after_timeout_is_consumed_once() -> "None":
    backend = InMemoryQueueBackend()
    event = _CountingEvent()
    backend._notification_event = cast("Any", event)

    assert await backend.wait_for_notifications(timeout=0.01) is False
    await backend.enqueue("tasks.after_timeout")

    assert await backend.wait_for_notifications(timeout=0.01) is True
    assert event.waits == 1
    assert backend._pending_read.has_pending is False
    # The consumed wakeup must not linger for a second waiter.
    assert await backend.wait_for_notifications(timeout=0.01) is False
    assert event.waits == 2
    await backend.close()


async def test_memory_backend_close_cancels_retained_notification_read() -> "None":
    backend = InMemoryQueueBackend()

    assert await backend.wait_for_notifications(timeout=0.01) is False
    assert backend._pending_read.has_pending is True

    await backend.close()
    assert backend._pending_read.has_pending is False


async def test_queue_service_memory_fixture_yields_running_service(queue_service_memory: "QueueService") -> "None":
    """The unit-tier `queue_service_memory` fixture yields a running QueueService."""
    assert queue_service_memory.config.queue_backend == "memory"
    assert queue_service_memory.config.execution_backend == "local"


async def test_queue_service_local_enqueue_persists_until_worker_processes_record() -> "None":
    @task("tasks.upper", retries=1)
    async def uppercase(value: "str") -> "str":
        return value.upper()

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        result = await service.enqueue(uppercase, "queue")

        pending_status = result.status
        assert pending_status == "pending"

        record = await service.claim_next()
        assert record is not None
        await service.execute_record(record)
        await result.refresh()

    completed_status = result.status
    assert completed_status == "completed"
    assert result.result == "QUEUE"


async def test_memory_backend_advertises_native_batch_claim() -> "None":
    assert InMemoryQueueBackend().capabilities.supports_batch_claim is True


async def test_memory_backend_claim_many_acquires_lock_once_for_non_empty_batch() -> "None":
    backend = InMemoryQueueBackend()
    for index in range(3):
        await backend.enqueue(f"tasks.batch.{index}")
    lock = _CountingAsyncLock()
    backend._lock = cast("Any", lock)

    claimed = await backend.claim_many(limit=3)

    assert lock.entries == 1
    assert len(claimed) == 3
    assert all(record.status == "running" for record in claimed)


async def test_memory_backend_claim_many_matches_sequential_claim_next() -> "None":
    specs = [
        EnqueueSpec(task_name="tasks.batch.low", priority=1),
        EnqueueSpec(task_name="tasks.batch.high", priority=10),
        EnqueueSpec(task_name="tasks.batch.mid", priority=5, execution_backend="cloudrun"),
        EnqueueSpec(task_name="tasks.batch.mid2", priority=5),
    ]

    sequential_backend = InMemoryQueueBackend()
    await sequential_backend.enqueue_many(specs)
    sequential: "list[Any]" = []
    while (record := await sequential_backend.claim_next()) is not None:
        sequential.append(record)

    native_backend = InMemoryQueueBackend()
    await native_backend.enqueue_many(specs)
    native = await native_backend.claim_many(limit=10)

    compared_fields = (
        "task_name",
        "queue",
        "execution_backend",
        "execution_profile",
        "priority",
        "retry_count",
        "scheduled_at",
        "status",
    )
    assert [{field: getattr(record, field) for field in compared_fields} for record in sequential] == [
        {field: getattr(record, field) for field in compared_fields} for record in native
    ]
    for record in native:
        assert record.status == "running"
        assert record.started_at is not None
        assert record.heartbeat_at == record.started_at


async def test_memory_backend_claim_many_handles_limits_filters_and_scheduled() -> "None":
    backend = InMemoryQueueBackend()
    later = datetime.now(timezone.utc) + timedelta(minutes=5)
    high = await backend.enqueue("tasks.batch.high", priority=10)
    mid = await backend.enqueue("tasks.batch.mid", priority=5)
    low = await backend.enqueue("tasks.batch.low", priority=1)
    await backend.enqueue("tasks.batch.scheduled", priority=100, scheduled_at=later)
    await backend.enqueue("tasks.batch.other_queue", priority=100, queue="reports")

    empty_backend = InMemoryQueueBackend()
    assert await empty_backend.claim_many(limit=5) == []

    assert [record.id for record in await backend.claim_many(limit=1, queue="default")] == [high.id]

    remaining = await backend.claim_many(limit=10, queue="default")
    assert [record.id for record in remaining] == [mid.id, low.id]

    reports = await backend.claim_many(limit=10, queue="reports")
    assert [record.task_name for record in reports] == ["tasks.batch.other_queue"]


async def test_memory_backend_claim_many_never_double_claims_under_contention() -> "None":
    backend = InMemoryQueueBackend()
    task_count = 40
    enqueued = {(await backend.enqueue(f"tasks.batch.contended.{index}")).id for index in range(task_count)}

    first, second = await asyncio.gather(backend.claim_many(limit=task_count), backend.claim_many(limit=task_count))
    claimed_ids = [record.id for record in (*first, *second)]

    assert len(claimed_ids) == task_count
    assert set(claimed_ids) == enqueued
    assert len(set(claimed_ids)) == len(claimed_ids)


async def test_memory_backend_claim_many_rejects_non_positive_limit() -> "None":
    backend = InMemoryQueueBackend()
    await backend.enqueue("tasks.batch.only")

    assert await backend.claim_many(limit=0) == []
    assert await backend.claim_many(limit=-3) == []


class _CountingAsyncLock:
    def __init__(self) -> "None":
        self.entries = 0

    async def __aenter__(self) -> "None":
        self.entries += 1

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> "None":
        return None


class _CountingEvent:
    def __init__(self) -> "None":
        self.clears = 0
        self.sets = 0
        self.waits = 0
        self._event = asyncio.Event()

    def is_set(self) -> "bool":
        return self._event.is_set()

    def set(self) -> "None":
        self.sets += 1
        self._event.set()

    def clear(self) -> "None":
        self.clears += 1
        self._event.clear()

    async def wait(self) -> "bool":
        self.waits += 1
        return await self._event.wait()

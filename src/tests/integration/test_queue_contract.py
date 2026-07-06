import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest

from litestar_queues import (
    EventConfig,
    HeartbeatTouch,
    HeartbeatTouchResult,
    InMemoryQueueEventSink,
    QueueConfig,
    QueueService,
    Task,
    TaskExecutionContext,
    task,
)
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.events import QueueEventPublisher
from litestar_queues.task import clear_task_registry

if TYPE_CHECKING:
    from litestar_queues.backends import BaseQueueBackend
    from litestar_queues.models import QueuedTaskRecord

pytestmark = pytest.mark.anyio


async def test_backend_contract_persists_execution_metadata_and_filters_claims(
    queue_backend: "BaseQueueBackend",
) -> "None":
    local = await queue_backend.enqueue("tasks.local", priority=100, execution_backend="local")
    external = await queue_backend.enqueue(
        "tasks.remote", execution_backend="cloudrun", execution_profile="batch-small"
    )

    pending = await queue_backend.list_pending(limit=10, execution_backend="cloudrun")
    claimed = await queue_backend.claim_next(execution_backend="cloudrun")

    assert [record.id for record in pending] == [external.id]
    assert claimed is not None
    assert claimed.id == external.id
    assert claimed.execution_backend == "cloudrun"
    assert claimed.execution_profile == "batch-small"
    assert claimed.execution_ref is None

    await queue_backend.set_execution_ref(claimed.id, "cloudrun", "jobs/abc-123", execution_profile="batch-small")
    running_external = await queue_backend.list_running_external()
    stored_local = await queue_backend.get_task(local.id)

    assert [record.id for record in running_external] == [external.id]
    assert running_external[0].execution_ref == "jobs/abc-123"
    assert stored_local is not None
    assert stored_local.status == "pending"


async def test_backend_contract_bulk_cancels_matching_domain_predicate(queue_backend: "BaseQueueBackend") -> "None":
    first = await queue_backend.enqueue(
        "tasks.bulk.cancel",
        kwargs={"workspace_id": "workspace-1", "collection_id": "collection-1"},
        metadata={"kind": "refresh"},
    )
    running = await queue_backend.enqueue(
        "tasks.bulk.cancel", kwargs={"workspace_id": "workspace-1"}, metadata={"kind": "refresh"}
    )
    wrong_workspace = await queue_backend.enqueue(
        "tasks.bulk.cancel", kwargs={"workspace_id": "workspace-2"}, metadata={"kind": "refresh"}
    )
    wrong_metadata = await queue_backend.enqueue(
        "tasks.bulk.cancel", kwargs={"workspace_id": "workspace-1"}, metadata={"kind": "other"}
    )
    wrong_task = await queue_backend.enqueue(
        "tasks.bulk.keep", kwargs={"workspace_id": "workspace-1"}, metadata={"kind": "refresh"}
    )
    claimed = await queue_backend.claim_task(running.id)
    assert claimed is not None

    without_running = await queue_backend.cancel_tasks(
        task_name="tasks.bulk.cancel", kwargs={"workspace_id": "workspace-1"}, metadata={"kind": "refresh"}
    )
    with_running = await queue_backend.cancel_tasks(
        task_name="tasks.bulk.cancel",
        kwargs={"workspace_id": "workspace-1"},
        metadata={"kind": "refresh"},
        include_running=True,
    )
    stored_first = await queue_backend.get_task(first.id)
    stored_running = await queue_backend.get_task(running.id)
    stored_wrong_workspace = await queue_backend.get_task(wrong_workspace.id)
    stored_wrong_metadata = await queue_backend.get_task(wrong_metadata.id)
    stored_wrong_task = await queue_backend.get_task(wrong_task.id)

    assert without_running == 1
    assert with_running == 1
    assert stored_first is not None
    assert stored_running is not None
    assert stored_wrong_workspace is not None
    assert stored_wrong_metadata is not None
    assert stored_wrong_task is not None
    assert stored_first.status == "cancelled"
    assert stored_running.status == "cancelled"
    assert stored_wrong_workspace.status == "pending"
    assert stored_wrong_metadata.status == "pending"
    assert stored_wrong_task.status == "pending"


async def test_backend_contract_exposes_operational_queries_and_cleanup(queue_backend: "BaseQueueBackend") -> "None":
    completed = await queue_backend.enqueue("tasks.report")
    claimed_completed = await queue_backend.claim_task(completed.id)
    assert claimed_completed is not None
    await queue_backend.complete_task(claimed_completed.id, result={"ok": True})

    running = await queue_backend.enqueue("tasks.running")
    claimed_running = await queue_backend.claim_task(running.id)
    assert claimed_running is not None
    assert claimed_running.heartbeat_at is not None

    await queue_backend.null_heartbeats([claimed_running.id])
    stored_running = await queue_backend.get_task(claimed_running.id)
    statistics = await queue_backend.get_statistics()
    completed_records = await queue_backend.list_completed_by_task("tasks.report")
    cleanup_count = await queue_backend.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1))

    assert stored_running is not None
    assert stored_running.heartbeat_at is None
    assert statistics.completed == 1
    assert statistics.running == 1
    assert [record.id for record in completed_records] == [completed.id]
    assert cleanup_count == 1
    assert await queue_backend.get_task(completed.id) is None
    assert await queue_backend.get_task(running.id) is not None


async def test_backend_contract_recovers_stale_running_records(queue_backend: "BaseQueueBackend") -> "None":
    requeued = await queue_backend.enqueue(
        "tasks.stale.requeue", priority=10, max_retries=2, metadata={"requeue_on_stale": True}
    )
    failed = await queue_backend.enqueue("tasks.stale.fail", max_retries=0, metadata={"requeue_on_stale": True})
    handler_needed = await queue_backend.enqueue(
        "tasks.stale.handler", max_retries=3, metadata={"requeue_on_stale": False}
    )
    claimed_requeued = await queue_backend.claim_task(requeued.id)
    assert claimed_requeued is not None
    retried_requeued = await queue_backend.fail_task(requeued.id, "first failure")
    assert retried_requeued is not None
    assert retried_requeued.status == "pending"
    claimed_requeued = await queue_backend.claim_task(requeued.id)
    assert claimed_requeued is not None
    for record in (failed, handler_needed):
        claimed = await queue_backend.claim_task(record.id)
        assert claimed is not None

    # A negative window puts the cutoff slightly in the future so every
    # just-claimed task counts as stale regardless of the adapter's timestamp
    # precision or whether the backend stores records by reference.
    result = await queue_backend.requeue_stale_running(stale_after=timedelta(seconds=-2))
    stored_requeued = await queue_backend.get_task(requeued.id)
    stored_failed = await queue_backend.get_task(failed.id)
    stored_handler_needed = await queue_backend.get_task(handler_needed.id)

    assert result.requeued == 1
    assert result.failed == 2
    assert result.handler_needed == 1
    assert stored_requeued is not None
    assert stored_requeued.status == "pending"
    assert stored_requeued.retry_count == 2
    assert stored_requeued.priority == 4
    assert stored_requeued.error == "first failure"
    assert stored_failed is not None
    assert stored_failed.status == "failed"
    assert stored_handler_needed is not None
    assert stored_handler_needed.status == "failed"


async def test_backend_contract_fences_heartbeat_and_terminal_updates(queue_backend: "BaseQueueBackend") -> "None":
    empty_result = await queue_backend.touch_heartbeats([])
    assert empty_result == HeartbeatTouchResult()

    record = await queue_backend.enqueue("tasks.stale.fenced", max_retries=1)
    claimed = await queue_backend.claim_task(record.id)
    assert claimed is not None
    expected_retry_count = claimed.retry_count

    touch_result = await queue_backend.touch_heartbeats(
        [
            HeartbeatTouch(task_id=record.id, expected_retry_count=expected_retry_count + 1),
            HeartbeatTouch(task_id=record.id, expected_retry_count=expected_retry_count),
        ]
    )
    assert touch_result.touched_task_ids == {record.id}
    assert touch_result.missed_task_ids == {record.id}
    assert touch_result.failed_task_ids == set()

    stale_result = await queue_backend.requeue_stale_running(stale_after=timedelta(seconds=-2))
    assert stale_result.requeued == 1

    reclaimed = await queue_backend.claim_task(record.id)
    assert reclaimed is not None
    heartbeat = reclaimed.heartbeat_at
    assert (
        await queue_backend.complete_task(record.id, result="late", expected_retry_count=expected_retry_count) is None
    )
    assert await queue_backend.fail_task(record.id, "late", expected_retry_count=expected_retry_count) is None
    await queue_backend.null_heartbeats([record.id], expected_retry_count=expected_retry_count)
    stored = await queue_backend.get_task(record.id)

    assert stored is not None
    assert stored.status == "running"
    assert stored.retry_count == 1
    assert stored.heartbeat_at == heartbeat


async def test_memory_backend_notifications_wake_waiters() -> "None":
    backend = InMemoryQueueBackend()
    waiter = asyncio.create_task(backend.wait_for_notifications(timeout=1))

    await backend.enqueue("tasks.notified")

    assert await waiter is True
    assert await backend.wait_for_notifications(timeout=0.01) is False


async def test_task_dependency_resolver_merges_kwargs_into_task_call(queue_backend: "BaseQueueBackend") -> "None":
    clear_task_registry()

    async def resolver(
        _task: "Task[..., object]", _record: "QueuedTaskRecord", _context: "TaskExecutionContext"
    ) -> "dict[str, object]":
        return {"injected_service": "from-resolver"}

    @task("contract.resolver.merge")
    async def consume(**kwargs: "object") -> "dict[str, object]":
        return {"injected_service": kwargs["injected_service"]}

    config = QueueConfig(execution_backend="immediate", task_dependency_resolver=resolver)
    service = QueueService(config, queue_backend=queue_backend)

    async with service:
        result = await service.enqueue("contract.resolver.merge")
        await result.refresh()

    assert result.status == "completed"
    assert isinstance(result.result, dict)
    assert result.result["injected_service"] == "from-resolver"


async def test_task_dependency_resolver_cannot_override_sentinels(queue_backend: "BaseQueueBackend") -> "None":
    clear_task_registry()

    async def resolver(
        _task: "Task[..., object]", _record: "QueuedTaskRecord", _context: "TaskExecutionContext"
    ) -> "dict[str, object]":
        return {"_job_id": "hijacked", "_task_context": "hijacked"}

    @task("contract.resolver.sentinels")
    async def sentinels(**kwargs: "object") -> "dict[str, object]":
        task_context = kwargs["_task_context"]
        assert isinstance(task_context, TaskExecutionContext)
        return {
            "job_id": kwargs["_job_id"],
            "ctx_type": type(task_context).__name__,
            "ctx_task_name": task_context.task_name,
        }

    config = QueueConfig(execution_backend="immediate", task_dependency_resolver=resolver)
    service = QueueService(config, queue_backend=queue_backend)

    async with service:
        result = await service.enqueue("contract.resolver.sentinels")
        await result.refresh()

    assert result.status == "completed"
    assert isinstance(result.result, dict)
    assert result.record is not None
    assert str(result.result["job_id"]) == str(result.record.id)
    assert result.result["ctx_type"] == "TaskExecutionContext"
    assert result.result["ctx_task_name"] == "contract.resolver.sentinels"


async def test_task_dependency_resolver_exception_records_failure_and_retries(
    queue_backend: "BaseQueueBackend",
) -> "None":
    clear_task_registry()
    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink)

    attempts = {"count": 0}

    async def resolver(
        _task: "Task[..., object]", _record: "QueuedTaskRecord", _context: "TaskExecutionContext"
    ) -> "dict[str, object]":
        attempts["count"] += 1
        if attempts["count"] == 1:
            msg = "resolver boom"
            raise RuntimeError(msg)
        return {}

    @task("contract.resolver.retry", retries=1)
    async def succeed() -> "str":
        return "ok"

    config = QueueConfig(task_dependency_resolver=resolver, execution_backend="local", event=EventConfig(enabled=True))
    service = QueueService(config, queue_backend=queue_backend, event_publisher=publisher)

    async with service:
        enqueued = await service.enqueue("contract.resolver.retry")
        assert enqueued.record is not None
        first_claim = await queue_backend.claim_task(enqueued.record.id)
        assert first_claim is not None
        first_outcome = await service.execute_record(first_claim)
        assert first_outcome.status == "pending"
        assert first_outcome.retry_count == 1
        assert first_outcome.error == "resolver boom"

        assert enqueued.record is not None
        second_claim = await queue_backend.claim_task(enqueued.record.id)
        assert second_claim is not None
        second_outcome = await service.execute_record(second_claim)

    assert attempts["count"] == 2
    assert second_outcome.status == "completed"

    event_types = [event.type for event in sink.events]
    assert "task.failed" in event_types
    failed_event = next(event for event in sink.events if event.type == "task.failed")
    assert failed_event.message == "resolver boom"
    failed_index = event_types.index("task.failed")
    completed_index = event_types.index("task.completed")
    assert failed_index < completed_index


async def test_task_dependency_resolver_default_is_none_with_no_invocation(queue_backend: "BaseQueueBackend") -> "None":
    clear_task_registry()
    config = QueueConfig(execution_backend="immediate")
    assert config.task_dependency_resolver is None

    @task("contract.resolver.default")
    async def default() -> "str":
        return "ok"

    service = QueueService(config, queue_backend=queue_backend)
    captured: "list[object]" = []

    original = Task.execute_record

    async def spy(self: "Task[..., object]", record: "QueuedTaskRecord", **kwargs: "object") -> "object":
        extra_kwargs = kwargs.get("extra_kwargs")
        task_context = kwargs.get("task_context")
        assert extra_kwargs is None or isinstance(extra_kwargs, dict)
        assert task_context is None or isinstance(task_context, TaskExecutionContext)
        captured.append(extra_kwargs if "extra_kwargs" in kwargs else "MISSING")
        return await original(self, record, task_context=task_context, extra_kwargs=extra_kwargs)

    from unittest.mock import patch

    with patch.object(Task, "execute_record", spy):
        async with service:
            result = await service.enqueue("contract.resolver.default")
            await result.refresh()

    assert result.status == "completed"
    assert captured == [None]


async def test_queue_service_runtime_overrides_preserve_execution_metadata_and_delay() -> "None":
    @task("tasks.external", execution_backend="local")
    async def external_task() -> "str":
        return "ok"

    delayed = external_task.using(
        description="external profile task",
        execution_backend="cloudrun",
        execution_profile="batch-small",
        log_level="debug",
        quiet_success=True,
        run_after=timedelta(minutes=5),
    )

    async with QueueService(QueueConfig(execution_backend="immediate")) as service:
        result = await service.enqueue(delayed)

    assert result.status == "scheduled"
    assert result.record is not None
    assert result.record.execution_backend == "cloudrun"
    assert result.record.execution_profile == "batch-small"
    assert result.record.metadata["description"] == "external profile task"
    assert result.record.metadata["log_level"] == "debug"
    assert result.record.metadata["quiet_success"] is True
    assert result.record.scheduled_at is not None
    assert result.record.scheduled_at > datetime.now(timezone.utc) + timedelta(minutes=4)

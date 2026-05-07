import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from litestar_queues import (
    InMemoryQueueEventSink,
    QueueConfig,
    QueueEventConfig,
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


@pytest.fixture(params=("memory", "sqlspec"))
async def queue_backend(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator["BaseQueueBackend"]:
    if request.param == "memory":
        yield InMemoryQueueBackend()
        return

    pytest.importorskip("aiosqlite")
    pytest.importorskip("sqlspec")
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    backend = SQLSpecQueueBackend(
        sqlspec_config=AiosqliteConfig(connection_config={"database": str(tmp_path / "queue.db")})
    )
    await backend.open()
    try:
        yield backend
    finally:
        await backend.close()


async def test_backend_contract_persists_execution_metadata_and_filters_claims(
    queue_backend: "BaseQueueBackend",
) -> None:
    local = await queue_backend.enqueue("tasks.local", priority=100, execution_backend="local")
    external = await queue_backend.enqueue(
        "tasks.remote",
        execution_backend="cloudrun",
        execution_profile="batch-small",
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


async def test_backend_contract_exposes_operational_queries_and_cleanup(
    queue_backend: "BaseQueueBackend",
) -> None:
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
    cleanup_count = await queue_backend.cleanup_terminal(datetime.now(UTC) + timedelta(seconds=1))

    assert stored_running is not None
    assert stored_running.heartbeat_at is None
    assert statistics.completed == 1
    assert statistics.running == 1
    assert [record.id for record in completed_records] == [completed.id]
    assert cleanup_count == 1
    assert await queue_backend.get_task(completed.id) is None
    assert await queue_backend.get_task(running.id) is not None


async def test_memory_backend_notifications_wake_waiters() -> None:
    backend = InMemoryQueueBackend()
    waiter = asyncio.create_task(backend.wait_for_notifications(timeout=1))

    await backend.enqueue("tasks.notified")

    assert await waiter is True
    assert await backend.wait_for_notifications(timeout=0.01) is False


async def test_task_dependency_resolver_merges_kwargs_into_task_call(
    queue_backend: "BaseQueueBackend",
) -> None:
    clear_task_registry()

    async def resolver(
        _task: "Task[Any, Any]",
        _record: "QueuedTaskRecord",
        _context: "TaskExecutionContext",
    ) -> dict[str, Any]:
        return {"injected_service": "from-resolver"}

    @task("contract.resolver.merge")
    async def consume(**kwargs: Any) -> dict[str, Any]:
        return {"injected_service": kwargs["injected_service"]}

    config = QueueConfig(task_dependency_resolver=resolver)
    service = QueueService(config, queue_backend=queue_backend)

    async with service:
        result = await service.enqueue("contract.resolver.merge")
        await result.refresh()

    assert result.status == "completed"
    assert isinstance(result.result, dict)
    assert result.result["injected_service"] == "from-resolver"


async def test_task_dependency_resolver_cannot_override_sentinels(
    queue_backend: "BaseQueueBackend",
) -> None:
    clear_task_registry()

    async def resolver(
        _task: "Task[Any, Any]",
        _record: "QueuedTaskRecord",
        _context: "TaskExecutionContext",
    ) -> dict[str, Any]:
        return {"_job_id": "hijacked", "_task_context": "hijacked"}

    @task("contract.resolver.sentinels")
    async def sentinels(**kwargs: Any) -> dict[str, Any]:
        return {
            "job_id": kwargs["_job_id"],
            "ctx_type": type(kwargs["_task_context"]).__name__,
            "ctx_task_name": kwargs["_task_context"].task_name,
        }

    config = QueueConfig(task_dependency_resolver=resolver)
    service = QueueService(config, queue_backend=queue_backend)

    async with service:
        result = await service.enqueue("contract.resolver.sentinels")
        await result.refresh()

    assert result.status == "completed"
    assert isinstance(result.result, dict)
    assert str(result.result["job_id"]) == str(result.record.id)
    assert result.result["ctx_type"] == "TaskExecutionContext"
    assert result.result["ctx_task_name"] == "contract.resolver.sentinels"


async def test_task_dependency_resolver_exception_records_failure_and_retries(
    queue_backend: "BaseQueueBackend",
) -> None:
    clear_task_registry()
    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink)

    attempts = {"count": 0}

    async def resolver(
        _task: "Task[Any, Any]",
        _record: "QueuedTaskRecord",
        _context: "TaskExecutionContext",
    ) -> dict[str, Any]:
        attempts["count"] += 1
        if attempts["count"] == 1:
            msg = "resolver boom"
            raise RuntimeError(msg)
        return {}

    @task("contract.resolver.retry", retries=1)
    async def succeed() -> str:
        return "ok"

    config = QueueConfig(
        task_dependency_resolver=resolver,
        execution_backend="local",
        event_config=QueueEventConfig(enabled=True),
    )
    service = QueueService(config, queue_backend=queue_backend, event_publisher=publisher)

    async with service:
        enqueued = await service.enqueue("contract.resolver.retry")
        first_claim = await queue_backend.claim_task(enqueued.record.id)
        assert first_claim is not None
        first_outcome = await service.execute_record(first_claim)
        assert first_outcome.status == "pending"
        assert first_outcome.retry_count == 1
        assert first_outcome.error == "resolver boom"

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


async def test_task_dependency_resolver_default_is_none_with_no_invocation(
    queue_backend: "BaseQueueBackend",
) -> None:
    clear_task_registry()
    config = QueueConfig()
    assert config.task_dependency_resolver is None

    @task("contract.resolver.default")
    async def default() -> str:
        return "ok"

    service = QueueService(config, queue_backend=queue_backend)
    captured: list[Any] = []

    original = Task.execute_record

    async def spy(self: "Task[Any, Any]", record: Any, **kwargs: Any) -> Any:
        captured.append(kwargs.get("extra_kwargs", "MISSING"))
        return await original(self, record, **kwargs)

    from unittest.mock import patch

    with patch.object(Task, "execute_record", spy):
        async with service:
            result = await service.enqueue("contract.resolver.default")
            await result.refresh()

    assert result.status == "completed"
    assert captured == [None]


async def test_queue_service_runtime_overrides_preserve_execution_metadata_and_delay() -> None:
    @task("tasks.external", execution_backend="local")
    async def external_task() -> str:
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
    assert result.record.scheduled_at > datetime.now(UTC) + timedelta(minutes=4)

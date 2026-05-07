from typing import Any

import pytest

from litestar_queues import QueueConfig, QueueService

pytestmark = pytest.mark.anyio


async def test_service_context_manager_returns_service() -> None:
    """Test that the service can be used as an async context manager."""
    config = QueueConfig()

    async with config.provide_service() as service:
        assert isinstance(service, QueueService)
        assert service.config is config


async def test_service_placeholder_enqueue_reports_unimplemented() -> None:
    """Test that service enqueue runs through the immediate backend."""
    from litestar_queues import task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    @task("example")
    async def example() -> str:
        return "ok"

    service = QueueService(QueueConfig())

    async with service:
        result = await service.enqueue("example")

    assert result.status == "completed"
    assert result.result == "ok"


async def test_execute_record_invokes_task_dependency_resolver_and_merges_kwargs() -> None:
    """Configured resolver fires before task body and its kwargs reach the callable."""
    from litestar_queues import Task, TaskExecutionContext, task
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    invocations: list[tuple[str, str]] = []

    async def resolver(
        _task: "Task[Any, Any]",
        record: "QueuedTaskRecord",
        context: "TaskExecutionContext",
    ) -> dict[str, Any]:
        invocations.append((str(record.id), context.task_id))
        return {"injected_service": "from_resolver"}

    @task("resolver.consume")
    async def consume(**kwargs: Any) -> dict[str, Any]:
        return dict(kwargs)

    config = QueueConfig(task_dependency_resolver=resolver)
    service = QueueService(config)

    async with service:
        result = await service.enqueue("resolver.consume")

    assert result.status == "completed"
    assert isinstance(result.result, dict)
    assert result.result["injected_service"] == "from_resolver"
    assert len(invocations) == 1


async def test_execute_record_invokes_resolver_after_started_lifecycle() -> None:
    """Resolver fires after the task.started event and before task.completed."""
    import time

    from litestar_queues import (
        InMemoryQueueEventSink,
        QueueEventConfig,
        Task,
        TaskExecutionContext,
        task,
    )
    from litestar_queues.events import QueueEventPublisher
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink)

    timeline: dict[str, float] = {}

    async def resolver(
        _task: "Task[Any, Any]",
        _record: "QueuedTaskRecord",
        _context: "TaskExecutionContext",
    ) -> dict[str, Any]:
        timeline["resolver"] = time.monotonic()
        return {}

    @task("resolver.order")
    async def order(**_kwargs: Any) -> str:
        timeline["body"] = time.monotonic()
        return "ok"

    config = QueueConfig(
        task_dependency_resolver=resolver,
        event_config=QueueEventConfig(enabled=True),
    )
    service = QueueService(config, event_publisher=publisher)

    async with service:
        result = await service.enqueue("resolver.order")

    assert result.status == "completed"

    event_types = [event.type for event in sink.events]
    assert "task.started" in event_types
    assert "task.completed" in event_types

    started_index = event_types.index("task.started")
    completed_index = event_types.index("task.completed")
    started_event = sink.events[started_index]
    completed_event = sink.events[completed_index]

    assert started_event.occurred_at.timestamp() <= time.time()
    assert "resolver" in timeline and "body" in timeline
    assert timeline["resolver"] <= timeline["body"]
    assert started_event.occurred_at <= completed_event.occurred_at
    assert started_index < completed_index


async def test_execute_record_no_resolver_skips_invocation_path() -> None:
    """No resolver configured -> no extra_kwargs reach Task.execute_record."""
    from unittest.mock import patch

    from litestar_queues import Task, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    @task("resolver.absent")
    async def absent() -> str:
        return "ok"

    config = QueueConfig()
    service = QueueService(config)

    original = Task.execute_record
    captured: list[Any] = []

    async def spy(self: "Task[Any, Any]", record: Any, **kwargs: Any) -> Any:
        captured.append(kwargs.get("extra_kwargs", "MISSING"))
        return await original(self, record, **kwargs)

    with patch.object(Task, "execute_record", spy):
        async with service:
            result = await service.enqueue("resolver.absent")

    assert result.status == "completed"
    assert captured == [None]

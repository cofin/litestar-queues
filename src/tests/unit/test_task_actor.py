from typing import Any

import pytest

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends.memory import InMemoryQueueBackend
from litestar_queues.events import (
    EventBufferConfig,
    InMemoryQueueEventSink,
    QueueEventActor,
    QueueEventPublisher,
    require_current_task_context,
)
from litestar_queues.exceptions import QueueConfigurationError

pytestmark = pytest.mark.anyio


def test_decorator_stores_literal_actor() -> "None":
    actor = QueueEventActor(type="system", id="cron")

    @task(actor=actor)
    async def work() -> None: ...

    assert work.actor is actor


def test_decorator_stores_callable_without_calling_it() -> "None":
    calls: list[int] = []

    def resolver() -> QueueEventActor:
        calls.append(1)
        return QueueEventActor(type="user", id="u1")

    @task(actor=resolver)
    async def work() -> None: ...

    assert calls == []
    assert work.actor is resolver


async def _execute_task_in_service(task_obj: "Any") -> "list[Any]":
    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink, buffer_config=EventBufferConfig())
    backend = InMemoryQueueBackend()
    service = QueueService(
        QueueConfig(worker=WorkerConfig(placement="server")), queue_backend=backend, event_publisher=publisher
    )
    record = await backend.enqueue(task_obj.name)
    await service.execute_record(record, worker_id="test-worker")
    return list(sink.events)


async def test_actor_resolved_once_before_body_runs() -> "None":
    seen: list[str | None] = []

    @task(actor=lambda: QueueEventActor(type="user", id="u1"))
    async def work_with_actor() -> None:
        ctx = require_current_task_context()
        seen.append(ctx.actor.id if ctx.actor is not None else None)

    await _execute_task_in_service(work_with_actor)
    assert seen == ["u1"]


async def test_raising_resolver_fails_the_attempt() -> "None":
    def failing_resolver() -> QueueEventActor:
        msg = "boom"
        raise RuntimeError(msg)

    @task(actor=failing_resolver)
    async def work_failing_resolver() -> None: ...

    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink, buffer_config=EventBufferConfig())
    backend = InMemoryQueueBackend()
    service = QueueService(
        QueueConfig(worker=WorkerConfig(placement="server")), queue_backend=backend, event_publisher=publisher
    )
    record = await backend.enqueue(work_failing_resolver.name)
    with pytest.raises(RuntimeError, match="boom"):
        await service.execute_record(record, worker_id="test-worker")


async def test_resolver_returning_non_actor_raises_configuration_error() -> "None":
    @task(actor=lambda: "nope")  # type: ignore[arg-type,return-value]
    async def work_invalid_actor() -> None: ...

    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink, buffer_config=EventBufferConfig())
    backend = InMemoryQueueBackend()
    service = QueueService(
        QueueConfig(worker=WorkerConfig(placement="server")), queue_backend=backend, event_publisher=publisher
    )
    record = await backend.enqueue(work_invalid_actor.name)
    with pytest.raises(QueueConfigurationError, match=work_invalid_actor.name):
        await service.execute_record(record, worker_id="test-worker")


async def test_every_lifecycle_event_carries_the_declared_actor() -> "None":
    @task(actor=QueueEventActor(type="user", id="u1"))
    async def work_lifecycle() -> None: ...

    events = await _execute_task_in_service(work_lifecycle)
    lifecycle = [e for e in events if e.type in {"task.started", "task.completed"}]
    assert lifecycle, "no lifecycle events captured"
    assert all(e.actor is not None and e.actor.id == "u1" for e in lifecycle)

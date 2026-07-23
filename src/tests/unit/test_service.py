from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest

from litestar_queues import EventDeliveryConfig, InMemoryQueueEventSink, QueueConfig, QueueService
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.events import QueueEventPublisher, QueueEventsConfig
from litestar_queues.execution.cloudrun import CloudRunExecutionConfig

if TYPE_CHECKING:
    from litestar_queues.events import (
        EventHistoryConfig,
        QueueEvent,
        QueueEventLog,
        QueueEventLogRecord,
        QueueEventStageSummary,
    )
    from litestar_queues.models import QueuedTaskRecord

pytestmark = pytest.mark.anyio


async def test_service_context_manager_returns_service() -> "None":
    """Test that the service can be used as an async context manager."""
    config = QueueConfig()

    async with config.provide_service() as service:
        assert isinstance(service, QueueService)
        assert service.config is config


def test_get_event_publisher_uses_noop_sink_when_events_are_disabled() -> "None":
    config = QueueConfig(events=None)

    publisher = config.get_event_publisher()

    assert not isinstance(publisher.sink, InMemoryQueueEventSink)


async def test_service_placeholder_enqueue_reports_unimplemented() -> "None":
    """Test that service enqueue runs through the immediate backend."""
    from litestar_queues import task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    @task("example")
    async def example() -> "str":
        return "ok"

    service = QueueService(QueueConfig(execution_backend="immediate"))

    async with service:
        result = await service.enqueue("example")

    assert result.status == "completed"
    assert result.result == "ok"


async def test_enqueue_can_override_requeue_on_stale_metadata() -> "None":
    from litestar_queues import task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    @task("stale.override", requeue_on_stale=True)
    async def stale_override() -> "str":
        return "ok"

    service = QueueService(QueueConfig(execution_backend="local"))

    async with service:
        result = await service.enqueue(stale_override, requeue_on_stale=False)

    assert result.record is not None
    assert result.record.metadata["requeue_on_stale"] is False


async def test_enqueue_uses_config_log_success_default() -> "None":
    from litestar_queues import task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    @task("quiet.config_default")
    async def config_default() -> "str":
        return "ok"

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        result = await service.enqueue(config_default)

    assert result.record is not None
    assert result.record.metadata["log_success"] is False


async def test_enqueue_respects_config_log_success_false_default() -> "None":
    from litestar_queues import task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    @task("quiet.config_false")
    async def config_false() -> "str":
        return "ok"

    async with QueueService(QueueConfig(execution_backend="local", log_success=False)) as service:
        result = await service.enqueue(config_false)

    assert result.record is not None
    assert result.record.metadata["log_success"] is False


async def test_enqueue_log_success_precedence() -> "None":
    from litestar_queues import task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    @task("quiet.metadata_only")
    async def metadata_only() -> "str":
        return "ok"

    @task("quiet.task_override", log_success=True)
    async def task_override() -> "str":
        return "ok"

    async with QueueService(QueueConfig(execution_backend="local", log_success=True)) as service:
        metadata_result = await service.enqueue(metadata_only, metadata={"log_success": False})
        task_result = await service.enqueue(task_override, metadata={"log_success": False})
        enqueue_result = await service.enqueue(task_override, log_success=False, metadata={"log_success": True})

    assert metadata_result.record is not None
    assert task_result.record is not None
    assert enqueue_result.record is not None
    assert metadata_result.record.metadata["log_success"] is False
    assert task_result.record.metadata["log_success"] is True
    assert enqueue_result.record.metadata["log_success"] is False


async def test_enqueue_immediate_override_executes_inline_when_configured_backend_is_external() -> "None":
    from litestar_queues import task

    @task("external.inline")
    async def inline() -> "str":
        return "ok"

    config = QueueConfig(
        execution_backend=CloudRunExecutionConfig(project_id="test-project", region="us-central1", job_name="worker")
    )

    async with QueueService(config) as service:
        result = await service.enqueue(inline.using(execution_backend="immediate"))

    assert result.status == "completed"
    assert result.result == "ok"
    assert result.record is not None
    assert result.record.execution_backend == "immediate"


async def test_enqueue_normalizes_naive_scheduled_at_to_utc() -> "None":
    from litestar_queues import task

    @task("scheduled.naive")
    async def naive_schedule() -> "str":
        return "ok"

    naive_scheduled_at = (datetime.now(timezone.utc) + timedelta(minutes=5)).replace(tzinfo=None)

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        result = await service.enqueue(naive_schedule, scheduled_at=naive_scheduled_at)

    assert result.status == "scheduled"
    assert result.record is not None
    assert result.record.scheduled_at == naive_scheduled_at.replace(tzinfo=timezone.utc)


async def test_execute_record_invokes_task_dependency_resolver_and_merges_kwargs() -> "None":
    """Configured resolver fires before task body and its kwargs reach the callable."""
    from litestar_queues import Task, TaskExecutionContext, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    invocations: "list[tuple[str, str]]" = []

    async def resolver(
        _task: "Task[..., object]", record: "QueuedTaskRecord", context: "TaskExecutionContext"
    ) -> "dict[str, object]":
        invocations.append((str(record.id), context.task_id))
        return {"injected_service": "from_resolver"}

    @task("resolver.consume")
    async def consume(**kwargs: "object") -> "dict[str, object]":
        return dict(kwargs)

    config = QueueConfig(execution_backend="immediate", task_dependency_resolver=resolver)
    service = QueueService(config)

    async with service:
        result = await service.enqueue("resolver.consume")

    assert result.status == "completed"
    assert isinstance(result.result, dict)
    assert result.result["injected_service"] == "from_resolver"
    assert len(invocations) == 1


async def test_execute_record_invokes_resolver_after_started_lifecycle() -> "None":
    """Resolver fires after the task.started event and before task.completed."""
    import time

    from litestar_queues import EventDeliveryConfig, InMemoryQueueEventSink, Task, TaskExecutionContext, task
    from litestar_queues.events import QueueEventPublisher
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink)

    timeline: "dict[str, float]" = {}

    async def resolver(
        _task: "Task[..., object]", _record: "QueuedTaskRecord", _context: "TaskExecutionContext"
    ) -> "dict[str, object]":
        timeline["resolver"] = time.monotonic()
        return {}

    @task("resolver.order")
    async def order(**_kwargs: "object") -> "str":
        timeline["body"] = time.monotonic()
        return "ok"

    config = QueueConfig(
        execution_backend="immediate",
        task_dependency_resolver=resolver,
        events=QueueEventsConfig(delivery=EventDeliveryConfig()),
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


async def test_execute_record_no_resolver_skips_invocation_path() -> "None":
    """No resolver configured -> no extra_kwargs reach Task.execute_record."""
    from unittest.mock import patch

    from litestar_queues import Task, TaskExecutionContext, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    @task("resolver.absent")
    async def absent() -> "str":
        return "ok"

    config = QueueConfig(execution_backend="immediate")
    service = QueueService(config)

    original = Task.execute_record
    captured: "list[object]" = []

    async def spy(self: "Task[..., object]", record: "QueuedTaskRecord", **kwargs: "object") -> "object":
        extra_kwargs = kwargs.get("extra_kwargs")
        task_context = kwargs.get("task_context")
        assert extra_kwargs is None or isinstance(extra_kwargs, dict)
        assert task_context is None or isinstance(task_context, TaskExecutionContext)
        captured.append(extra_kwargs if "extra_kwargs" in kwargs else "MISSING")
        return await original(self, record, task_context=task_context, extra_kwargs=extra_kwargs)

    with patch.object(Task, "execute_record", spy):
        async with service:
            result = await service.enqueue("resolver.absent")

    assert result.status == "completed"
    assert captured == [None]


async def test_recover_stale_tasks_publishes_summary_event() -> "None":
    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink)
    backend = InMemoryQueueBackend()
    record = await backend.enqueue("tasks.stale", max_retries=0, metadata={"requeue_on_stale": True})
    claimed = await backend.claim_task(record.id)
    assert claimed is not None
    claimed.heartbeat_at = datetime.now(timezone.utc) - timedelta(minutes=10)

    async with QueueService(
        QueueConfig(execution_backend="local", events=QueueEventsConfig(delivery=EventDeliveryConfig())),
        queue_backend=backend,
        event_publisher=publisher,
    ) as service:
        result = await service.recover_stale_tasks(stale_after=timedelta(seconds=1), worker_id="worker-stale")

    assert result.failed == 1
    event = next(event for event in sink.events if event.type == "worker.stale_recovery")
    assert event.scope == "worker"
    assert event.worker_id == "worker-stale"
    assert event.payload == {"requeued": 0, "failed": 1, "skipped": 0, "handler_needed": 0}


async def test_event_log_config_is_public_and_memory_backend_is_supported() -> "None":
    from litestar_queues import events

    event_log_config_type = getattr(events, "EventHistoryConfig", None)
    assert event_log_config_type is not None

    config = QueueConfig(events=QueueEventsConfig(history=event_log_config_type()))

    async with QueueService(config) as service:
        assert service.get_queue_backend().get_event_log(event_log_config_type()) is not None


async def test_backend_event_log_records_events_when_live_events_are_disabled() -> "None":
    from litestar_queues import events, task
    from litestar_queues.events import publish_task_log
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    event_log_config_type = getattr(events, "EventHistoryConfig", None)
    assert event_log_config_type is not None
    event_log = _RecordingEventLog()

    @task("tasks.event_history")
    async def event_history_task() -> "None":
        await publish_task_log("history only", payload={"stage": "load"})

    config = QueueConfig(execution_backend="immediate", events=QueueEventsConfig(history=event_log_config_type()))

    async with QueueService(config, queue_backend=_EventLogBackend(event_log)) as service:
        result = await service.enqueue(event_history_task)

    assert result.status == "completed"
    assert [event.type for event in event_log.events] == ["task.started", "task.log", "task.completed"]
    assert event_log.flushed is True


async def test_backend_event_log_and_live_sink_are_independent() -> "None":
    from litestar_queues import events, task
    from litestar_queues.events import publish_task_log
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    event_log_config_type = getattr(events, "EventHistoryConfig", None)
    assert event_log_config_type is not None
    event_log = _RecordingEventLog()
    sink = InMemoryQueueEventSink()

    @task("tasks.event_history_with_live_sink")
    async def event_history_with_live_sink_task() -> "None":
        await publish_task_log("history and live", payload={"stage": "load"})

    config = QueueConfig(
        execution_backend="immediate",
        events=QueueEventsConfig(delivery=EventDeliveryConfig(sinks=(sink,)), history=event_log_config_type()),
    )

    async with QueueService(config, queue_backend=_EventLogBackend(event_log)) as service:
        result = await service.enqueue(event_history_with_live_sink_task)

    assert result.status == "completed"
    assert [event.type for event in event_log.events] == ["task.started", "task.log", "task.completed"]
    assert [event.type for event in sink.events] == ["task.started", "task.log", "task.completed"]


async def test_initialize_schedules_uses_task_priority_for_schedule_record() -> "None":
    from litestar_queues import task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    @task("tasks.priority_schedule", interval=60, priority=5)
    async def priority_schedule() -> "None":
        return None

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        records = await service.initialize_schedules()

    assert len(records) == 1
    assert records[0].task_name == priority_schedule.name
    assert records[0].priority == 5


async def test_initialize_schedules_applies_config_log_success_default_and_task_override() -> "None":
    from litestar_queues import task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    @task("tasks.quiet_schedule_default", interval=60)
    async def quiet_schedule_default() -> "None":
        return None

    @task("tasks.quiet_schedule_override", interval=60, log_success=False)
    async def quiet_schedule_override() -> "None":
        return None

    async with QueueService(QueueConfig(execution_backend="local", log_success=True)) as service:
        records = await service.initialize_schedules()

    by_task_name = {record.task_name: record for record in records}
    assert by_task_name["tasks.quiet_schedule_default"].metadata["log_success"] is True
    assert by_task_name["tasks.quiet_schedule_override"].metadata["log_success"] is False


async def test_recover_stale_tasks_invokes_registered_stale_failure_hook() -> "None":
    from litestar_queues import task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    sink = InMemoryQueueEventSink()
    called: "list[str]" = []

    async def on_stale_failure(record: "QueuedTaskRecord") -> "None":
        called.append(str(record.id))

    @task("tasks.stale_hook", requeue_on_stale=False, on_stale_failure=on_stale_failure)
    async def stale_hook() -> "None":
        return None

    backend = InMemoryQueueBackend()
    record = await backend.enqueue(stale_hook.name, max_retries=3, metadata=stale_hook.metadata())
    claimed = await backend.claim_task(record.id)
    assert claimed is not None
    claimed.heartbeat_at = datetime.now(timezone.utc) - timedelta(minutes=10)

    async with QueueService(
        QueueConfig(execution_backend="local", events=QueueEventsConfig(delivery=EventDeliveryConfig())),
        queue_backend=backend,
        event_publisher=QueueEventPublisher(sink),
    ) as service:
        result = await service.recover_stale_tasks(stale_after=timedelta(seconds=1), worker_id="worker-stale")

    assert result.failed == 1
    assert called == [str(record.id)]
    assert [event.type for event in sink.events] == ["task.stale_failed", "worker.stale_recovery"]


async def test_execute_record_sanitizes_persisted_error_and_failed_event() -> "None":
    from litestar_queues import task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    sink = InMemoryQueueEventSink()

    def sanitize_error(exc: "BaseException", record: "QueuedTaskRecord") -> "str":
        return f"{record.task_name}:{type(exc).__name__}:redacted"

    @task("tasks.sanitize_error")
    async def sanitize_error_task() -> "None":
        msg = "secret-token"
        raise RuntimeError(msg)

    config = QueueConfig(
        execution_backend="local",
        events=QueueEventsConfig(delivery=EventDeliveryConfig()),
        error_sanitizer=sanitize_error,
    )

    async with QueueService(config, event_publisher=QueueEventPublisher(sink)) as service:
        result = await service.enqueue(sanitize_error_task)
        claimed = await service.get_queue_backend().claim_next()
        assert claimed is not None
        updated = await service.execute_record(claimed)

    failed_event = next(event for event in sink.events if event.type == "task.failed")
    assert updated.status == "failed"
    assert updated.error == "tasks.sanitize_error:RuntimeError:redacted"
    assert failed_event.message == "tasks.sanitize_error:RuntimeError:redacted"
    assert result.record is not None
    assert result.record.error == "tasks.sanitize_error:RuntimeError:redacted"


class _RecordingEventLog:
    def __init__(self) -> "None":
        self.events: "list[QueueEvent]" = []
        self.flushed = False

    async def publish_event(self, event: "QueueEvent") -> "None":
        self.events.append(event)

    async def flush_events(self) -> "None":
        self.flushed = True

    async def list_events(
        self, *, task_id: "str | None" = None, task_name: "str | None" = None, limit: "int | None" = None
    ) -> "list[QueueEventLogRecord]":
        del task_id, task_name, limit
        return []

    async def summarize_stages(self, *, task_name: "str | None" = None) -> "list[QueueEventStageSummary]":
        del task_name
        return []

    async def cleanup_before(self, before: "datetime", *, limit: "int | None" = None) -> "int":
        del before, limit
        return 0


class _EventLogBackend(InMemoryQueueBackend):
    def __init__(self, event_log: "_RecordingEventLog") -> "None":
        super().__init__()
        self._event_log = event_log

    def get_event_log(self, config: "EventHistoryConfig") -> "QueueEventLog | None":
        del config
        return self._event_log

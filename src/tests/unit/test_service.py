import asyncio
import contextlib
import threading
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

import pytest

from litestar_queues import EventDeliveryConfig, InMemoryQueueEventSink, QueueConfig, QueueService, WorkerConfig
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.events import EventHistoryConfig, QueueEventPublisher, QueueEventsConfig
from litestar_queues.execution import BaseExecutionBackend
from litestar_queues.execution.cloudrun import CloudRunExecutionConfig

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping, Sequence
    from uuid import UUID

    from litestar_queues import Task, TaskDependencyProvider, TaskExecutionContext
    from litestar_queues.events import QueueEvent, QueueEventLog, QueueEventLogRecord, QueueEventStageSummary
    from litestar_queues.models import QueuedTaskRecord

pytestmark = pytest.mark.anyio


class _LifecycleQueueBackend(InMemoryQueueBackend):
    __slots__ = ("_close_error", "_lifecycle_event_log", "_lifecycle_order")

    def __init__(
        self, order: "list[str]", event_log: "_LifecycleEventLog", *, close_error: "BaseException | None" = None
    ) -> "None":
        super().__init__()
        self._lifecycle_order = order
        self._lifecycle_event_log = event_log
        self._close_error = close_error

    async def open(self) -> "bool":
        self._lifecycle_order.append("queue.open")
        return await super().open()

    async def close(self) -> "None":
        self._lifecycle_order.append("queue.close")
        if self._close_error is not None:
            raise self._close_error
        await super().close()

    def get_event_log(self, config: "EventHistoryConfig") -> "QueueEventLog | None":
        del config
        return cast("QueueEventLog", self._lifecycle_event_log)


class _DelayedClaimBackend(InMemoryQueueBackend):
    async def claim_task_with_expired(
        self, task_id: "UUID", *, expected_retry_count: "int | None" = None, expected_execution_ref: "str | None" = None
    ) -> "tuple[QueuedTaskRecord | None, QueuedTaskRecord | None]":
        await asyncio.sleep(0.1)
        return await super().claim_task_with_expired(
            task_id, expected_retry_count=expected_retry_count, expected_execution_ref=expected_execution_ref
        )


class _LifecycleExecutionBackend(BaseExecutionBackend):
    __slots__ = ("_close_error", "_fail_open", "_lifecycle_order")

    def __init__(
        self, order: "list[str]", *, fail_open: "bool" = False, close_error: "BaseException | None" = None
    ) -> "None":
        super().__init__()
        self._lifecycle_order = order
        self._fail_open = fail_open
        self._close_error = close_error

    async def open(self) -> "bool":
        self._lifecycle_order.append("execution.open")
        if self._fail_open:
            msg = "execution open failed"
            raise RuntimeError(msg)
        return True

    async def close(self) -> "None":
        self._lifecycle_order.append("execution.close")
        if self._close_error is not None:
            raise self._close_error


class _LifecycleEventLog:
    def __init__(self, order: "list[str]", *, flush_error: "BaseException | None" = None) -> "None":
        self._lifecycle_order = order
        self._flush_error = flush_error

    async def flush_events(self) -> "None":
        self._lifecycle_order.append("event_log.flush")
        if self._flush_error is not None:
            raise self._flush_error


class _LifecycleSink:
    def __init__(
        self, order: "list[str]", *, fail_open: "bool" = False, close_error: "BaseException | None" = None
    ) -> "None":
        self._lifecycle_order = order
        self._fail_open = fail_open
        self._close_error = close_error

    async def open(self) -> "None":
        self._lifecycle_order.append("sink.open")
        if self._fail_open:
            msg = "sink open failed"
            raise RuntimeError(msg)

    async def close(self) -> "None":
        self._lifecycle_order.append("sink.close")
        if self._close_error is not None:
            raise self._close_error

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> "None":
        del event, channels


class _LifecyclePublisher(QueueEventPublisher):
    __slots__ = ("_lifecycle_order", "_stop_error")

    def __init__(
        self, sink: "_LifecycleSink", order: "list[str]", *, stop_error: "BaseException | None" = None
    ) -> "None":
        super().__init__(sink)
        self._lifecycle_order = order
        self._stop_error = stop_error

    def start_buffer(self) -> "None":
        self._lifecycle_order.append("buffer.start")

    async def stop_buffer(self) -> "None":
        self._lifecycle_order.append("buffer.stop")
        if self._stop_error is not None:
            raise self._stop_error


class _LifecycleSyncExecutor:
    def __init__(self, order: "list[str]", *, shutdown_error: "BaseException | None" = None) -> "None":
        self._lifecycle_order = order
        self._shutdown_error = shutdown_error

    def shutdown(self, *, wait: "bool", cancel_futures: "bool") -> "None":
        assert wait is True
        assert cancel_futures is True
        self._lifecycle_order.append("executor.shutdown")
        if self._shutdown_error is not None:
            raise self._shutdown_error


async def test_service_rolls_back_every_resource_when_execution_open_fails() -> "None":
    order: "list[str]" = []
    event_log = _LifecycleEventLog(order)
    service = QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(history=EventHistoryConfig()),
        ),
        queue_backend=_LifecycleQueueBackend(order, event_log),
        execution_backend=_LifecycleExecutionBackend(order, fail_open=True),
    )

    with pytest.raises(RuntimeError, match="execution open failed"):
        await service.open()

    await service.close()
    assert order == ["queue.open", "execution.open", "execution.close", "event_log.flush", "queue.close"]


async def test_service_rolls_back_every_resource_when_sink_open_fails() -> "None":
    order: "list[str]" = []
    event_log = _LifecycleEventLog(order)
    service = QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(history=EventHistoryConfig()),
        ),
        queue_backend=_LifecycleQueueBackend(order, event_log),
        execution_backend=_LifecycleExecutionBackend(order),
        event_publisher=QueueEventPublisher(_LifecycleSink(order, fail_open=True)),
    )

    with pytest.raises(RuntimeError, match="sink open failed"):
        await service.open()

    await service.close()
    assert order == [
        "queue.open",
        "execution.open",
        "sink.open",
        "sink.close",
        "execution.close",
        "event_log.flush",
        "queue.close",
    ]


async def test_service_rollback_preserves_primary_failure_and_attempts_every_close() -> "None":
    order: "list[str]" = []
    event_log = _LifecycleEventLog(order, flush_error=RuntimeError("event log flush failed"))
    service = QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(history=EventHistoryConfig()),
        ),
        queue_backend=_LifecycleQueueBackend(order, event_log, close_error=RuntimeError("queue close failed")),
        execution_backend=_LifecycleExecutionBackend(
            order, fail_open=True, close_error=RuntimeError("execution close failed")
        ),
    )

    with pytest.raises(RuntimeError, match="execution open failed"):
        await service.open()

    await service.close()
    assert order == ["queue.open", "execution.open", "execution.close", "event_log.flush", "queue.close"]


async def test_service_open_and_close_are_idempotent() -> "None":
    order: "list[str]" = []
    event_log = _LifecycleEventLog(order)
    publisher = _LifecyclePublisher(_LifecycleSink(order), order)
    service = QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(history=EventHistoryConfig()),
        ),
        queue_backend=_LifecycleQueueBackend(order, event_log),
        execution_backend=_LifecycleExecutionBackend(order),
        event_publisher=publisher,
    )

    await service.open()
    await service.open()
    await service.close()
    await service.close()

    assert order == [
        "queue.open",
        "execution.open",
        "sink.open",
        "buffer.start",
        "execution.close",
        "event_log.flush",
        "buffer.stop",
        "queue.close",
        "sink.close",
    ]


async def test_service_close_attempts_every_resource_and_raises_first_error(
    monkeypatch: "pytest.MonkeyPatch",
) -> "None":
    order: "list[str]" = []
    event_log = _LifecycleEventLog(order, flush_error=RuntimeError("event log flush failed"))
    publisher = _LifecyclePublisher(
        _LifecycleSink(order, close_error=RuntimeError("sink close failed")),
        order,
        stop_error=RuntimeError("buffer stop failed"),
    )
    executor = _LifecycleSyncExecutor(order, shutdown_error=RuntimeError("executor shutdown failed"))
    monkeypatch.setattr("litestar_queues.service.ThreadPoolExecutor", lambda **_kwargs: executor)
    service = QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(history=EventHistoryConfig()),
            sync_thread_pool_size=1,
        ),
        queue_backend=_LifecycleQueueBackend(order, event_log, close_error=RuntimeError("queue close failed")),
        execution_backend=_LifecycleExecutionBackend(order, close_error=RuntimeError("execution close failed")),
        event_publisher=publisher,
    )
    await service.open()

    with pytest.raises(RuntimeError, match="execution close failed"):
        await service.close()
    await service.close()

    assert order[-6:] == [
        "execution.close",
        "event_log.flush",
        "buffer.stop",
        "queue.close",
        "sink.close",
        "executor.shutdown",
    ]
    assert service._sync_executor is None


@pytest.mark.parametrize("control_error", [asyncio.CancelledError(), SystemExit(), KeyboardInterrupt()])
async def test_service_close_control_flow_takes_precedence_and_all_resources_close(
    monkeypatch: "pytest.MonkeyPatch", control_error: "BaseException"
) -> "None":
    order: "list[str]" = []
    event_log = _LifecycleEventLog(order, flush_error=control_error)
    publisher = _LifecyclePublisher(_LifecycleSink(order), order)
    executor = _LifecycleSyncExecutor(order)
    monkeypatch.setattr("litestar_queues.service.ThreadPoolExecutor", lambda **_kwargs: executor)
    service = QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(history=EventHistoryConfig()),
            sync_thread_pool_size=1,
        ),
        queue_backend=_LifecycleQueueBackend(order, event_log),
        execution_backend=_LifecycleExecutionBackend(order, close_error=RuntimeError("execution close failed")),
        event_publisher=publisher,
    )
    await service.open()

    with pytest.raises(type(control_error)):
        await service.close()

    assert order[-6:] == [
        "execution.close",
        "event_log.flush",
        "buffer.stop",
        "queue.close",
        "sink.close",
        "executor.shutdown",
    ]
    assert service._sync_executor is None


class _LifecycleDependencyProvider:
    """Provider object that records its own lifecycle into the shared order list."""

    def __init__(
        self, order: "list[str]", *, fail_open: "bool" = False, close_error: "BaseException | None" = None
    ) -> "None":
        self._order = order
        self._fail_open = fail_open
        self._close_error = close_error

    async def open(self) -> "None":
        self._order.append("provider.open")
        if self._fail_open:
            msg = "provider open failed"
            raise RuntimeError(msg)

    async def close(self) -> "None":
        self._order.append("provider.close")
        if self._close_error is not None:
            raise self._close_error

    @contextlib.asynccontextmanager
    async def __call__(
        self, task: "object", record: "object", context: "object"
    ) -> "AsyncIterator[Mapping[str, object]]":
        yield {}


class _PlainCallableProvider:
    """Provider object that does not expose open/close methods."""

    @contextlib.asynccontextmanager
    async def __call__(
        self, task: "object", record: "object", context: "object"
    ) -> "AsyncIterator[Mapping[str, object]]":
        yield {}


async def test_provider_opens_first_and_closes_last() -> "None":
    order: "list[str]" = []
    event_log = _LifecycleEventLog(order)
    publisher = _LifecyclePublisher(_LifecycleSink(order), order)
    service = QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(history=EventHistoryConfig()),
            task_dependency_provider=_LifecycleDependencyProvider(order),
        ),
        queue_backend=_LifecycleQueueBackend(order, event_log),
        execution_backend=_LifecycleExecutionBackend(order),
        event_publisher=publisher,
    )

    await service.open()
    await service.close()

    assert order == [
        "provider.open",
        "queue.open",
        "execution.open",
        "sink.open",
        "buffer.start",
        "execution.close",
        "event_log.flush",
        "buffer.stop",
        "queue.close",
        "sink.close",
        "provider.close",
    ]


async def test_provider_is_rolled_back_when_a_later_resource_fails_to_open() -> "None":
    order: "list[str]" = []
    event_log = _LifecycleEventLog(order)
    service = QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(history=EventHistoryConfig()),
            task_dependency_provider=_LifecycleDependencyProvider(order),
        ),
        queue_backend=_LifecycleQueueBackend(order, event_log),
        execution_backend=_LifecycleExecutionBackend(order, fail_open=True),
    )

    with pytest.raises(RuntimeError, match="execution open failed"):
        await service.open()

    await service.close()

    assert order == [
        "provider.open",
        "queue.open",
        "execution.open",
        "execution.close",
        "event_log.flush",
        "queue.close",
        "provider.close",
    ]


async def test_provider_close_error_does_not_hide_the_primary_failure() -> "None":
    order: "list[str]" = []
    event_log = _LifecycleEventLog(order)
    service = QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(history=EventHistoryConfig()),
            task_dependency_provider=_LifecycleDependencyProvider(
                order, close_error=RuntimeError("provider close failed")
            ),
        ),
        queue_backend=_LifecycleQueueBackend(order, event_log),
        execution_backend=_LifecycleExecutionBackend(order, fail_open=True),
    )

    with pytest.raises(RuntimeError, match="execution open failed"):
        await service.open()

    assert order == [
        "provider.open",
        "queue.open",
        "execution.open",
        "execution.close",
        "event_log.flush",
        "queue.close",
        "provider.close",
    ]


async def test_provider_open_failure_is_not_followed_by_close() -> "None":
    order: "list[str]" = []
    event_log = _LifecycleEventLog(order)
    service = QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(history=EventHistoryConfig()),
            task_dependency_provider=_LifecycleDependencyProvider(order, fail_open=True),
        ),
        queue_backend=_LifecycleQueueBackend(order, event_log),
        execution_backend=_LifecycleExecutionBackend(order),
    )

    with pytest.raises(RuntimeError, match="provider open failed"):
        await service.open()

    assert order == ["provider.open"]


async def test_plain_callable_provider_needs_no_lifecycle_methods() -> "None":
    order: "list[str]" = []
    event_log = _LifecycleEventLog(order)
    service = QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(history=EventHistoryConfig()),
            task_dependency_provider=_PlainCallableProvider(),
        ),
        queue_backend=_LifecycleQueueBackend(order, event_log),
        execution_backend=_LifecycleExecutionBackend(order),
    )

    await service.open()
    await service.close()

    assert "provider.open" not in order
    assert "provider.close" not in order


async def test_service_context_manager_returns_service() -> "None":
    """Test that the service can be used as an async context manager."""
    config = QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory")

    async with QueueService(config) as service:
        assert isinstance(service, QueueService)
        assert service.config is config


def test_get_event_publisher_uses_noop_sink_when_events_are_disabled() -> "None":
    config = QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", events=None)

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

    service = QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="immediate")
    )

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

    service = QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local")
    )

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

    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local")
    ) as service:
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

    async with QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            execution_backend="local",
            log_success=False,
        )
    ) as service:
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

    async with QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            execution_backend="local",
            log_success=True,
        )
    ) as service:
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
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend=CloudRunExecutionConfig(project_id="test-project", region="us-central1", job_name="worker"),
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

    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local")
    ) as service:
        result = await service.enqueue(naive_schedule, scheduled_at=naive_scheduled_at)

    assert result.status == "scheduled"
    assert result.record is not None
    assert result.record.scheduled_at == naive_scheduled_at.replace(tzinfo=timezone.utc)


async def test_enqueue_rejects_both_expires_in_and_expires_at() -> "None":
    from litestar_queues import task

    @task("expiry.both")
    async def expires_with_both() -> "str":
        return "ok"

    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local")
    ) as service:
        with pytest.raises(ValueError, match="both expires_in and expires_at"):
            await service.enqueue(
                expires_with_both, expires_in=60, expires_at=datetime.now(timezone.utc) + timedelta(minutes=1)
            )


async def test_enqueue_relative_expires_in_uses_enqueue_time() -> "None":
    from litestar_queues import task

    @task("expiry.immediate")
    async def expires_from_enqueue() -> "str":
        return "ok"

    before = datetime.now(timezone.utc)
    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local")
    ) as service:
        result = await service.enqueue(expires_from_enqueue, expires_in=30)
    after = datetime.now(timezone.utc)

    assert result.record is not None
    expires_at = result.record.expires_at
    assert expires_at is not None
    assert before + timedelta(seconds=30) <= expires_at <= after + timedelta(seconds=30)


async def test_immediate_enqueue_publishes_expiration_when_deadline_crosses_during_claim() -> "None":
    from litestar_queues import task

    calls: "list[str]" = []

    @task("expiry.immediate_claim")
    async def immediate_claim() -> "None":
        calls.append("executed")

    sink = InMemoryQueueEventSink()
    queue_backend = _DelayedClaimBackend()
    async with QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            execution_backend="immediate",
            events=QueueEventsConfig(delivery=EventDeliveryConfig(sinks=(sink,))),
        ),
        queue_backend=queue_backend,
    ) as service:
        result = await service.enqueue(immediate_claim, expires_in=0.05)
        stored = await queue_backend.get_task(result.id)

    assert calls == []
    assert stored is not None
    assert stored.status == "expired"
    assert [event.type for event in sink.events] == ["task.expired"]


async def test_enqueue_relative_expires_in_uses_scheduled_at() -> "None":
    from litestar_queues import task

    @task("expiry.scheduled")
    async def expires_from_schedule() -> "str":
        return "ok"

    scheduled_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local")
    ) as service:
        result = await service.enqueue(expires_from_schedule, scheduled_at=scheduled_at, expires_in=30)

    assert result.record is not None
    assert result.record.expires_at == scheduled_at + timedelta(seconds=30)


async def test_enqueue_absolute_expires_at_is_stored_as_utc() -> "None":
    from litestar_queues import task

    @task("expiry.absolute")
    async def expires_absolute() -> "str":
        return "ok"

    naive_expires_at = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=1)
    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local")
    ) as service:
        result = await service.enqueue(expires_absolute, expires_at=naive_expires_at)

    assert result.record is not None
    assert result.record.expires_at == naive_expires_at.replace(tzinfo=timezone.utc)


async def test_enqueue_uses_and_overrides_task_expires_in_default() -> "None":
    from litestar_queues import task

    @task("expiry.default", expires_in=45)
    async def expires_by_default() -> "str":
        return "ok"

    before = datetime.now(timezone.utc)
    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local")
    ) as service:
        default_result = await service.enqueue(expires_by_default)
        override_result = await service.enqueue(expires_by_default, expires_in=90)
    after = datetime.now(timezone.utc)

    assert default_result.record is not None
    assert override_result.record is not None
    default_expires_at = default_result.record.expires_at
    override_expires_at = override_result.record.expires_at
    assert default_expires_at is not None
    assert override_expires_at is not None
    assert before + timedelta(seconds=45) <= default_expires_at <= after + timedelta(seconds=45)
    assert before + timedelta(seconds=90) <= override_expires_at <= after + timedelta(seconds=90)


async def test_enqueue_rejects_negative_expires_in_and_accepts_zero() -> "None":
    from litestar_queues import task

    @task("expiry.bounds")
    async def expiry_bounds() -> "str":
        return "ok"

    sink = InMemoryQueueEventSink()
    async with QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            execution_backend="local",
            events=QueueEventsConfig(delivery=EventDeliveryConfig(sinks=(sink,))),
        )
    ) as service:
        with pytest.raises(ValueError, match="expires_in must not be negative"):
            await service.enqueue(expiry_bounds, expires_in=-1)
        result = await service.enqueue(expiry_bounds, expires_in=0)

    assert result.record is not None
    assert result.record.status == "expired"
    assert [event.type for event in sink.events].count("task.expired") == 1


async def test_execute_record_invokes_task_dependency_resolver_and_merges_kwargs() -> "None":
    """Configured resolver fires before task body and its kwargs reach the callable."""
    from litestar_queues import task
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

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="immediate",
        task_dependency_resolver=resolver,
    )
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

    from litestar_queues import EventDeliveryConfig, InMemoryQueueEventSink, task
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
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
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

    config = QueueConfig(
        worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="immediate"
    )
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


def _recording_provider(
    events: "list[str]",
    *,
    acquire_error: "BaseException | None" = None,
    cleanup_error: "BaseException | None" = None,
    payload: "Mapping[str, object] | None" = None,
) -> "TaskDependencyProvider":
    """Build a provider that appends an ordered trace to ``events``."""

    @contextlib.asynccontextmanager
    async def provider(
        task: "Task[..., object]", record: "QueuedTaskRecord", context: "TaskExecutionContext"
    ) -> "AsyncIterator[Mapping[str, object]]":
        events.append("acquire")
        if acquire_error is not None:
            raise acquire_error
        try:
            yield dict(payload or {"scoped": "value"})
        finally:
            events.append("cleanup")
            if cleanup_error is not None:
                raise cleanup_error

    return provider


async def test_provider_scope_wraps_successful_attempt() -> "None":
    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []

    @task("scoped.success")
    async def run(scoped: str) -> str:
        assert scoped == "value"
        events.append("body")
        return "done"

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="immediate",
        task_dependency_provider=_recording_provider(events),
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.success")

    await result.refresh()
    assert events == ["acquire", "body", "cleanup"]
    assert result.status == "completed"


async def test_provider_scope_closes_on_retryable_body_failure() -> "None":
    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []
    boom_msg = "boom"

    @task("scoped.retryable")
    async def run(scoped: str) -> str:
        events.append("body")
        raise RuntimeError(boom_msg)

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="immediate",
        task_dependency_provider=_recording_provider(events),
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.retryable", retries=1)

    await result.refresh()
    assert events == ["acquire", "body", "cleanup"]
    assert result.status in {"pending", "scheduled"}
    assert result.error and "boom" in result.error


async def test_provider_scope_closes_on_terminal_body_failure() -> "None":
    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.exceptions import NonRetryableError
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []
    nope_msg = "nope"

    @task("scoped.terminal")
    async def run(scoped: str) -> str:
        events.append("body")
        raise NonRetryableError(nope_msg)

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="immediate",
        task_dependency_provider=_recording_provider(events),
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.terminal")

    await result.refresh()
    assert events == ["acquire", "body", "cleanup"]
    assert result.status == "failed"


async def test_provider_acquisition_failure_never_cleans_up() -> "None":
    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []

    @task("scoped.acquire_fail")
    async def run(scoped: str) -> str:
        events.append("body")
        return "done"

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="immediate",
        task_dependency_provider=_recording_provider(events, acquire_error=RuntimeError("no session")),
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.acquire_fail", retries=1)

    await result.refresh()
    assert events == ["acquire"]
    assert result.status in {"pending", "scheduled"}
    assert result.error and "no session" in result.error


async def test_provider_scope_receives_task_record_and_context() -> "None":
    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    captured_args: dict[str, Any] = {}

    @contextlib.asynccontextmanager
    async def provider(
        task_obj: "Task[..., object]", record: "QueuedTaskRecord", context: "TaskExecutionContext"
    ) -> "AsyncIterator[dict[str, object]]":
        captured_args["task"] = task_obj
        captured_args["record"] = record
        captured_args["context"] = context
        yield {}

    @task("scoped.args")
    async def run() -> str:
        return "done"

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="immediate",
        task_dependency_provider=cast("Any", provider),
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.args")

    await result.refresh()
    assert captured_args["task"].name == "scoped.args"
    assert captured_args["record"].id == result.id
    assert captured_args["context"].task_id == str(result.id)


async def test_provider_output_never_overrides_task_context() -> "None":
    from litestar_queues import QueueConfig, QueueService, TaskExecutionContext, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []

    @task("scoped.override")
    async def run(_task_context: "TaskExecutionContext") -> str:
        assert isinstance(_task_context, TaskExecutionContext)
        events.append("body")
        return "done"

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="immediate",
        task_dependency_provider=_recording_provider(events, payload={"_task_context": "hijacked"}),
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.override")

    await result.refresh()
    assert events == ["acquire", "body", "cleanup"]


async def test_cleanup_failure_after_success_fails_the_attempt() -> "None":
    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []

    @task("scoped.cleanup_fail")
    async def run(scoped: str) -> str:
        events.append("body")
        return "done"

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="immediate",
        task_dependency_provider=_recording_provider(events, cleanup_error=RuntimeError("commit failed")),
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.cleanup_fail", retries=1)

    await result.refresh()
    assert events == ["acquire", "body", "cleanup"]
    assert result.status in {"pending", "scheduled"}
    assert result.error and "commit failed" in result.error


async def test_cleanup_failure_after_body_failure_preserves_the_body_error() -> "None":
    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []
    primary_msg = "primary"
    secondary_msg = "secondary"

    @task("scoped.both_fail")
    async def run(scoped: str) -> str:
        events.append("body")
        raise ValueError(primary_msg)

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="immediate",
        task_dependency_provider=_recording_provider(events, cleanup_error=RuntimeError(secondary_msg)),
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.both_fail", retries=0)

    await result.refresh()
    assert result.status == "failed"
    assert result.error and "primary" in result.error
    assert "secondary" not in result.error


async def test_cleanup_failure_after_body_failure_is_logged_at_error(caplog: "pytest.LogCaptureFixture") -> "None":
    import logging

    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []
    primary_msg = "primary"
    secondary_msg = "secondary"

    @task("scoped.log_fail")
    async def run(scoped: str) -> str:
        events.append("body")
        raise ValueError(primary_msg)

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="immediate",
        task_dependency_provider=_recording_provider(events, cleanup_error=RuntimeError(secondary_msg)),
    )

    with caplog.at_level(logging.ERROR, logger=config.names.logger("service")):
        async with QueueService(config) as service:
            result = await service.enqueue("scoped.log_fail", retries=0)

    await result.refresh()
    matching_records = [
        rec
        for rec in caplog.records
        if rec.message == "Queue task dependency scope cleanup failed"
        and getattr(rec, "queue_task_id", None) == str(result.id)
    ]
    assert matching_records
    record = matching_records[0]
    payload = getattr(record, "queue_task_event_payload", {})
    assert payload.get("primary_error") == "ValueError"
    assert "secondary" in payload.get("error", "")


async def test_cleanup_failure_text_passes_through_error_sanitizer(caplog: "pytest.LogCaptureFixture") -> "None":
    import logging

    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []
    primary_msg = "primary"
    secret_msg = "s3cret"

    @task("scoped.sanitize")
    async def run(scoped: str) -> str:
        events.append("body")
        raise ValueError(primary_msg)

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="immediate",
        error_sanitizer=lambda _exc, _rec: "[redacted]",
        task_dependency_provider=_recording_provider(events, cleanup_error=RuntimeError(secret_msg)),
    )

    with caplog.at_level(logging.ERROR, logger=config.names.logger("service")):
        async with QueueService(config) as service:
            result = await service.enqueue("scoped.sanitize", retries=0)

    await result.refresh()
    matching_records = [rec for rec in caplog.records if rec.message == "Queue task dependency scope cleanup failed"]
    assert matching_records
    record = matching_records[0]
    payload = getattr(record, "queue_task_event_payload", {})
    assert payload.get("error") == "[redacted]"
    # ensure "s3cret" does not appear anywhere in logs
    assert all("s3cret" not in rec.message for rec in caplog.records)


async def test_provider_cannot_suppress_the_body_exception() -> "None":
    """A truthy __aexit__ is ignored: there is no result to complete with."""
    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []

    class SuppressingProvider:
        def __call__(self, task_obj: "Any", record: "Any", context: "Any") -> "Any":
            return self

        async def __aenter__(self) -> "dict[str, object]":
            events.append("acquire")
            return {}

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object
        ) -> bool:
            events.append("cleanup")
            return True

    boom_msg = "boom"

    @task("scoped.suppress")
    async def run() -> str:
        events.append("body")
        raise RuntimeError(boom_msg)

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="immediate",
        task_dependency_provider=cast("Any", SuppressingProvider()),
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.suppress", retries=0)

    await result.refresh()
    assert events == ["acquire", "body", "cleanup"]
    assert result.status == "failed"
    assert result.error and "boom" in result.error


async def test_provider_cannot_suppress_cancellation() -> "None":
    import asyncio

    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    class SuppressingProvider:
        def __call__(self, task_obj: "Any", record: "Any", context: "Any") -> "Any":
            return self

        async def __aenter__(self) -> "dict[str, object]":
            return {}

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object
        ) -> bool:
            return True

    @task("scoped.cancel")
    async def run() -> str:
        await asyncio.sleep(1)
        return "done"

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        task_dependency_provider=cast("Any", SuppressingProvider()),
    )
    async with QueueService(config) as service:
        res = await service.enqueue("scoped.cancel", retries=0)
        record = await service.get_queue_backend().claim_task(res.id)
        assert record is not None
        task_coro = asyncio.create_task(service.execute_record(record))
        await asyncio.sleep(0)  # let it start
        task_coro.cancel()

        cancelled_caught = False
        try:
            await task_coro
        except asyncio.CancelledError:
            cancelled_caught = True
        assert cancelled_caught is True

        await res.refresh()
        # Should not be terminal because execution stopped cooperatively and didn't write failed
        assert res.status not in {"completed", "failed"}


async def test_recover_stale_tasks_publishes_summary_event() -> "None":
    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink)
    backend = InMemoryQueueBackend()
    record = await backend.enqueue("tasks.stale", max_retries=0, metadata={"requeue_on_stale": True})
    claimed = await backend.claim_task(record.id)
    assert claimed is not None
    claimed.heartbeat_at = datetime.now(timezone.utc) - timedelta(minutes=10)

    async with QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            execution_backend="local",
            events=QueueEventsConfig(delivery=EventDeliveryConfig()),
        ),
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

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        events=QueueEventsConfig(history=event_log_config_type()),
    )

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

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="immediate",
        events=QueueEventsConfig(history=event_log_config_type()),
    )

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
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
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

    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local")
    ) as service:
        records = await service.initialize_schedules()

    assert len(records) == 1
    assert records[0].task_name == priority_schedule.name
    assert records[0].priority == 5


async def test_cancel_task_enforces_running_boundary_and_publishes_once() -> "None":
    from litestar_queues import task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    sink = InMemoryQueueEventSink()

    @task("tasks.cancel_facade")
    async def cancel_facade() -> "None":
        return None

    async with QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(delivery=EventDeliveryConfig()),
        ),
        event_publisher=QueueEventPublisher(sink),
    ) as service:
        pending = await service.enqueue(cancel_facade)
        assert await service.cancel_task(pending.id) is True
        assert await service.cancel_task(pending.id) is False

        running = await service.enqueue(cancel_facade)
        claimed = await service.get_queue_backend().claim_task(running.id)
        assert claimed is not None
        assert await service.cancel_task(running.id) is False
        assert await service.cancel_task(running.id, include_running=True) is True
        assert await service.cancel_task(running.id, include_running=True) is False

    cancelled_events = [event for event in sink.events if event.type == "task.cancelled"]
    assert [event.task_id for event in cancelled_events] == [str(pending.id), str(running.id)]
    assert all(event.payload["status"] == "cancelled" for event in cancelled_events)


async def test_running_cancel_hints_workers_even_without_a_persisted_owner() -> "None":
    """The push hint must not depend on a backend that records ``worker_id``."""
    from litestar_queues import task
    from litestar_queues.backends import InMemoryQueueBackend
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    hints: "list[str | None]" = []

    class _OwnerlessBackend(InMemoryQueueBackend):
        __slots__ = ()

        async def notify_worker_control(self, worker_id: "str | None") -> "None":
            hints.append(worker_id)

    @task("tasks.ownerless_cancel")
    async def ownerless_cancel() -> "None":
        return None

    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory"),
        queue_backend=_OwnerlessBackend(),
    ) as service:
        record = await service.enqueue(ownerless_cancel)
        claimed = await service.get_queue_backend().claim_task(record.id)
        assert claimed is not None
        assert claimed.worker_id is None

        assert await service.cancel_task(record.id, include_running=True) is True

    assert hints == [None]


async def test_job_cancelled_error_uses_cancel_task_facade_once() -> "None":
    from litestar_queues import job_cancelled, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    sink = InMemoryQueueEventSink()

    @task("tasks.self_cancel_facade")
    async def self_cancel_facade() -> "None":
        job_cancelled("stop")

    async with QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(delivery=EventDeliveryConfig()),
        ),
        event_publisher=QueueEventPublisher(sink),
    ) as service:
        result = await service.enqueue(self_cancel_facade)
        claimed = await service.get_queue_backend().claim_task(result.id)
        assert claimed is not None
        updated = await service.execute_record(claimed)

    assert updated.status == "cancelled"
    assert [event.type for event in sink.events] == ["task.started", "task.cancelled"]
    assert sink.events[-1].payload == {"status": "cancelled", "retry_count": 0}


async def test_retry_backoff_schedules_and_caps_exponential_delays() -> "None":
    from litestar_queues import RetryBackoff, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    sink = InMemoryQueueEventSink()

    @task("tasks.backoff", retries=3, retry_backoff=RetryBackoff(initial_delay=2.0, multiplier=3.0, max_delay=5.0))
    async def backoff() -> "None":
        message = "retry"
        raise RuntimeError(message)

    async with QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(delivery=EventDeliveryConfig()),
        ),
        event_publisher=QueueEventPublisher(sink),
    ) as service:
        result = await service.enqueue(backoff)
        first = await service.get_queue_backend().claim_task(result.id)
        assert first is not None
        first_retry = await service.execute_record(first)
        first_retry.scheduled_at = None
        first_retry.status = "pending"
        second = await service.get_queue_backend().claim_task(result.id)
        assert second is not None
        await service.execute_record(second)

    failures = [event for event in sink.events if event.type == "task.failed"]
    assert [event.payload["retry_delay"] for event in failures] == [2.0, 5.0]
    assert all(event.payload["will_retry"] is True for event in failures)


async def test_initialize_schedules_applies_task_expiration_from_each_run_time() -> "None":
    from litestar_queues import task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    backend = InMemoryQueueBackend()

    @task("tasks.expiring_schedule", interval=60, expires_in=30)
    async def expiring_schedule() -> "None":
        return None

    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory"), queue_backend=backend
    ) as service:
        records = await service.initialize_schedules()
        first = records[0]
        assert first.scheduled_at is not None
        assert first.expires_at == first.scheduled_at + timedelta(seconds=30)

        first.status = "completed"
        first.completed_at = datetime.now(timezone.utc)
        await service._reschedule_if_needed(first)
        rescheduled = await backend.get_task_by_key("scheduled:tasks.expiring_schedule")

    assert rescheduled is not None
    assert rescheduled.scheduled_at is not None
    assert rescheduled.expires_at == rescheduled.scheduled_at + timedelta(seconds=30)


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

    async with QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            execution_backend="local",
            log_success=True,
        )
    ) as service:
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
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            execution_backend="local",
            events=QueueEventsConfig(delivery=EventDeliveryConfig()),
        ),
        queue_backend=backend,
        event_publisher=QueueEventPublisher(sink),
    ) as service:
        result = await service.recover_stale_tasks(stale_after=timedelta(seconds=1), worker_id="worker-stale")

    assert result.failed == 1
    assert called == [str(record.id)]
    assert [event.type for event in sink.events] == ["task.stale_failed", "worker.stale_recovery"]


async def test_recover_stale_tasks_offloads_a_sync_stale_failure_hook_to_a_worker_thread() -> "None":
    from litestar_queues import task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    observed: "list[int]" = []

    def on_stale_failure(record: "QueuedTaskRecord") -> "None":
        del record
        observed.append(threading.get_ident())

    @task("tasks.sync_stale_hook", requeue_on_stale=False, on_stale_failure=on_stale_failure)
    async def stale_hook() -> "None":
        return None

    backend = InMemoryQueueBackend()
    record = await backend.enqueue(stale_hook.name, max_retries=3, metadata=stale_hook.metadata())
    claimed = await backend.claim_task(record.id)
    assert claimed is not None
    claimed.heartbeat_at = datetime.now(timezone.utc) - timedelta(minutes=10)

    async with QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            execution_backend="local",
            events=QueueEventsConfig(delivery=EventDeliveryConfig()),
        ),
        queue_backend=backend,
        event_publisher=QueueEventPublisher(InMemoryQueueEventSink()),
    ) as service:
        result = await service.recover_stale_tasks(stale_after=timedelta(seconds=1), worker_id="worker-stale")

    assert result.failed == 1
    assert observed == [observed[0]]
    assert observed[0] != threading.get_ident()


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
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
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
        self,
        *,
        task_id: "str | None" = None,
        task_name: "str | None" = None,
        actor_id: "str | None" = None,
        actor_type: "str | None" = None,
        limit: "int | None" = None,
        extra: "Mapping[str, str] | None" = None,
    ) -> "list[QueueEventLogRecord]":
        del task_id, task_name, actor_id, actor_type, limit, extra
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


async def _interrupt_cycles(
    backend: "InMemoryQueueBackend", service: "QueueService", task_id: "UUID", count: "int"
) -> "None":
    for _ in range(count):
        claimed = await backend.claim_task(task_id)
        assert claimed is not None
        assert await backend.assign_worker(task_id, worker_id="worker-a", expected_retry_count=claimed.retry_count)
        assert await service.interrupt_task(claimed, worker_id="worker-a", max_interruptions=3) is not None


async def test_interrupt_task_over_the_cap_consumes_the_retry_budget() -> "None":
    backend = InMemoryQueueBackend()
    service = QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory"), queue_backend=backend
    )
    async with service:
        record = await backend.enqueue("tasks.capped", max_retries=2)
        await _interrupt_cycles(backend, service, record.id, 3)
        claimed = await backend.claim_task(record.id)
        assert claimed is not None
        assert await backend.assign_worker(record.id, worker_id="worker-a", expected_retry_count=claimed.retry_count)

        updated = await service.interrupt_task(claimed, worker_id="worker-a", max_interruptions=3)

        assert updated is not None
        assert updated.status == "pending"
        assert updated.retry_count == 4
        assert updated.metadata.get("interruptions") == 3
        assert updated.error == "Interrupted during shutdown"


async def test_interrupt_task_over_the_cap_fails_terminally_without_retries() -> "None":
    backend = InMemoryQueueBackend()
    service = QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory"), queue_backend=backend
    )
    async with service:
        record = await backend.enqueue("tasks.capped.terminal", max_retries=0)
        await _interrupt_cycles(backend, service, record.id, 3)
        claimed = await backend.claim_task(record.id)
        assert claimed is not None
        assert await backend.assign_worker(record.id, worker_id="worker-a", expected_retry_count=claimed.retry_count)

        updated = await service.interrupt_task(claimed, worker_id="worker-a", max_interruptions=3)

        assert updated is not None
        assert updated.status == "failed"
        assert updated.error == "Interrupted during shutdown"


async def test_interrupt_task_under_the_cap_requeues() -> "None":
    backend = InMemoryQueueBackend()
    service = QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory"), queue_backend=backend
    )
    async with service:
        record = await backend.enqueue("tasks.uncapped", max_retries=0)
        await _interrupt_cycles(backend, service, record.id, 1)
        stored = await backend.get_task(record.id)

        assert stored is not None
        assert stored.status == "pending"
        assert stored.metadata.get("interruptions") == 1
        assert stored.error is None


class _RecordingClaimBackend(InMemoryQueueBackend):
    """Records the kwargs every native batch-claim call receives."""

    def __init__(self) -> "None":
        super().__init__()
        self.claim_calls: "list[Mapping[str, int] | None]" = []
        self.base_loop_calls = 0

    async def claim_many_with_expired(
        self,
        *,
        limit: "int",
        queues: "tuple[str, ...]" = (),
        execution_backend: "str | None" = None,
        queue_limits: "Mapping[str, int] | None" = None,
    ) -> "tuple[list[QueuedTaskRecord], list[QueuedTaskRecord]]":
        self.claim_calls.append(queue_limits)
        return await super().claim_many_with_expired(
            limit=limit, queues=queues, execution_backend=execution_backend, queue_limits=queue_limits
        )

    async def claim_many(
        self,
        *,
        limit: "int",
        queues: "tuple[str, ...]" = (),
        execution_backend: "str | None" = None,
        queue_limits: "Mapping[str, int] | None" = None,
    ) -> "list[QueuedTaskRecord]":
        self.base_loop_calls += 1
        return await super().claim_many(
            limit=limit, queues=queues, execution_backend=execution_backend, queue_limits=queue_limits
        )


async def test_claim_tasks_forwards_queue_limits_to_backend() -> "None":
    """Per-queue caps reach the backend through one batch-claim call."""
    backend = _RecordingClaimBackend()
    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory"), queue_backend=backend
    ) as service:
        await backend.enqueue("tasks.email.first", queue="email")
        await backend.enqueue("tasks.email.second", queue="email")

        claimed = await service.claim_tasks(limit=2, queue_limits={"email": 1})

    assert len(claimed) == 1
    assert backend.claim_calls == [{"email": 1}]


async def test_provider_scope_closes_when_the_attempt_times_out() -> "None":
    """The attempt timeout cancels the body; the scope still closes exactly once."""
    import asyncio

    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []

    class CapturingProvider:
        def __call__(self, task_obj: "Any", record: "Any", context: "Any") -> "Any":
            return self

        async def __aenter__(self) -> "dict[str, object]":
            events.append("acquire")
            return {}

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object
        ) -> bool:
            events.append(exc_type.__name__ if exc_type else "None")
            events.append("cleanup")
            return False

    @task("scoped.timeout", timeout=0.05)
    async def run() -> str:
        events.append("body")
        await asyncio.sleep(5)
        return "done"

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="immediate",
        task_dependency_provider=cast("Any", CapturingProvider()),
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.timeout", retries=0)

    await result.refresh()
    assert events == ["acquire", "body", "CancelledError", "cleanup"]
    assert result.status == "failed"
    assert result.error is not None  # TimeoutError string representation can be empty in older Python versions


async def test_provider_acquisition_is_inside_the_attempt_timeout() -> "None":
    import asyncio

    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []

    class SlowProvider:
        def __call__(self, task_obj: "Any", record: "Any", context: "Any") -> "Any":
            return self

        async def __aenter__(self) -> "dict[str, object]":
            events.append("acquire")
            await asyncio.sleep(5)
            return {}

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object
        ) -> bool:
            events.append("cleanup")
            return False

    @task("scoped.timeout.acquire", timeout=0.05)
    async def run() -> str:
        events.append("body")
        return "done"

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="immediate",
        task_dependency_provider=cast("Any", SlowProvider()),
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.timeout.acquire", retries=0)

    await result.refresh()
    assert events == ["acquire"]
    assert result.status == "failed"
    assert result.error is not None  # TimeoutError string representation can be empty in older Python versions


async def test_provider_scope_closes_on_cooperative_cancellation() -> "None":
    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []

    class CapturingProvider:
        def __call__(self, task_obj: "Any", record: "Any", context: "Any") -> "Any":
            return self

        async def __aenter__(self) -> dict[str, object]:
            events.append("acquire")
            return {}

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object
        ) -> bool:
            events.append(exc_type.__name__ if exc_type else "None")
            events.append("cleanup")
            return False

    @task("scoped.cooperative_cancel")
    async def run(_task_context: "TaskExecutionContext") -> str:
        events.append("body")
        _task_context.mark_cancelled()
        _task_context.raise_if_cancelled()
        return "done"

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="immediate",
        task_dependency_provider=cast("Any", CapturingProvider()),
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.cooperative_cancel", retries=0)

    await result.refresh()
    assert events == ["acquire", "body", "JobCancelledError", "cleanup"]
    assert result.status == "cancelled"


async def test_provider_scope_closes_on_durable_cancellation() -> "None":
    import asyncio

    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.events.context import _cancel_task_context
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []

    class CapturingProvider:
        def __call__(self, task_obj: "Any", record: "Any", context: "Any") -> "Any":
            return self

        async def __aenter__(self) -> dict[str, object]:
            events.append("acquire")
            return {}

        async def __aexit__(
            self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object
        ) -> bool:
            events.append(exc_type.__name__ if exc_type else "None")
            events.append("cleanup")
            return False

    started = asyncio.Event()

    @task("scoped.durable_cancel")
    async def run() -> str:
        events.append("body")
        started.set()
        await asyncio.sleep(5)
        return "done"

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        task_dependency_provider=cast("Any", CapturingProvider()),
    )
    async with QueueService(config) as service:
        res = await service.enqueue("scoped.durable_cancel", retries=0)
        record = await service.get_queue_backend().claim_task(res.id)
        assert record is not None
        task_coro = asyncio.create_task(service.execute_record(record))
        await started.wait()

        _cancel_task_context(str(record.id))
        task_coro.cancel()

        cancelled_caught = False
        try:
            await task_coro
        except asyncio.CancelledError:
            cancelled_caught = True
        assert cancelled_caught is True

        # no terminal write happened from this attempt, so it stays scheduled/pending since we didn't write terminal status
        await res.refresh()
        assert res.status not in {"completed", "failed", "cancelled"}
        assert events == ["acquire", "body", "CancelledError", "cleanup"]

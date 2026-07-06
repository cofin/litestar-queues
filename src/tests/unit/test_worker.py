import asyncio
import logging
import os
import threading
from contextlib import suppress
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from litestar import Litestar
from litestar.testing import AsyncTestClient

from litestar_queues import (
    QueueConfig,
    QueuePlugin,
    QueueService,
    Worker,
    get_current_task_context,
    non_retryable,
    task,
)
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.events import EventConfig, InMemoryQueueEventSink

if TYPE_CHECKING:
    from collections.abc import Sequence
    from uuid import UUID

    from litestar_queues.models import HeartbeatTouch, HeartbeatTouchResult, QueuedTaskRecord, StaleTaskRecoveryResult
    from litestar_queues.task import TaskResult

pytestmark = pytest.mark.anyio


async def test_worker_run_once_processes_pending_local_task() -> "None":
    @task("tasks.worker")
    async def worker_task(value: "int") -> "int":
        return value + 1

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        result = await service.enqueue(worker_task, 41)
        worker = Worker(service)

        assert await worker.run_once() == 1
        await result.wait(timeout=1, poll_interval=0.01)

    assert result.status == "completed"
    assert result.result == 42


async def test_worker_retries_failed_task_until_success() -> "None":
    attempts = 0

    @task("tasks.flaky", retries=1)
    async def flaky() -> "str":
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            msg = "not yet"
            raise RuntimeError(msg)
        return "ok"

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        result = await service.enqueue(flaky)
        worker = Worker(service)

        assert await worker.run_once() == 1
        await _wait_for_record_status(result, "pending")

        assert await worker.run_once() == 1
        await result.wait(timeout=1, poll_interval=0.01)

    assert attempts == 2
    completed_status = result.status
    assert completed_status == "completed"
    assert result.result == "ok"


async def test_worker_non_retryable_failure_skips_retries_and_injects_job_id() -> "None":
    captured_job_id: "str | None" = None

    @task("tasks.permanent", retries=3)
    async def permanent_failure(*, _job_id: "str") -> "None":
        nonlocal captured_job_id
        captured_job_id = _job_id
        non_retryable("permanent failure")

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        result = await service.enqueue(permanent_failure)
        worker = Worker(service)

        assert await worker.run_once() == 1
        await result.wait(timeout=1, poll_interval=0.01)

    assert captured_job_id == str(result.id)
    assert result.status == "failed"
    assert result.error == "permanent failure"
    assert result.record is not None
    assert result.record.retry_count == 0


async def test_worker_processes_batch_with_configured_concurrency() -> "None":
    started = 0
    both_started = asyncio.Event()
    release = asyncio.Event()

    @task("tasks.concurrent")
    async def concurrent_task(value: "int") -> "int":
        nonlocal started
        started += 1
        if started == 2:
            both_started.set()
        await release.wait()
        return value

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        first = await service.enqueue(concurrent_task, 1)
        second = await service.enqueue(concurrent_task, 2)
        worker = Worker(service, batch_size=2, max_concurrency=2)

        run_once = asyncio.create_task(worker.run_once())
        await asyncio.wait_for(both_started.wait(), timeout=1)
        if not run_once.done():
            release.set()
            await asyncio.wait_for(run_once, timeout=1)
            pytest.fail("run_once should return after scheduling claimed records")
        assert await run_once == 2
        release.set()
        await first.wait(timeout=1, poll_interval=0.01)
        await second.wait(timeout=1, poll_interval=0.01)

    assert first.status == "completed"
    assert second.status == "completed"


async def test_worker_claims_local_records_through_claim_next() -> "None":
    backend = _ClaimNextRecordingInMemoryQueueBackend()

    @task("tasks.capacity")
    async def capacity(value: "int") -> "int":
        return value

    async with QueueService(QueueConfig(execution_backend="local"), queue_backend=backend) as service:
        for value in range(5):
            await service.enqueue(capacity, value)
        worker = Worker(service, batch_size=10, max_concurrency=2)

        assert await worker.run_once() == 2

    assert backend.claim_next_calls == [(None, "local"), (None, "local")]
    assert backend.list_pending_calls == []
    assert backend.claim_task_calls == []


async def test_worker_queue_filter_restricts_claimed_records() -> "None":
    @task("tasks.filtered")
    async def filtered(value: "str") -> "str":
        return value

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        default_result = await service.enqueue(filtered, "default")
        priority_result = await service.enqueue(filtered.using(queue="priority"), "priority")
        worker = Worker(service, queues=("priority",))

        assert await worker.run_once() == 1
        await priority_result.wait(timeout=1, poll_interval=0.01)
        await default_result.refresh()
        await priority_result.refresh()

    assert default_result.status == "pending"
    assert priority_result.status == "completed"


async def test_worker_start_refills_open_slots_without_waiting_for_slow_batch_member() -> "None":
    slow_started = asyncio.Event()
    release_slow = asyncio.Event()
    fast_values: "list[int]" = []
    fast_tasks_finished = asyncio.Event()

    @task("tasks.refill")
    async def refill(value: "int") -> "int":
        if value == 0:
            slow_started.set()
            await release_slow.wait()
            return value
        fast_values.append(value)
        if sorted(fast_values) == [1, 2]:
            fast_tasks_finished.set()
        return value

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        slow = await service.enqueue(refill, 0)
        first_fast = await service.enqueue(refill, 1)
        second_fast = await service.enqueue(refill, 2)
        worker = Worker(service, batch_size=2, max_concurrency=2, poll_interval=0.01)
        worker_task = asyncio.create_task(worker.start())
        refilled = False

        try:
            await asyncio.wait_for(slow_started.wait(), timeout=1)
            await asyncio.wait_for(fast_tasks_finished.wait(), timeout=0.5)
            assert release_slow.is_set() is False
            refilled = True
        finally:
            release_slow.set()
            if not refilled:
                await worker.stop(force=True)
                with suppress(asyncio.CancelledError):
                    await asyncio.wait_for(worker_task, timeout=1)

        await slow.wait(timeout=1, poll_interval=0.01)
        await first_fast.wait(timeout=1, poll_interval=0.01)
        await second_fast.wait(timeout=1, poll_interval=0.01)
        await worker.stop()
        with suppress(asyncio.CancelledError):
            await asyncio.wait_for(worker_task, timeout=1)

    assert slow.status == "completed"
    assert first_fast.status == "completed"
    assert second_fast.status == "completed"


async def test_worker_start_logs_and_continues_after_transient_loop_error(caplog: "pytest.LogCaptureFixture") -> "None":
    recovered = asyncio.Event()
    async with QueueService(QueueConfig(execution_backend="local")) as service:
        worker = _TransientRunOnceWorker(service, recovered=recovered, poll_interval=0.01)

        with caplog.at_level(logging.ERROR, logger="litestar_queues.worker"):
            await worker.start()

    assert worker.run_once_calls == 2
    assert recovered.is_set()
    assert "Queue worker loop iteration failed" in caplog.text


async def test_worker_start_wakes_from_backend_notifications() -> "None":
    @task("tasks.notified_worker")
    async def notified_worker(value: "int") -> "int":
        return value + 1

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        worker = Worker(service, poll_interval=60)
        worker_task = asyncio.create_task(worker.start())
        await asyncio.sleep(0)

        result = await service.enqueue(notified_worker, 41)
        await result.wait(timeout=1, poll_interval=0.01)

        await worker.stop()
        with suppress(asyncio.CancelledError):
            await asyncio.wait_for(worker_task, timeout=1)

    assert result.status == "completed"
    assert result.result == 42


async def test_worker_periodic_requeue_calls_backend_on_cadence() -> "None":
    backend = _CountingInMemoryQueueBackend()
    async with QueueService(QueueConfig(execution_backend="local"), queue_backend=backend) as service:
        worker = Worker(service, stale_after=timedelta(seconds=30), stale_check_interval=0.0)

        await worker._maybe_requeue_stale()
        await worker._maybe_requeue_stale()

    assert len(backend.requeue_calls) >= 2
    assert backend.requeue_calls[0] == timedelta(seconds=30)


async def test_worker_skips_periodic_requeue_when_stale_after_is_none() -> "None":
    backend = _CountingInMemoryQueueBackend()
    async with QueueService(QueueConfig(execution_backend="local"), queue_backend=backend) as service:
        worker = Worker(service)  # stale_after defaults to None

        for _ in range(3):
            await worker._maybe_requeue_stale()

    assert backend.requeue_calls == []


async def test_worker_periodic_requeue_respects_cadence_window() -> "None":
    backend = _CountingInMemoryQueueBackend()
    async with QueueService(QueueConfig(execution_backend="local"), queue_backend=backend) as service:
        worker = Worker(
            service,
            stale_after=timedelta(seconds=30),
            stale_check_interval=3600.0,  # one hour: only the first call fires
        )

        await worker._maybe_requeue_stale()
        await worker._maybe_requeue_stale()
        await worker._maybe_requeue_stale()

    assert len(backend.requeue_calls) == 1


async def test_worker_periodic_requeue_uses_backend_fleet_lock() -> "None":
    backend = _LockingCountingInMemoryQueueBackend()
    async with QueueService(QueueConfig(execution_backend="local"), queue_backend=backend) as service:
        first = Worker(service, stale_after=timedelta(seconds=30), stale_check_interval=0.0, worker_id="worker-1")
        second = Worker(service, stale_after=timedelta(seconds=30), stale_check_interval=0.0, worker_id="worker-2")

        await first._maybe_requeue_stale()
        await second._maybe_requeue_stale()

    assert backend.lock_calls == ["stale_recovery", "stale_recovery"]
    assert len(backend.requeue_calls) == 1


async def test_worker_periodic_reconcile_skips_calls_inside_cadence_window() -> "None":
    backend = _CountingInMemoryQueueBackend()
    async with QueueService(QueueConfig(execution_backend="local"), queue_backend=backend) as service:
        worker = Worker(service, reconcile_interval=3600.0)

        await worker._maybe_reconcile_external()
        await worker._maybe_reconcile_external()
        await worker._maybe_reconcile_external()

    assert backend.list_running_external_calls == 1


async def test_worker_periodic_reconcile_uses_backend_fleet_lock() -> "None":
    backend = _LockingCountingInMemoryQueueBackend()
    async with QueueService(QueueConfig(execution_backend="local"), queue_backend=backend) as service:
        first = Worker(service, reconcile_interval=0.0, worker_id="worker-1")
        second = Worker(service, reconcile_interval=0.0, worker_id="worker-2")

        await first._maybe_reconcile_external()
        await second._maybe_reconcile_external()

    assert backend.lock_calls == ["external_reconcile", "external_reconcile"]
    assert backend.list_running_external_calls == 1


async def test_worker_reconcile_external_skips_unknown_backend_names(caplog: "pytest.LogCaptureFixture") -> "None":
    backend = InMemoryQueueBackend()
    async with QueueService(QueueConfig(execution_backend="local"), queue_backend=backend) as service:
        record = await backend.enqueue("tasks.remote", execution_backend="missing-backend")
        claimed = await backend.claim_task(record.id)
        assert claimed is not None
        await backend.set_execution_ref(claimed.id, "missing-backend", "jobs/missing")
        worker = Worker(service)

        with caplog.at_level(logging.WARNING, logger="litestar_queues.worker"):
            assert await worker.reconcile_external() == 0

    assert "Skipping external queue record with unknown execution backend" in caplog.text


async def test_worker_default_worker_id_uses_pid() -> "None":
    async with QueueService(QueueConfig()) as service:
        worker = Worker(service)

    assert worker.worker_id == f"worker-{os.getpid()}"


async def test_worker_explicit_worker_id_overrides_default() -> "None":
    async with QueueService(QueueConfig()) as service:
        worker = Worker(service, worker_id="worker-alpha-7")

    assert worker.worker_id == "worker-alpha-7"


async def test_worker_heartbeat_miss_threshold_comes_from_config() -> "None":
    config = QueueConfig()
    assert config.worker_heartbeat_miss_threshold == 2

    custom_config = QueueConfig(worker_heartbeat_miss_threshold=3)
    assert custom_config.worker_heartbeat_miss_threshold == 3

    async with QueueService(custom_config) as service:
        worker = Worker(service, heartbeat_miss_threshold=custom_config.worker_heartbeat_miss_threshold)

    assert worker._heartbeat_manager._miss_threshold == 3


async def test_plugin_worker_uses_configured_heartbeat_miss_threshold() -> "None":
    plugin = QueuePlugin(
        QueueConfig(
            execution_backend="local", in_app_worker=True, worker_heartbeat_miss_threshold=5, worker_poll_interval=0.01
        )
    )
    app = Litestar(plugins=[plugin])

    async with AsyncTestClient(app=app):
        worker = app.state[plugin.config.queue_worker_state_key]

    assert worker._heartbeat_manager._miss_threshold == 5


async def test_execute_claimed_registers_and_unregisters() -> "None":
    events: "list[tuple[object, ...]]" = []
    backend = _HeartbeatCleanupRecordingBackend(events)

    @task("tasks.heartbeat_cleanup")
    async def heartbeat_cleanup() -> "str":
        return "ok"

    async with QueueService(QueueConfig(execution_backend="local"), queue_backend=backend) as service:
        result = await service.enqueue(heartbeat_cleanup)
        worker = Worker(service)
        worker._heartbeat_manager = _SpyHeartbeatManager(events)

        assert await worker.run_once() == 1
        await result.wait(timeout=1, poll_interval=0.01)

    assert events == [("register", result.id, 0), ("unregister", result.id), ("null_heartbeats", (result.id,), 0)]


async def test_worker_start_stop_manages_tick() -> "None":
    async with QueueService(QueueConfig(execution_backend="local")) as service:
        worker = Worker(service, poll_interval=60)
        worker_task = asyncio.create_task(worker.start())

        try:
            await _wait_for_heartbeat_manager_task(worker)
            manager_task = worker._heartbeat_manager._task
            assert manager_task is not None
            assert manager_task.done() is False

            await worker.stop()
            await asyncio.wait_for(worker_task, timeout=1)
        finally:
            if not worker_task.done():
                await worker.stop(force=True)
                with suppress(asyncio.CancelledError, asyncio.TimeoutError):
                    await asyncio.wait_for(worker_task, timeout=1)

    assert manager_task.done() is True


def test_no_per_task_heartbeat_coroutine() -> "None":
    assert not hasattr(Worker, "_heartbeat")


async def test_run_once_starts_heartbeat_tick_for_standalone_execution() -> "None":
    started = asyncio.Event()
    release = asyncio.Event()
    backend = _HeartbeatTouchRecordingBackend()

    @task("tasks.standalone_heartbeat")
    async def standalone_heartbeat() -> "str":
        started.set()
        await release.wait()
        return "ok"

    async with QueueService(QueueConfig(execution_backend="local"), queue_backend=backend) as service:
        result = await service.enqueue(standalone_heartbeat)
        worker = Worker(service, heartbeat_interval=0.01)

        assert await worker.run_once() == 1
        await asyncio.wait_for(started.wait(), timeout=1)
        await asyncio.wait_for(backend.touch_recorded.wait(), timeout=1)

        release.set()
        await result.wait(timeout=1, poll_interval=0.01)
        await _wait_for_worker_tasks_done(worker)

    assert backend.touch_calls
    assert backend.touch_calls[0][0].task_id == result.id
    assert worker._heartbeat_manager._task is not None
    assert worker._heartbeat_manager._task.done() is True


async def test_worker_stop_keeps_heartbeat_manager_running_until_drain_completes() -> "None":
    started = asyncio.Event()
    release = asyncio.Event()
    backend = _HeartbeatTouchRecordingBackend()

    @task("tasks.drain_heartbeat")
    async def drain_heartbeat() -> "str":
        started.set()
        await release.wait()
        return "ok"

    async with QueueService(QueueConfig(execution_backend="local"), queue_backend=backend) as service:
        result = await service.enqueue(drain_heartbeat)
        worker = Worker(service, heartbeat_interval=0.01, poll_interval=0.01)
        worker_task = asyncio.create_task(worker.start())

        await asyncio.wait_for(started.wait(), timeout=1)
        stop_task = asyncio.create_task(worker.stop())
        await asyncio.sleep(0.05)
        manager_task = worker._heartbeat_manager._task
        assert manager_task is not None
        assert manager_task.done() is False
        assert stop_task.done() is False
        await asyncio.wait_for(backend.touch_recorded.wait(), timeout=1)

        release.set()
        await asyncio.wait_for(stop_task, timeout=1)
        await asyncio.wait_for(worker_task, timeout=1)
        await result.refresh()

    assert result.status == "completed"
    assert manager_task.done() is True


async def test_execute_claimed_nulls_heartbeat_when_unregister_fails() -> "None":
    events: "list[tuple[object, ...]]" = []
    backend = _HeartbeatCleanupRecordingBackend(events)

    @task("tasks.unregister_failure_cleanup")
    async def unregister_failure_cleanup() -> "str":
        return "ok"

    async with QueueService(QueueConfig(execution_backend="local"), queue_backend=backend) as service:
        result = await service.enqueue(unregister_failure_cleanup)
        worker = Worker(service)
        worker._heartbeat_manager = _FailingUnregisterHeartbeatManager(events)

        assert await worker.run_once() == 1
        await result.wait(timeout=1, poll_interval=0.01)
        await _wait_for_worker_tasks_done(worker)

    assert result.status == "completed"
    assert events == [("register", result.id, 0), ("unregister", result.id), ("null_heartbeats", (result.id,), 0)]


async def test_worker_id_propagates_into_published_events() -> "None":
    sink = InMemoryQueueEventSink()

    @task("tasks.worker_id_event")
    async def worker_id_task() -> "str":
        return "ok"

    async with QueueService(
        QueueConfig(execution_backend="local", event=EventConfig(enabled=True, sink=sink))
    ) as service:
        result = await service.enqueue(worker_id_task)
        worker = Worker(service, worker_id="worker-test")

        assert await worker.run_once() == 1
        await result.wait(timeout=1, poll_interval=0.01)

    assert sink.events, "Expected lifecycle events to be published"
    lifecycle_types = {"task.started", "task.completed", "task.failed"}
    lifecycle_events = [event for event in sink.events if event.type in lifecycle_types]
    assert lifecycle_events, "Expected at least one lifecycle event"
    for event in lifecycle_events:
        assert event.worker_id == "worker-test"


async def test_worker_stop_cancels_stuck_task_after_drain_timeout() -> "None":
    started = asyncio.Event()
    cancelled = asyncio.Event()

    @task("tasks.stuck")
    async def stuck() -> "None":
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        result = await service.enqueue(stuck)
        worker = Worker(service, graceful_shutdown_timeout=0.01, final_cancel_timeout=0.2)
        worker_task = asyncio.create_task(worker.start())
        await asyncio.wait_for(started.wait(), timeout=1)

        await worker.stop()
        await asyncio.wait_for(worker_task, timeout=1)
        await result.refresh()

    assert cancelled.is_set()
    assert result.status == "running"


async def test_plugin_shutdown_waits_for_in_flight_worker_task() -> "None":
    started = asyncio.Event()
    release = asyncio.Event()

    @task("tasks.plugin_drain")
    async def plugin_drain() -> "str":
        started.set()
        await release.wait()
        return "ok"

    plugin = QueuePlugin(
        QueueConfig(
            execution_backend="local", in_app_worker=True, worker_poll_interval=0.01, worker_graceful_shutdown_timeout=1
        )
    )
    app = Litestar(plugins=[plugin])

    async with AsyncTestClient(app=app):
        service = app.state[plugin.config.queue_service_state_key]
        result = await service.enqueue(plugin_drain)
        await asyncio.wait_for(started.wait(), timeout=1)
        release.set()

    await result.refresh()
    assert result.status == "completed"


async def test_plugin_logs_when_in_app_worker_task_dies(monkeypatch: "pytest.MonkeyPatch") -> "None":
    from litestar_queues import plugin as plugin_module

    messages: "list[tuple[str, dict[str, object]]]" = []

    def log_error(message: "str", **kwargs: "object") -> "None":
        messages.append((message, kwargs))

    monkeypatch.setattr(plugin_module, "Worker", _FailingWorker)
    monkeypatch.setattr(plugin_module.logger, "error", log_error)
    plugin = QueuePlugin(QueueConfig(in_app_worker=True, execution_backend="local"))
    app = Litestar(plugins=[plugin])

    await plugin._on_startup(app)
    await asyncio.sleep(0)

    if plugin._service is not None:
        await plugin._service.close()

    assert messages
    assert messages[0][0] == "In-app queue worker stopped unexpectedly"
    assert "exc_info" in messages[0][1]


async def test_sync_task_uses_configured_executor_and_preserves_task_context() -> "None":
    @task("tasks.sync_context")
    def sync_context(*, _job_id: "str") -> "dict[str, str | None]":
        context = get_current_task_context()
        assert context is not None
        return {"job_id": _job_id, "context_task_id": context.task_id, "thread_name": threading.current_thread().name}

    async with QueueService(
        QueueConfig(execution_backend="immediate", sync_executor_max_workers=1, sync_executor_thread_name_prefix="lq")
    ) as service:
        result = await service.enqueue(sync_context)

    assert isinstance(result.result, dict)
    assert result.result["job_id"] == result.result["context_task_id"]
    assert str(result.result["thread_name"]).startswith("lq")


async def _wait_for_record_status(result: "TaskResult", expected_status: "str", *, timeout: "float" = 1.0) -> "None":
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        await result.refresh()
        if result.status == expected_status:
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"record {result.id} did not reach {expected_status!r}; status={result.status!r}")


async def _wait_for_heartbeat_manager_task(worker: "Worker", *, timeout: "float" = 1.0) -> "None":
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if hasattr(worker, "_heartbeat_manager") and worker._heartbeat_manager._task is not None:
            return
        await asyncio.sleep(0.01)
    pytest.fail("heartbeat manager task was not started")


async def _wait_for_worker_tasks_done(worker: "Worker", *, timeout: "float" = 1.0) -> "None":
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if not worker._running_tasks:
            return
        await asyncio.sleep(0.01)
    pytest.fail("worker tasks did not finish")


class _ClaimNextRecordingInMemoryQueueBackend(InMemoryQueueBackend):
    __slots__ = ("claim_next_calls", "claim_task_calls", "list_pending_calls")

    def __init__(self) -> "None":
        super().__init__()
        self.claim_next_calls: "list[tuple[str | None, str | None]]" = []
        self.claim_task_calls: "list[object]" = []
        self.list_pending_calls: "list[tuple[int, str | None, str | None]]" = []

    async def claim_next(
        self, *, queue: "str | None" = None, execution_backend: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        self.claim_next_calls.append((queue, execution_backend))
        records = await InMemoryQueueBackend.list_pending(
            self, limit=1, queue=queue, execution_backend=execution_backend
        )
        if not records:
            return None
        return await InMemoryQueueBackend.claim_task(self, records[0].id)

    async def claim_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        self.claim_task_calls.append(task_id)
        return await super().claim_task(task_id)

    async def list_pending(
        self, *, limit: "int" = 1, queue: "str | None" = None, execution_backend: "str | None" = None
    ) -> 'list["QueuedTaskRecord"]':
        self.list_pending_calls.append((limit, queue, execution_backend))
        return await super().list_pending(limit=limit, queue=queue, execution_backend=execution_backend)


class _HeartbeatCleanupRecordingBackend(InMemoryQueueBackend):
    __slots__ = ("events",)

    def __init__(self, events: "list[tuple[object, ...]]") -> "None":
        super().__init__()
        self.events = events

    async def null_heartbeats(self, task_ids: "list[UUID]", *, expected_retry_count: "int | None" = None) -> "None":
        self.events.append(("null_heartbeats", tuple(task_ids), expected_retry_count))
        await super().null_heartbeats(task_ids, expected_retry_count=expected_retry_count)


class _HeartbeatTouchRecordingBackend(InMemoryQueueBackend):
    __slots__ = ("touch_calls", "touch_recorded")

    def __init__(self) -> "None":
        super().__init__()
        self.touch_calls: "list[tuple[HeartbeatTouch, ...]]" = []
        self.touch_recorded = asyncio.Event()

    async def touch_heartbeats(self, touches: "Sequence[HeartbeatTouch]") -> "HeartbeatTouchResult":
        self.touch_calls.append(tuple(touches))
        self.touch_recorded.set()
        return await super().touch_heartbeats(touches)


class _CountingInMemoryQueueBackend(InMemoryQueueBackend):
    __slots__ = ("list_running_external_calls", "requeue_calls")

    def __init__(self) -> "None":
        super().__init__()
        self.requeue_calls: "list[timedelta]" = []
        self.list_running_external_calls = 0

    async def list_running_external(self, *, limit: "int | None" = None) -> 'list["QueuedTaskRecord"]':
        self.list_running_external_calls += 1
        return await super().list_running_external(limit=limit)

    async def requeue_stale_running(self, *, stale_after: "timedelta") -> "StaleTaskRecoveryResult":
        self.requeue_calls.append(stale_after)
        return await super().requeue_stale_running(stale_after=stale_after)


class _LockingCountingInMemoryQueueBackend(_CountingInMemoryQueueBackend):
    __slots__ = ("_held_locks", "lock_calls")

    def __init__(self) -> "None":
        super().__init__()
        self._held_locks: "set[str]" = set()
        self.lock_calls: "list[str]" = []

    async def acquire_worker_lock(self, name: "str", *, ttl: "timedelta") -> "bool":
        del ttl
        self.lock_calls.append(name)
        if name in self._held_locks:
            return False
        self._held_locks.add(name)
        return True


class _FailingWorker:
    __slots__ = ()

    def __init__(self, *_args: "object", **_kwargs: "object") -> "None":
        pass

    async def start(self) -> "None":
        msg = "worker boom"
        raise RuntimeError(msg)

    async def stop(self, *, force: "bool" = False) -> "bool":
        return False


class _SpyHeartbeatManager:
    __slots__ = ("_registrations", "_task", "events")

    def __init__(self, events: "list[tuple[object, ...]]") -> "None":
        self.events = events
        self._registrations: "dict[UUID, None]" = {}
        self._task: "asyncio.Task[None] | None" = None

    @property
    def has_registrations(self) -> "bool":
        return bool(self._registrations)

    def register(self, task_id: "UUID", *, expected_retry_count: "int | None") -> "None":
        self._registrations[task_id] = None
        self.events.append(("register", task_id, expected_retry_count))

    def unregister(self, task_id: "UUID") -> "None":
        self._registrations.pop(task_id, None)
        self.events.append(("unregister", task_id))

    async def start(self) -> "None":
        return None

    async def aclose(self) -> "None":
        return None


class _FailingUnregisterHeartbeatManager(_SpyHeartbeatManager):
    __slots__ = ()

    def unregister(self, task_id: "UUID") -> "None":
        self._registrations.pop(task_id, None)
        self.events.append(("unregister", task_id))
        msg = "heartbeat unregister failed"
        raise RuntimeError(msg)


class _TransientRunOnceWorker(Worker):
    __slots__ = ("recovered", "run_once_calls")

    def __init__(self, service: "QueueService", *, recovered: "asyncio.Event", poll_interval: "float") -> "None":
        super().__init__(service, poll_interval=poll_interval)
        self.recovered = recovered
        self.run_once_calls = 0

    async def run_once(self) -> "int":
        self.run_once_calls += 1
        if self.run_once_calls == 1:
            msg = "transient backend failure"
            raise RuntimeError(msg)
        self.recovered.set()
        await self.stop()
        return 0

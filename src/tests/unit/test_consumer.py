"""Unit tests for the click-free external-executor consumer core."""

import asyncio
import sys
from types import ModuleType
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from litestar_queues import WorkerConfig
from litestar_queues.backends import InMemoryQueueBackend

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime
    from uuid import UUID

    from litestar_queues.models import HeartbeatTouch, HeartbeatTouchResult, QueuedTaskRecord
    from litestar_queues.service import QueueService

pytestmark = pytest.mark.anyio


class _NoopServiceContext:
    def __init__(self, service: "QueueService") -> "None":
        self.service = service

    async def __aenter__(self) -> "QueueService":
        return self.service

    async def __aexit__(self, *_exc_info: object) -> "None":
        return None


class _RecordingHeartbeatBackend(InMemoryQueueBackend):
    __slots__ = ("touch_calls",)

    def __init__(self) -> "None":
        super().__init__()
        self.touch_calls: "list[tuple[HeartbeatTouch, ...]]" = []

    async def touch_heartbeats(self, touches: "Sequence[HeartbeatTouch]") -> "HeartbeatTouchResult":
        self.touch_calls.append(tuple(touches))
        return await super().touch_heartbeats(touches)


class _BeatDetailRecordingBackend(InMemoryQueueBackend):
    """Records every heartbeat touch and signals once one carries beat detail."""

    __slots__ = ("beat_delivered", "touch_calls")

    def __init__(self) -> "None":
        super().__init__()
        self.touch_calls: "list[tuple[HeartbeatTouch, ...]]" = []
        self.beat_delivered = asyncio.Event()

    async def touch_heartbeats(self, touches: "Sequence[HeartbeatTouch]") -> "HeartbeatTouchResult":
        self.touch_calls.append(tuple(touches))
        result = await super().touch_heartbeats(touches)
        if any(touch.metadata_patch for touch in touches):
            self.beat_delivered.set()
        return result


class _MultiTouchRecordingBackend(InMemoryQueueBackend):
    """Records heartbeat touches and signals once at least ``required`` occurred."""

    __slots__ = ("_required", "enough_touches", "touch_calls")

    def __init__(self, *, required: "int") -> "None":
        super().__init__()
        self.touch_calls: "list[tuple[HeartbeatTouch, ...]]" = []
        self.enough_touches = asyncio.Event()
        self._required = required

    async def touch_heartbeats(self, touches: "Sequence[HeartbeatTouch]") -> "HeartbeatTouchResult":
        self.touch_calls.append(tuple(touches))
        if len(self.touch_calls) >= self._required:
            self.enough_touches.set()
        return await super().touch_heartbeats(touches)


class _DelayedClaimBackend(InMemoryQueueBackend):
    async def claim_task_with_expired(
        self, task_id: "UUID", *, expected_retry_count: "int | None" = None, expected_execution_ref: "str | None" = None
    ) -> "tuple[QueuedTaskRecord | None, QueuedTaskRecord | None]":
        await asyncio.sleep(0.1)
        return await super().claim_task_with_expired(
            task_id, expected_retry_count=expected_retry_count, expected_execution_ref=expected_execution_ref
        )


async def test_consume_one_claims_and_executes_persisted_record() -> "None":
    from litestar_queues import QueueConfig, QueueService, task
    from litestar_queues.consumer import TaskExitCode, consume_one

    @task("tasks.consumer")
    async def consumer_task(value: "int") -> "int":
        return value + 1

    queue_backend = InMemoryQueueBackend()
    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="cloudrun"),
        queue_backend=queue_backend,
    ) as service:
        result = await service.enqueue(consumer_task.using(execution_backend="cloudrun"), 41)
        record = await queue_backend.get_task(result.id)
        assert record is not None
        exit_code = await consume_one(service, record.id)
        await result.refresh()

    assert exit_code == TaskExitCode.SUCCESS
    assert result.status == "completed"
    assert result.result == 42


async def test_consume_one_rejects_stale_external_attempt() -> "None":
    from litestar_queues import QueueConfig, QueueService, task
    from litestar_queues.consumer import TaskExitCode, consume_one

    @task("tasks.fenced-consumer")
    async def fenced_consumer() -> "None":
        return None

    queue_backend = InMemoryQueueBackend()
    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="cloudrun"),
        queue_backend=queue_backend,
    ) as service:
        result = await service.enqueue(fenced_consumer.using(execution_backend="cloudrun"))
        reserved = await queue_backend.reserve_external_dispatch(
            result.id, "cloudrun", "sqs:1:1000:attempt", expected_retry_count=0
        )
        assert reserved is not None
        exit_code = await consume_one(
            service, result.id, expected_retry_count=1, expected_execution_ref="sqs:1:1000:attempt"
        )

    assert exit_code == TaskExitCode.CLAIM_LOST


async def test_consume_one_publishes_expiration_when_deadline_crosses_during_claim() -> "None":
    from litestar_queues import (
        EventDeliveryConfig,
        InMemoryQueueEventSink,
        QueueConfig,
        QueueEventsConfig,
        QueueService,
        task,
    )
    from litestar_queues.consumer import TaskExitCode, consume_one

    @task("tasks.consumer_claim_expiry")
    async def consumer_claim_expiry() -> "None":
        return None

    sink = InMemoryQueueEventSink()
    queue_backend = _DelayedClaimBackend()
    async with QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            execution_backend="cloudrun",
            events=QueueEventsConfig(delivery=EventDeliveryConfig(sinks=(sink,))),
        ),
        queue_backend=queue_backend,
    ) as service:
        result = await service.enqueue(consumer_claim_expiry.using(execution_backend="cloudrun"), expires_in=0.05)
        exit_code = await consume_one(service, result.id)
        stored = await queue_backend.get_task(result.id)

    assert exit_code == TaskExitCode.CLAIM_LOST
    assert stored is not None
    assert stored.status == "expired"
    assert [event.type for event in sink.events] == ["task.expired"]


async def test_consume_one_claims_dispatched_record_after_queue_deadline() -> "None":
    from litestar_queues import QueueConfig, QueueService, task
    from litestar_queues.consumer import TaskExitCode, consume_one

    @task("tasks.consumer_dispatched_before_expiry")
    async def consumer_dispatched_before_expiry() -> "str":
        return "completed"

    queue_backend = InMemoryQueueBackend()
    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="cloudrun"),
        queue_backend=queue_backend,
    ) as service:
        result = await service.enqueue(
            consumer_dispatched_before_expiry.using(execution_backend="cloudrun"), expires_in=0.5
        )
        await queue_backend.set_execution_ref(result.id, "cloudrun", "operations/accepted-before-deadline")
        await asyncio.sleep(0.55)

        exit_code = await consume_one(service, result.id)
        await result.refresh()

    assert exit_code == TaskExitCode.SUCCESS
    assert result.status == "completed"
    assert result.result == "completed"


async def test_run_task_loads_factory_before_neutral_task_id() -> "None":
    from litestar_queues import QueueConfig, QueueService, task
    from litestar_queues.consumer import TaskExitCode, run_task
    from litestar_queues.execution.cloudrun import CloudRunExecutionConfig

    @task("tasks.consumer_prefixed")
    async def consumer_prefixed(value: "int") -> "int":
        return value + 1

    queue_backend = InMemoryQueueBackend()
    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend=CloudRunExecutionConfig(project_id="test-project", job_name="worker", env_prefix="PREFIX"),
    )
    factory_module = ModuleType("consumer_test_config_factory")
    sys.modules[factory_module.__name__] = factory_module
    try:
        async with QueueService(config, queue_backend=queue_backend) as service:
            factory_module.create_service = lambda: _NoopServiceContext(service)  # type: ignore[attr-defined]
            result = await service.enqueue(consumer_prefixed.using(execution_backend="cloudrun"), 41)
            record = await queue_backend.get_task(result.id)
            assert record is not None

            exit_code = await run_task(
                env={
                    "QUEUES_CONFIG_FACTORY": f"{factory_module.__name__}:create_service",
                    "QUEUES_TASK_ID": str(record.id),
                }
            )
            await result.refresh()
    finally:
        sys.modules.pop(factory_module.__name__, None)

    assert exit_code == TaskExitCode.SUCCESS
    assert result.status == "completed"
    assert result.result == 42


async def test_run_task_requires_config_factory() -> "None":
    from litestar_queues.consumer import TaskExitCode, run_task

    exit_code = await run_task(env={"QUEUES_TASK_ID": str(uuid4())})

    assert exit_code == TaskExitCode.MISSING_CONFIG_FACTORY


async def test_consume_one_returns_claim_lost_when_heartbeat_loses_ownership() -> "None":
    from litestar_queues import QueueConfig, QueueService, task
    from litestar_queues.consumer import TaskExitCode, consume_one

    heartbeat_seen = asyncio.Event()
    release_task = asyncio.Event()
    task_id: "UUID | None" = None

    @task("tasks.consumer_claim_lost")
    async def consumer_claim_lost() -> "str":
        assert task_id is not None
        stored = await queue_backend.get_task(task_id)
        assert stored is not None
        stored.status = "pending"
        stored.retry_count += 1
        stored.started_at = None
        stored.heartbeat_at = None
        heartbeat_seen.set()
        await release_task.wait()
        return "too late"

    queue_backend = _RecordingHeartbeatBackend()
    async with QueueService(
        QueueConfig(
            queue_backend="memory",
            execution_backend="cloudrun",
            worker=WorkerConfig(placement="external", heartbeat_interval=0.01),
        ),
        queue_backend=queue_backend,
    ) as service:
        result = await service.enqueue(consumer_claim_lost.using(execution_backend="cloudrun"), retries=1)
        task_id = result.id
        record = await queue_backend.get_task(result.id)
        assert record is not None
        runner = asyncio.create_task(consume_one(service, record.id))
        await asyncio.wait_for(heartbeat_seen.wait(), timeout=1)
        try:
            exit_code = await runner
        finally:
            release_task.set()
        stored = await queue_backend.get_task(result.id)

    assert exit_code == TaskExitCode.CLAIM_LOST
    assert stored is not None
    assert stored.status == "pending"
    assert stored.retry_count == 1
    assert len(queue_backend.touch_calls) == 1
    assert queue_backend.touch_calls[0][0].task_id == result.id
    assert queue_backend.touch_calls[0][0].expected_retry_count == 0


async def test_run_task_missing_and_invalid_task_id() -> "None":
    from litestar_queues import QueueConfig, QueueService
    from litestar_queues.consumer import TaskExitCode, run_task

    async with QueueService(QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory")) as service:
        missing = await run_task(service=service, env={})
        invalid = await run_task(service=service, env={"QUEUES_TASK_ID": "not-a-uuid"})

    assert missing == TaskExitCode.MISSING_TASK_ID
    assert invalid == TaskExitCode.INVALID_TASK_ID


async def test_consume_one_delivers_beat_detail_on_next_heartbeat_touch() -> "None":
    from litestar_queues import QueueConfig, QueueService, beat, task
    from litestar_queues.consumer import TaskExitCode, consume_one

    ready = asyncio.Event()
    release = asyncio.Event()

    @task("tasks.consumer_beat_detail")
    async def consumer_beat_detail() -> "str":
        beat("phase one")
        ready.set()
        await release.wait()
        return "ok"

    queue_backend = _BeatDetailRecordingBackend()
    async with QueueService(
        QueueConfig(
            queue_backend="memory",
            execution_backend="cloudrun",
            worker=WorkerConfig(placement="external", heartbeat_interval=0.01),
        ),
        queue_backend=queue_backend,
    ) as service:
        result = await service.enqueue(consumer_beat_detail.using(execution_backend="cloudrun"))
        record = await queue_backend.get_task(result.id)
        assert record is not None
        runner = asyncio.create_task(consume_one(service, record.id))
        await asyncio.wait_for(ready.wait(), timeout=1)
        await asyncio.wait_for(queue_backend.beat_delivered.wait(), timeout=1)
        release.set()
        exit_code = await runner
        stored = await queue_backend.get_task(result.id)

    delivered = [touch.metadata_patch for calls in queue_backend.touch_calls for touch in calls if touch.metadata_patch]
    assert exit_code == TaskExitCode.SUCCESS
    assert delivered == [{"progress_detail": "phase one"}]
    assert stored is not None
    assert stored.status == "completed"


async def test_consume_one_beat_detail_is_last_value_wins_and_capped_at_256() -> "None":
    from litestar_queues import QueueConfig, QueueService, beat, task
    from litestar_queues.consumer import TaskExitCode, consume_one

    ready = asyncio.Event()
    release = asyncio.Event()

    @task("tasks.consumer_beat_overwrite")
    async def consumer_beat_overwrite() -> "str":
        beat("row 1")
        beat("x" * 500)
        ready.set()
        await release.wait()
        return "ok"

    queue_backend = _BeatDetailRecordingBackend()
    async with QueueService(
        QueueConfig(
            queue_backend="memory",
            execution_backend="cloudrun",
            worker=WorkerConfig(placement="external", heartbeat_interval=0.01),
        ),
        queue_backend=queue_backend,
    ) as service:
        result = await service.enqueue(consumer_beat_overwrite.using(execution_backend="cloudrun"))
        record = await queue_backend.get_task(result.id)
        assert record is not None
        runner = asyncio.create_task(consume_one(service, record.id))
        await asyncio.wait_for(ready.wait(), timeout=1)
        await asyncio.wait_for(queue_backend.beat_delivered.wait(), timeout=1)
        release.set()
        exit_code = await runner

    delivered = [touch.metadata_patch for calls in queue_backend.touch_calls for touch in calls if touch.metadata_patch]
    assert exit_code == TaskExitCode.SUCCESS
    assert delivered == [{"progress_detail": "x" * 256}]


async def test_consume_one_clears_beat_detail_after_successful_touch() -> "None":
    from litestar_queues import QueueConfig, QueueService, beat, task
    from litestar_queues.consumer import TaskExitCode, consume_one

    ready = asyncio.Event()
    release = asyncio.Event()

    @task("tasks.consumer_beat_clear")
    async def consumer_beat_clear() -> "str":
        beat("only once")
        ready.set()
        await release.wait()
        return "ok"

    queue_backend = _MultiTouchRecordingBackend(required=2)
    async with QueueService(
        QueueConfig(
            queue_backend="memory",
            execution_backend="cloudrun",
            worker=WorkerConfig(placement="external", heartbeat_interval=0.01),
        ),
        queue_backend=queue_backend,
    ) as service:
        result = await service.enqueue(consumer_beat_clear.using(execution_backend="cloudrun"))
        record = await queue_backend.get_task(result.id)
        assert record is not None
        runner = asyncio.create_task(consume_one(service, record.id))
        await asyncio.wait_for(ready.wait(), timeout=1)
        await asyncio.wait_for(queue_backend.enough_touches.wait(), timeout=1)
        release.set()
        exit_code = await runner

    assert exit_code == TaskExitCode.SUCCESS
    assert queue_backend.touch_calls[0][0].metadata_patch == {"progress_detail": "only once"}
    assert queue_backend.touch_calls[1][0].metadata_patch is None


async def test_consume_one_without_beat_calls_stays_healthy_across_intervals() -> "None":
    from litestar_queues import QueueConfig, QueueService, task
    from litestar_queues.consumer import TaskExitCode, consume_one

    started = asyncio.Event()
    release = asyncio.Event()

    @task("tasks.consumer_no_beat")
    async def consumer_no_beat() -> "str":
        started.set()
        await release.wait()
        return "ok"

    queue_backend = _MultiTouchRecordingBackend(required=3)
    async with QueueService(
        QueueConfig(
            queue_backend="memory",
            execution_backend="cloudrun",
            worker=WorkerConfig(placement="external", heartbeat_interval=0.01),
        ),
        queue_backend=queue_backend,
    ) as service:
        result = await service.enqueue(consumer_no_beat.using(execution_backend="cloudrun"))
        record = await queue_backend.get_task(result.id)
        assert record is not None
        runner = asyncio.create_task(consume_one(service, record.id))
        await asyncio.wait_for(started.wait(), timeout=1)
        await asyncio.wait_for(queue_backend.enough_touches.wait(), timeout=1)
        release.set()
        exit_code = await runner
        stored = await queue_backend.get_task(result.id)

    assert exit_code == TaskExitCode.SUCCESS
    assert len(queue_backend.touch_calls) >= 3
    assert all(touch.metadata_patch is None for calls in queue_backend.touch_calls for touch in calls)
    assert stored is not None
    assert stored.status == "completed"


class _RunningOnlyFailureBackend(InMemoryQueueBackend):
    """Refuses to fail a record nobody is holding.

    Every persistent backend behaves this way -- ``fail_task`` is a conditional
    update over a row that is still ``running`` -- and the in-memory backend is
    the one that does not, which is exactly why an ordering mistake here can
    pass the unit tier and strand records in production.
    """

    async def fail_task(
        self,
        task_id: "UUID",
        error: "str",
        *,
        retry: "bool" = True,
        expected_retry_count: "int | None" = None,
        retry_at: "datetime | None" = None,
        queued_at: "datetime | None" = None,
    ) -> "QueuedTaskRecord | None":
        current = await self.get_task(task_id)
        if current is None or current.status != "running":
            return None
        return await super().fail_task(
            task_id,
            error,
            retry=retry,
            expected_retry_count=expected_retry_count,
            retry_at=retry_at,
            queued_at=queued_at,
        )


async def test_an_unregistered_task_fails_durably_on_a_backend_that_needs_the_claim() -> "None":
    """A task name this process cannot resolve has to end the record, not skip it.

    Nothing polls a self-dispatching queue, so a record left pending here is a
    record that waits forever. The failure therefore has to be written under the
    claim, which means claiming before resolving the name.
    """
    from litestar_queues import QueueConfig, QueueService
    from litestar_queues.consumer import TaskExitCode, consume_one

    queue_backend = _RunningOnlyFailureBackend()
    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="cloudrun"),
        queue_backend=queue_backend,
    ) as service:
        record = await queue_backend.enqueue("tasks.never_registered", max_retries=0)
        exit_code = await consume_one(service, record.id)
        stored = await queue_backend.get_task(record.id)

    assert exit_code == TaskExitCode.UNKNOWN_TASK
    assert stored is not None
    assert stored.status == "failed"
    assert stored.is_terminal


async def test_an_unregistered_task_that_someone_else_claimed_first_is_not_failed() -> "None":
    """Losing the claim means another owner decides this attempt's outcome."""
    from litestar_queues import QueueConfig, QueueService
    from litestar_queues.consumer import TaskExitCode, consume_one

    queue_backend = InMemoryQueueBackend()
    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="cloudrun"),
        queue_backend=queue_backend,
    ) as service:
        record = await queue_backend.enqueue("tasks.never_registered_claimed", max_retries=0)
        claimed = await queue_backend.claim_task(record.id)
        assert claimed is not None
        exit_code = await consume_one(service, record.id)
        stored = await queue_backend.get_task(record.id)

    assert exit_code == TaskExitCode.CLAIM_LOST
    assert stored is not None
    assert stored.status == "running"


async def test_a_cancelled_consumer_reports_no_outcome_at_all() -> "None":
    """Cancelling the caller is not the queue deciding anything.

    A consumer that answered here would be telling its caller the delivery is
    settled while the record is still running. The cancellation has to reach the
    caller instead, so nothing is acknowledged.
    """
    from litestar_queues import QueueConfig, QueueService, task
    from litestar_queues.consumer import consume_one

    started = asyncio.Event()
    body_cancelled = asyncio.Event()

    @task("tasks.consumer_cancelled")
    async def consumer_cancelled() -> "None":
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            body_cancelled.set()
            raise

    queue_backend = _RecordingHeartbeatBackend()
    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="cloudrun"),
        queue_backend=queue_backend,
    ) as service:
        result = await service.enqueue(consumer_cancelled.using(execution_backend="cloudrun"))
        record = await queue_backend.get_task(result.id)
        assert record is not None
        runner = asyncio.create_task(consume_one(service, record.id))
        await asyncio.wait_for(started.wait(), timeout=1)
        runner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await runner
        stored = await queue_backend.get_task(record.id)

    assert body_cancelled.is_set()
    assert stored is not None
    assert stored.status == "running"
    assert stored.heartbeat_at is None


async def test_two_consumers_racing_one_record_run_the_task_once() -> "None":
    """At-least-once delivery is the transport's contract; running once is the queue's.

    Two consumer processes genuinely can hold the same delivery -- a response
    the transport never saw is re-sent while the first attempt is still
    running. Only one may execute the body and reach a terminal state; the
    other has to find out it lost and say so without touching the record.
    """
    from litestar_queues import QueueConfig, QueueService, task
    from litestar_queues.consumer import TaskExitCode, consume_one

    started = asyncio.Event()
    release = asyncio.Event()
    executions: "list[int]" = []

    @task("tasks.consumer_race")
    async def consumer_race() -> "None":
        executions.append(1)
        started.set()
        await release.wait()

    queue_backend = InMemoryQueueBackend()
    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="cloudrun"),
        queue_backend=queue_backend,
    ) as service:
        result = await service.enqueue(consumer_race.using(execution_backend="cloudrun"))
        winner = asyncio.create_task(consume_one(service, result.id))
        await asyncio.wait_for(started.wait(), timeout=2)
        # The second delivery arrives while the first is mid-flight.
        loser_code = await consume_one(service, result.id)
        release.set()
        winner_code = await asyncio.wait_for(winner, timeout=2)
        stored = await queue_backend.get_task(result.id)

    assert executions == [1]
    assert winner_code == TaskExitCode.SUCCESS
    assert loser_code == TaskExitCode.CLAIM_LOST
    assert stored is not None
    assert stored.status == "completed"
    assert stored.retry_count == 0
    # The winner's cleanup ran even though a second consumer was interleaved
    # with it, so stale recovery has nothing to reconsider.
    assert stored.heartbeat_at is None


async def test_consumer_runtime_manages_dependency_provider() -> "None":
    from litestar_queues import QueueConfig, QueueService, task
    from litestar_queues.consumer import TaskExitCode, run_task
    import contextlib

    class _LifecycleDependencyProvider:
        def __init__(self, order: "list[str]") -> "None":
            self.order = order

        async def open(self) -> "None":
            self.order.append("provider.open")

        async def close(self) -> "None":
            self.order.append("provider.close")

        from typing import AsyncIterator
        from litestar_queues import Task, TaskExecutionContext
        from litestar_queues.models import QueuedTaskRecord

        @contextlib.asynccontextmanager
        async def __call__(
            self, _task: "Task[..., object]", _record: "QueuedTaskRecord", _context: "TaskExecutionContext"
        ) -> "AsyncIterator[dict[str, object]]":
            self.order.append("provider.call")
            yield {}

    order: "list[str]" = []
    
    @task("tasks.consumer_provider_test")
    async def consumer_provider_test() -> "str":
        order.append("task.run")
        return "ok"

    queue_backend = InMemoryQueueBackend()
    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="cloudrun",
        task_dependency_provider=_LifecycleDependencyProvider(order),
    )
    
    factory_module = ModuleType("consumer_provider_test_factory")
    sys.modules[factory_module.__name__] = factory_module
    try:
        async with QueueService(config, queue_backend=queue_backend) as service:
            factory_module.create_service = lambda: QueueService(config, queue_backend=queue_backend)  # type: ignore[attr-defined]
            result = await service.enqueue(consumer_provider_test.using(execution_backend="cloudrun"))
            record = await queue_backend.get_task(result.id)
            assert record is not None
            
            # The enqueuing service's provider is opened. We care about the consumer's.
            order.clear()

            exit_code = await run_task(
                env={
                    "QUEUES_CONFIG_FACTORY": f"{factory_module.__name__}:create_service",
                    "QUEUES_TASK_ID": str(record.id),
                }
            )

            assert exit_code == TaskExitCode.SUCCESS
            assert order == ["provider.open", "provider.call", "task.run", "provider.close"]
    finally:
        sys.modules.pop(factory_module.__name__, None)

"""Unit tests for the click-free external-executor consumer core."""

import asyncio
import sys
from types import ModuleType
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from litestar_queues.backends import InMemoryQueueBackend

if TYPE_CHECKING:
    from collections.abc import Sequence
    from uuid import UUID

    from litestar_queues.models import HeartbeatTouch, HeartbeatTouchResult
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


async def test_consume_one_claims_and_executes_persisted_record() -> "None":
    from litestar_queues import QueueConfig, QueueService, task
    from litestar_queues._consumer import TaskExitCode, consume_one

    @task("tasks.consumer")
    async def consumer_task(value: "int") -> "int":
        return value + 1

    queue_backend = InMemoryQueueBackend()
    async with QueueService(QueueConfig(execution_backend="cloudrun"), queue_backend=queue_backend) as service:
        result = await service.enqueue(consumer_task.using(execution_backend="cloudrun"), 41)
        record = await queue_backend.get_task(result.id)
        assert record is not None
        exit_code = await consume_one(service, record.id)
        await result.refresh()

    assert exit_code == TaskExitCode.SUCCESS
    assert result.status == "completed"
    assert result.result == 42


async def test_run_task_loads_factory_before_prefixed_task_id() -> "None":
    from litestar_queues import QueueConfig, QueueService, task
    from litestar_queues._consumer import TaskExitCode, run_task
    from litestar_queues.execution.cloudrun import CloudRunExecutionConfig

    @task("tasks.consumer_prefixed")
    async def consumer_prefixed(value: "int") -> "int":
        return value + 1

    queue_backend = InMemoryQueueBackend()
    config = QueueConfig(
        execution_backend=CloudRunExecutionConfig(project_id="test-project", job_name="worker", env_prefix="PREFIX")
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
                    "LITESTAR_QUEUES_CONFIG_FACTORY": f"{factory_module.__name__}:create_service",
                    "PREFIX_TASK_ID": str(record.id),
                }
            )
            await result.refresh()
    finally:
        sys.modules.pop(factory_module.__name__, None)

    assert exit_code == TaskExitCode.SUCCESS
    assert result.status == "completed"
    assert result.result == 42


async def test_run_task_requires_config_factory() -> "None":
    from litestar_queues._consumer import TaskExitCode, run_task

    exit_code = await run_task(env={"LITESTAR_QUEUES_TASK_ID": str(uuid4())})

    assert exit_code == TaskExitCode.MISSING_CONFIG_FACTORY


async def test_consume_one_returns_claim_lost_when_heartbeat_loses_ownership() -> "None":
    from litestar_queues import QueueConfig, QueueService, task
    from litestar_queues._consumer import TaskExitCode, consume_one

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
        QueueConfig(execution_backend="cloudrun", worker_heartbeat_interval=0.01), queue_backend=queue_backend
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
    from litestar_queues._consumer import TaskExitCode, run_task

    async with QueueService(QueueConfig()) as service:
        missing = await run_task(service=service, env={})
        invalid = await run_task(service=service, env={"LITESTAR_QUEUES_TASK_ID": "not-a-uuid"})

    assert missing == TaskExitCode.MISSING_TASK_ID
    assert invalid == TaskExitCode.INVALID_TASK_ID

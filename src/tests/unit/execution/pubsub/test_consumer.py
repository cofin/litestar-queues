import asyncio
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest

from litestar_queues import QueueConfig, QueueService, WorkerConfig
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.consumer import TaskExitCode
from litestar_queues.execution.pubsub import PubSubExecutionBackend, PubSubExecutionConfig
from litestar_queues.execution.pubsub.backend import _StreamingPullRequests

pytestmark = pytest.mark.anyio


def _delivery(data: "bytes", attempt: "str | None", *, ack_id: "str" = "ack-1") -> "SimpleNamespace":
    attributes = {} if attempt is None else {"litestar_queues_attempt": attempt}
    return SimpleNamespace(ack_id=ack_id, message=SimpleNamespace(data=data, attributes=attributes))


async def _next_control_request(requests: "_StreamingPullRequests") -> "Any":
    await requests.__anext__()
    return await requests.__anext__()


async def test_request_stream_starts_subscription_then_carries_ack_and_nack() -> "None":
    config = PubSubExecutionConfig(project_id="project", topic_id="tasks", subscription_id="workers")
    requests = _StreamingPullRequests(config, 7)

    initial = await requests.__anext__()
    await requests.ack("ack-1")
    ack = await requests.__anext__()
    await requests.nack("ack-2")
    nack = await requests.__anext__()

    assert initial.subscription == config.subscription_path
    assert initial.stream_ack_deadline_seconds == config.ack_deadline
    assert initial.max_outstanding_messages == 7
    assert list(ack.ack_ids) == ["ack-1"]
    assert list(nack.modify_deadline_ack_ids) == ["ack-2"]
    assert list(nack.modify_deadline_seconds) == [0]


@pytest.mark.parametrize(
    "delivery", [_delivery(b"not-a-uuid", None), _delivery(str(UUID(int=1)).encode(), "invalid-attempt")]
)
async def test_poison_delivery_is_acknowledged(delivery: "SimpleNamespace") -> "None":
    execution_config = PubSubExecutionConfig(project_id="project", topic_id="tasks", subscription_id="workers")
    backend = PubSubExecutionBackend(execution_config=execution_config)
    requests = _StreamingPullRequests(execution_config, 1)
    config = QueueConfig(
        queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
    )
    async with QueueService(config, queue_backend=InMemoryQueueBackend(), execution_backend=backend) as service:
        await backend._consume_message(service, delivery, requests, asyncio.Semaphore(1))

    ack = await _next_control_request(requests)
    assert list(ack.ack_ids) == [delivery.ack_id]


async def test_missing_delivery_is_acknowledged() -> "None":
    execution_config = PubSubExecutionConfig(project_id="project", topic_id="tasks", subscription_id="workers")
    backend = PubSubExecutionBackend(execution_config=execution_config)
    requests = _StreamingPullRequests(execution_config, 1)
    attempt = f"pubsub:0:1:{UUID(int=2)}"
    delivery = _delivery(str(UUID(int=3)).encode(), attempt)
    config = QueueConfig(
        queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
    )
    async with QueueService(config, queue_backend=InMemoryQueueBackend(), execution_backend=backend) as service:
        await backend._consume_message(service, delivery, requests, asyncio.Semaphore(1))

    ack = await _next_control_request(requests)
    assert list(ack.ack_ids) == [delivery.ack_id]


async def test_successful_delivery_passes_attempt_fence_then_acknowledges(monkeypatch: "pytest.MonkeyPatch") -> "None":
    execution_config = PubSubExecutionConfig(project_id="project", topic_id="tasks", subscription_id="workers")
    config = QueueConfig(
        queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
    )
    queue_backend = InMemoryQueueBackend()
    record = await queue_backend.enqueue("tasks.pubsub-consume", execution_backend="pubsub")
    attempt = f"pubsub:0:1:{UUID(int=4)}"
    await queue_backend.reserve_external_dispatch(record.id, "pubsub", attempt, expected_retry_count=0)
    delivery = _delivery(str(record.id).encode(), attempt)
    seen: "list[tuple[UUID, int, str]]" = []

    async def fake_consume_one(
        _service: "QueueService", task_id: "UUID", *, expected_retry_count: "int", expected_execution_ref: "str"
    ) -> "TaskExitCode":
        seen.append((task_id, expected_retry_count, expected_execution_ref))
        return TaskExitCode.SUCCESS

    monkeypatch.setattr("litestar_queues.execution.pubsub.backend.consume_one", fake_consume_one)
    backend = PubSubExecutionBackend(config, execution_config=execution_config)
    requests = _StreamingPullRequests(execution_config, 1)
    async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
        await backend._consume_message(service, delivery, requests, asyncio.Semaphore(1))

    ack = await _next_control_request(requests)
    assert seen == [(record.id, 0, attempt)]
    assert list(ack.ack_ids) == [delivery.ack_id]


async def test_cancelled_delivery_is_nacked(monkeypatch: "pytest.MonkeyPatch") -> "None":
    execution_config = PubSubExecutionConfig(project_id="project", topic_id="tasks", subscription_id="workers")
    config = QueueConfig(
        queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
    )
    queue_backend = InMemoryQueueBackend()
    record = await queue_backend.enqueue("tasks.pubsub-cancel", execution_backend="pubsub")
    attempt = f"pubsub:0:1:{UUID(int=5)}"
    await queue_backend.reserve_external_dispatch(record.id, "pubsub", attempt, expected_retry_count=0)

    async def cancelled(*args: "object", **kwargs: "object") -> "TaskExitCode":
        del args, kwargs
        raise asyncio.CancelledError

    monkeypatch.setattr("litestar_queues.execution.pubsub.backend.consume_one", cancelled)
    backend = PubSubExecutionBackend(config, execution_config=execution_config)
    requests = _StreamingPullRequests(execution_config, 1)
    delivery = _delivery(str(record.id).encode(), attempt)
    async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
        with pytest.raises(asyncio.CancelledError):
            await backend._consume_message(service, delivery, requests, asyncio.Semaphore(1))

    nack = await _next_control_request(requests)
    assert list(nack.modify_deadline_ack_ids) == [delivery.ack_id]
    assert list(nack.modify_deadline_seconds) == [0]

from base64 import b64encode
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from litestar.testing import AsyncTestClient

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.consumer import TaskExitCode
from tests.helpers.eventarc_receiver import EVENT_TYPE, create_eventarc_receiver

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

pytestmark = pytest.mark.anyio

TOPIC = "projects/test/topics/jobs"


def _delivery(task_id: "object", attempt: "object", **overrides: "Any") -> "dict[str, Any]":
    headers = {
        "ce-id": "message-1",
        "ce-source": f"//pubsub.googleapis.com/{TOPIC}",
        "ce-specversion": "1.0",
        "ce-type": EVENT_TYPE,
        "ce-time": "2026-08-09T00:00:00Z",
        "content-type": "application/json",
    }
    headers.update(overrides.pop("headers", {}))
    body = {
        "message": {
            "data": b64encode(str(task_id).encode()).decode(),
            "attributes": {"litestar_queues_attempt": attempt},
            "messageId": "message-1",
            "publishTime": "2026-08-09T00:00:00Z",
        }
    }
    body["message"].update(overrides.pop("message", {}))
    body.update(overrides.pop("body", {}))
    return {"headers": headers, "json": body, **overrides}


@asynccontextmanager
async def _receiver_service() -> "AsyncGenerator[tuple[QueueService, InMemoryQueueBackend, AsyncTestClient], None]":
    config = QueueConfig(
        queue_backend="memory", execution_backend="cloudrun", worker=WorkerConfig(placement="external")
    )
    backend = InMemoryQueueBackend()
    async with (
        QueueService(config, queue_backend=backend) as service,
        AsyncTestClient(app=create_eventarc_receiver(queue_service=service, topic=TOPIC)) as client,
    ):
        yield service, backend, client


async def test_receiver_runs_fenced_pubsub_delivery(monkeypatch: "pytest.MonkeyPatch") -> "None":
    task_id = uuid4()
    attempt = f"pubsub:2:123:{uuid4()}"
    consume = AsyncMock(return_value=TaskExitCode.SUCCESS)
    monkeypatch.setattr("tests.helpers.eventarc_receiver.consume_one", consume)
    config = QueueConfig(queue_backend="memory", worker=WorkerConfig(placement="external"))
    service = QueueService(config, queue_backend=InMemoryQueueBackend())
    app = create_eventarc_receiver(queue_service=service, topic=TOPIC)

    assert app.request_max_body_size == 64 * 1024

    async with AsyncTestClient(app=app) as client:
        response = await client.post("/eventarc/pubsub", **_delivery(task_id, attempt))

    assert response.status_code == 204
    consume.assert_awaited_once_with(
        app.state.eventarc_queue_service, task_id, expected_retry_count=2, expected_execution_ref=attempt
    )


@pytest.mark.parametrize(
    "outcome", [TaskExitCode.FAILURE, TaskExitCode.CANCELLED, TaskExitCode.MISSING_RECORD, TaskExitCode.CLAIM_LOST]
)
async def test_durable_outcomes_are_acknowledged(monkeypatch: "pytest.MonkeyPatch", outcome: "TaskExitCode") -> "None":
    consume = AsyncMock(return_value=outcome)
    monkeypatch.setattr("tests.helpers.eventarc_receiver.consume_one", consume)
    async with _receiver_service() as (_service, _backend, client):
        response = await client.post("/eventarc/pubsub", **_delivery(uuid4(), f"pubsub:0:123:{uuid4()}"))

    assert response.status_code == 204


async def test_duplicate_delivery_executes_task_once() -> "None":
    executions: "list[int]" = []

    @task("tests.eventarc.duplicate")
    async def delivered() -> "None":
        executions.append(1)

    attempt = f"pubsub:0:123:{uuid4()}"
    async with _receiver_service() as (service, backend, client):
        result = await service.enqueue(delivered.using(execution_backend="cloudrun"))
        assert await backend.reserve_external_dispatch(result.id, "pubsub", attempt, expected_retry_count=0) is not None
        first = await client.post("/eventarc/pubsub", **_delivery(result.id, attempt))
        duplicate = await client.post("/eventarc/pubsub", **_delivery(result.id, attempt))

    assert first.status_code == 204
    assert duplicate.status_code == 204
    assert executions == [1]


@pytest.mark.parametrize("state", ["cancelled", "stale"])
async def test_cancelled_or_stale_delivery_does_not_execute(state: "str") -> "None":
    executions: "list[int]" = []

    @task(f"tests.eventarc.{state}")
    async def delivered() -> "None":
        executions.append(1)

    current = f"pubsub:0:123:{uuid4()}"
    delivered_attempt = current
    async with _receiver_service() as (service, backend, client):
        result = await service.enqueue(delivered.using(execution_backend="cloudrun"))
        assert await backend.reserve_external_dispatch(result.id, "pubsub", current, expected_retry_count=0) is not None
        if state == "cancelled":
            assert await service.cancel_task(result.id, include_running=True)
        else:
            replacement = f"pubsub:0:124:{uuid4()}"
            assert await backend.replace_execution_ref(result.id, 0, current, replacement) is not None
        response = await client.post("/eventarc/pubsub", **_delivery(result.id, delivered_attempt))

    assert response.status_code == 204
    assert executions == []


@pytest.mark.parametrize(
    "delivery",
    [
        _delivery(uuid4(), f"pubsub:0:123:{uuid4()}", headers={"ce-specversion": "0.3"}),
        _delivery(uuid4(), f"pubsub:0:123:{uuid4()}", headers={"ce-type": "wrong"}),
        _delivery(uuid4(), f"pubsub:0:123:{uuid4()}", headers={"ce-source": "//pubsub.googleapis.com/wrong"}),
        _delivery(uuid4(), f"pubsub:0:123:{uuid4()}", headers={"ce-id": ""}),
        _delivery(uuid4(), f"pubsub:0:123:{uuid4()}", headers={"ce-time": ""}),
        _delivery(uuid4(), "missing-attempt"),
        _delivery("not-a-uuid", f"pubsub:0:123:{uuid4()}"),
        _delivery(uuid4(), f"pubsub:0:123:{uuid4()}", message={"data": "not-base64!"}),
        _delivery(uuid4(), f"pubsub:0:123:{uuid4()}", message={"messageId": "different"}),
        _delivery(uuid4(), f"pubsub:0:123:{uuid4()}", body={"unexpected": True}),
    ],
)
async def test_malformed_delivery_is_rejected(delivery: "dict[str, Any]") -> "None":
    async with _receiver_service() as (_service, _backend, client):
        response = await client.post("/eventarc/pubsub", **delivery)

    assert response.status_code == 400


async def test_infrastructure_error_is_retryable(monkeypatch: "pytest.MonkeyPatch") -> "None":
    consume = AsyncMock(side_effect=ConnectionError("storage unavailable"))
    monkeypatch.setattr("tests.helpers.eventarc_receiver.consume_one", consume)
    async with _receiver_service() as (_service, _backend, client):
        response = await client.post("/eventarc/pubsub", **_delivery(uuid4(), f"pubsub:0:123:{uuid4()}"))

    assert response.status_code == 503


async def test_programming_error_is_not_marked_retryable(monkeypatch: "pytest.MonkeyPatch") -> "None":
    consume = AsyncMock(side_effect=ValueError("bug"))
    monkeypatch.setattr("tests.helpers.eventarc_receiver.consume_one", consume)
    async with _receiver_service() as (_service, _backend, client):
        response = await client.post("/eventarc/pubsub", **_delivery(uuid4(), f"pubsub:0:123:{uuid4()}"))

    assert response.status_code == 500


async def test_oversized_delivery_is_rejected() -> "None":
    oversized = "x" * (64 * 1024)
    async with _receiver_service() as (_service, _backend, client):
        response = await client.post(
            "/eventarc/pubsub", **_delivery(uuid4(), f"pubsub:0:123:{uuid4()}", message={"data": oversized})
        )

    assert response.status_code == 413

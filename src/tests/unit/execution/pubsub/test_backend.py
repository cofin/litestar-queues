from typing import Any
from uuid import UUID

import pytest
from google.api_core.exceptions import InvalidArgument

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.exceptions import QueueDispatchError
from litestar_queues.execution.pubsub import PubSubExecutionBackend, PubSubExecutionConfig

pytestmark = pytest.mark.anyio


class FakePublisher:
    def __init__(self, error: "Exception | None" = None) -> "None":
        self.error = error
        self.requests: "list[dict[str, Any]]" = []
        self.closed = False

    async def publish(self, *, request: "dict[str, Any]", timeout: "float") -> "object":
        self.requests.append({"request": request, "timeout": timeout})
        if self.error is not None:
            raise self.error
        return object()

    async def close(self) -> "None":
        self.closed = True


@pytest.mark.parametrize("priority", [-5, 0, 40])
async def test_dispatch_publishes_only_uuid_and_attempt_attribute(priority: "int") -> "None":
    @task("tasks.pubsub-secret", priority=priority)
    async def secret_task(secret: "str") -> "None":
        del secret

    publisher = FakePublisher()
    execution_config = PubSubExecutionConfig(project_id="project", topic_id="tasks", subscription_id="workers")
    config = QueueConfig(
        queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
    )
    queue_backend = InMemoryQueueBackend()
    backend = PubSubExecutionBackend(config, execution_config=execution_config, publisher=publisher)
    async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
        result = await service.enqueue(secret_task.using(execution_backend="pubsub"), "never-on-the-wire")
        record = await queue_backend.get_task(result.id)
        assert record is not None
        attempt_ref = await backend.dispatch(service, record)

    request = publisher.requests[0]["request"]
    assert request["topic"] == execution_config.topic_path
    assert request["messages"] == [
        {"data": str(result.id).encode(), "attributes": {"litestar_queues_attempt": attempt_ref}}
    ]
    assert "never-on-the-wire" not in repr(request)


async def test_repair_rotates_stale_attempt_before_republishing(monkeypatch: "pytest.MonkeyPatch") -> "None":
    publisher = FakePublisher()
    execution_config = PubSubExecutionConfig(
        project_id="project", topic_id="tasks", subscription_id="workers", dispatch_stale_after=1
    )
    config = QueueConfig(
        queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
    )
    queue_backend = InMemoryQueueBackend()
    backend = PubSubExecutionBackend(config, execution_config=execution_config, publisher=publisher)
    record = await queue_backend.enqueue("tasks.pubsub-repair", execution_backend="pubsub")
    old_ref = f"pubsub:0:1:{UUID(int=1)}"
    await queue_backend.reserve_external_dispatch(record.id, "pubsub", old_ref, expected_retry_count=0)
    monkeypatch.setattr("litestar_queues.execution.pubsub.backend.time.time", lambda: 10.0)

    async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
        repaired = await backend.repair(service, limit=1)

    stored = await queue_backend.get_task(record.id)
    assert repaired.examined == 1
    assert repaired.changed == 1
    assert stored is not None
    assert stored.execution_ref != old_ref
    assert publisher.requests[0]["request"]["messages"][0]["data"] == str(record.id).encode()


@pytest.mark.parametrize(
    "error, retained",
    [(TimeoutError(), True), (InvalidArgument("invalid"), False)],  # type: ignore[no-untyped-call]
)
async def test_dispatch_retains_only_ambiguous_publish_reservations(error: "Exception", retained: "bool") -> "None":
    publisher = FakePublisher(error)
    execution_config = PubSubExecutionConfig(project_id="project", topic_id="tasks", subscription_id="workers")
    config = QueueConfig(
        queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
    )
    queue_backend = InMemoryQueueBackend()
    backend = PubSubExecutionBackend(config, execution_config=execution_config, publisher=publisher)
    record = await queue_backend.enqueue("tasks.pubsub-failure", execution_backend="pubsub")

    async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
        if retained:
            with pytest.raises(QueueDispatchError, match="outcome is unknown") as exc_info:
                await backend.dispatch(service, record)
            assert exc_info.value.committed is True
        else:
            with pytest.raises(InvalidArgument):
                await backend.dispatch(service, record)

    stored = await queue_backend.get_task(record.id)
    assert stored is not None
    assert (stored.execution_ref is not None) is retained


async def test_close_does_not_close_an_injected_publisher() -> "None":
    publisher = FakePublisher()
    backend = PubSubExecutionBackend(
        execution_config=PubSubExecutionConfig(project_id="project", topic_id="tasks", subscription_id="workers"),
        publisher=publisher,
    )

    await backend.close()

    assert publisher.closed is False

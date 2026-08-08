from typing import Any
from uuid import UUID

import pytest

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.execution.rabbitmq import RabbitMQExecutionBackend, RabbitMQExecutionConfig

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def supported_python(monkeypatch: "pytest.MonkeyPatch") -> "None":
    monkeypatch.setattr("litestar_queues.execution.rabbitmq.config._PYTHON_VERSION", (3, 11))


class FakeExchange:
    def __init__(self) -> "None":
        self.published: "list[tuple[Any, str, bool]]" = []

    async def publish(self, message: "Any", routing_key: "str", mandatory: "bool") -> "bool":
        self.published.append((message, routing_key, mandatory))
        return True


class FakeChannel:
    def __init__(self) -> "None":
        self.default_exchange = FakeExchange()
        self.declarations: "list[tuple[str, dict[str, Any]]]" = []

    async def declare_queue(self, name: "str", **kwargs: "Any") -> "object":
        self.declarations.append((name, kwargs))
        return object()


class FakeConnection:
    def __init__(self) -> "None":
        self.channel_value = FakeChannel()
        self.server_properties = {"version": "4.3.0"}

    async def channel(self, **_kwargs: "Any") -> "FakeChannel":
        return self.channel_value

    async def close(self) -> "None":
        return None


class FakeMessage:
    def __init__(self, body: "bytes", attempt: "object") -> "None":
        self.body = body
        self.headers = {"litestar_queues_attempt": attempt}
        self.acked = False
        self.nacked = False

    async def ack(self) -> "None":
        self.acked = True

    async def nack(self, *, requeue: "bool") -> "None":
        assert requeue is True
        self.nacked = True


class StorageUnavailableError(Exception):
    pass


async def test_dispatch_publishes_persistent_uuid_with_attempt_and_priority() -> "None":
    aio_pika = pytest.importorskip("aio_pika")

    @task("tasks.rabbit-secret", priority=100)
    async def secret_task(secret: "str") -> "None":
        del secret

    connection = FakeConnection()
    execution_config = RabbitMQExecutionConfig(amqp_url="amqp://guest:guest@localhost/")
    config = QueueConfig(
        namespace="tests",
        queue_backend="memory",
        execution_backend=execution_config,
        worker=WorkerConfig(placement="external"),
    )
    queue_backend = InMemoryQueueBackend()
    backend = RabbitMQExecutionBackend(config, execution_config=execution_config, connection=connection)
    async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
        result = await service.enqueue(secret_task.using(execution_backend="rabbitmq"), "never-on-the-wire")
        record = await queue_backend.get_task(result.id)
        assert record is not None
        attempt_ref = await backend.dispatch(service, record)

    message, routing_key, mandatory = connection.channel_value.default_exchange.published[0]
    assert message.body == str(result.id).encode()
    assert message.message_id == str(result.id)
    assert message.headers["litestar_queues_attempt"] == attempt_ref
    assert message.priority == 31
    assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    assert routing_key == "tests-rabbitmq"
    assert mandatory is True
    name, declaration = connection.channel_value.declarations[0]
    assert name == routing_key
    assert declaration["durable"] is True
    assert declaration["arguments"]["x-queue-type"] == "quorum"
    assert "x-max-priority" not in declaration["arguments"]


async def test_poison_delivery_is_acknowledged() -> "None":
    backend = RabbitMQExecutionBackend(
        execution_config=RabbitMQExecutionConfig(amqp_url="amqp://localhost", queue_name="tasks"),
        connection=FakeConnection(),
    )
    message = FakeMessage(b"not-a-uuid", "bad-attempt")

    await backend._consume_message(object(), message)  # type: ignore[arg-type]

    assert message.acked is True
    assert message.nacked is False


async def test_storage_failure_nacks_for_redelivery() -> "None":
    class FailingService:
        async def get_task(self, _task_id: "object") -> "None":
            raise StorageUnavailableError

    task_id = UUID(int=1)
    message = FakeMessage(task_id.hex.encode(), f"rabbitmq:0:1:{UUID(int=2)}")
    backend = RabbitMQExecutionBackend(
        execution_config=RabbitMQExecutionConfig(amqp_url="amqp://localhost", queue_name="tasks"),
        connection=FakeConnection(),
    )

    await backend._consume_message(FailingService(), message)  # type: ignore[arg-type]

    assert message.acked is False
    assert message.nacked is True

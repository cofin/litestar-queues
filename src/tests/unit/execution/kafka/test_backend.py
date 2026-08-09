import asyncio
from types import SimpleNamespace
from typing import Any, NamedTuple
from uuid import UUID

import pytest

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.execution.kafka import KafkaExecutionBackend, KafkaExecutionConfig

pytestmark = pytest.mark.anyio


class TopicPartition(NamedTuple):
    topic: "str"
    partition: "int"


class FakeProducer:
    def __init__(self) -> "None":
        self.sent: "list[tuple[str, bytes, list[tuple[str, bytes]]]]" = []

    async def send_and_wait(self, topic: "str", value: "bytes", *, headers: "list[tuple[str, bytes]]") -> "object":
        self.sent.append((topic, value, headers))
        return object()


class FakeConsumer:
    def __init__(self, messages: "list[Any]") -> "None":
        self.messages = messages
        self.commits: "list[dict[Any, int]]" = []
        self.started = False
        self.stopped = False
        self.committed = asyncio.Event()

    async def start(self) -> "None":
        self.started = True

    async def stop(self) -> "None":
        self.stopped = True

    async def getmany(self, *, timeout_ms: "int", max_records: "int") -> "dict[Any, list[Any]]":
        del timeout_ms, max_records
        if self.messages:
            message = self.messages.pop(0)
            return {message.topic_partition: [message]}
        await asyncio.sleep(10)
        return {}

    async def commit(self, offsets: "dict[Any, int]") -> "None":
        self.commits.append(offsets)
        self.committed.set()


async def test_dispatch_publishes_uuid_and_attempt_header() -> "None":
    @task("tasks.kafka-secret")
    async def secret_task(secret: "str") -> "None":
        del secret

    producer = FakeProducer()
    execution_config = KafkaExecutionConfig(bootstrap_servers="localhost:9092", topic="tasks")
    config = QueueConfig(
        namespace="tests",
        queue_backend="memory",
        execution_backend=execution_config,
        worker=WorkerConfig(placement="external"),
    )
    queue_backend = InMemoryQueueBackend()
    backend = KafkaExecutionBackend(config, execution_config=execution_config, producer=producer)
    async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
        result = await service.enqueue(secret_task.using(execution_backend="kafka"), "never-on-the-wire")
        record = await queue_backend.get_task(result.id)
        assert record is not None
        attempt_ref = await backend.dispatch(service, record)

    topic, value, headers = producer.sent[0]
    assert attempt_ref is not None
    assert topic == "tasks"
    assert value == str(result.id).encode()
    assert dict(headers)["litestar_queues_attempt"] == attempt_ref.encode()


async def test_consumer_commits_next_offset_only_after_durable_outcome(monkeypatch: "pytest.MonkeyPatch") -> "None":
    task_id = UUID(int=1)
    attempt = f"kafka:0:1:{UUID(int=2)}"
    partition = TopicPartition(topic="tasks", partition=0)
    message = SimpleNamespace(
        value=str(task_id).encode(),
        headers=[("litestar_queues_attempt", attempt.encode())],
        offset=4,
        topic_partition=partition,
    )
    consumer = FakeConsumer([message])
    backend = KafkaExecutionBackend(
        execution_config=KafkaExecutionConfig(bootstrap_servers="localhost:9092"), consumer=consumer
    )
    service = SimpleNamespace(get_task=lambda _task_id: None)

    async def get_task(_task_id: "UUID") -> "object":
        return SimpleNamespace(is_terminal=True)

    service.get_task = get_task
    consumer_task = asyncio.create_task(
        backend.run_consumer(service, max_concurrency=1, drain_timeout=0)  # type: ignore[arg-type]
    )
    await consumer.committed.wait()
    consumer_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consumer_task

    assert consumer.commits == [{partition: 5}]


async def test_cancelled_delivery_is_not_committed(monkeypatch: "pytest.MonkeyPatch") -> "None":
    task_id = UUID(int=1)
    attempt = f"kafka:0:1:{UUID(int=2)}"
    partition = TopicPartition(topic="tasks", partition=0)
    message = SimpleNamespace(
        value=str(task_id).encode(),
        headers=[("litestar_queues_attempt", attempt.encode())],
        offset=4,
        topic_partition=partition,
    )
    consumer = FakeConsumer([message])
    backend = KafkaExecutionBackend(
        execution_config=KafkaExecutionConfig(bootstrap_servers="localhost:9092"), consumer=consumer
    )

    async def get_task(_task_id: "UUID") -> "object":
        raise asyncio.CancelledError

    service = SimpleNamespace(get_task=get_task)
    consumer_task = asyncio.create_task(
        backend.run_consumer(service, max_concurrency=1, drain_timeout=0)  # type: ignore[arg-type]
    )
    await asyncio.sleep(0)
    consumer_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consumer_task

    assert consumer.commits == []

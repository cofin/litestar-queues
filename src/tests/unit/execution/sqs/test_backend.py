import asyncio
from typing import Any
from uuid import UUID

import pytest

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.exceptions import QueueDispatchError
from litestar_queues.execution.sqs import SqsExecutionBackend, SqsExecutionConfig

pytestmark = pytest.mark.anyio


class FakeSqsClient:
    def __init__(self) -> "None":
        self.sent: "list[dict[str, Any]]" = []
        self.visibility_changes: "list[dict[str, Any]]" = []

    async def send_message(self, **request: "Any") -> "dict[str, str]":
        self.sent.append(request)
        return {"MessageId": "message-1"}

    async def change_message_visibility(self, **request: "Any") -> "None":
        self.visibility_changes.append(request)


class FailingSqsClient(FakeSqsClient):
    def __init__(self, error: "Exception") -> "None":
        super().__init__()
        self.error = error

    async def send_message(self, **request: "Any") -> "dict[str, str]":
        self.sent.append(request)
        raise self.error


class ClientError(Exception):
    def __init__(self, status: "int") -> "None":
        self.response = {"ResponseMetadata": {"HTTPStatusCode": status}}


@pytest.mark.parametrize("fifo", [False, True])
async def test_dispatch_sends_uuid_only_with_private_attempt_attribute(fifo: "bool") -> "None":
    @task("tasks.sqs-secret")
    async def secret_task(secret: "str") -> "None":
        del secret

    client = FakeSqsClient()
    execution_config = SqsExecutionConfig(queue_url="http://sqs.test/queue", fifo=fifo)
    config = QueueConfig(
        queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
    )
    queue_backend = InMemoryQueueBackend()
    backend = SqsExecutionBackend(config, execution_config=execution_config, client=client)
    async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
        result = await service.enqueue(secret_task.using(execution_backend="sqs"), "never-on-the-wire")
        record = await queue_backend.get_task(result.id)
        assert record is not None
        attempt_ref = await backend.dispatch(service, record)

    request = client.sent[0]
    assert request["MessageBody"] == str(result.id)
    assert "never-on-the-wire" not in repr(request)
    assert request["MessageAttributes"]["litestar_queues_attempt"]["StringValue"] == attempt_ref
    assert ("MessageGroupId" in request) is fifo
    assert ("MessageDeduplicationId" in request) is fifo


async def test_repair_rotates_stale_attempt_before_republishing(monkeypatch: "pytest.MonkeyPatch") -> "None":
    client = FakeSqsClient()
    execution_config = SqsExecutionConfig(queue_url="http://sqs.test/queue", dispatch_stale_after=1)
    config = QueueConfig(
        queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
    )
    queue_backend = InMemoryQueueBackend()
    backend = SqsExecutionBackend(config, execution_config=execution_config, client=client)
    record = await queue_backend.enqueue("tasks.sqs-repair", execution_backend="sqs")
    old_ref = f"sqs:0:1:{UUID(int=1)}"
    await queue_backend.reserve_external_dispatch(record.id, "sqs", old_ref, expected_retry_count=0)
    monkeypatch.setattr("litestar_queues.execution.sqs.backend.time.time", lambda: 10.0)

    async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
        repaired = await backend.repair(service, limit=1)

    stored = await queue_backend.get_task(record.id)
    assert repaired.examined == 1
    assert repaired.changed == 1
    assert stored is not None
    assert stored.execution_ref != old_ref
    assert client.sent[0]["MessageBody"] == str(record.id)
    assert client.sent[0]["MessageAttributes"]["litestar_queues_attempt"]["StringValue"] == stored.execution_ref


@pytest.mark.parametrize("error, retained", [(TimeoutError(), True), (ClientError(400), False)])
async def test_dispatch_retains_only_ambiguous_send_reservations(error: "Exception", retained: "bool") -> "None":
    client = FailingSqsClient(error)
    execution_config = SqsExecutionConfig(queue_url="http://sqs.test/queue")
    config = QueueConfig(
        queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
    )
    queue_backend = InMemoryQueueBackend()
    backend = SqsExecutionBackend(config, execution_config=execution_config, client=client)
    record = await queue_backend.enqueue("tasks.sqs-failure", execution_backend="sqs")

    async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
        if retained:
            with pytest.raises(QueueDispatchError, match="outcome is unknown") as exc_info:
                await backend.dispatch(service, record)
            assert exc_info.value.committed is True
        else:
            with pytest.raises(ClientError):
                await backend.dispatch(service, record)

    stored = await queue_backend.get_task(record.id)
    assert stored is not None
    assert (stored.execution_ref is not None) is retained


async def test_visibility_extension_targets_only_the_active_receipt(monkeypatch: "pytest.MonkeyPatch") -> "None":
    client = FakeSqsClient()
    config = SqsExecutionConfig(queue_url="http://sqs.test/queue", visibility_timeout=2, visibility_extension_interval=1)
    backend = SqsExecutionBackend(execution_config=config, client=client)
    first_sleep = True
    parked = asyncio.Event()
    real_sleep = asyncio.sleep

    async def controlled_sleep(_seconds: "float") -> "None":
        nonlocal first_sleep
        if first_sleep:
            first_sleep = False
            return
        await parked.wait()

    monkeypatch.setattr("litestar_queues.execution.sqs.backend.asyncio.sleep", controlled_sleep)
    task = asyncio.create_task(backend._extend_visibility("receipt-1"))
    while not client.visibility_changes:
        await real_sleep(0)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert client.visibility_changes == [
        {
            "QueueUrl": config.queue_url,
            "ReceiptHandle": "receipt-1",
            "VisibilityTimeout": config.visibility_timeout,
        }
    ]

import asyncio
from typing import TYPE_CHECKING, Any

import pytest

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.execution.sqs import SqsExecutionBackend, SqsExecutionConfig

if TYPE_CHECKING:
    from tests.plugins.localstack import LocalStackService

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize("fifo", [False, True])
async def test_localstack_dispatch_consume_and_delete(
    localstack_service: "LocalStackService", monkeypatch: "pytest.MonkeyPatch", fifo: "bool"
) -> "None":
    session_module = pytest.importorskip("aiobotocore.session")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", localstack_service.access_key)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", localstack_service.secret_key)
    monkeypatch.setenv("AWS_DEFAULT_REGION", localstack_service.region)
    queue_name = f"{localstack_service.resource_prefix}dispatch-consume{'-fifo.fifo' if fifo else ''}"

    @task(f"tests.sqs.localstack.{'fifo' if fifo else 'standard'}")
    async def delivered() -> "str":
        return "done"

    session = session_module.get_session()
    async with session.create_client(
        "sqs", region_name=localstack_service.region, endpoint_url=localstack_service.endpoint_url
    ) as client:
        create_request: "dict[str, Any]" = {"QueueName": queue_name}
        if fifo:
            create_request["Attributes"] = {"FifoQueue": "true"}
        queue_url = (await client.create_queue(**create_request))["QueueUrl"]
        execution_config = SqsExecutionConfig(queue_url=queue_url, fifo=fifo)
        config = QueueConfig(
            queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
        )
        queue_backend = InMemoryQueueBackend()
        backend = SqsExecutionBackend(config, execution_config=execution_config, client=client)
        async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
            result = await service.enqueue(delivered.using(execution_backend="sqs"))
            record = await queue_backend.get_task(result.id)
            assert record is not None
            await backend.dispatch(service, record)
            response = await client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=1,
                MessageAttributeNames=["litestar_queues_attempt"],
            )
            await backend._consume_message(service, response["Messages"][0], asyncio.Semaphore(1))

        stored = await queue_backend.get_task(result.id)
        assert stored is not None
        assert stored.status == "completed"
        remaining = await client.receive_message(QueueUrl=queue_url, WaitTimeSeconds=1)
        assert "Messages" not in remaining

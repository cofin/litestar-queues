import asyncio
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.execution.rabbitmq import RabbitMQExecutionBackend, RabbitMQExecutionConfig

if TYPE_CHECKING:
    from tests.plugins.rabbitmq import RabbitMQService

pytestmark = pytest.mark.anyio


async def test_dispatch_consume_and_complete(rabbitmq_service: "RabbitMQService") -> "None":
    pytest.importorskip("aio_pika")
    completed = asyncio.Event()

    @task(f"tasks.rabbitmq-{uuid4()}")
    async def delivered(value: "str") -> "None":
        assert value == "persisted-only"
        completed.set()

    execution_config = RabbitMQExecutionConfig(
        amqp_url=rabbitmq_service.amqp_url, queue_name=f"litestar-queues-{uuid4()}"
    )
    config = QueueConfig(
        queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
    )
    queue_backend = InMemoryQueueBackend()
    backend = RabbitMQExecutionBackend(config, execution_config=execution_config)
    async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
        result = await service.enqueue(delivered.using(execution_backend="rabbitmq"), "persisted-only")
        record = await queue_backend.get_task(result.id)
        assert record is not None
        consumer = asyncio.create_task(backend.run_consumer(service, max_concurrency=1, drain_timeout=5))
        await backend.dispatch(service, record)
        await asyncio.wait_for(completed.wait(), timeout=30)
        stored = await queue_backend.get_task(result.id)
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)

    assert stored is not None
    assert stored.status == "completed"

import asyncio
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.execution.kafka import KafkaExecutionBackend, KafkaExecutionConfig

if TYPE_CHECKING:
    from tests.plugins.redpanda import RedpandaService

pytestmark = pytest.mark.anyio


async def test_kafka_redpanda_dispatch_consume_and_commit(redpanda_service: "RedpandaService") -> "None":
    pytest.importorskip("aiokafka")
    topic = f"{redpanda_service.topic_prefix}dispatch_{uuid4().hex}"

    @task("tests.kafka.redpanda.delivered")
    async def delivered() -> "str":
        return "done"

    execution_config = KafkaExecutionConfig(
        bootstrap_servers=redpanda_service.bootstrap_servers, topic=topic, consumer_group=f"{topic}-workers"
    )
    config = QueueConfig(
        queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
    )
    queue_backend = InMemoryQueueBackend()
    backend = KafkaExecutionBackend(config, execution_config=execution_config)
    try:
        async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
            result = await service.enqueue(delivered.using(execution_backend="kafka"))
            record = await queue_backend.get_task(result.id)
            assert record is not None
            await backend.dispatch(service, record)
            consumer = asyncio.create_task(backend.run_consumer(service, max_concurrency=1, drain_timeout=1))
            try:
                stored: "Any | None" = None
                for _ in range(200):
                    stored = await queue_backend.get_task(result.id)
                    if stored is not None and stored.status == "completed":
                        break
                    await asyncio.sleep(0.05)
                assert stored is not None
                assert stored.status == "completed"
            finally:
                consumer.cancel()
                await asyncio.gather(consumer, return_exceptions=True)
    finally:
        await backend.close()

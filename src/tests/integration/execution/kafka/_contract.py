import asyncio
from typing import Any

import pytest

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.execution.kafka import KafkaExecutionBackend, KafkaExecutionConfig


async def assert_kafka_transport_contract(*, bootstrap_servers: "str", topic: "str") -> "None":
    """Assert dispatch, execution, and durable group-offset settlement."""
    aiokafka = pytest.importorskip("aiokafka")

    @task(f"tests.kafka.{topic}.delivered")
    async def delivered() -> "str":
        return "done"

    consumer_group = f"{topic}-workers"
    execution_config = KafkaExecutionConfig(
        bootstrap_servers=bootstrap_servers, topic=topic, consumer_group=consumer_group
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
            runner = asyncio.create_task(backend.run_consumer(service, max_concurrency=1, drain_timeout=1))
            try:
                stored: "Any | None" = None
                for _ in range(200):
                    stored = await queue_backend.get_task(result.id)
                    if stored is not None and stored.status == "completed":
                        break
                    await asyncio.sleep(0.05)
                assert stored is not None
                assert stored.status == "completed"
                verifier = aiokafka.AIOKafkaConsumer(
                    bootstrap_servers=bootstrap_servers, group_id=consumer_group, enable_auto_commit=False
                )
                await verifier.start()
                try:
                    partition = aiokafka.TopicPartition(topic, 0)
                    committed: "int | None" = None
                    for _ in range(100):
                        committed = await verifier.committed(partition)
                        if committed == 1:
                            break
                        await asyncio.sleep(0.05)
                finally:
                    await verifier.stop()
                assert committed == 1
            finally:
                runner.cancel()
                await asyncio.gather(runner, return_exceptions=True)
    finally:
        await backend.close()

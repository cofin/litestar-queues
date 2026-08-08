import asyncio
from typing import TYPE_CHECKING, Any

import grpc  # type: ignore[import-untyped]
import pytest

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.execution.pubsub import PubSubExecutionBackend, PubSubExecutionConfig

if TYPE_CHECKING:
    from tests.plugins.pubsub import PubSubEmulatorService

pytestmark = pytest.mark.anyio


async def test_official_emulator_dispatches_and_streaming_consumer_executes(
    pubsub_emulator_service: "PubSubEmulatorService",
) -> "None":
    publisher_module = pytest.importorskip("google.pubsub_v1.services.publisher")
    subscriber_module = pytest.importorskip("google.pubsub_v1.services.subscriber")
    publisher_transport_module = pytest.importorskip("google.pubsub_v1.services.publisher.transports.grpc_asyncio")
    subscriber_transport_module = pytest.importorskip("google.pubsub_v1.services.subscriber.transports.grpc_asyncio")
    channel = grpc.aio.insecure_channel(pubsub_emulator_service.endpoint)
    publisher = publisher_module.PublisherAsyncClient(
        transport=publisher_transport_module.PublisherGrpcAsyncIOTransport(channel=channel)
    )
    subscriber = subscriber_module.SubscriberAsyncClient(
        transport=subscriber_transport_module.SubscriberGrpcAsyncIOTransport(channel=channel)
    )
    topic_id = f"{pubsub_emulator_service.resource_prefix}dispatch"
    subscription_id = f"{pubsub_emulator_service.resource_prefix}workers"
    topic_path = publisher.topic_path(pubsub_emulator_service.project_id, topic_id)
    subscription_path = subscriber.subscription_path(pubsub_emulator_service.project_id, subscription_id)
    await publisher.create_topic(request={"name": topic_path})
    await subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path})

    @task("tests.pubsub.emulator.delivered")
    async def delivered() -> "str":
        return "done"

    execution_config = PubSubExecutionConfig(
        project_id=pubsub_emulator_service.project_id,
        topic_id=topic_id,
        subscription_id=subscription_id,
        api_endpoint=pubsub_emulator_service.endpoint,
        api_insecure=True,
    )
    config = QueueConfig(
        queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
    )
    queue_backend = InMemoryQueueBackend()
    backend = PubSubExecutionBackend(
        config, execution_config=execution_config, publisher=publisher, subscriber=subscriber
    )
    try:
        async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
            result = await service.enqueue(delivered.using(execution_backend="pubsub"))
            record = await queue_backend.get_task(result.id)
            assert record is not None
            await backend.dispatch(service, record)
            consumer = asyncio.create_task(backend.run_consumer(service, max_concurrency=1, drain_timeout=1))
            try:
                stored: "Any | None" = None
                for _ in range(100):
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
        await subscriber.delete_subscription(request={"subscription": subscription_path})
        await publisher.delete_topic(request={"topic": topic_path})
        await subscriber.transport.close()
        await publisher.transport.close()
        await channel.close()

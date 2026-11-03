"""Configuring Pub/Sub, Amazon SQS, RabbitMQ, and Apache Kafka execution transports."""

from litestar_queues import QueueConfig, WorkerConfig
from litestar_queues.backends.redis import RedisBackendConfig
from litestar_queues.execution.kafka import KafkaExecutionConfig
from litestar_queues.execution.pubsub import PubSubExecutionConfig
from litestar_queues.execution.rabbitmq import RabbitMQExecutionConfig
from litestar_queues.execution.sqs import SqsExecutionConfig


def build_broker_configs() -> dict[str, QueueConfig]:
    """Construct QueueConfig instances for all four message broker execution transports."""
    storage = RedisBackendConfig(url="redis://redis.internal:6379/0")
    return {
        "pubsub": QueueConfig(
            queue_backend=storage,
            execution_backend=PubSubExecutionConfig(
                project_id="my-gcp-project",
                topic_id="litestar-queue-tasks",
                subscription_id="litestar-queue-tasks-sub",
                ack_deadline=60,
                ack_extension_interval=30,
            ),
            worker=WorkerConfig(placement="external"),
        ),
        "sqs": QueueConfig(
            queue_backend=storage,
            execution_backend=SqsExecutionConfig(
                queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/litestar-queues",
                region_name="us-east-1",
                wait_time_seconds=20,
                visibility_timeout=60,
                visibility_extension_interval=30,
            ),
            worker=WorkerConfig(placement="external"),
        ),
        "rabbitmq": QueueConfig(
            queue_backend=storage,
            execution_backend=RabbitMQExecutionConfig(
                amqp_url="amqps://app:secret@rabbitmq.internal:5671/prod",
                queue_name="litestar-queues.tasks",
                declare_queue=True,
                delayed_retry_type="returned",
            ),
            worker=WorkerConfig(placement="external"),
        ),
        "kafka": QueueConfig(
            queue_backend=storage,
            execution_backend=KafkaExecutionConfig(
                bootstrap_servers="kafka-1.internal:9092,kafka-2.internal:9092",
                topic="litestar-queues-tasks",
                consumer_group="litestar-queues-consumers",
            ),
            worker=WorkerConfig(placement="external"),
        ),
    }


async def main() -> None:
    """Verify broker execution configurations validate cleanly."""
    configs = build_broker_configs()
    assert set(configs) == {"pubsub", "sqs", "rabbitmq", "kafka"}
    pubsub_backend = configs["pubsub"].execution_backend
    assert isinstance(pubsub_backend, PubSubExecutionConfig)
    assert pubsub_backend.topic_path == "projects/my-gcp-project/topics/litestar-queue-tasks"

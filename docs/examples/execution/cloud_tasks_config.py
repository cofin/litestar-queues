"""Configuring CloudTasksExecutionConfig for consumer and producer-only services."""

from litestar_queues import QueueConfig, WorkerConfig
from litestar_queues.backends.redis import RedisBackendConfig
from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig

DISPATCH_DEADLINE_SECONDS = 900


def build_consumer_cloud_tasks_config() -> QueueConfig:
    """Configure a Cloud Run service that both enqueues and receives Cloud Tasks HTTP deliveries."""
    return QueueConfig(
        queue_backend=RedisBackendConfig(url="redis://redis.internal:6379/0"),
        execution_backend=CloudTasksExecutionConfig(
            project_id="my-gcp-project",
            location="us-central1",
            queue_id="primary-tasks",
            service_url="https://worker-service-uc.a.run.app",
            service_account_email="cloud-tasks-invoker@my-gcp-project.iam.gserviceaccount.com",
            dispatch_deadline=DISPATCH_DEADLINE_SECONDS,
            response_margin=30.0,
            default_task_timeout=840.0,
            mount_route=True,
            trust_platform_auth=True,
            verify_oidc=True,
        ),
        worker=WorkerConfig(placement="external", stale_after=930.0),
    )


def build_producer_only_cloud_tasks_config() -> QueueConfig:
    """Configure a public web tier that dispatches to Cloud Tasks without mounting the delivery route."""
    return QueueConfig(
        queue_backend=RedisBackendConfig(url="redis://redis.internal:6379/0"),
        execution_backend=CloudTasksExecutionConfig(
            project_id="my-gcp-project",
            location="us-central1",
            queue_id="primary-tasks",
            service_url="https://worker-service-uc.a.run.app",
            service_account_email="cloud-tasks-invoker@my-gcp-project.iam.gserviceaccount.com",
            mount_route=False,
        ),
        worker=WorkerConfig(placement="external"),
    )


async def main() -> None:
    """Verify consumer and producer-only CloudTasksExecutionConfig settings."""
    consumer = build_consumer_cloud_tasks_config()
    producer = build_producer_only_cloud_tasks_config()
    assert isinstance(consumer.execution_backend, CloudTasksExecutionConfig)
    assert isinstance(producer.execution_backend, CloudTasksExecutionConfig)
    assert consumer.execution_backend.mount_route is True
    assert consumer.execution_backend.verify_oidc is True
    assert consumer.execution_backend.dispatch_deadline == DISPATCH_DEADLINE_SECONDS
    assert producer.execution_backend.mount_route is False

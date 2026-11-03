"""Routing tasks across multiple execution backends with QueueConfig.execution_backends."""

from litestar_queues import QueueConfig, WorkerConfig, task
from litestar_queues.backends.redis import RedisBackendConfig
from litestar_queues.execution.cloudrun import CloudRunExecutionConfig
from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig


@task("webhooks.deliver_event", queue="webhooks", execution_backend="cloudtasks")
async def deliver_event(event_id: str) -> str:
    """Deliver a short-lived webhook via Google Cloud Tasks."""
    return f"delivered:{event_id}"


@task("ml.backfill_embeddings", queue="batch", execution_backend="cloudrun", execution_profile="gpu-batch")
async def backfill_embeddings(dataset_id: str) -> str:
    """Execute a long-running batch job via Google Cloud Run Jobs."""
    return f"backfilled:{dataset_id}"


def build_hybrid_execution_config() -> QueueConfig:
    """Register both Cloud Tasks and Cloud Run Jobs alongside a default execution backend."""
    cloud_tasks = CloudTasksExecutionConfig(
        project_id="my-gcp-project",
        location="us-central1",
        queue_id="webhooks-queue",
        service_url="https://api-worker-uc.a.run.app",
        service_account_email="tasks-invoker@my-gcp-project.iam.gserviceaccount.com",
        trust_platform_auth=True,
        verify_oidc=True,
    )
    cloud_run = CloudRunExecutionConfig(
        project_id="my-gcp-project",
        region="us-central1",
        job_name="queue-worker-default",
        profiles={"gpu-batch": "queue-worker-gpu"},
        schedules_on_enqueue=True,
    )
    return QueueConfig(
        queue_backend=RedisBackendConfig(url="redis://redis.internal:6379/0"),
        execution_backend=cloud_tasks,
        execution_backends=(cloud_tasks, cloud_run),
        worker=WorkerConfig(placement="external"),
    )


async def main() -> None:
    """Verify multi-backend registration and resolution."""
    config = build_hybrid_execution_config()
    resolved = config.resolved_execution_backends()
    assert set(resolved) == {"cloudtasks", "cloudrun"}
    assert deliver_event.execution_backend == "cloudtasks"
    assert backfill_embeddings.execution_backend == "cloudrun"
    assert backfill_embeddings.execution_profile == "gpu-batch"

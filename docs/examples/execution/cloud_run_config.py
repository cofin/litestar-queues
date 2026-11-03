"""Configuring CloudRunExecutionConfig with execution profiles and schedules_on_enqueue."""

from litestar_queues import QueueConfig, WorkerConfig, task
from litestar_queues.backends.redis import RedisBackendConfig
from litestar_queues.execution.cloudrun import CloudRunExecutionConfig


@task("reports.render_annual_pdf", queue="reports", execution_backend="cloudrun", execution_profile="high-memory")
async def render_annual_pdf(organization_id: str) -> str:
    """Render a large PDF inside a high-memory Cloud Run Job execution."""
    return f"pdf:{organization_id}"


def build_cloud_run_config() -> QueueConfig:
    """Configure Cloud Run Jobs execution with profile mapping and direct enqueue dispatch."""
    execution_config = CloudRunExecutionConfig(
        project_id="my-gcp-project",
        region="us-central1",
        job_name="litestar-worker-standard",
        profiles={"default": "litestar-worker-standard", "high-memory": "litestar-worker-highmem"},
        schedules_on_enqueue=True,
    )
    return QueueConfig(
        queue_backend=RedisBackendConfig(url="redis://redis.internal:6379/0"),
        execution_backend=execution_config,
        worker=WorkerConfig(placement="external"),
    )


async def main() -> None:
    """Verify Cloud Run profile resolution and task routing."""
    config = build_cloud_run_config()
    assert isinstance(config.execution_backend, CloudRunExecutionConfig)
    assert config.execution_backend.schedules_on_enqueue is True
    assert config.execution_backend.resolve_job_name("high-memory") == "litestar-worker-highmem"
    assert config.execution_backend.resolve_job_name(None) == "litestar-worker-standard"
    assert render_annual_pdf.execution_profile == "high-memory"

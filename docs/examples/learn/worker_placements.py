"""Configuring worker placement, concurrency, adaptive polling, heartbeats, and terminal cleanup."""

from litestar_queues import QueueConfig, WorkerConfig

TERMINAL_RETENTION_SECONDS = 3600.0


def build_production_worker_config() -> QueueConfig:
    """Construct a QueueConfig tuned for external workers with adaptive polling and lease recovery."""
    return QueueConfig(
        queue_backend="memory",
        execution_backend="local",
        worker=WorkerConfig(
            placement="external",
            queues=("critical", "default", "bulk"),
            max_concurrency=20,
            queue_concurrency={"critical": 12, "bulk": 4},
            batch_size=10,
            poll_interval=0.25,
            poll_backoff_max=5.0,
            poll_backoff_multiplier=1.5,
            poll_jitter=0.2,
            heartbeat_interval=10.0,
            heartbeat_miss_threshold=2,
            stale_after=60.0,
            stale_check_interval=30.0,
            terminal_retention=TERMINAL_RETENTION_SECONDS,
            terminal_check_interval=300.0,
            requeue_on_shutdown=True,
            max_interruptions=3,
            run_app_lifespan=True,
        ),
    )


async def main() -> None:
    """Verify WorkerConfig validation and field assignment."""
    config = build_production_worker_config()
    assert config.worker.placement == "external"
    assert config.worker.queue_concurrency == {"critical": 12, "bulk": 4}
    assert config.worker.terminal_retention == TERMINAL_RETENTION_SECONDS
    assert config.worker.run_app_lifespan is True

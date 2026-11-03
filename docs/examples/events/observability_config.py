"""Configuring OpenTelemetry, Prometheus, and SQLCommenter integration with ObservabilityConfig."""

from litestar_queues import QueueConfig, WorkerConfig
from litestar_queues.observability import DEFAULT_DURATION_BUCKETS, ObservabilityConfig


def build_observability_queue_config() -> QueueConfig:
    """Configure queue observability with automatic plugin detection and custom metric prefix."""
    return QueueConfig(
        queue_backend="memory",
        execution_backend="local",
        worker=WorkerConfig(placement="external"),
        observability=ObservabilityConfig(
            enable_otel=None,
            enable_prometheus=None,
            enable_sqlcommenter=True,
            metric_prefix="billing_queues",
            duration_buckets=DEFAULT_DURATION_BUCKETS,
        ),
    )


async def main() -> None:
    """Verify ObservabilityConfig policy evaluation."""
    config = build_observability_queue_config()
    assert config.observability is not None
    assert config.observability.should_enable_sqlcommenter() is True
    assert config.observability.metric_prefix == "billing_queues"

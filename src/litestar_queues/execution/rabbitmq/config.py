import sys
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, ClassVar, Literal
from urllib.parse import urlsplit

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig

__all__ = ("RabbitMQExecutionConfig",)

_PYTHON_VERSION = sys.version_info[:2]


@dataclass(slots=True)
class RabbitMQExecutionConfig:
    """RabbitMQ execution-dispatch configuration."""

    backend_name: "ClassVar[str]" = "rabbitmq"
    amqp_url: "str" = field(repr=False)
    """AMQP or AMQPS connection URL; excluded from representations and telemetry."""
    queue_name: "str | None" = None
    """Broker queue name, derived from the queue namespace when omitted."""
    declare_queue: "bool" = True
    """Declare the quorum queue when true; otherwise verify it passively."""
    dispatch_stale_after: "int" = 60
    """Age in seconds after which an owned attempt can be rotated and republished."""
    api_timeout: "float" = 30
    """Timeout in seconds for connection and publish operations."""
    delayed_retry_type: "Literal['disabled', 'all', 'failed', 'returned']" = "returned"
    """RabbitMQ 4.3 quorum-queue delayed retry category."""
    delayed_retry_min_ms: "int" = 1_000
    """Minimum broker-managed redelivery delay in milliseconds."""
    delayed_retry_max_ms: "int" = 30_000
    """Maximum broker-managed redelivery delay in milliseconds."""
    consumer_timeout_ms: "int | None" = None
    """Optional acknowledgement timeout; must exceed legitimate task duration."""

    def __post_init__(self) -> "None":
        if _PYTHON_VERSION < (3, 11):
            msg = "RabbitMQ execution requires Python 3.11 or newer."
            raise QueueConfigurationError(msg)
        parsed = urlsplit(self.amqp_url)
        if parsed.scheme not in {"amqp", "amqps"} or not parsed.hostname:
            msg = "RabbitMQExecutionConfig.amqp_url must be an absolute amqp:// or amqps:// URL."
            raise QueueConfigurationError(msg)
        if self.queue_name is not None and not self.queue_name.strip():
            msg = "RabbitMQExecutionConfig.queue_name must not be empty."
            raise QueueConfigurationError(msg)
        if self.dispatch_stale_after <= 0 or self.api_timeout <= 0:
            msg = "dispatch_stale_after and api_timeout must be positive."
            raise QueueConfigurationError(msg)
        if self.delayed_retry_type not in {"disabled", "all", "failed", "returned"}:
            msg = "delayed_retry_type must be disabled, all, failed, or returned."
            raise QueueConfigurationError(msg)
        if self.delayed_retry_min_ms <= 0 or self.delayed_retry_max_ms < self.delayed_retry_min_ms:
            msg = "delayed retry bounds must be positive and ordered."
            raise QueueConfigurationError(msg)
        if self.consumer_timeout_ms is not None and self.consumer_timeout_ms <= 0:
            msg = "consumer_timeout_ms must be positive when configured."
            raise QueueConfigurationError(msg)


def _execution_config_from_queue_config(config: "QueueConfig | None") -> "RabbitMQExecutionConfig":
    if config is not None and isinstance(config.execution_backend, RabbitMQExecutionConfig):
        execution_config = config.execution_backend
        if execution_config.queue_name is None:
            return replace(execution_config, queue_name=config.names.resource("rabbitmq"))
        return execution_config
    msg = "RabbitMQ execution requires QueueConfig.execution_backend=RabbitMQExecutionConfig(...)."
    raise QueueConfigurationError(msg)

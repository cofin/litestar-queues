from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig

__all__ = ("KafkaExecutionConfig",)


@dataclass(slots=True)
class KafkaExecutionConfig:
    """Kafka execution-dispatch configuration."""

    backend_name: "ClassVar[str]" = "kafka"
    bootstrap_servers: "str"
    """Comma-separated Kafka bootstrap servers."""
    topic: "str" = "litestar-queues"
    """Topic receiving task-id dispatch records."""
    consumer_group: "str" = "litestar-queues"
    """Consumer group used by ``litestar queues run-consumer``."""
    dispatch_stale_after: "int" = 60
    """Age after which a reserved attempt can be rotated and republished."""
    api_timeout: "float" = 30
    """Timeout applied to producer delivery acknowledgements."""
    producer_options: "dict[str, Any]" = field(default_factory=dict, repr=False)
    """Additional ``AIOKafkaProducer`` options, such as TLS or SASL settings."""
    consumer_options: "dict[str, Any]" = field(default_factory=dict, repr=False)
    """Additional ``AIOKafkaConsumer`` options, such as TLS or SASL settings."""

    def __post_init__(self) -> "None":
        for field_name in ("bootstrap_servers", "topic", "consumer_group"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                msg = f"KafkaExecutionConfig.{field_name} must not be empty."
                raise QueueConfigurationError(msg)
        if self.dispatch_stale_after <= 0 or self.api_timeout <= 0:
            msg = "dispatch_stale_after and api_timeout must be positive."
            raise QueueConfigurationError(msg)
        producer_owned = {"acks", "bootstrap_servers"}.intersection(self.producer_options)
        consumer_owned = {"auto_offset_reset", "bootstrap_servers", "enable_auto_commit", "group_id"}.intersection(
            self.consumer_options
        )
        invalid = [
            *(f"producer_options.{name}" for name in sorted(producer_owned)),
            *(f"consumer_options.{name}" for name in sorted(consumer_owned)),
        ]
        if invalid:
            msg = f"Kafka client options are owned by the backend and cannot be overridden: {', '.join(invalid)}."
            raise QueueConfigurationError(msg)

    def __repr__(self) -> "str":
        return (
            "KafkaExecutionConfig(bootstrap_servers='<redacted>', "
            f"topic={self.topic!r}, consumer_group={self.consumer_group!r}, "
            f"dispatch_stale_after={self.dispatch_stale_after!r}, api_timeout={self.api_timeout!r})"
        )


def _execution_config_from_queue_config(config: "QueueConfig | None") -> "KafkaExecutionConfig":
    if config is not None and isinstance(config.execution_backend, KafkaExecutionConfig):
        return config.execution_backend
    msg = "Kafka execution requires QueueConfig.execution_backend=KafkaExecutionConfig(...)."
    raise QueueConfigurationError(msg)

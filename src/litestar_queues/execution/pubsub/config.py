from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig

__all__ = ("PubSubExecutionConfig",)

MIN_ACK_DEADLINE = 10
MAX_ACK_DEADLINE = 600


@dataclass(slots=True)
class PubSubExecutionConfig:
    """Google Cloud Pub/Sub execution-dispatch configuration."""

    backend_name: "ClassVar[str]" = "pubsub"
    project_id: "str"
    """Google Cloud project containing the topic and subscription."""
    topic_id: "str"
    """Topic receiving task-id dispatch messages."""
    subscription_id: "str"
    """Pull subscription consumed by ``litestar queues run-consumer``."""
    ack_deadline: "int" = 60
    """Initial acknowledgment deadline in seconds; never the queue lease."""
    ack_extension_interval: "int" = 30
    """Courtesy acknowledgment-deadline extension cadence during execution."""
    dispatch_stale_after: "int" = 60
    """Age after which a reserved attempt can be rotated and republished."""
    api_timeout: "float" = 30
    """Timeout applied to unary Pub/Sub API calls."""
    api_endpoint: "str | None" = None
    """Optional API host override, including the official emulator host."""
    api_insecure: "bool" = False
    """Use plaintext gRPC for a configured local emulator endpoint."""

    def __post_init__(self) -> "None":
        for field_name in ("project_id", "topic_id", "subscription_id"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                msg = f"PubSubExecutionConfig.{field_name} must not be empty."
                raise QueueConfigurationError(msg)
        if not MIN_ACK_DEADLINE <= self.ack_deadline <= MAX_ACK_DEADLINE:
            msg = "ack_deadline must be between 10 and 600 seconds."
            raise QueueConfigurationError(msg)
        if not 0 < self.ack_extension_interval < self.ack_deadline:
            msg = "ack_extension_interval must be below ack_deadline."
            raise QueueConfigurationError(msg)
        if self.dispatch_stale_after <= 0 or self.api_timeout <= 0:
            msg = "dispatch_stale_after and api_timeout must be positive."
            raise QueueConfigurationError(msg)
        if self.api_insecure and not self.api_endpoint:
            msg = "api_insecure=True requires api_endpoint."
            raise QueueConfigurationError(msg)

    @property
    def topic_path(self) -> "str":
        """Return the fully qualified Pub/Sub topic path."""
        return f"projects/{self.project_id}/topics/{self.topic_id}"

    @property
    def subscription_path(self) -> "str":
        """Return the fully qualified Pub/Sub subscription path."""
        return f"projects/{self.project_id}/subscriptions/{self.subscription_id}"


def _execution_config_from_queue_config(config: "QueueConfig | None") -> "PubSubExecutionConfig":
    if config is not None and isinstance(config.execution_backend, PubSubExecutionConfig):
        return config.execution_backend
    msg = "Pub/Sub execution requires QueueConfig.execution_backend=PubSubExecutionConfig(...)."
    raise QueueConfigurationError(msg)

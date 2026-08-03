# ruff: noqa: PLR2004

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig

__all__ = ("SqsExecutionConfig",)


@dataclass(slots=True)
class SqsExecutionConfig:
    """Amazon SQS execution-dispatch configuration."""

    backend_name: "ClassVar[str]" = "sqs"
    queue_url: "str"
    """Absolute URL of the target SQS queue."""
    region_name: "str | None" = None
    """Optional AWS region override; the normal SDK chain supplies the default."""
    endpoint_url: "str | None" = None
    """Optional endpoint override, primarily for LocalStack."""
    fifo: "bool" = False
    """Whether to emit FIFO-only group and deduplication fields."""
    message_group_id: "str | None" = None
    """Explicit FIFO group; when omitted a stable queue-name group is derived."""
    wait_time_seconds: "int" = 20
    """SQS long-poll duration."""
    receive_batch_size: "int" = 10
    """Maximum number of messages requested per receive."""
    visibility_timeout: "int" = 60
    """Initial SQS delivery visibility, independent from queue heartbeat leases."""
    visibility_extension_interval: "int" = 30
    """Courtesy visibility-extension cadence during local execution."""
    dispatch_stale_after: "int" = 60
    """Age after which an owned attempt can be atomically rotated and republished."""
    api_timeout: "float" = 30
    """Timeout applied to individual SQS API operations."""

    def __post_init__(self) -> "None":
        if not self.queue_url:
            msg = "SqsExecutionConfig.queue_url must not be empty."
            raise QueueConfigurationError(msg)
        if not 0 <= self.wait_time_seconds <= 20:
            msg = "wait_time_seconds must be between 0 and 20."
            raise QueueConfigurationError(msg)
        if not 1 <= self.receive_batch_size <= 10:
            msg = "receive_batch_size must be between 1 and 10."
            raise QueueConfigurationError(msg)
        if not 0 <= self.visibility_timeout <= 43200:
            msg = "visibility_timeout must be between 0 and 43200."
            raise QueueConfigurationError(msg)
        if not 0 < self.visibility_extension_interval < self.visibility_timeout:
            msg = "visibility_extension_interval must be below visibility_timeout."
            raise QueueConfigurationError(msg)
        if self.dispatch_stale_after <= 0 or self.api_timeout <= 0:
            msg = "dispatch_stale_after and api_timeout must be positive."
            raise QueueConfigurationError(msg)
        if self.message_group_id is not None and not self.fifo:
            msg = "message_group_id requires fifo=True."
            raise QueueConfigurationError(msg)


def _execution_config_from_queue_config(config: "QueueConfig | None") -> "SqsExecutionConfig":
    if config is not None and isinstance(config.execution_backend, SqsExecutionConfig):
        return config.execution_backend
    msg = "SQS execution requires QueueConfig.execution_backend=SqsExecutionConfig(...)."
    raise QueueConfigurationError(msg)

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig

__all__ = ("CloudRunExecutionConfig",)


@dataclass(slots=True)
class CloudRunExecutionConfig:
    """Configuration for Cloud Run Jobs execution."""

    backend_name: "ClassVar[str]" = "cloudrun"
    project_id: "str"
    region: "str" = "us-central1"
    job_name: "str | None" = None
    profiles: "dict[str, str]" = field(default_factory=dict)
    timeout: "int" = 300
    poll_interval: "float" = 5.0
    env_prefix: "str" = "LITESTAR_QUEUES"
    extra_env: "dict[str, str]" = field(default_factory=dict)
    fallback_execution_backend: "str | None" = "local"

    def resolve_job_name(self, profile: "str | None" = None) -> "str":
        """Return the Cloud Run Job name for a profile.

        Returns:
            The resolved Cloud Run Job name.

        Raises:
            QueueConfigurationError: If no job name can be resolved.
        """
        if profile is not None and profile in self.profiles:
            return self.profiles[profile]
        if self.job_name is not None:
            return self.job_name
        if "default" in self.profiles:
            return self.profiles["default"]
        msg = "CloudRunExecutionConfig requires job_name or profiles['default']."
        raise QueueConfigurationError(msg)

    def env_name(self, suffix: "str") -> "str":
        """Return an environment variable name using the configured prefix."""
        normalized = suffix.upper().removeprefix(f"{self.env_prefix}_")
        return f"{self.env_prefix}_{normalized}"


def _execution_config_from_queue_config(config: "QueueConfig | None") -> "CloudRunExecutionConfig":
    """Resolve Cloud Run execution config from a QueueConfig.

    Returns:
        The resolved Cloud Run execution config.

    Raises:
        QueueConfigurationError: If no Cloud Run execution config is available.
    """
    if config is not None and isinstance(config.execution_backend, CloudRunExecutionConfig):
        return config.execution_backend

    msg = "Cloud Run execution requires QueueConfig.execution_backend with a CloudRunExecutionConfig value."
    raise QueueConfigurationError(msg)

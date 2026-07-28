from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

from litestar_queues._environment import TASK_ID_ENV
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig
    from litestar_queues.namespace import QueueNamespace

__all__ = ("CloudRunExecutionConfig",)


@dataclass(slots=True)
class CloudRunExecutionConfig:
    """Configuration for Cloud Run Jobs execution."""

    backend_name: "ClassVar[str]" = "cloudrun"
    project_id: "str"
    """Google Cloud project containing the target jobs."""

    region: "str" = "us-central1"
    """Google Cloud region containing the target jobs."""

    job_name: "str | None" = None
    """Default Cloud Run Job name; ``None`` requires a matching profile."""

    profiles: "dict[str, str]" = field(default_factory=dict)
    """Execution-profile names mapped to Cloud Run Job names."""

    timeout: "int" = 300
    """Cloud Run API operation timeout in seconds."""

    env_prefix: "str | None" = None
    """Explicit environment prefix; ``None`` derives it from ``QueueConfig.namespace``."""

    extra_env: "dict[str, str]" = field(default_factory=dict)
    """Additional environment variables passed to every Cloud Run execution."""

    fallback_execution_backend: "str | None" = None
    """Backend used after dispatch failure; ``None`` propagates the failure."""

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

    def env_name(self, suffix: "str", *, namespace: "QueueNamespace | None" = None) -> "str":
        """Return an environment variable name using the configured prefix."""
        if suffix.upper() == "TASK_ID":
            return TASK_ID_ENV
        prefix = self.env_prefix or (namespace.root.upper() if namespace is not None else "LITESTAR_QUEUES")
        normalized = suffix.upper().removeprefix(f"{prefix}_")
        return f"{prefix}_{normalized}"


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

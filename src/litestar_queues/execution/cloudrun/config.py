from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig

__all__ = ("CloudRunExecutionConfig", "cloudrun_config_from_queue_config")


@dataclass(slots=True)
class CloudRunExecutionConfig:
    """Configuration for Cloud Run Jobs execution."""

    project_id: str
    region: str = "us-central1"
    job_name: str | None = None
    profiles: dict[str, str] = field(default_factory=dict)
    timeout: int = 300
    poll_interval: float = 5.0
    env_prefix: str = "LITESTAR_QUEUES"
    extra_env: dict[str, str] = field(default_factory=dict)
    fallback_execution_backend: str | None = "local"

    def resolve_job_name(self, profile: str | None = None) -> str:
        """Return the Cloud Run Job name for a profile."""
        if profile is not None and profile in self.profiles:
            return self.profiles[profile]
        if self.job_name is not None:
            return self.job_name
        if "default" in self.profiles:
            return self.profiles["default"]
        msg = "CloudRunExecutionConfig requires job_name or profiles['default']."
        raise QueueConfigurationError(msg)

    def env_name(self, suffix: str) -> str:
        """Return an environment variable name using the configured prefix."""
        normalized = suffix.upper().removeprefix(f"{self.env_prefix}_")
        return f"{self.env_prefix}_{normalized}"


def cloudrun_config_from_queue_config(config: "QueueConfig | None") -> CloudRunExecutionConfig:
    """Resolve Cloud Run execution config from a QueueConfig."""
    raw_config: Any = None
    if config is not None:
        raw_config = config.execution_backend_config
        if isinstance(raw_config, dict) and "cloudrun" in raw_config:
            raw_config = raw_config["cloudrun"]

    if isinstance(raw_config, CloudRunExecutionConfig):
        return raw_config

    if isinstance(raw_config, dict) and raw_config:
        return CloudRunExecutionConfig(**raw_config)

    msg = "Cloud Run execution requires QueueConfig.execution_backend_config with CloudRunExecutionConfig values."
    raise QueueConfigurationError(msg)

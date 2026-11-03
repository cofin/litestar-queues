"""Redis queue backend configuration."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from redis.asyncio.client import Redis

__all__ = ("DEFAULT_WAKEUP_CHANNEL", "RedisBackendConfig")

DEFAULT_WAKEUP_CHANNEL = "litestar_queues:worker_wakeups"


@dataclass(slots=True)
class RedisBackendConfig:
    """Configuration for the Redis queue backend."""

    backend_name: "ClassVar[str]" = "redis"
    """Registered queue backend identifier (`"redis"`)."""

    url: "str" = "redis://localhost:6379/0"
    """Redis connection URL used when no client is injected."""

    key_prefix: "str | None" = None
    """Prefix applied to every queue key; ``None`` derives it from ``QueueConfig.namespace``."""

    worker_wakeups: "bool" = True
    """Whether workers subscribe for Redis wakeup hints between polling passes."""

    wakeup_channel: "str | None" = None
    """Worker-wakeup pub/sub channel; ``None`` derives it from ``QueueConfig.namespace``."""

    client: "Redis | None" = None
    """Injected async Redis client; ``None`` creates one from ``url``."""

    terminal_ttl: "float | None" = None
    """Native TTL in seconds applied to terminal task records; ``None`` retains records until maintenance cleanup."""

    def __post_init__(self) -> "None":
        """Validate Redis backend configuration values."""
        if self.terminal_ttl is not None and self.terminal_ttl <= 0:
            msg = "RedisBackendConfig.terminal_ttl must be greater than 0."
            raise QueueConfigurationError(msg)

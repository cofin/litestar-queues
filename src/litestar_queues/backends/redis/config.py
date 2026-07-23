"""Redis queue backend configuration."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from redis.asyncio.client import Redis

__all__ = ("DEFAULT_WAKEUP_CHANNEL", "RedisBackendConfig")

DEFAULT_WAKEUP_CHANNEL = "litestar_queues:worker_wakeups"


@dataclass(slots=True)
class RedisBackendConfig:
    """Configuration for the Redis queue backend."""

    backend_name: "ClassVar[str]" = "redis"
    url: "str" = "redis://localhost:6379/0"
    """Redis connection URL used when no client is injected."""

    key_prefix: "str" = "litestar_queues"
    """Prefix applied to every queue key stored in Redis."""

    worker_wakeups: "bool" = True
    """Whether workers subscribe for Redis wakeup hints between polling passes."""

    wakeup_channel: "str" = DEFAULT_WAKEUP_CHANNEL
    """Redis pub/sub channel used for worker wakeup hints."""

    client: "Redis | None" = None
    """Injected async Redis client; ``None`` creates one from ``url``."""

"""Redis queue backend."""

from typing import TYPE_CHECKING, Any

from litestar_queues.backends._redis_like import RedisLikeQueueBackend
from litestar_queues.backends.redis._typing import create_redis_client
from litestar_queues.backends.redis.config import RedisBackendConfig

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig

__all__ = ("RedisBackendConfig", "RedisQueueBackend")


class RedisQueueBackend(RedisLikeQueueBackend):
    """Redis-backed queue backend."""

    def __init__(
        self,
        config: "QueueConfig | None" = None,
        *,
        backend_config: RedisBackendConfig | None = None,
        client: Any | None = None,
        url: str | None = None,
        key_prefix: str | None = None,
        notifications: bool | None = None,
        notification_channel: str | None = None,
        lock_timeout: float | None = None,
        poll_interval: float | None = None,
    ) -> None:
        """Initialize the Redis queue backend."""
        backend_config = backend_config or RedisBackendConfig()
        super().__init__(
            config=config,
            backend_name="redis",
            client=client if client is not None else backend_config.client,
            url=url if url is not None else backend_config.url,
            key_prefix=key_prefix if key_prefix is not None else backend_config.key_prefix,
            notifications=notifications if notifications is not None else backend_config.notifications,
            notification_channel=(
                notification_channel if notification_channel is not None else backend_config.notification_channel
            ),
            lock_timeout=lock_timeout if lock_timeout is not None else backend_config.lock_timeout,
            poll_interval=poll_interval if poll_interval is not None else backend_config.poll_interval,
        )

    def _create_client(self, url: str) -> Any:
        return create_redis_client(url)

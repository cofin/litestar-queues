"""Valkey queue backend.

The Valkey wire protocol is API-compatible with Redis, so this backend
inherits the full ``RedisQueueBackend`` implementation and only overrides
the client factory (uses ``valkey.asyncio`` instead of ``redis.asyncio``)
plus the ``_backend_name`` ClassVar that drives lock-name error messages
and the ``valkey-pubsub`` notification capability label.
"""

from typing import TYPE_CHECKING, Any, ClassVar, cast

from valkey import asyncio as valkey_asyncio

from litestar_queues.backends.redis.backend import RedisQueueBackend
from litestar_queues.backends.valkey.config import ValkeyBackendConfig

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig

__all__ = ("ValkeyBackendConfig", "ValkeyQueueBackend")


class ValkeyQueueBackend(RedisQueueBackend):
    """Valkey-backed queue backend."""

    _backend_name: ClassVar[str] = "valkey"

    def __init__(
        self,
        config: "QueueConfig | None" = None,
        *,
        backend_config: ValkeyBackendConfig | None = None,
        client: Any | None = None,
        url: str | None = None,
        key_prefix: str | None = None,
        notifications: bool | None = None,
        notification_channel: str | None = None,
        lock_timeout: float | None = None,
        poll_interval: float | None = None,
    ) -> None:
        backend_config = backend_config or ValkeyBackendConfig()
        # ValkeyBackendConfig is structurally identical to RedisBackendConfig;
        # passing the unpacked values through the parent constructor is safe.
        super().__init__(
            config=config,
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
        from_url = cast("Any", valkey_asyncio.from_url)
        return from_url(url, decode_responses=True)

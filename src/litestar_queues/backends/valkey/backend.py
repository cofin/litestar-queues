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

__all__ = ("ValkeyQueueBackend",)


class ValkeyQueueBackend(RedisQueueBackend):
    """Valkey-backed queue backend."""

    _backend_name: "ClassVar[str]" = "valkey"

    def __init__(
        self, config: "QueueConfig | None" = None, *, backend_config: "ValkeyBackendConfig | None" = None
    ) -> "None":
        backend_config = backend_config or ValkeyBackendConfig()
        super().__init__(config=config, backend_config=cast("Any", backend_config))

    def _create_client(self, url: "str") -> "Any":
        from_url = cast("Any", valkey_asyncio.from_url)
        return from_url(url, decode_responses=True)

"""Valkey queue backend configuration."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from valkey.asyncio.client import Valkey

__all__ = ("DEFAULT_NOTIFICATION_CHANNEL", "ValkeyBackendConfig")

DEFAULT_NOTIFICATION_CHANNEL = "litestar_queues:queue_notifications"


@dataclass(slots=True)
class ValkeyBackendConfig:
    """Configuration for the Valkey queue backend."""

    backend_name: "ClassVar[str]" = "valkey"
    url: "str" = "redis://localhost:6379/0"
    key_prefix: "str" = "litestar_queues"
    notifications: "bool" = True
    notification_channel: "str" = DEFAULT_NOTIFICATION_CHANNEL
    lock_timeout: "float" = 5.0
    poll_interval: "float" = 0.1
    client: "Valkey | None" = None

"""Valkey queue backend configuration."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from valkey.asyncio.client import Valkey

__all__ = ("DEFAULT_WAKEUP_CHANNEL", "ValkeyBackendConfig")

DEFAULT_WAKEUP_CHANNEL = "litestar_queues:worker_wakeups"


@dataclass(slots=True)
class ValkeyBackendConfig:
    """Configuration for the Valkey queue backend."""

    backend_name: "ClassVar[str]" = "valkey"
    url: "str" = "redis://localhost:6379/0"
    """Valkey connection URL used when no client is injected."""

    key_prefix: "str" = "litestar_queues"
    """Prefix applied to every queue key stored in Valkey."""

    worker_wakeups: "bool" = True
    """Whether workers subscribe for Valkey wakeup hints between polling passes."""

    wakeup_channel: "str" = DEFAULT_WAKEUP_CHANNEL
    """Valkey pub/sub channel used for worker wakeup hints."""

    client: "Valkey | None" = None
    """Injected async Valkey client; ``None`` creates one from ``url``."""

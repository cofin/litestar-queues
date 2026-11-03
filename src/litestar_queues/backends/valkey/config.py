"""Valkey queue backend configuration."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

from litestar_queues.exceptions import QueueConfigurationError

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

    key_prefix: "str | None" = None
    """Prefix applied to every queue key; ``None`` derives it from ``QueueConfig.namespace``."""

    worker_wakeups: "bool" = True
    """Whether workers subscribe for Valkey wakeup hints between polling passes."""

    wakeup_channel: "str | None" = None
    """Worker-wakeup pub/sub channel; ``None`` derives it from ``QueueConfig.namespace``."""

    client: "Valkey | None" = None
    """Injected async Valkey client; ``None`` creates one from ``url``."""

    terminal_ttl: "float | None" = None
    """Optional retention window in seconds for terminal task hashes.

    When set, Lua scripts atomically apply ``PEXPIRE`` to ``{prefix}:task:{id}``
    upon entering a terminal status (``completed``, ``failed``, ``cancelled``,
    ``expired``) and prune expired secondary index references. ``None`` retains
    terminal records until ``cleanup_terminal()`` removes them.
    """

    def __post_init__(self) -> "None":
        """Validate configuration bounds."""
        if self.terminal_ttl is not None and self.terminal_ttl <= 0:
            msg = "ValkeyBackendConfig.terminal_ttl must be greater than 0."
            raise QueueConfigurationError(msg)

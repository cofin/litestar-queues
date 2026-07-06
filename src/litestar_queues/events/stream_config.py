"""Configuration for plugin-owned queue event streaming endpoints."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from typing import Any

    from litestar.connection import ASGIConnection
    from litestar.types import Guard

    from litestar_queues.events.models import QueueEventScope

    ChannelAuthorizer = Callable[
        ["ASGIConnection", "QueueEventScope", "str | None"], "bool | Awaitable[bool]"
    ]

__all__ = ("EventStreamConfig",)

_DEFAULT_STREAM_SCOPES: "tuple[QueueEventScope, ...]" = ("task", "queue", "worker", "global", "custom")


def _default_stream_scopes() -> "set[QueueEventScope]":
    return set(_DEFAULT_STREAM_SCOPES)


@dataclass(slots=True)
class EventStreamConfig:
    """Configuration for plugin-registered WebSocket queue-event streaming."""

    enabled: "bool" = True
    path: "str" = "/queues/events"
    guards: "list[Guard] | None" = None
    channel_authorizer: "ChannelAuthorizer | None" = None
    scopes: "set[QueueEventScope]" = field(default_factory=_default_stream_scopes)
    heartbeat_interval: "float" = 25.0
    history: "int" = 0
    include_in_schema: "bool" = False
    opt: "dict[str, Any] | None" = None

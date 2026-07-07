"""Configuration for plugin-owned queue event streaming endpoints."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from litestar_queues.events.models import QueueEventScope

if TYPE_CHECKING:
    from litestar.connection import ASGIConnection
    from litestar.handlers.base import BaseRouteHandler
    from litestar.types import SyncOrAsyncUnion

    Guard = Callable[["ASGIConnection", "BaseRouteHandler"], "SyncOrAsyncUnion[None]"]
    ChannelAuthorizer = Callable[["ASGIConnection", QueueEventScope, str | None], bool | Awaitable[bool]]
else:
    Guard = Callable[[Any, Any], Any]
    ChannelAuthorizer = Callable[[Any, QueueEventScope, str | None], bool | Awaitable[bool]]

__all__ = ("ChannelAuthorizer", "EventStreamConfig", "Guard")

_DEFAULT_STREAM_SCOPES: "tuple[QueueEventScope, ...]" = ("task", "queue", "worker", "global", "custom")


def _default_stream_scopes() -> "set[QueueEventScope]":
    return set(_DEFAULT_STREAM_SCOPES)


@dataclass(slots=True)
class EventStreamConfig:
    """Configuration for plugin-registered WebSocket queue-event streaming."""

    enabled: "bool" = True
    sse: "bool" = True
    path: "str" = "/queues/events"
    guards: list[Guard] | None = None
    channel_authorizer: ChannelAuthorizer | None = None
    scopes: set[QueueEventScope] = field(default_factory=_default_stream_scopes)
    heartbeat_interval: "float" = 25.0
    history: "int" = 0
    include_in_schema: "bool" = False
    opt: dict[str, Any] | None = None

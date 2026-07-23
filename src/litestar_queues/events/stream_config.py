"""Configuration for plugin-owned queue event streaming endpoints."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from litestar_queues.events.models import QueueEventScope
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from litestar.connection import ASGIConnection
    from litestar.handlers.base import BaseRouteHandler
    from litestar.types import SyncOrAsyncUnion

    Guard = Callable[["ASGIConnection", "BaseRouteHandler"], "SyncOrAsyncUnion[None]"]
    ChannelAuthorizer = Callable[["ASGIConnection", QueueEventScope, str | None], bool | Awaitable[bool]]
else:
    Guard = Callable[[Any, Any], Any]
    ChannelAuthorizer = Callable[[Any, QueueEventScope, str | None], bool | Awaitable[bool]]

__all__ = ("ChannelAuthorizer", "EventStreamConfig", "EventStreamTransport", "Guard", "UnauthenticatedAccess")

EventStreamTransport = Literal["sse", "websocket"]
UnauthenticatedAccess = Literal["warn", "allow", "error"]

_DEFAULT_STREAM_SCOPES: "tuple[QueueEventScope, ...]" = ("task", "queue", "worker", "global", "custom")


def _default_stream_scopes() -> "set[QueueEventScope]":
    return set(_DEFAULT_STREAM_SCOPES)


@dataclass(slots=True)
class EventStreamConfig:
    """Configuration for plugin-registered WebSocket queue-event streaming."""

    transports: "set[EventStreamTransport]" = field(default_factory=lambda: {"sse", "websocket"})
    """Enabled browser stream transports."""

    path: "str" = "/queues/events"
    """Leading-slash route path shared by configured stream transports."""

    guards: list[Guard] | None = None
    """Litestar route guards applied to stream endpoints; ``None`` adds none."""

    channel_authorizer: ChannelAuthorizer | None = None
    """Per-subscription authorizer; ``None`` leaves channel selection unrestricted."""

    unauthenticated_access: "UnauthenticatedAccess" = "warn"
    """Policy used when stream endpoints have neither guards nor an authorizer."""

    scopes: set[QueueEventScope] = field(default_factory=_default_stream_scopes)
    """Task-event scopes clients may subscribe to."""

    heartbeat_interval: "float" = 25.0
    """SSE keepalive interval in seconds; zero disables keepalives."""

    replay_limit: "int" = 0
    """Maximum retained Channels messages replayed on subscription; zero disables replay."""

    include_in_schema: "bool" = False
    """Whether generated stream routes appear in OpenAPI schema output."""

    opt: dict[str, Any] | None = None
    """Litestar route-handler metadata; ``None`` supplies no metadata."""

    def __post_init__(self) -> "None":
        """Validate stream routing, authorization, and replay bounds."""
        if not self.transports or not self.transports <= {"sse", "websocket"}:
            msg = "EventStreamConfig.transports must contain sse and/or websocket."
            raise QueueConfigurationError(msg)
        if not self.path.startswith("/"):
            msg = "EventStreamConfig.path must start with '/'."
            raise QueueConfigurationError(msg)
        if not self.scopes or not self.scopes <= set(_DEFAULT_STREAM_SCOPES):
            msg = "EventStreamConfig.scopes must contain supported queue event scopes."
            raise QueueConfigurationError(msg)
        if self.heartbeat_interval < 0:
            msg = "EventStreamConfig.heartbeat_interval must be greater than or equal to 0."
            raise QueueConfigurationError(msg)
        if self.replay_limit < 0:
            msg = "EventStreamConfig.replay_limit must be greater than or equal to 0."
            raise QueueConfigurationError(msg)

"""Litestar Channels helpers for queue events."""

import inspect
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager, suppress
from typing import Any, cast

from litestar_queues.events.models import QueueEvent

__all__ = ("ChannelsQueueEventSink", "stream_queue_events")


class ChannelsQueueEventSink:
    """Event sink that publishes to an app-owned Litestar Channels object."""

    __slots__ = ("_channels_backend",)

    def __init__(self, channels_backend: object) -> None:
        self._channels_backend = channels_backend

    @property
    def channels_backend(self) -> object:
        """Return the wrapped Channels backend or plugin."""
        return self._channels_backend

    async def publish(self, event: QueueEvent, *, channels: Sequence[str]) -> None:
        """Publish an event to Litestar Channels."""
        data = event.to_json()
        channels_backend = cast("Any", self._channels_backend)
        if hasattr(self._channels_backend, "wait_published"):
            result = channels_backend.wait_published(data, list(channels))
        else:
            result = channels_backend.publish(data, list(channels))
        if inspect.isawaitable(result):
            await result


async def stream_queue_events(
    socket: Any,
    channels: Sequence[str],
    *,
    history: int = 0,
    channels_backend: object | None = None,
) -> None:
    """Stream queue events from an app-owned Channels subscription to a WebSocket.

    The caller owns route paths, guards, tenant filtering, and authorization.

    Raises:
        RuntimeError: If no Channels backend or plugin can be resolved.
    """
    backend = channels_backend or _resolve_channels_backend(socket)
    if backend is None:
        msg = "A Litestar Channels backend or plugin is required to stream queue events."
        raise RuntimeError(msg)

    await socket.accept()
    seen_dedup_keys: set[str] = set()
    async with _event_stream(backend, channels, history=history) as events:
        async for raw_event in events:
            event = _decode_event(raw_event)
            if event is None:
                continue
            dedup_key = event.idempotency_key if event.idempotency_key is not None else event.id
            if dedup_key in seen_dedup_keys:
                continue
            seen_dedup_keys.add(dedup_key)
            try:
                await socket.send_json(event.to_dict())
            except (OSError, RuntimeError):
                break
            except Exception as exc:
                if exc.__class__.__name__ == "WebSocketDisconnect":
                    break
                raise


def _resolve_channels_backend(socket: Any) -> object | None:
    if hasattr(socket, "channels_plugin"):
        return cast("object", socket.channels_plugin)
    scope = getattr(socket, "scope", None)
    if isinstance(scope, dict):
        scoped = scope.get("channels") or scope.get("queue_event_channels")
        if scoped is not None:
            return cast("object", scoped)
    app = getattr(socket, "app", None)
    state = getattr(app, "state", None)
    if state is not None:
        for key in ("queue_event_channels_backend", "channels", "queue_event_channels"):
            with suppress(KeyError, TypeError):
                value = state[key]
                if value is not None:
                    return cast("object", value)
            value = getattr(state, key, None)
            if value is not None:
                return cast("object", value)
    return None


@asynccontextmanager
async def _event_stream(
    backend: object,
    channels: Sequence[str],
    *,
    history: int,
) -> AsyncIterator[AsyncIterator[bytes]]:
    if hasattr(backend, "start_subscription"):
        typed_backend = cast("Any", backend)
        async with typed_backend.start_subscription(list(channels), history=history) as subscriber:
            yield subscriber.iter_events()
        return

    if not hasattr(backend, "subscribe") or not hasattr(backend, "stream_events"):
        msg = "Queue event streaming requires a ChannelsPlugin or ChannelsBackend-like object."
        raise RuntimeError(msg)

    typed_backend = cast("Any", backend)
    await typed_backend.subscribe(list(channels))
    try:
        yield _backend_events(cast("Any", backend).stream_events(), set(channels))
    finally:
        await backend.unsubscribe(list(channels))  # type: ignore[attr-defined]


async def _backend_events(events: AsyncIterator[tuple[str, bytes]], channels: set[str]) -> AsyncIterator[bytes]:
    async for channel, payload in events:
        if channel in channels:
            yield payload


def _decode_event(raw_event: bytes | str) -> QueueEvent | None:
    try:
        return QueueEvent.from_json(raw_event)
    except (KeyError, TypeError, ValueError):
        return None

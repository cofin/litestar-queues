"""Litestar Channels helpers for queue events."""

import inspect
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING, Any, cast

from litestar.channels import ChannelsPlugin

from litestar_queues.events.chunking import estimate_event_payload_bytes, split_event_batch_by_size
from litestar_queues.events.models import QueueEvent
from litestar_queues.events.sinks import _call_optional_lifecycle

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from litestar_queues.events.chunking import QueueEventSizeEstimator
    from litestar_queues.typing import (
        ChannelsLike,
        ChannelsPublishBackend,
        ChannelsPublishManyBackend,
        ChannelsStreamBackend,
        ChannelsSubscriptionBackend,
        ChannelsWaitPublishedBackend,
    )

__all__ = ("ChannelsQueueEventSink",)

_STREAM_DEDUP_MAX_KEYS = 1024


class ChannelsQueueEventSink:
    """Event sink that publishes to an app-owned Litestar Channels object."""

    __slots__ = (
        "_channels_backend",
        "_lifecycle_opened",
        "_lifecycle_resource",
        "_manage_lifecycle",
        "_max_payload_bytes",
        "_payload_size_estimator",
    )

    def __init__(
        self,
        channels_backend: "ChannelsLike",
        *,
        manage_lifecycle: "bool" = False,
        max_payload_bytes: "int | None" = None,
        payload_size_estimator: "QueueEventSizeEstimator | None" = None,
    ) -> "None":
        self._channels_backend = channels_backend
        self._manage_lifecycle = manage_lifecycle
        self._lifecycle_opened = False
        self._lifecycle_resource: "object | None" = None
        self._max_payload_bytes = max_payload_bytes
        self._payload_size_estimator = payload_size_estimator

    @property
    def channels_backend(self) -> "ChannelsLike":
        """Wrapped Channels backend or plugin."""
        return self._channels_backend

    @property
    def manages_lifecycle(self) -> "bool":
        """Whether this sink owns its Channels target lifecycle."""
        return self._manage_lifecycle

    async def open(self) -> "None":
        """Open a worker-owned Channels lifecycle when configured."""
        if not self._manage_lifecycle or self._lifecycle_opened:
            return
        if isinstance(self._channels_backend, ChannelsPlugin):
            resource = await self._channels_backend.__aenter__()
            self._lifecycle_resource = resource
            self._lifecycle_opened = True
            return
        if await _call_optional_lifecycle(self._channels_backend, "on_startup"):
            self._lifecycle_resource = self._channels_backend
            self._lifecycle_opened = True

    async def close(self) -> "None":
        """Close only the Channels lifecycle opened by this sink."""
        if not self._lifecycle_opened:
            return
        self._lifecycle_opened = False
        try:
            if isinstance(self._channels_backend, ChannelsPlugin):
                await self._channels_backend.__aexit__(None, None, None)
            else:
                await _call_optional_lifecycle(self._channels_backend, "on_shutdown")
        finally:
            self._lifecycle_resource = None

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> "None":
        """Publish an event to Litestar Channels."""
        for event_chunk in self._event_chunks(event):
            await self._publish_one(event_chunk, channels=channels)

    async def publish_many(self, batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> "None":
        """Publish grouped events to Litestar Channels."""
        grouped: "dict[tuple[str, ...], list[QueueEvent]]" = {}
        for event, channels in batch:
            grouped.setdefault(tuple(channels), []).extend(self._event_chunks(event))
        for channels, events in grouped.items():
            await self._publish_group(events, channels=channels)

    def _event_chunks(self, event: "QueueEvent") -> "Sequence[QueueEvent]":
        if self._max_payload_bytes is None:
            return (event,)
        estimator = self._payload_size_estimator or estimate_event_payload_bytes
        return split_event_batch_by_size(event, max_bytes=self._max_payload_bytes, size_estimator=estimator)

    async def _publish_group(self, events: "Sequence[QueueEvent]", *, channels: "Sequence[str]") -> "None":
        if hasattr(self._channels_backend, "publish_many"):
            batch_backend = cast("ChannelsPublishManyBackend", self._channels_backend)
            await batch_backend.publish_many([event.to_json() for event in events], list(channels))
            return
        for event in events:
            await self._publish_one(event, channels=channels)

    async def _publish_one(self, event: "QueueEvent", *, channels: "Sequence[str]") -> "None":
        data = event.to_json()
        if hasattr(self._channels_backend, "wait_published"):
            wait_backend = cast("ChannelsWaitPublishedBackend", self._channels_backend)
            result = wait_backend.wait_published(data, list(channels))
        else:
            publish_backend = cast("ChannelsPublishBackend", self._channels_backend)
            result = publish_backend.publish(data, list(channels))
        if inspect.isawaitable(result):
            await result


def _resolve_channels_backend(socket: "Any") -> "ChannelsLike | None":
    if hasattr(socket, "channels_plugin"):
        return cast("ChannelsLike", socket.channels_plugin)
    scope = getattr(socket, "scope", None)
    if isinstance(scope, dict):
        scoped = scope.get("channels") or scope.get("queue_event_channels")
        if scoped is not None:
            return cast("ChannelsLike", scoped)
    app = getattr(socket, "app", None)
    state = getattr(app, "state", None)
    if state is not None:
        for key in ("queue_event_channels_backend", "channels", "queue_event_channels"):
            with suppress(KeyError, TypeError):
                value = state[key]
                if value is not None:
                    return cast("ChannelsLike", value)
            value = getattr(state, key, None)
            if value is not None:
                return cast("ChannelsLike", value)
    return None


@asynccontextmanager
async def _event_stream(
    backend: "ChannelsLike", channels: "Sequence[str]", *, history: "int"
) -> "AsyncIterator[AsyncIterator[bytes]]":
    if hasattr(backend, "start_subscription"):
        subscription_backend = cast("ChannelsSubscriptionBackend", backend)
        async with subscription_backend.start_subscription(list(channels), history=history) as subscriber:
            yield subscriber.iter_events()
        return

    if not hasattr(backend, "subscribe") or not hasattr(backend, "stream_events"):
        msg = "Queue event streaming requires a ChannelsPlugin or ChannelsBackend-like object."
        raise RuntimeError(msg)

    stream_backend = cast("ChannelsStreamBackend", backend)
    await stream_backend.subscribe(list(channels))
    try:
        yield _backend_events(stream_backend.stream_events(), set(channels))
    finally:
        await stream_backend.unsubscribe(list(channels))


async def _backend_events(events: "AsyncIterator[tuple[str, bytes]]", channels: "set[str]") -> "AsyncIterator[bytes]":
    async for channel, payload in events:
        if channel in channels:
            yield payload


def _decode_event(raw_event: "bytes | str") -> "QueueEvent | None":
    try:
        return QueueEvent.from_json(raw_event)
    except (KeyError, TypeError, ValueError):
        return None

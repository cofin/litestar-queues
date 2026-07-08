"""Litestar Channels helpers for queue events."""

import inspect
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING, Any, cast

from litestar_queues.events.models import QueueEvent

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from litestar_queues.events.chunking import QueueEventSizeEstimator
    from litestar_queues.typing import (
        ChannelsLike,
        ChannelsPublishBackend,
        ChannelsStreamBackend,
        ChannelsSubscriptionBackend,
        ChannelsWaitPublishedBackend,
        ChannelsWaitPublishedManyBackend,
    )

__all__ = ("ChannelsQueueEventSink",)

_STREAM_DEDUP_MAX_KEYS = 1024


class ChannelsQueueEventSink:
    """Event sink that publishes to an app-owned Litestar Channels object."""

    __slots__ = ("_channels_backend", "_max_payload_bytes", "_payload_size_estimator")

    def __init__(
        self,
        channels_backend: "ChannelsLike",
        *,
        max_payload_bytes: "int | None" = None,
        payload_size_estimator: "QueueEventSizeEstimator | None" = None,
    ) -> "None":
        self._channels_backend = channels_backend
        self._max_payload_bytes = max_payload_bytes
        self._payload_size_estimator = payload_size_estimator

    @property
    def channels_backend(self) -> "ChannelsLike":
        """Wrapped Channels backend or plugin."""
        return self._channels_backend

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> "None":
        """Publish an event to Litestar Channels."""
        for event_chunk in self._event_chunks(event):
            await self._publish_one(event_chunk, channels=channels)

    async def publish_many(self, batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> "None":
        """Publish grouped events to Litestar Channels.

        Returns:
            None.
        """
        grouped: "dict[tuple[str, ...], list[QueueEvent]]" = {}
        for event, channels in batch:
            grouped.setdefault(tuple(channels), []).extend(self._event_chunks(event))
        for channels, events in grouped.items():
            await self._publish_group(events, channels=channels)

    def _event_chunks(self, event: "QueueEvent") -> "Sequence[QueueEvent]":
        if self._max_payload_bytes is None:
            return (event,)
        from litestar_queues.events.chunking import estimate_event_payload_bytes, split_event_batch_by_size

        estimator = self._payload_size_estimator or estimate_event_payload_bytes
        return split_event_batch_by_size(event, max_bytes=self._max_payload_bytes, size_estimator=estimator)

    async def _publish_group(self, events: "Sequence[QueueEvent]", *, channels: "Sequence[str]") -> "None":
        if hasattr(self._channels_backend, "wait_published_many"):
            wait_backend = cast("ChannelsWaitPublishedManyBackend", self._channels_backend)
            result = wait_backend.wait_published_many([event.to_json() for event in events], list(channels))
            if inspect.isawaitable(result):
                await result
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

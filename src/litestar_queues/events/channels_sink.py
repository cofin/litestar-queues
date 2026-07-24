"""Litestar Channels event sink for queue events."""

import inspect
from typing import TYPE_CHECKING, cast

from litestar.channels import ChannelsPlugin

from litestar_queues.events.chunking import estimate_event_payload_bytes, split_event_batch_by_size
from litestar_queues.events.sinks import _call_optional_lifecycle

if TYPE_CHECKING:
    from collections.abc import Sequence

    from litestar_queues.events.chunking import QueueEventSizeEstimator
    from litestar_queues.events.models import QueueEvent
    from litestar_queues.typing import (
        ChannelsLike,
        ChannelsPublishBackend,
        ChannelsPublishManyBackend,
        ChannelsWaitPublishedBackend,
    )

__all__ = ("ChannelsQueueEventSink",)


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

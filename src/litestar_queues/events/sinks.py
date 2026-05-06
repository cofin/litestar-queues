"""Queue event sink protocols and core implementations."""

import asyncio
from collections import defaultdict
from collections.abc import Sequence
from typing import Protocol

from litestar_queues.events.models import QueueEvent

__all__ = ("InMemoryQueueEventSink", "NoopQueueEventSink", "QueueEventSink")


class QueueEventSink(Protocol):
    """Transport boundary for queue event delivery."""

    async def publish(self, event: QueueEvent, *, channels: Sequence[str]) -> None:
        """Publish an event to the requested channels."""


class NoopQueueEventSink:
    """Event sink that accepts events and drops them."""

    __slots__ = ()

    async def publish(self, event: QueueEvent, *, channels: Sequence[str]) -> None:
        """Drop an event publish."""


class InMemoryQueueEventSink:
    """In-process event sink for tests, examples, and local demos."""

    __slots__ = ("_channel_events", "_lock", "_published")

    def __init__(self) -> None:
        self._published: list[tuple[QueueEvent, tuple[str, ...]]] = []
        self._channel_events: defaultdict[str, list[QueueEvent]] = defaultdict(list)
        self._lock = asyncio.Lock()

    @property
    def events(self) -> list[QueueEvent]:
        """Return published events in publish order."""
        return [event for event, _ in self._published]

    @property
    def published(self) -> list[tuple[QueueEvent, tuple[str, ...]]]:
        """Return events with their publish channels."""
        return list(self._published)

    def events_for(self, channel: str) -> list[QueueEvent]:
        """Return events published to a channel."""
        return list(self._channel_events.get(channel, []))

    async def publish(self, event: QueueEvent, *, channels: Sequence[str]) -> None:
        """Store an event in process."""
        channel_tuple = tuple(channels)
        async with self._lock:
            self._published.append((event, channel_tuple))
            for channel in channel_tuple:
                self._channel_events[channel].append(event)

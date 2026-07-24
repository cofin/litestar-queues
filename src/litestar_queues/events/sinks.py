"""Queue event sink protocols and core implementations."""

import asyncio
import inspect
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Sequence

    from litestar_queues.events.models import QueueEvent

__all__ = (
    "CompositeQueueEventSink",
    "InMemoryQueueEventSink",
    "NoopQueueEventSink",
    "QueueEventSink",
    "default_publish_many",
)

logger = logging.getLogger(__name__)


async def _call_optional_lifecycle(
    target: "object | None", method_name: "str", fallback_method_name: "str | None" = None
) -> "bool":
    """Call an optional synchronous or asynchronous lifecycle method.

    Returns:
        Whether the target exposed a callable lifecycle method.
    """
    if target is None:
        return False
    method = getattr(target, method_name, None)
    if method is None and fallback_method_name is not None:
        method = getattr(target, fallback_method_name, None)
    if not callable(method):
        return False
    result = method()
    if inspect.isawaitable(result):
        await result
    return True


def _select_lifecycle_error(errors: "Sequence[BaseException]") -> "BaseException | None":
    """Prefer control-flow exceptions over the first ordinary lifecycle error."""
    for error in errors:
        if not isinstance(error, Exception):
            return error
    return errors[0] if errors else None


class QueueEventSink(Protocol):
    """Transport boundary for queue event delivery."""

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> "None":
        """Publish an event to the requested channels."""


async def default_publish_many(sink: "QueueEventSink", batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> "None":
    """Publish a batch by looping over a sink's single-event publish method."""
    for event, channels in batch:
        await sink.publish(event, channels=channels)


class NoopQueueEventSink:
    """Event sink that accepts events and drops them."""

    __slots__ = ()

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> "None":
        """Drop an event publish."""

    async def publish_many(self, batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> "None":
        """Drop a batch publish."""
        del batch


class CompositeQueueEventSink:
    """Deliver events to multiple sinks in deterministic order."""

    __slots__ = ("_opened_sinks", "_sinks", "_strict")

    def __init__(self, sinks: "Sequence[QueueEventSink]", *, strict: "bool" = False) -> "None":
        self._sinks = tuple(sinks)
        self._strict = strict
        self._opened_sinks: "tuple[QueueEventSink, ...]" = ()

    @property
    def sinks(self) -> "tuple[QueueEventSink, ...]":
        """Configured sinks in delivery order."""
        return self._sinks

    async def open(self) -> "None":
        """Open child sinks in declaration order."""
        if self._opened_sinks:
            return
        opened: "list[QueueEventSink]" = []
        try:
            for sink in self._sinks:
                if await _call_optional_lifecycle(sink, "open"):
                    opened.append(sink)
        except BaseException:
            for sink in reversed(opened):
                try:
                    await _call_optional_lifecycle(sink, "close")
                except BaseException:  # noqa: PERF203
                    logger.warning("Queue event sink rollback failed", exc_info=True)
            raise
        self._opened_sinks = tuple(opened)

    async def close(self) -> "None":
        """Close opened child sinks in reverse declaration order."""
        opened = self._opened_sinks
        self._opened_sinks = ()
        errors: "list[BaseException]" = []
        for sink in reversed(opened):
            try:
                await _call_optional_lifecycle(sink, "close")
            except BaseException as exc:  # noqa: PERF203
                errors.append(exc)
                if not self._strict and isinstance(exc, Exception):
                    logger.warning("Queue event sink close failed", exc_info=True)
        error = _select_lifecycle_error(errors)
        if error is not None and (self._strict or not isinstance(error, Exception)):
            raise error

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> "None":
        """Publish to every sink, continuing after non-strict failures."""
        for sink in self._sinks:
            await self._publish_to_sink(sink, event, channels)

    async def publish_many(self, batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> "None":
        """Publish a batch to every sink in order."""
        for sink in self._sinks:
            await self._publish_batch_to_sink(sink, batch)

    async def _publish_to_sink(self, sink: "QueueEventSink", event: "QueueEvent", channels: "Sequence[str]") -> "None":
        try:
            await sink.publish(event, channels=channels)
        except Exception:
            if self._strict:
                raise
            logger.warning("Queue event sink publish failed", exc_info=True)

    async def _publish_batch_to_sink(
        self, sink: "QueueEventSink", batch: "Sequence[tuple[QueueEvent, Sequence[str]]]"
    ) -> "None":
        try:
            publish_many = getattr(sink, "publish_many", None)
            if publish_many is None:
                await default_publish_many(sink, batch)
            else:
                await publish_many(batch)
        except Exception:
            if self._strict:
                raise
            logger.warning("Queue event sink batch publish failed", exc_info=True)


class InMemoryQueueEventSink:
    """In-process event sink for tests, examples, and local demos."""

    __slots__ = ("_channel_events", "_lock", "_published")

    def __init__(self) -> "None":
        self._published: "list[tuple[QueueEvent, tuple[str, ...]]]" = []
        self._channel_events: "defaultdict[str, list[QueueEvent]]" = defaultdict(list)
        self._lock = asyncio.Lock()

    @property
    def events(self) -> "list[QueueEvent]":
        """Published events in publish order."""
        return [event for event, _ in self._published]

    @property
    def published(self) -> "list[tuple[QueueEvent, tuple[str, ...]]]":
        """Published events with their channels."""
        return list(self._published)

    def events_for(self, channel: "str") -> "list[QueueEvent]":
        """Return events published to a channel."""
        return list(self._channel_events.get(channel, []))

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> "None":
        """Store an event in process."""
        channel_tuple = tuple(channels)
        async with self._lock:
            self._published.append((event, channel_tuple))
            for channel in channel_tuple:
                self._channel_events[channel].append(event)

    async def publish_many(self, batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> "None":
        """Store a batch of events in process."""
        async with self._lock:
            for event, channels in batch:
                channel_tuple = tuple(channels)
                self._published.append((event, channel_tuple))
                for channel in channel_tuple:
                    self._channel_events[channel].append(event)

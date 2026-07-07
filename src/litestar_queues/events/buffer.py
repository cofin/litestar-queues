"""Producer-side live event buffering."""

import asyncio
import contextlib
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias

from litestar_queues.exceptions import QueueEventBufferFull

if TYPE_CHECKING:
    from litestar_queues.events.models import QueueEvent
    from litestar_queues.events.publisher import EventBufferConfig

__all__ = ("LiveEventBuffer", "event_buffer_key")

logger = logging.getLogger(__name__)

EventBufferKey: TypeAlias = str | tuple[str, str, str | None]
SinkPublish = Callable[["QueueEvent", Sequence[str]], Awaitable[None]]
RecordDrop = Callable[[str], None]


@dataclass(slots=True)
class _BufferedEvent:
    key: "EventBufferKey"
    event: "QueueEvent"
    channels: "tuple[str, ...]"


class LiveEventBuffer:
    """Bounded producer-side buffer for live queue event delivery."""

    __slots__ = (
        "_condition",
        "_config",
        "_order",
        "_pending",
        "_record_drop",
        "_sink_publish",
        "_stop_event",
        "_task",
        "_warned_drop",
    )

    def __init__(
        self, config: "EventBufferConfig", *, sink_publish: "SinkPublish", record_drop: "RecordDrop"
    ) -> "None":
        self._config = config
        self._sink_publish = sink_publish
        self._record_drop = record_drop
        self._condition = asyncio.Condition()
        self._order: "deque[_BufferedEvent]" = deque()
        self._pending: "dict[EventBufferKey, list[_BufferedEvent]]" = {}
        self._stop_event = asyncio.Event()
        self._task: "asyncio.Task[None] | None" = None
        self._warned_drop = False

    async def add(self, event: "QueueEvent", channels: "Sequence[str]") -> "None":
        """Add an event to the buffer, applying configured overflow behavior.

        Returns:
            None.
        """
        item = _BufferedEvent(key=event_buffer_key(event), event=event, channels=tuple(channels))
        should_flush = False
        async with self._condition:
            while len(self._order) >= self._max_pending:
                overflow = self._config.overflow
                if overflow == "drop_oldest":
                    self._drop_oldest()
                    break
                if overflow == "drop_newest":
                    self._record_drop_for_event(event)
                    return
                if overflow == "error":
                    msg = f"Queue event buffer is full at {self._max_pending} pending events."
                    raise QueueEventBufferFull(msg)
                await self._condition.wait()
            self._append(item)
            should_flush = len(self._order) >= self._buffer_size
        if should_flush:
            await self.flush()

    async def flush(self, *, key: "EventBufferKey | None" = None) -> "None":
        """Drain all buffered events, or only events matching ``key``."""
        async with self._condition:
            items = self._drain(key=key)
            self._condition.notify_all()
        for item in items:
            await self._sink_publish(item.event, item.channels)

    def start(self) -> "None":
        """Start the interval flush loop if it is not already running.

        Returns:
            None.
        """
        if self._task is not None and not self._task.done():
            return
        if self._stop_event.is_set():
            self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> "None":
        """Stop the interval loop and drain all remaining buffered events."""
        task = self._task
        self._stop_event.set()
        if task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await task
            self._task = None
        await self.flush()

    async def _run(self) -> "None":
        try:
            while not self._stop_event.is_set():
                if await self._wait_until_next_flush():
                    await self.flush()
        finally:
            await self.flush()

    async def _wait_until_next_flush(self) -> "bool":
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=self._config.flush_interval)
        except asyncio.TimeoutError:
            return True
        return False

    @property
    def _buffer_size(self) -> "int":
        return max(1, self._config.buffer_size)

    @property
    def _max_pending(self) -> "int":
        return max(1, self._config.max_pending)

    def _append(self, item: "_BufferedEvent") -> "None":
        self._order.append(item)
        self._pending.setdefault(item.key, []).append(item)

    def _drop_oldest(self) -> "None":
        item = self._order.popleft()
        self._remove_from_pending(item)
        self._record_drop_for_event(item.event)
        self._condition.notify_all()

    def _drain(self, *, key: "EventBufferKey | None") -> "list[_BufferedEvent]":
        if key is None:
            items = list(self._order)
            self._order.clear()
            self._pending.clear()
            return items
        items = self._pending.pop(key, [])
        if not items:
            return []
        item_ids = {id(item) for item in items}
        self._order = deque(item for item in self._order if id(item) not in item_ids)
        return items

    def _remove_from_pending(self, item: "_BufferedEvent") -> "None":
        items = self._pending.get(item.key)
        if not items:
            return
        with contextlib.suppress(ValueError):
            items.remove(item)
        if not items:
            self._pending.pop(item.key, None)

    def _record_drop_for_event(self, event: "QueueEvent") -> "None":
        self._record_drop(event.scope)
        if self._warned_drop:
            return
        self._warned_drop = True
        logger.warning(
            "Queue event buffer full; dropping event",
            extra={"queue_event_scope": event.scope, "queue_event_type": event.type},
        )


def event_buffer_key(event: "QueueEvent") -> "EventBufferKey":
    """Return the buffer key used for scoped flushes."""
    if event.task_id is not None:
        return event.task_id
    return ("scope", event.scope, event.scope_key)

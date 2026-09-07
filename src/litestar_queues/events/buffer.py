"""Producer-side live event buffering."""

import asyncio
import contextlib
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias

from litestar_queues.exceptions import QueueConfigurationError, QueueEventBufferFull

if TYPE_CHECKING:
    from litestar_queues.events.models import QueueEvent
    from litestar_queues.events.publisher import EventBufferConfig

__all__ = ("LiveEventBuffer", "event_buffer_key")

logger = logging.getLogger(__name__)

EventBufferKey: TypeAlias = str | tuple[str, str, str | None]
SinkPublish = Callable[[Sequence[tuple["QueueEvent", Sequence[str]]]], Awaitable[None]]
RecordDrop = Callable[[str], None]


@dataclass(slots=True)
class _BufferedEvent:
    key: "EventBufferKey"
    event: "QueueEvent"
    channels: "tuple[str, ...]"


@dataclass(slots=True)
class _Delivery:
    items: "list[_BufferedEvent]"
    release: "Callable[[], Awaitable[None]] | None" = None

    @property
    def size(self) -> "int":
        return max(1, len(self.items))


@dataclass(slots=True)
class _DrainToken:
    owner: "LiveEventBuffer"
    active: "bool" = True


_active_drain: "ContextVar[_DrainToken | None]" = ContextVar("queue_live_event_drain", default=None)


def _in_live_event_callback() -> "bool":
    """Return whether this call descends from a still-active live drain."""
    token = _active_drain.get()
    return token is not None and token.active


class LiveEventBuffer:
    """Bounded producer-side buffer for live queue event delivery."""

    __slots__ = (
        "_condition",
        "_config",
        "_deferred",
        "_deferred_size",
        "_drain_lock",
        "_logger",
        "_order",
        "_pending",
        "_record_drop",
        "_sink_publish",
        "_stop_event",
        "_task",
        "_warned_drop",
    )

    def __init__(
        self,
        config: "EventBufferConfig",
        *,
        sink_publish: "SinkPublish",
        record_drop: "RecordDrop",
        runtime_logger: "logging.Logger | None" = None,
    ) -> "None":
        self._config = config
        self._logger = runtime_logger or logger
        self._sink_publish = sink_publish
        self._record_drop = record_drop
        self._condition = asyncio.Condition()
        self._drain_lock = asyncio.Lock()
        self._deferred: "deque[_Delivery]" = deque()
        self._deferred_size = 0
        self._order: "deque[_BufferedEvent]" = deque()
        self._pending: "dict[EventBufferKey, list[_BufferedEvent]]" = {}
        self._stop_event = asyncio.Event()
        self._task: "asyncio.Task[None] | None" = None
        self._warned_drop = False

    async def add(self, event: "QueueEvent", channels: "Sequence[str]") -> "None":
        """Add an event to the buffer, applying configured overflow behavior."""
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
                if self._drain_token() is not None:
                    msg = "A reentrant live event callback cannot block on its own full buffer."
                    raise QueueEventBufferFull(msg)
                await self._condition.wait()
            if (
                self._drain_token() is not None
                and len(self._order) + 1 >= self._batch_size
                and self._deferred_size + len(self._order) + 1 > self._max_pending
            ):
                msg = "The reentrant live event delivery queue is full."
                raise QueueEventBufferFull(msg)
            self._append(item)
            should_flush = len(self._order) >= self._batch_size
        if should_flush:
            await self.flush()

    async def flush(self, *, key: "EventBufferKey | None" = None) -> "None":
        """Finish prior live delivery and drain buffered events matching ``key``."""
        await self._dispatch(key=key, release=None)

    async def publish_immediate(self, *, key: "EventBufferKey", release: "Callable[[], Awaitable[None]]") -> "None":
        """Deliver after earlier batches; reentrant callbacks queue bounded work.

        A callback cannot block on its own full queue: explicit overflow raises
        ``QueueEventBufferFull`` while ordinary producer overflow is unchanged.
        """
        await self._dispatch(key=key, release=release)

    def bind_release(self, release: "Callable[[], Awaitable[None]]") -> "Callable[[], Awaitable[None]]":
        """Carry an active drain into a history worker's delayed live callback."""
        token = self._drain_token()

        async def bound_release() -> "None":
            # A long-lived history worker may carry an unrelated active token.
            # Install the captured context even when it is empty or expired.
            reset = _active_drain.set(token if token is not None and token.active else None)
            try:
                await release()
            finally:
                _active_drain.reset(reset)

        return bound_release

    def _drain_token(self) -> "_DrainToken | None":
        token = _active_drain.get()
        return token if token is not None and token.owner is self and token.active else None

    async def _dispatch(
        self, *, key: "EventBufferKey | None", release: "Callable[[], Awaitable[None]] | None"
    ) -> "None":
        if self._drain_token() is not None:
            async with self._condition:
                count = len(self._order) if key is None else len(self._pending.get(key, ()))
                if not count and release is None:
                    return
                if self._deferred_size + max(1, count) > self._max_pending:
                    msg = "The reentrant live event delivery queue is full."
                    raise QueueEventBufferFull(msg)
                operation = _Delivery(self._drain(key=key), release)
                self._deferred.append(operation)
                self._deferred_size += operation.size
                self._condition.notify_all()
            return
        # Acquire before extracting: a waiting terminal must account for the
        # active batch, and blocked producers cannot accumulate extracted batches.
        async with self._drain_lock:
            token = _DrainToken(self)
            reset = _active_drain.set(token)
            try:
                error = await self._drain_deferred()
                async with self._condition:
                    operation = _Delivery(self._drain(key=key), release)
                    self._condition.notify_all()
                error = self._retain_error(error, await self._deliver(operation))
                error = self._retain_error(error, await self._drain_deferred())
                if error is not None:
                    raise error
            finally:
                token.active = False
                _active_drain.reset(reset)

    async def _drain_deferred(self) -> "Exception | None":
        error: Exception | None = None
        while self._deferred:
            operation = self._deferred.popleft()
            self._deferred_size -= operation.size
            error = self._retain_error(error, await self._deliver(operation))
        return error

    async def _deliver(self, operation: "_Delivery") -> "Exception | None":
        error: Exception | None = None
        if operation.release is not None:
            # Reserve its control slot until invocation, including cancellation
            # during the preceding batch's live delivery attempt.
            self._deferred_size += 1
        if operation.items:
            try:
                await self._sink_publish(tuple((item.event, item.channels) for item in operation.items))
            except asyncio.CancelledError:
                if operation.release is not None:
                    self._deferred.appendleft(_Delivery([], operation.release))
                raise
            except Exception as exc:  # noqa: BLE001 - attempt the accepted terminal before reraising.
                error = exc
        if operation.release is not None:
            self._deferred_size -= 1
            try:
                await operation.release()
            except Exception as exc:  # noqa: BLE001 - preserve the first delivery error.
                error = self._retain_error(error, exc)
        return error

    def _retain_error(self, primary: "Exception | None", secondary: "Exception | None") -> "Exception | None":
        if primary is None:
            return secondary
        if secondary is not None:
            with contextlib.suppress(Exception):
                self._logger.warning(
                    "Additional live event delivery failed.",
                    exc_info=(type(secondary), secondary, secondary.__traceback__),
                )
        return primary

    def start(self) -> "None":
        """Start the interval flush loop if it is not already running."""
        if self._task is not None and not self._task.done():
            return
        if self._stop_event.is_set():
            self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> "None":
        """Stop the interval loop and drain all remaining buffered events."""
        if self._drain_token() is not None:
            msg = "Cannot stop the live event buffer from its active delivery callback."
            raise QueueConfigurationError(msg)
        task, self._task = self._task, None
        self._stop_event.set()
        error: BaseException | None = None
        if task is not None:
            try:
                outcomes = await asyncio.gather(task, return_exceptions=True)
                if isinstance(outcomes[0], BaseException) and not isinstance(outcomes[0], asyncio.CancelledError):
                    error = outcomes[0]
            except asyncio.CancelledError as exc:
                error = exc
        try:
            await self.flush()
        except (Exception, asyncio.CancelledError) as exc:
            if error is None:
                error = exc
            else:
                secondary = exc
                if isinstance(error, Exception) and isinstance(exc, asyncio.CancelledError):
                    secondary, error = error, exc
                with contextlib.suppress(Exception):
                    self._logger.warning(
                        "Final live event drain also failed.",
                        exc_info=(type(secondary), secondary, secondary.__traceback__),
                    )
        if error is not None:
            raise error

    async def _run(self) -> "None":
        while not self._stop_event.is_set():
            if await self._wait_until_next_flush():
                await self.flush()

    async def _wait_until_next_flush(self) -> "bool":
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=self._config.flush_interval)
        except asyncio.TimeoutError:
            return True
        return False

    @property
    def _batch_size(self) -> "int":
        return self._config.batch_size

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
        self._logger.warning(
            "Queue event buffer full; dropping event",
            extra={"queue_event_scope": event.scope, "queue_event_type": event.type},
        )


def event_buffer_key(event: "QueueEvent") -> "EventBufferKey":
    """Return the buffer key used for scoped flushes."""
    if event.task_id is not None:
        return event.task_id
    return ("scope", event.scope, event.scope_key)

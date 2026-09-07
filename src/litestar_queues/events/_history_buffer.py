"""Bounded persistence and ordered commit-aware release for event history."""

import asyncio
import logging
from collections import deque
from contextlib import suppress
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, field
from time import monotonic
from typing import TYPE_CHECKING

from litestar_queues.events.buffer import _in_live_event_callback
from litestar_queues.exceptions import QueueConfigurationError, QueueEventBufferFull

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from litestar_queues.events.history import EventHistoryConfig, QueueEventLogRecord

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _Entry:
    sequence: int
    record: "QueueEventLogRecord"
    admitted_at: float
    release: "Callable[[], Awaitable[None]] | None"
    committed: bool = False
    released: asyncio.Event = field(default_factory=asyncio.Event)
    error: BaseException | None = None


@dataclass(slots=True)
class _ReleaseToken:
    owner: "_HistoryBuffer"
    boundary: int
    active: bool = True


_release_context: ContextVar[_ReleaseToken | None] = ContextVar("queue_history_release", default=None)


def _in_event_release_callback() -> bool:
    """Identify an active callback that cannot await its owning lifecycle."""
    token = _release_context.get()
    return (token is not None and token.active) or _in_live_event_callback()


class _HistoryBuffer:
    """Serialize bounded history batches before releasing their live callbacks."""

    __slots__ = (
        "_active_release",
        "_background_error",
        "_close_task",
        "_committed",
        "_config",
        "_fatal_error",
        "_next_retry",
        "_pending",
        "_release_progress",
        "_release_task",
        "_release_wake",
        "_sequence",
        "_size",
        "_state",
        "_timer",
        "_wake",
        "_write_batch",
        "_write_lock",
    )

    def __init__(
        self, config: "EventHistoryConfig", write_batch: "Callable[[Sequence[QueueEventLogRecord]], Awaitable[None]]"
    ) -> None:
        self._config = config
        self._write_batch = write_batch
        self._pending: deque[_Entry] = deque()
        self._committed: deque[_Entry] = deque()
        self._active_release: _Entry | None = None
        self._write_lock = asyncio.Lock()
        self._release_wake = asyncio.Event()
        self._release_progress = asyncio.Event()
        self._release_task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._timer: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._background_error: BaseException | None = None
        self._fatal_error: QueueConfigurationError | None = None
        self._next_retry = 0.0
        self._sequence = 0
        self._size = 0
        self._state = "accepting"

    def start(self) -> None:
        """Start one timer while the buffer is accepting events."""
        self._require_accepting()
        if self._timer is None or self._timer.done():
            self._timer = asyncio.create_task(self._run_timer(), name="queue-history-flush")
        self._start_release_worker()

    def _start_release_worker(self) -> None:
        if self._release_task is None or self._release_task.done():
            self._release_task = asyncio.create_task(self._run_releases(), name="queue-history-release")
            self._release_task.add_done_callback(self._release_finished)
            if self._committed:
                self._release_wake.set()

    def _release_finished(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled():
            error = task.exception()
            if error is not None and self._background_error is None:
                self._background_error = error
        self._release_progress.set()
        self._wake.set()

    async def enqueue(
        self,
        record: "QueueEventLogRecord",
        *,
        release: "Callable[[], Awaitable[None]] | None" = None,
        barrier: bool = False,
    ) -> None:
        """Accept an immutable snapshot, optionally waiting through its commit."""
        self._raise_background_error()
        self._require_accepting()
        if self._size >= self._config.max_pending:
            msg = "Queue event history buffer is full."
            raise QueueEventBufferFull(msg)
        snapshot = deepcopy(record)
        self._sequence += 1
        boundary = self._sequence
        self._pending.append(_Entry(boundary, snapshot, monotonic(), release))
        self._size += 1
        self.start()
        self._wake.set()
        if barrier or (len(self._pending) >= self._config.batch_size and monotonic() >= self._next_retry):
            await self._flush_to(boundary)

    async def flush(self) -> None:
        """Attempt all entries accepted before this call, including live release."""
        self._raise_background_error()
        self._require_accepting()
        self.start()
        await self._flush_to(self._sequence)
        self._wake.set()

    async def stop(self) -> None:
        """Stop admission and finish cleanup even if the caller is cancelled."""
        token = _release_context.get()
        if (token is not None and token.owner is self and token.active) or _in_live_event_callback():
            msg = "Queue event history cannot close from its own release callback."
            raise QueueConfigurationError(msg)
        if self._state == "closed":
            return
        if self._close_task is None:
            self._state = "closing"
            self._close_task = asyncio.create_task(self._close(), name="queue-history-close")
        cancelled: asyncio.CancelledError | None = None
        while True:
            try:
                await asyncio.shield(self._close_task)
            except asyncio.CancelledError as exc:  # noqa: PERF203 - repeated cancellation must still await cleanup.
                if self._close_task.cancelled():
                    raise
                cancelled = exc
            except Exception:
                if cancelled is not None:
                    logger.warning("Queue event history cleanup failed during cancellation", exc_info=True)
                    raise cancelled from None
                raise
            else:
                break
        if cancelled is not None:
            raise cancelled

    def _require_accepting(self) -> None:
        if self._fatal_error is not None:
            raise self._fatal_error
        if self._state != "accepting":
            msg = "Queue event history buffer is closing or closed."
            raise QueueConfigurationError(msg)

    def _raise_background_error(self) -> None:
        error, self._background_error = self._background_error, None
        if error is not None:
            raise error

    async def _flush_to(self, boundary: int, *, background: bool = False) -> None:
        active = (self._active_release,) if self._active_release is not None else ()
        targets = tuple(entry for entry in (*active, *self._committed, *self._pending) if entry.sequence <= boundary)
        inherited = _release_context.get()
        reentrant = inherited is not None and inherited.owner is self and inherited.active
        error: BaseException | None = None
        try:
            await self._persist_to(boundary)
        except Exception as exc:  # noqa: BLE001 - committed entries still need their release attempt.
            error = exc
        if reentrant and inherited is not None:
            inherited.boundary = max(inherited.boundary, boundary)
        # A live callback may be holding the drain needed by an earlier history
        # release. Confirm persistence here and let release finish after it returns.
        elif not background and not _in_live_event_callback():
            error = await self._wait_for_releases(targets, error)
        if error is not None:
            if not background and self._background_error is error:
                self._background_error = None
            raise error

    async def _wait_for_releases(
        self, targets: "Sequence[_Entry]", error: BaseException | None
    ) -> BaseException | None:
        for entry in targets:
            if entry.committed:
                while not entry.released.is_set():
                    self._release_progress.clear()
                    self._start_release_worker()
                    await self._release_progress.wait()
                if entry.error is not None and (error is None or isinstance(entry.error, asyncio.CancelledError)):
                    error = entry.error
        return error

    async def _persist_to(self, boundary: int) -> None:
        async with self._write_lock:
            while self._pending and self._pending[0].sequence <= boundary:
                if self._fatal_error is not None:
                    raise self._fatal_error
                batch: list[_Entry] = []
                for entry in self._pending:
                    if entry.sequence > boundary or len(batch) >= self._config.batch_size:
                        break
                    batch.append(entry)
                try:
                    await self._write_batch(tuple(entry.record for entry in batch))
                except Exception as exc:
                    self._next_retry = monotonic() + self._config.flush_interval
                    if isinstance(exc, QueueConfigurationError):
                        self._fatal_error = exc
                        raise
                    if self._config.strict:
                        raise
                    logger.warning("Queue event history write failed; retained %d records", self._size, exc_info=True)
                    return
                for entry in batch:
                    self._pending.popleft()
                    entry.committed = True
                    self._committed.append(entry)
                self._next_retry = 0.0
                self._release_wake.set()

    async def _run_releases(self) -> None:
        while True:
            await self._release_wake.wait()
            self._release_wake.clear()
            token = _ReleaseToken(self, self._sequence)
            context = _release_context.set(token)
            try:
                while self._committed and self._committed[0].sequence <= token.boundary:
                    entry = self._committed.popleft()
                    self._active_release = entry
                    try:
                        if entry.release is not None:
                            await entry.release()
                    except asyncio.CancelledError as exc:
                        entry.error = exc
                        if self._background_error is None:
                            self._background_error = exc
                        raise
                    except Exception as exc:
                        entry.error = exc
                        if self._background_error is None:
                            self._background_error = exc
                        else:
                            logger.warning("Additional queue event history release failed", exc_info=True)
                    finally:
                        self._active_release = None
                        entry.released.set()
                        self._release_progress.set()
                        self._size -= 1
            finally:
                token.active = False
                _release_context.reset(context)
            if self._committed:
                self._release_wake.set()

    async def _run_timer(self) -> None:
        while self._state == "accepting":
            self._wake.clear()
            try:
                if self._committed:
                    self._start_release_worker()
                if self._background_error is not None or not self._pending:
                    await self._wake.wait()
                    continue
                deadline = self._pending[0].admitted_at + self._config.flush_interval
                delay = max(deadline, self._next_retry) - monotonic()
                if delay > 0:
                    try:
                        await asyncio.wait_for(self._wake.wait(), timeout=delay)
                    except asyncio.TimeoutError:
                        pass
                    else:
                        continue
                await self._flush_to(self._sequence, background=True)
            except asyncio.CancelledError as exc:
                if self._state == "accepting" and self._background_error is None:
                    self._background_error = exc
                raise
            except Exception as exc:  # noqa: BLE001 - background errors are surfaced once by the owner.
                if self._background_error is None:
                    self._background_error = exc
                if self._fatal_error is not None:
                    return

    async def _close(self) -> None:
        error, self._background_error = self._background_error, None
        try:
            if self._timer is not None:
                self._timer.cancel()
                with suppress(asyncio.CancelledError):
                    await self._timer
            try:
                await self._flush_to(self._sequence)
            except (Exception, asyncio.CancelledError) as exc:
                if error is None or isinstance(exc, asyncio.CancelledError):
                    error = exc
                else:
                    logger.warning("Queue event history final drain also failed", exc_info=True)
            if self._size:
                logger.warning("Queue event history closed with %d unresolved records", self._size)
        finally:
            if self._release_task is not None:
                self._release_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._release_task
            self._background_error = None
            self._state = "closed"
        if error is not None:
            raise error

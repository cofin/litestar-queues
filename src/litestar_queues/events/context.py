"""Task execution context and helper APIs for queue event publishing."""

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from litestar_queues.events.models import QueueEvent
from litestar_queues.exceptions import JobCancelledError

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from litestar_queues.events.publisher import QueueEventPublisher


__all__ = (
    "TaskBeatSink",
    "TaskExecutionContext",
    "beat",
    "bind_beat_sink",
    "bind_task_context",
    "get_current_task_context",
    "publish_task_event",
    "publish_task_log",
    "publish_task_progress",
    "require_current_task_context",
)


class TaskBeatSink(Protocol):
    """Receives last-value-wins beat progress for a running task."""

    def record_beat(self, task_id: "str", detail: "str | None") -> "None":
        """Record the latest beat detail reported by ``task_id``."""
        ...


_current_task_context: 'ContextVar["TaskExecutionContext | None"]' = ContextVar(
    "litestar_queues_task_context", default=None
)
_current_beat_sink: 'ContextVar["TaskBeatSink | None"]' = ContextVar("litestar_queues_beat_sink", default=None)
_active_task_contexts: "dict[str, TaskExecutionContext]" = {}


@dataclass(slots=True)
class TaskExecutionContext:
    """Context bound while a queue task is executing."""

    task_id: "str"
    task_name: "str"
    queue: "str"
    worker_id: "str | None"
    execution_backend: "str"
    execution_profile: "str | None"
    attempt: "int"
    event_publisher: "QueueEventPublisher"
    _sequence: "int" = field(default=0, init=False, repr=False)
    _cancelled: "asyncio.Event" = field(default_factory=asyncio.Event, init=False, repr=False)

    @property
    def is_cancelled(self) -> "bool":
        """Whether durable cancellation has reached this execution."""
        return self._cancelled.is_set()

    async def wait_cancelled(self) -> "None":
        """Wait until durable cancellation reaches this execution."""
        await self._cancelled.wait()

    def raise_if_cancelled(self) -> "None":
        """Raise :class:`JobCancelledError` after cancellation is requested."""
        if self.is_cancelled:
            raise JobCancelledError

    def mark_cancelled(self) -> "None":
        self._cancelled.set()

    async def progress(
        self,
        *,
        current: "float | None" = None,
        total: "float | None" = None,
        percent: "float | None" = None,
        message: "str | None" = None,
        payload: "dict[str, Any] | None" = None,
        channels: "Sequence[str] | None" = None,
        immediate: "bool" = False,
    ) -> "None":
        """Publish a task progress event."""
        progress_percent = percent
        if progress_percent is None and current is not None and total:
            progress_percent = float(current) / float(total) * 100
        await self.publish(
            "task.progress",
            message=message,
            progress_current=current,
            progress_total=total,
            progress_percent=progress_percent,
            payload=payload,
            channels=channels,
            immediate=immediate,
        )

    async def log(
        self,
        message: "str",
        *,
        level: "str" = "info",
        payload: "dict[str, Any] | None" = None,
        channels: "Sequence[str] | None" = None,
        immediate: "bool" = False,
    ) -> "None":
        """Publish a task log event."""
        await self.publish(
            "task.log", level=level, message=message, payload=payload, channels=channels, immediate=immediate
        )

    async def event(
        self,
        event_type: "str",
        *,
        message: "str | None" = None,
        payload: "dict[str, Any] | None" = None,
        channels: "Sequence[str] | None" = None,
        immediate: "bool" = False,
    ) -> "None":
        """Publish a custom task event."""
        await self.publish(event_type, message=message, payload=payload, channels=channels, immediate=immediate)

    async def lifecycle(
        self, event_type: "str", *, message: "str | None" = None, payload: "dict[str, Any] | None" = None
    ) -> "None":
        """Publish a worker-owned lifecycle event."""
        await self.publish(event_type, message=message, payload=payload)

    def beat(self, detail: "str | None" = None) -> "None":
        """Record last-value-wins progress for the next heartbeat tick."""
        sink = _current_beat_sink.get()
        if sink is None:
            return
        sink.record_beat(self.task_id, detail)

    async def publish(
        self,
        event_type: "str",
        *,
        level: "str | None" = None,
        message: "str | None" = None,
        progress_current: "float | None" = None,
        progress_total: "float | None" = None,
        progress_percent: "float | None" = None,
        payload: "dict[str, Any] | None" = None,
        channels: "Sequence[str] | None" = None,
        immediate: "bool" = False,
    ) -> "QueueEvent":
        """Build and publish an event for this task context.

        Returns:
            The published queue event.
        """
        event = QueueEvent(
            type=event_type,
            scope="task",
            task_id=self.task_id,
            task_name=self.task_name,
            queue=self.queue,
            worker_id=self.worker_id,
            execution_backend=self.execution_backend,
            execution_profile=self.execution_profile,
            attempt=self.attempt,
            sequence=self._next_sequence(),
            level=level,
            message=message,
            progress_current=progress_current,
            progress_total=progress_total,
            progress_percent=progress_percent,
            payload=dict(payload or {}),
        )
        await self.event_publisher.publish(event, channels=channels, immediate=immediate)
        return event

    def _next_sequence(self) -> "int":
        self._sequence += 1
        return self._sequence


def get_current_task_context() -> "TaskExecutionContext | None":
    """Return the task execution context for the current async context."""
    return _current_task_context.get()


def require_current_task_context() -> "TaskExecutionContext":
    """Return the current task context or raise if none is bound.

    Raises:
        RuntimeError: If no task context is bound.
    """
    context = get_current_task_context()
    if context is None:
        msg = "No queue task execution context is currently bound."
        raise RuntimeError(msg)
    return context


async def publish_task_progress(
    *,
    current: "float | None" = None,
    total: "float | None" = None,
    percent: "float | None" = None,
    message: "str | None" = None,
    payload: "dict[str, Any] | None" = None,
    channels: "Sequence[str] | None" = None,
    immediate: "bool" = False,
) -> "None":
    """Publish progress through the currently bound task context."""
    await require_current_task_context().progress(
        current=current,
        total=total,
        percent=percent,
        message=message,
        payload=payload,
        channels=channels,
        immediate=immediate,
    )


async def publish_task_log(
    message: "str",
    *,
    level: "str" = "info",
    payload: "dict[str, Any] | None" = None,
    channels: "Sequence[str] | None" = None,
    immediate: "bool" = False,
) -> "None":
    """Publish a log event through the currently bound task context."""
    await require_current_task_context().log(
        message, level=level, payload=payload, channels=channels, immediate=immediate
    )


async def publish_task_event(
    event_type: "str",
    *,
    message: "str | None" = None,
    payload: "dict[str, Any] | None" = None,
    channels: "Sequence[str] | None" = None,
    immediate: "bool" = False,
) -> "None":
    """Publish a custom event through the currently bound task context."""
    await require_current_task_context().event(
        event_type, message=message, payload=payload, channels=channels, immediate=immediate
    )


def beat(detail: "str | None" = None) -> "None":
    """Record progress through the currently bound task context, if any."""
    context = get_current_task_context()
    if context is not None:
        context.beat(detail)


@contextmanager
def bind_task_context(context: "TaskExecutionContext") -> "Iterator[TaskExecutionContext]":
    """Bind ``context`` as the current task execution context.

    This is the supported entry point for external runtimes adopting the events
    subpackage standalone. While bound, :func:`require_current_task_context` and
    the module-level publish helpers resolve to ``context``.

    Yields:
        The bound task execution context.
    """
    _active_task_contexts[context.task_id] = context
    token = _current_task_context.set(context)
    try:
        yield context
    finally:
        _active_task_contexts.pop(context.task_id, None)
        _current_task_context.reset(token)


@contextmanager
def bind_beat_sink(sink: "TaskBeatSink") -> "Iterator[TaskBeatSink]":
    """Bind ``sink`` to receive :meth:`TaskExecutionContext.beat` calls.

    Yields:
        The bound beat sink.
    """
    token = _current_beat_sink.set(sink)
    try:
        yield sink
    finally:
        _current_beat_sink.reset(token)


def _cancel_task_context(task_id: "str") -> "None":
    context = _active_task_contexts.get(task_id)
    if context is not None:
        context.mark_cancelled()

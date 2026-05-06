"""Task execution context and helper APIs for queue event publishing."""

from collections.abc import Sequence
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any

from litestar_queues.events.models import QueueEvent
from litestar_queues.events.publisher import QueueEventPublisher

__all__ = (
    "TaskExecutionContext",
    "get_current_task_context",
    "publish_task_event",
    "publish_task_log",
    "publish_task_progress",
    "require_current_task_context",
)

_current_task_context: ContextVar["TaskExecutionContext | None"] = ContextVar(
    "litestar_queues_task_context",
    default=None,
)


@dataclass(slots=True)
class TaskExecutionContext:
    """Context bound while a queue task is executing."""

    task_id: str
    task_name: str
    queue: str
    worker_id: str | None
    execution_backend: str
    execution_profile: str | None
    attempt: int
    event_publisher: QueueEventPublisher
    _sequence: int = field(default=0, init=False, repr=False)

    async def progress(
        self,
        *,
        current: int | float | None = None,
        total: int | float | None = None,
        percent: float | None = None,
        message: str | None = None,
        payload: dict[str, Any] | None = None,
        channels: Sequence[str] | None = None,
    ) -> None:
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
        )

    async def log(
        self,
        message: str,
        *,
        level: str = "info",
        payload: dict[str, Any] | None = None,
        channels: Sequence[str] | None = None,
    ) -> None:
        """Publish a task log event."""
        await self.publish("task.log", level=level, message=message, payload=payload, channels=channels)

    async def event(
        self,
        event_type: str,
        *,
        message: str | None = None,
        payload: dict[str, Any] | None = None,
        channels: Sequence[str] | None = None,
    ) -> None:
        """Publish a custom task event."""
        await self.publish(event_type, message=message, payload=payload, channels=channels)

    async def lifecycle(
        self,
        event_type: str,
        *,
        message: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        """Publish a worker-owned lifecycle event."""
        await self.publish(event_type, message=message, payload=payload)

    async def publish(
        self,
        event_type: str,
        *,
        level: str | None = None,
        message: str | None = None,
        progress_current: int | float | None = None,
        progress_total: int | float | None = None,
        progress_percent: float | None = None,
        payload: dict[str, Any] | None = None,
        channels: Sequence[str] | None = None,
    ) -> QueueEvent:
        """Build and publish an event for this task context."""
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
        await self.event_publisher.publish(event, channels=channels)
        return event

    def _next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence


def get_current_task_context() -> TaskExecutionContext | None:
    """Return the task execution context for the current async context."""
    return _current_task_context.get()


def require_current_task_context() -> TaskExecutionContext:
    """Return the current task context or raise if none is bound."""
    context = get_current_task_context()
    if context is None:
        msg = "No queue task execution context is currently bound."
        raise RuntimeError(msg)
    return context


async def publish_task_progress(
    *,
    current: int | float | None = None,
    total: int | float | None = None,
    percent: float | None = None,
    message: str | None = None,
    payload: dict[str, Any] | None = None,
    channels: Sequence[str] | None = None,
) -> None:
    """Publish progress through the currently bound task context."""
    await require_current_task_context().progress(
        current=current,
        total=total,
        percent=percent,
        message=message,
        payload=payload,
        channels=channels,
    )


async def publish_task_log(
    message: str,
    *,
    level: str = "info",
    payload: dict[str, Any] | None = None,
    channels: Sequence[str] | None = None,
) -> None:
    """Publish a log event through the currently bound task context."""
    await require_current_task_context().log(message, level=level, payload=payload, channels=channels)


async def publish_task_event(
    event_type: str,
    *,
    message: str | None = None,
    payload: dict[str, Any] | None = None,
    channels: Sequence[str] | None = None,
) -> None:
    """Publish a custom event through the currently bound task context."""
    await require_current_task_context().event(event_type, message=message, payload=payload, channels=channels)


def _bind_task_context(context: TaskExecutionContext) -> "Token[TaskExecutionContext | None]":
    return _current_task_context.set(context)


def _reset_task_context(token: "Token[TaskExecutionContext | None]") -> None:
    _current_task_context.reset(token)

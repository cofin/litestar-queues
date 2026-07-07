"""Producer facade for queue event publishing."""

from typing import TYPE_CHECKING, Any, Literal

from litestar_queues.events.models import QueueEvent

if TYPE_CHECKING:
    from litestar_queues.events.publisher import QueueEventPublisher

__all__ = ("QueueEventProducer",)


class QueueEventProducer:
    """Thin facade over a queue event publisher."""

    __slots__ = ("_publisher",)

    def __init__(self, publisher: "QueueEventPublisher") -> "None":
        self._publisher = publisher

    def task(self, task_id: "str") -> "_TaskEventHandle":
        """Return a task-scoped event handle."""
        return _TaskEventHandle(self._publisher, task_id)

    def queue(self, name: "str") -> "_ScopeEventHandle":
        """Return a queue-scoped event handle."""
        return _ScopeEventHandle(self._publisher, "queue", name)

    def worker(self, worker_id: "str") -> "_ScopeEventHandle":
        """Return a worker-scoped event handle."""
        return _ScopeEventHandle(self._publisher, "worker", worker_id)

    def channel(self, scope_key: "str") -> "_ScopeEventHandle":
        """Return a custom-channel event handle."""
        return _ScopeEventHandle(self._publisher, "custom", scope_key)


class _ScopeEventHandle:
    __slots__ = ("_key_value", "_publisher", "_scope")

    def __init__(
        self,
        publisher: "QueueEventPublisher",
        scope: "Literal['task', 'queue', 'worker', 'custom']",
        key_value: "str",
    ) -> "None":
        self._publisher = publisher
        self._scope = scope
        self._key_value = key_value

    async def publish(
        self,
        event_type: "str",
        *,
        message: "str | None" = None,
        payload: "dict[str, Any] | None" = None,
        immediate: "bool" = False,
    ) -> "QueueEvent":
        """Publish an event to this handle's scope.

        The ``immediate`` flag is accepted for API stability and becomes active
        when buffered publishing lands.

        Returns:
            The published queue event.
        """
        del immediate
        event = self._event(event_type, message=message, payload=payload)
        await self._publisher.publish(event)
        return event

    def _event(
        self, event_type: "str", *, message: "str | None", payload: "dict[str, Any] | None"
    ) -> "QueueEvent":
        if self._scope == "task":
            return QueueEvent(
                type=event_type,
                scope="task",
                task_id=self._key_value,
                message=message,
                payload=dict(payload or {}),
            )
        if self._scope == "queue":
            return QueueEvent(
                type=event_type,
                scope="queue",
                scope_key=self._key_value,
                queue=self._key_value,
                message=message,
                payload=dict(payload or {}),
            )
        if self._scope == "worker":
            return QueueEvent(
                type=event_type,
                scope="worker",
                worker_id=self._key_value,
                message=message,
                payload=dict(payload or {}),
            )
        return QueueEvent(
            type=event_type,
            scope="custom",
            scope_key=self._key_value,
            message=message,
            payload=dict(payload or {}),
        )


class _TaskEventHandle(_ScopeEventHandle):
    __slots__ = ()

    def __init__(self, publisher: "QueueEventPublisher", task_id: "str") -> "None":
        super().__init__(publisher, "task", task_id)

    async def log(
        self,
        message: "str",
        *,
        level: "str" = "info",
        payload: "dict[str, Any] | None" = None,
        immediate: "bool" = False,
    ) -> "QueueEvent":
        """Publish a task log event.

        Returns:
            The published queue event.
        """
        del immediate
        event = self._task_event("task.log", level=level, message=message, payload=payload)
        await self._publisher.publish(event)
        return event

    async def progress(
        self,
        *,
        current: "float | None" = None,
        total: "float | None" = None,
        percent: "float | None" = None,
        message: "str | None" = None,
        payload: "dict[str, Any] | None" = None,
        immediate: "bool" = False,
    ) -> "QueueEvent":
        """Publish a task progress event.

        Returns:
            The published queue event.
        """
        del immediate
        progress_percent = percent
        if progress_percent is None and current is not None and total:
            progress_percent = float(current) / float(total) * 100
        event = self._task_event(
            "task.progress",
            message=message,
            progress_current=current,
            progress_total=total,
            progress_percent=progress_percent,
            payload=payload,
        )
        await self._publisher.publish(event)
        return event

    async def event(
        self,
        event_type: "str",
        *,
        message: "str | None" = None,
        payload: "dict[str, Any] | None" = None,
        immediate: "bool" = False,
    ) -> "QueueEvent":
        """Publish a custom task event.

        Returns:
            The published queue event.
        """
        del immediate
        event = self._task_event(event_type, message=message, payload=payload)
        await self._publisher.publish(event)
        return event

    def _task_event(
        self,
        event_type: "str",
        *,
        level: "str | None" = None,
        message: "str | None" = None,
        progress_current: "float | None" = None,
        progress_total: "float | None" = None,
        progress_percent: "float | None" = None,
        payload: "dict[str, Any] | None" = None,
    ) -> "QueueEvent":
        return QueueEvent(
            type=event_type,
            scope="task",
            task_id=self._key_value,
            level=level,
            message=message,
            progress_current=progress_current,
            progress_total=progress_total,
            progress_percent=progress_percent,
            payload=dict(payload or {}),
        )

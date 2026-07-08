"""Producer facade for queue event publishing."""

import inspect
from typing import TYPE_CHECKING, Any, Literal

from litestar_queues.events.models import QueueEvent
from litestar_queues.events.publisher import QueueEventPublisher
from litestar_queues.events.sinks import NoopQueueEventSink, QueueEventSink

if TYPE_CHECKING:
    from types import TracebackType

    from litestar_queues.config import QueueConfig

__all__ = ("QueueEventProducer", "create_event_producer")


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


def create_event_producer(config: "QueueConfig") -> "_ExternalProducer":
    """Return an external producer context manager for queue event publishing."""
    return _ExternalProducer(config)


class _ExternalProducer:
    __slots__ = ("_config", "_producer", "_resource")

    def __init__(self, config: "QueueConfig") -> "None":
        self._config = config
        self._producer: "QueueEventProducer | None" = None
        self._resource: "object | None" = None

    async def __aenter__(self) -> "QueueEventProducer":
        sink, resource, publisher = self._build_publisher()
        self._resource = resource if resource is not None else sink
        await _call_optional_lifecycle(self._resource, "open", "on_startup")
        self._producer = QueueEventProducer(publisher)
        return self._producer

    async def __aexit__(
        self,
        exc_type: "type[BaseException] | None",  # noqa: PYI036
        exc_val: "BaseException | None",  # noqa: PYI036
        exc_tb: "TracebackType | None",  # noqa: PYI036
    ) -> "None":
        await self.aclose()

    async def aclose(self) -> "None":
        """Close the external producer transport if it is open."""
        resource = self._resource
        self._producer = None
        self._resource = None
        await _call_optional_lifecycle(resource, "close", "on_shutdown")

    async def close(self) -> "None":
        """Close the external producer transport if it is open."""
        await self.aclose()

    def _build_publisher(self) -> "tuple[QueueEventSink, object | None, QueueEventPublisher]":
        event_config = self._config.event
        resource: "object | None" = None
        if event_config is None:
            sink: "QueueEventSink" = NoopQueueEventSink()
            publisher = QueueEventPublisher(sink)
            return sink, resource, publisher
        if not event_config.enabled:
            sink = NoopQueueEventSink()
        elif event_config.sink is not None:
            sink = event_config.sink
            resource = sink
        elif event_config.channels_backend is not None:
            from litestar_queues.events.litestar import ChannelsQueueEventSink

            sink = ChannelsQueueEventSink(
                event_config.channels_backend,
                max_payload_bytes=event_config.max_payload_bytes,
                payload_size_estimator=event_config.payload_size_estimator,
            )
            resource = event_config.channels_backend
        else:
            sink = NoopQueueEventSink()
        publisher = QueueEventPublisher(
            sink,
            strict=event_config.strict,
            publish_task_channel=event_config.publish_task_channel,
            publish_queue_channel=event_config.publish_queue_channel,
            publish_global_lifecycle=event_config.publish_global_lifecycle,
        )
        return sink, resource, publisher


class _ScopeEventHandle:
    __slots__ = ("_key_value", "_publisher", "_scope")

    def __init__(
        self, publisher: "QueueEventPublisher", scope: "Literal['task', 'queue', 'worker', 'custom']", key_value: "str"
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

        Returns:
            The published queue event.
        """
        event = self._event(event_type, message=message, payload=payload)
        await self._publisher.publish(event, immediate=immediate)
        return event

    def _event(self, event_type: "str", *, message: "str | None", payload: "dict[str, Any] | None") -> "QueueEvent":
        if self._scope == "task":
            return QueueEvent(
                type=event_type, scope="task", task_id=self._key_value, message=message, payload=dict(payload or {})
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
                type=event_type, scope="worker", worker_id=self._key_value, message=message, payload=dict(payload or {})
            )
        return QueueEvent(
            type=event_type, scope="custom", scope_key=self._key_value, message=message, payload=dict(payload or {})
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
        event = self._task_event("task.log", level=level, message=message, payload=payload)
        await self._publisher.publish(event, immediate=immediate)
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
        await self._publisher.publish(event, immediate=immediate)
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
        event = self._task_event(event_type, message=message, payload=payload)
        await self._publisher.publish(event, immediate=immediate)
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


async def _call_optional_lifecycle(resource: "object | None", primary: "str", fallback: "str") -> "None":
    if resource is None:
        return
    method = getattr(resource, primary, None)
    if method is None:
        method = getattr(resource, fallback, None)
    if method is None:
        return
    result = method()
    if inspect.isawaitable(result):
        await result

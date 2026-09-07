"""Producer facade for queue event publishing."""

import logging
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Literal

from litestar_queues.events._history_buffer import _in_event_release_callback
from litestar_queues.events.models import QueueEvent
from litestar_queues.events.sinks import _call_optional_lifecycle, _select_lifecycle_error
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from types import TracebackType

    from litestar_queues.config import QueueConfig
    from litestar_queues.events.publisher import QueueEventPublisher

__all__ = ("QueueEventProducer", "create_event_producer")

logger = logging.getLogger(__name__)


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
    __slots__ = ("_config", "_producer", "_publisher", "_resources")

    def __init__(self, config: "QueueConfig") -> "None":
        self._config = config
        self._producer: "QueueEventProducer | None" = None
        self._publisher: "QueueEventPublisher | None" = None
        self._resources: "tuple[object, ...]" = ()

    async def __aenter__(self) -> "QueueEventProducer":
        self._require_lifecycle_boundary()
        if self._producer is not None:
            return self._producer
        publisher, resources = self._build_publisher()
        try:
            for resource in resources:
                if await _call_optional_lifecycle(resource, "open", "on_startup"):
                    self._resources += (resource,)
            publisher.start_buffer()
            self._publisher = publisher
            self._producer = QueueEventProducer(publisher)
        except BaseException as primary:
            await self._close(primary=primary)
            raise
        return self._producer

    async def __aexit__(
        self,
        exc_type: "type[BaseException] | None",  # noqa: PYI036
        exc_val: "BaseException | None",  # noqa: PYI036
        exc_tb: "TracebackType | None",  # noqa: PYI036
    ) -> "None":
        await self._close(primary=exc_val)

    async def aclose(self) -> "None":
        """Drain live delivery and close acquired transports in reverse order."""
        await self._close()

    async def _close(self, *, primary: "BaseException | None" = None) -> "None":
        self._require_lifecycle_boundary()
        publisher, self._publisher = self._publisher, None
        resources, self._resources = self._resources, ()
        self._producer = None
        errors: list[BaseException] = []
        if publisher is not None:
            try:
                await publisher.stop_buffer()
            except BaseException as exc:  # noqa: BLE001 - complete all acquired cleanup before selecting the error.
                errors.append(exc)
        for resource in reversed(resources):
            try:
                await _call_optional_lifecycle(resource, "close", "on_shutdown")
            except BaseException as exc:  # noqa: BLE001, PERF203 - every acquired resource gets one cleanup attempt.
                errors.append(exc)
        selected = primary if primary is not None else _select_lifecycle_error(errors)
        for error in errors:
            if error is not selected:
                # Reporting must never replace the primary or interrupt cleanup.
                with suppress(BaseException):
                    logger.warning(
                        "External queue event producer cleanup also failed.",
                        exc_info=(type(error), error, error.__traceback__),
                    )
        if primary is None and selected is not None:
            raise selected

    @staticmethod
    def _require_lifecycle_boundary() -> "None":
        if _in_event_release_callback():
            message = "External queue event producer lifecycle cannot change from an active event release callback."
            raise QueueConfigurationError(message)

    def _build_publisher(self) -> "tuple[QueueEventPublisher, tuple[object, ...]]":
        events_config = self._config.events
        publisher = self._config.get_event_publisher()
        if events_config is None or events_config.delivery is None:
            return publisher, ()
        resources: "list[object]" = []
        if events_config.channels is not None:
            resources.append(events_config.channels)
        resources.extend(events_config.delivery.sinks)
        seen: set[int] = set()
        unique = []
        for resource in resources:
            if id(resource) not in seen:
                seen.add(id(resource))
                unique.append(resource)
        return publisher, tuple(unique)


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

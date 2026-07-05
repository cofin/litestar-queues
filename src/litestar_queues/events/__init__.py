"""Realtime queue event contracts and publishers."""

from typing import TYPE_CHECKING, Any

from litestar_queues.events.channels import QueueChannels
from litestar_queues.events.context import (
    TaskExecutionContext,
    get_current_task_context,
    publish_task_event,
    publish_task_log,
    publish_task_progress,
    require_current_task_context,
)
from litestar_queues.events.log import QueueEventLog, QueueEventLogConfig, QueueEventLogRecord, QueueEventStageSummary
from litestar_queues.events.models import (
    QueueEvent,
    QueueEventActor,
    QueueEventEntityRef,
    QueueEventScope,
    QueueEventType,
)
from litestar_queues.events.publisher import QueueEventConfig, QueueEventPublisher
from litestar_queues.events.sinks import InMemoryQueueEventSink, NoopQueueEventSink, QueueEventSink

if TYPE_CHECKING:
    from litestar_queues.events.litestar import ChannelsQueueEventSink, stream_queue_events

__all__ = (
    "ChannelsQueueEventSink",
    "InMemoryQueueEventSink",
    "NoopQueueEventSink",
    "QueueChannels",
    "QueueEvent",
    "QueueEventActor",
    "QueueEventConfig",
    "QueueEventEntityRef",
    "QueueEventLog",
    "QueueEventLogConfig",
    "QueueEventLogRecord",
    "QueueEventPublisher",
    "QueueEventScope",
    "QueueEventSink",
    "QueueEventStageSummary",
    "QueueEventType",
    "TaskExecutionContext",
    "get_current_task_context",
    "publish_task_event",
    "publish_task_log",
    "publish_task_progress",
    "require_current_task_context",
    "stream_queue_events",
)


def __getattr__(name: "str") -> "Any":
    """Lazy load Litestar integration classes to avoid unnecessary imports.

    Returns:
        The requested optional Litestar integration export.
    """
    if name == "ChannelsQueueEventSink":
        from litestar_queues.events.litestar import ChannelsQueueEventSink

        return ChannelsQueueEventSink
    if name == "stream_queue_events":
        from litestar_queues.events.litestar import stream_queue_events

        return stream_queue_events
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)

"""Realtime queue event contracts and publishers."""

from typing import TYPE_CHECKING, Any

from litestar_queues.events.channels import QueueChannels
from litestar_queues.events.context import (
    TaskExecutionContext,
    beat,
    get_current_task_context,
    publish_task_event,
    publish_task_log,
    publish_task_progress,
    require_current_task_context,
)
from litestar_queues.events.log import EventLogConfig, QueueEventLog, QueueEventLogRecord, QueueEventStageSummary
from litestar_queues.events.models import (
    QueueEvent,
    QueueEventActor,
    QueueEventEntityRef,
    QueueEventScope,
    QueueEventType,
)
from litestar_queues.events.producer import QueueEventProducer
from litestar_queues.events.publisher import EventConfig, QueueEventPublisher
from litestar_queues.events.sinks import InMemoryQueueEventSink, NoopQueueEventSink, QueueEventSink
from litestar_queues.events.stream_config import EventStreamConfig

if TYPE_CHECKING:
    from litestar_queues.events.litestar import ChannelsQueueEventSink, stream_queue_events

__all__ = (
    "ChannelsQueueEventSink",
    "EventConfig",
    "EventLogConfig",
    "EventStreamConfig",
    "InMemoryQueueEventSink",
    "NoopQueueEventSink",
    "QueueChannels",
    "QueueEvent",
    "QueueEventActor",
    "QueueEventEntityRef",
    "QueueEventLog",
    "QueueEventLogRecord",
    "QueueEventProducer",
    "QueueEventPublisher",
    "QueueEventScope",
    "QueueEventSink",
    "QueueEventStageSummary",
    "QueueEventType",
    "TaskExecutionContext",
    "beat",
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

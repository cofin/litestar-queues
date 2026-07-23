"""Realtime queue event contracts and publishers."""

from typing import TYPE_CHECKING, Any

from litestar_queues.events.channels import QueueChannels
from litestar_queues.events.config import EventDeliveryConfig, QueueEventsConfig
from litestar_queues.events.context import (
    TaskExecutionContext,
    beat,
    get_current_task_context,
    publish_task_event,
    publish_task_log,
    publish_task_progress,
    require_current_task_context,
)
from litestar_queues.events.history import (
    EventHistoryConfig,
    QueueEventLog,
    QueueEventLogRecord,
    QueueEventStageSummary,
)
from litestar_queues.events.models import (
    QueueEvent,
    QueueEventActor,
    QueueEventEntityRef,
    QueueEventScope,
    QueueEventType,
)
from litestar_queues.events.producer import QueueEventProducer, create_event_producer
from litestar_queues.events.publisher import EventBufferConfig, QueueEventPublisher
from litestar_queues.events.sinks import (
    CompositeQueueEventSink,
    InMemoryQueueEventSink,
    NoopQueueEventSink,
    QueueEventSink,
)
from litestar_queues.events.stream_config import (
    ChannelAuthorizer,
    EventStreamConfig,
    EventStreamTransport,
    Guard,
    UnauthenticatedAccess,
)

if TYPE_CHECKING:
    from litestar_queues.events.litestar import ChannelsQueueEventSink

__all__ = (
    "ChannelAuthorizer",
    "ChannelsQueueEventSink",
    "CompositeQueueEventSink",
    "EventBufferConfig",
    "EventDeliveryConfig",
    "EventHistoryConfig",
    "EventStreamConfig",
    "EventStreamTransport",
    "Guard",
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
    "QueueEventsConfig",
    "TaskExecutionContext",
    "UnauthenticatedAccess",
    "beat",
    "create_event_producer",
    "get_current_task_context",
    "publish_task_event",
    "publish_task_log",
    "publish_task_progress",
    "require_current_task_context",
)


def __getattr__(name: "str") -> "Any":
    """Lazy load Litestar integration classes to avoid unnecessary imports.

    Returns:
        The requested optional Litestar integration export.
    """
    if name == "ChannelsQueueEventSink":
        from litestar_queues.events.litestar import ChannelsQueueEventSink

        return ChannelsQueueEventSink
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)

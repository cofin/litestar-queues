"""Realtime queue event contracts and publishers."""

from typing import TYPE_CHECKING, Any

from litestar_queues.events._log_records import event_actor_key, event_entity_key
from litestar_queues.events.channels import QueueChannels
from litestar_queues.events.config import EventDeliveryConfig, QueueEventsConfig
from litestar_queues.events.context import (
    TaskBeatSink,
    TaskExecutionContext,
    beat,
    bind_beat_sink,
    bind_task_context,
    get_current_task_context,
    publish_task_event,
    publish_task_log,
    publish_task_progress,
    require_current_task_context,
)
from litestar_queues.events.history import (
    EventHistoryConfig,
    EventHistoryExtraColumn,
    QueueEventLog,
    QueueEventLogRecord,
    QueueEventStageSummary,
    extract_event_extras,
    validate_event_extra_filter,
    validate_event_history_extra_columns,
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
from litestar_queues.events.query import (
    EVENT_LEVEL_RANKS,
    QueueEventOrder,
    QueueEventQuery,
    QueueEventRetentionRule,
    event_level_for_rank,
    event_level_rank,
)
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
    from litestar_queues.events.channels_sink import ChannelsQueueEventSink

__all__ = (
    "EVENT_LEVEL_RANKS",
    "ChannelAuthorizer",
    "ChannelsQueueEventSink",
    "CompositeQueueEventSink",
    "EventBufferConfig",
    "EventDeliveryConfig",
    "EventHistoryConfig",
    "EventHistoryExtraColumn",
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
    "QueueEventOrder",
    "QueueEventProducer",
    "QueueEventPublisher",
    "QueueEventQuery",
    "QueueEventRetentionRule",
    "QueueEventScope",
    "QueueEventSink",
    "QueueEventStageSummary",
    "QueueEventType",
    "QueueEventsConfig",
    "TaskBeatSink",
    "TaskExecutionContext",
    "UnauthenticatedAccess",
    "beat",
    "bind_beat_sink",
    "bind_task_context",
    "create_event_producer",
    "event_actor_key",
    "event_entity_key",
    "event_level_for_rank",
    "event_level_rank",
    "extract_event_extras",
    "get_current_task_context",
    "publish_task_event",
    "publish_task_log",
    "publish_task_progress",
    "require_current_task_context",
    "validate_event_extra_filter",
    "validate_event_history_extra_columns",
)


def __getattr__(name: "str") -> "Any":
    """Lazy load Litestar integration classes to avoid unnecessary imports.

    Returns:
        The requested optional Litestar integration export.
    """
    if name == "ChannelsQueueEventSink":
        from litestar_queues.events.channels_sink import ChannelsQueueEventSink

        return ChannelsQueueEventSink
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)

"""Consolidated queue event configuration."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from litestar_queues.events.publisher import EventBufferConfig
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from litestar_queues.events.chunking import QueueEventSizeEstimator
    from litestar_queues.events.history import EventHistoryConfig
    from litestar_queues.events.sinks import QueueEventSink
    from litestar_queues.events.stream_config import EventStreamConfig
    from litestar_queues.typing import ChannelsLike

__all__ = ("EventDeliveryConfig", "QueueEventsConfig")


@dataclass(slots=True)
class EventDeliveryConfig:
    """Configuration for live queue event delivery."""

    buffer: "EventBufferConfig | None" = field(default_factory=EventBufferConfig)
    """Optional producer buffer; ``None`` delivers every event immediately."""

    sinks: "tuple[QueueEventSink, ...]" = ()
    """Additional live-delivery sinks, invoked in tuple order."""

    max_payload_bytes: "int | None" = None
    """Optional maximum encoded Channels payload size in bytes."""

    payload_size_estimator: "QueueEventSizeEstimator | None" = None
    """Optional encoded-size estimator used for Channels payload chunking."""

    strict: "bool" = False
    """Whether the first delivery failure should propagate."""

    publish_task_channel: "bool" = True
    """Whether task-scoped events also target the canonical task channel."""

    publish_queue_channel: "bool" = True
    """Whether task-scoped events also target the canonical queue channel."""

    publish_global_lifecycle: "bool" = False
    """Whether lifecycle events also target the global channel."""

    def __post_init__(self) -> "None":
        """Validate delivery limits."""
        if self.max_payload_bytes is not None and self.max_payload_bytes <= 0:
            msg = "EventDeliveryConfig.max_payload_bytes must be greater than 0."
            raise QueueConfigurationError(msg)


@dataclass(slots=True)
class QueueEventsConfig:
    """Group queue event delivery, streaming, and history capabilities."""

    channels: "ChannelsLike | None" = None
    """Explicit shared Channels target; ``None`` permits app discovery only."""

    delivery: "EventDeliveryConfig | None" = None
    """Live delivery configuration; ``None`` disables live publishing."""

    stream: "EventStreamConfig | None" = None
    """Application stream endpoints; ``None`` registers no endpoints."""

    history: "EventHistoryConfig | None" = None
    """Backend-owned event history; ``None`` disables persistence."""

    def __post_init__(self) -> "None":
        """Reject empty and unused event groups."""
        if self.delivery is None and self.stream is None and self.history is None:
            if self.channels is not None:
                msg = "QueueEventsConfig.channels is unused without delivery or stream."
            else:
                msg = "QueueEventsConfig requires at least one of delivery, stream, or history."
            raise QueueConfigurationError(msg)

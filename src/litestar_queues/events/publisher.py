"""Queue event publisher."""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from litestar_queues.events.channels import QueueChannels
from litestar_queues.events.sinks import NoopQueueEventSink, QueueEventSink

if TYPE_CHECKING:
    from collections.abc import Sequence

    from litestar_queues.events._typing import ChannelsLike
    from litestar_queues.events.models import QueueEvent

__all__ = ("QueueEventConfig", "QueueEventPublisher")

logger = logging.getLogger(__name__)

_LIFECYCLE_EVENT_TYPES = frozenset({
    "task.started",
    "task.completed",
    "task.failed",
    "task.cancelled",
    "task.claim_lost",
    "task.stale_failed",
})


@dataclass(slots=True)
class QueueEventConfig:
    """Configuration for queue event publishing."""

    enabled: "bool" = False
    sink: "QueueEventSink | None" = None
    channels_backend: "ChannelsLike | None" = None
    strict: "bool" = False
    publish_task_channel: "bool" = True
    publish_queue_channel: "bool" = True
    publish_global_lifecycle: "bool" = False


class QueueEventPublisher:
    """Publish queue events through a configured sink."""

    __slots__ = ("_sink", "publish_global_lifecycle", "publish_queue_channel", "publish_task_channel", "strict")

    def __init__(
        self,
        sink: "QueueEventSink | None" = None,
        *,
        strict: "bool" = False,
        publish_task_channel: "bool" = True,
        publish_queue_channel: "bool" = True,
        publish_global_lifecycle: "bool" = False,
    ) -> "None":
        self._sink = sink or NoopQueueEventSink()
        self.strict = strict
        self.publish_task_channel = publish_task_channel
        self.publish_queue_channel = publish_queue_channel
        self.publish_global_lifecycle = publish_global_lifecycle

    @property
    def sink(self) -> "QueueEventSink":
        """Configured event sink."""
        return self._sink

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str] | None" = None) -> "None":
        """Publish an event to canonical and explicitly supplied channels."""
        resolved_channels = self.resolve_channels(event, channels=channels)
        try:
            await self._sink.publish(event, channels=resolved_channels)
        except Exception:
            if self.strict:
                raise
            logger.warning(
                "Queue event publish failed",
                exc_info=True,
                extra={"queue_event_type": event.type, "queue_event_id": event.id},
            )

    def resolve_channels(self, event: "QueueEvent", *, channels: "Sequence[str] | None" = None) -> "tuple[str, ...]":
        """Return canonical publish channels for an event plus explicit extras."""
        resolved: "list[str]" = []
        if self.publish_task_channel and event.task_id is not None:
            resolved.append(QueueChannels.task(event.task_id))
        if event.scope == "queue" and event.scope_key is not None:
            resolved.append(QueueChannels.queue(event.scope_key))
        if self.publish_queue_channel and event.queue is not None:
            resolved.append(QueueChannels.queue(event.queue))
        if event.scope == "worker" and event.worker_id is not None:
            resolved.append(QueueChannels.worker(event.worker_id))
        if event.scope == "global":
            resolved.append(QueueChannels.global_channel())
        if event.scope == "custom" and event.scope_key is not None:
            resolved.append(QueueChannels.custom(event.scope_key))
        if self.publish_global_lifecycle and event.type in _LIFECYCLE_EVENT_TYPES:
            resolved.append(QueueChannels.global_channel())
        if channels:
            resolved.extend(channels)
        return _dedupe(resolved or [QueueChannels.global_channel()])


def _dedupe(channels: "Sequence[str]") -> "tuple[str, ...]":
    seen: "set[str]" = set()
    resolved: "list[str]" = []
    for channel in channels:
        if channel in seen:
            continue
        seen.add(channel)
        resolved.append(channel)
    return tuple(resolved)

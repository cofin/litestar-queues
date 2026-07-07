"""Queue event publisher."""

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from litestar_queues.events.buffer import LiveEventBuffer, event_buffer_key
from litestar_queues.events.channels import QueueChannels
from litestar_queues.events.sinks import NoopQueueEventSink, QueueEventSink

if TYPE_CHECKING:
    from collections.abc import Sequence

    from litestar_queues.events._typing import ChannelsLike
    from litestar_queues.events.log import QueueEventLog
    from litestar_queues.events.models import QueueEvent

__all__ = ("EventBufferConfig", "EventConfig", "QueueEventPublisher")

logger = logging.getLogger(__name__)

_LIFECYCLE_EVENT_TYPES = frozenset({
    "task.started",
    "task.completed",
    "task.failed",
    "task.cancelled",
    "task.claim_lost",
    "task.stale_failed",
})
_TERMINAL_EVENT_TYPES = frozenset({
    "task.completed",
    "task.failed",
    "task.cancelled",
    "task.claim_lost",
    "task.stale_failed",
})


@dataclass(slots=True)
class EventBufferConfig:
    """Producer-side micro-batch buffer for live event delivery."""

    enabled: "bool" = True
    buffer_size: "int" = 20
    flush_interval: "float" = 0.5
    max_pending: "int" = 2000
    overflow: 'Literal["drop_oldest", "drop_newest", "block", "error"]' = "drop_oldest"


@dataclass(slots=True)
class EventConfig:
    """Configuration for queue event publishing."""

    enabled: "bool" = True
    buffer: "EventBufferConfig" = field(default_factory=EventBufferConfig)
    sink: "QueueEventSink | None" = None
    channels_backend: "ChannelsLike | None" = None
    strict: "bool" = False
    publish_task_channel: "bool" = True
    publish_queue_channel: "bool" = True
    publish_global_lifecycle: "bool" = False


class QueueEventPublisher:
    """Publish queue events through a configured sink."""

    __slots__ = (
        "_buffer",
        "_event_log",
        "_event_log_strict",
        "_sink",
        "publish_global_lifecycle",
        "publish_queue_channel",
        "publish_task_channel",
        "strict",
    )

    def __init__(
        self,
        sink: "QueueEventSink | None" = None,
        *,
        event_log: "QueueEventLog | None" = None,
        event_log_strict: "bool" = False,
        buffer_config: "EventBufferConfig | None" = None,
        strict: "bool" = False,
        publish_task_channel: "bool" = True,
        publish_queue_channel: "bool" = True,
        publish_global_lifecycle: "bool" = False,
    ) -> "None":
        self._sink = sink or NoopQueueEventSink()
        self._event_log = event_log
        self._event_log_strict = event_log_strict
        self._buffer = (
            LiveEventBuffer(buffer_config, sink_publish=self._deliver_live, record_drop=_ignore_buffer_drop)
            if buffer_config is not None and buffer_config.enabled
            else None
        )
        self.strict = strict
        self.publish_task_channel = publish_task_channel
        self.publish_queue_channel = publish_queue_channel
        self.publish_global_lifecycle = publish_global_lifecycle

    @property
    def sink(self) -> "QueueEventSink":
        """Configured event sink."""
        return self._sink

    def set_event_log(self, event_log: "QueueEventLog", *, strict: "bool" = False) -> "None":
        """Attach backend-owned durable event history to this publisher."""
        self._event_log = event_log
        self._event_log_strict = strict

    async def publish(
        self, event: "QueueEvent", *, channels: "Sequence[str] | None" = None, immediate: "bool" = False
    ) -> "None":
        """Publish an event to canonical and explicitly supplied channels.

        Returns:
            None.
        """
        resolved_channels = self.resolve_channels(event, channels=channels)
        await self._record_event(event)
        if self._buffer is not None and not immediate and event.type not in _TERMINAL_EVENT_TYPES:
            try:
                await self._buffer.add(event, resolved_channels)
            except Exception:
                if self.strict:
                    raise
                logger.warning(
                    "Queue event buffer publish failed",
                    exc_info=True,
                    extra={"queue_event_type": event.type, "queue_event_id": event.id},
                )
            return
        if self._buffer is not None:
            try:
                await self._buffer.flush(key=event_buffer_key(event))
            except Exception:
                if self.strict:
                    raise
                logger.warning(
                    "Queue event buffer flush failed",
                    exc_info=True,
                    extra={"queue_event_type": event.type, "queue_event_id": event.id},
                )
        await self._deliver_live(event, resolved_channels)

    async def flush_buffer(self) -> "None":
        """Flush all buffered live events.

        Returns:
            None.
        """
        if self._buffer is not None:
            await self._buffer.flush()

    def start_buffer(self) -> "None":
        """Start the live event buffer flush loop.

        Returns:
            None.
        """
        if self._buffer is not None:
            self._buffer.start()

    async def stop_buffer(self) -> "None":
        """Stop and drain the live event buffer.

        Returns:
            None.
        """
        if self._buffer is not None:
            await self._buffer.stop()

    async def _deliver_live(self, event: "QueueEvent", channels: "Sequence[str]") -> "None":
        try:
            await self._sink.publish(event, channels=channels)
        except Exception:
            if self.strict:
                raise
            logger.warning(
                "Queue event publish failed",
                exc_info=True,
                extra={"queue_event_type": event.type, "queue_event_id": event.id},
            )

    async def _record_event(self, event: "QueueEvent") -> "None":
        if self._event_log is None:
            return
        try:
            await self._event_log.publish_event(event)
        except Exception:
            if self._event_log_strict:
                raise
            logger.warning(
                "Queue event history publish failed",
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


def _ignore_buffer_drop(_scope: "str") -> "None":
    return None

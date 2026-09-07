"""Queue event publisher."""

import logging
import time
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

from litestar_queues.events.buffer import LiveEventBuffer, event_buffer_key
from litestar_queues.events.channels import QueueChannels
from litestar_queues.events.sinks import NoopQueueEventSink, QueueEventSink, default_publish_many
from litestar_queues.exceptions import QueueConfigurationError, QueueEventBufferFull
from litestar_queues.namespace import QueueNamespace

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from litestar_queues.events.models import QueueEvent
    from litestar_queues.observability import QueueObservabilityRuntimeProtocol

__all__ = ("EventBufferConfig", "QueueEventPublisher")


class _QueueEventHistoryWriter(Protocol):
    async def publish_event_after_commit(
        self, event: "QueueEvent", *, release: "Callable[[], Awaitable[None]]", barrier: "bool" = False
    ) -> "None":
        """Release an accepted event only after its history commits."""

    async def aclose(self) -> "None":
        """Drain and close owned history resources."""


@runtime_checkable
class _QueueEventBatchSink(QueueEventSink, Protocol):
    async def publish_many(self, batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> "None":
        """Publish a batch of events to their requested channels."""


_LIFECYCLE_EVENT_TYPES = frozenset({
    "task.started",
    "task.completed",
    "task.failed",
    "task.cancelled",
    "task.interrupted",
    "task.claim_lost",
    "task.stale_failed",
})
_TERMINAL_EVENT_TYPES = frozenset({
    "task.completed",
    "task.failed",
    "task.cancelled",
    "task.interrupted",
    "task.claim_lost",
    "task.stale_failed",
})


@dataclass(slots=True)
class EventBufferConfig:
    """Producer-side micro-batch buffer for live event delivery."""

    batch_size: "int" = 20
    """Maximum live events delivered in one batch."""

    flush_interval: "float" = 0.5
    """Maximum delay before flushing a partial live-event batch in seconds."""

    max_pending: "int" = 2000
    """Maximum live events waiting in the producer-side buffer."""

    overflow: 'Literal["drop_oldest", "drop_newest", "block", "error"]' = "drop_oldest"
    """Action taken when the pending live-event limit is reached."""

    def __post_init__(self) -> "None":
        """Validate buffering bounds."""
        if self.batch_size <= 0:
            msg = "EventBufferConfig.batch_size must be greater than 0."
            raise QueueConfigurationError(msg)
        if self.flush_interval <= 0:
            msg = "EventBufferConfig.flush_interval must be greater than 0."
            raise QueueConfigurationError(msg)
        if self.max_pending <= 0:
            msg = "EventBufferConfig.max_pending must be greater than 0."
            raise QueueConfigurationError(msg)


class QueueEventPublisher:
    """Publish queue events through a configured sink."""

    __slots__ = (
        "_buffer",
        "_event_log",
        "_live_failure_signature",
        "_logger",
        "_namespace",
        "_observability_runtime",
        "_sink",
        "_transport",
        "publish_global_lifecycle",
        "publish_queue_channel",
        "publish_task_channel",
        "strict",
    )

    def __init__(
        self,
        sink: "QueueEventSink | None" = None,
        *,
        event_log: "_QueueEventHistoryWriter | None" = None,
        buffer_config: "EventBufferConfig | None" = None,
        strict: "bool" = False,
        publish_task_channel: "bool" = True,
        publish_queue_channel: "bool" = True,
        publish_global_lifecycle: "bool" = False,
        namespace: "QueueNamespace | str | None" = None,
        observability_runtime: "QueueObservabilityRuntimeProtocol | None" = None,
        transport: "str | None" = None,
    ) -> "None":
        self._namespace = (
            namespace if isinstance(namespace, QueueNamespace) else QueueNamespace(namespace or "litestar_queues")
        )
        self._logger = logging.getLogger(self._namespace.logger("events", "publisher"))
        self._sink = sink or NoopQueueEventSink()
        self._observability_runtime = observability_runtime
        self._transport = transport or _event_transport(self._sink)
        self._validate_event_log(event_log)
        self._event_log = event_log
        self._buffer = (
            LiveEventBuffer(
                buffer_config,
                sink_publish=self._deliver_live_many,
                record_drop=self._record_buffer_drop,
                runtime_logger=self._logger,
            )
            if buffer_config is not None
            else None
        )
        self.strict = strict
        self.publish_task_channel = publish_task_channel
        self.publish_queue_channel = publish_queue_channel
        self.publish_global_lifecycle = publish_global_lifecycle
        self._live_failure_signature: "tuple[str, str] | None" = None

    @property
    def sink(self) -> "QueueEventSink":
        """Configured event sink."""
        return self._sink

    def set_event_log(self, event_log: "_QueueEventHistoryWriter") -> "None":
        """Attach backend-owned durable event history to this publisher."""
        self._validate_event_log(event_log)
        self._event_log = event_log

    def set_observability_runtime(self, runtime: "QueueObservabilityRuntimeProtocol") -> "None":
        """Attach the service-owned runtime used for live delivery metrics."""
        self._observability_runtime = runtime

    async def publish(
        self, event: "QueueEvent", *, channels: "Sequence[str] | None" = None, immediate: "bool" = False
    ) -> "None":
        """Publish an event to canonical and explicitly supplied channels."""
        snapshot = deepcopy(event)
        resolved_channels = self.resolve_channels(snapshot, channels=channels)
        barrier = immediate or snapshot.type in _TERMINAL_EVENT_TYPES

        async def deliver() -> None:
            await self._deliver_live(snapshot, resolved_channels)

        async def release() -> None:
            if self._buffer is None:
                await deliver()
                return
            try:
                if barrier:
                    await self._buffer.publish_immediate(key=event_buffer_key(snapshot), release=deliver)
                else:
                    await self._buffer.add(snapshot, resolved_channels)
            except (QueueConfigurationError, QueueEventBufferFull):
                raise
            except Exception:
                if self.strict:
                    raise
                self._logger.warning(
                    "Queue event buffer publish failed",
                    exc_info=True,
                    extra={"queue_event_type": snapshot.type, "queue_event_id": snapshot.id},
                )

        retained_release = self._buffer.bind_release(release) if self._buffer is not None else release
        if self._event_log is None:
            await retained_release()
        else:
            await self._event_log.publish_event_after_commit(snapshot, release=retained_release, barrier=barrier)

    @staticmethod
    def _validate_event_log(event_log: "_QueueEventHistoryWriter | None") -> None:
        if event_log is not None and (
            not callable(getattr(event_log, "publish_event_after_commit", None))
            or not callable(getattr(event_log, "aclose", None))
        ):
            message = "Queue event history providers must implement publish_event_after_commit() and aclose()."
            raise QueueConfigurationError(message)

    async def flush_buffer(self) -> "None":
        """Flush all buffered live events."""
        if self._buffer is not None:
            await self._buffer.flush()

    def start_buffer(self) -> "None":
        """Start the live event buffer flush loop."""
        if self._buffer is not None:
            self._buffer.start()

    async def stop_buffer(self) -> "None":
        """Stop and drain the live event buffer."""
        if self._buffer is not None:
            await self._buffer.stop()

    async def _deliver_live(self, event: "QueueEvent", channels: "Sequence[str]") -> "None":
        try:
            await self._sink.publish(event, channels=channels)
        except Exception:
            if self.strict:
                raise
            self._logger.warning(
                "Queue event publish failed",
                exc_info=True,
                extra={"queue_event_type": event.type, "queue_event_id": event.id},
            )

    async def _deliver_live_many(self, batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> "None":
        started_at = time.perf_counter()
        outcome = "success"
        try:
            if isinstance(self._sink, _QueueEventBatchSink):
                await self._sink.publish_many(batch)
            else:
                await default_publish_many(self._sink, batch)
        except Exception as exc:
            outcome = "failed"
            if self.strict:
                raise
            self._log_batch_delivery_failure(exc, len(batch))
        else:
            self._live_failure_signature = None
        finally:
            self._record_live_batch(len(batch), time.perf_counter() - started_at, outcome=outcome)

    def _record_buffer_drop(self, _scope: "str") -> "None":
        runtime = self._observability_runtime
        if runtime is not None:
            runtime.record_counter(
                "litestar_queues.event.dropped",
                attributes={"queue.transport": self._transport, "queue.outcome": "overflow"},
            )

    def _record_live_batch(self, size: "int", seconds: "float", *, outcome: "str") -> "None":
        runtime = self._observability_runtime
        if runtime is None:
            return
        attributes = {"queue.transport": self._transport, "queue.outcome": outcome}
        runtime.record_histogram("litestar_queues.event.flush.size", size, unit="events", attributes=attributes)
        runtime.record_duration("litestar_queues.event.flush.duration", seconds, attributes=attributes)

    def _log_batch_delivery_failure(self, exc: "BaseException", count: "int") -> "None":
        # Warn-once dampener: the first failure logs at WARNING with a traceback; consecutive
        # identical failures drop to DEBUG without a traceback so a misordered shutdown sink
        # (e.g. a torn-down Channels backend during graceful drain) degrades quietly instead of
        # spamming a WARNING per batch. Reset happens on the next successful delivery.
        signature = (type(exc).__name__, str(exc))
        if signature == self._live_failure_signature:
            self._logger.debug("Queue event batch publish failed", extra={"queue_event_count": count})
            return
        self._live_failure_signature = signature
        self._logger.warning("Queue event batch publish failed", exc_info=exc, extra={"queue_event_count": count})

    def resolve_channels(self, event: "QueueEvent", *, channels: "Sequence[str] | None" = None) -> "tuple[str, ...]":
        """Return canonical publish channels for an event plus explicit extras."""
        resolved: "list[str]" = []
        if self.publish_task_channel and event.task_id is not None:
            resolved.append(QueueChannels.task(event.task_id, namespace=self._namespace))
        if event.scope == "queue" and event.scope_key is not None:
            resolved.append(QueueChannels.queue(event.scope_key, namespace=self._namespace))
        if self.publish_queue_channel and event.queue is not None:
            resolved.append(QueueChannels.queue(event.queue, namespace=self._namespace))
        if event.scope == "worker" and event.worker_id is not None:
            resolved.append(QueueChannels.worker(event.worker_id, namespace=self._namespace))
        if event.scope == "global":
            resolved.append(QueueChannels.global_channel(namespace=self._namespace))
        if event.scope == "custom" and event.scope_key is not None:
            resolved.append(QueueChannels.custom(event.scope_key, namespace=self._namespace))
        if self.publish_global_lifecycle and event.type in _LIFECYCLE_EVENT_TYPES:
            resolved.append(QueueChannels.global_channel(namespace=self._namespace))
        if channels:
            resolved.extend(channels)
        return _dedupe(resolved or [QueueChannels.global_channel(namespace=self._namespace)])


def _dedupe(channels: "Sequence[str]") -> "tuple[str, ...]":
    seen: "set[str]" = set()
    resolved: "list[str]" = []
    for channel in channels:
        if channel in seen:
            continue
        seen.add(channel)
        resolved.append(channel)
    return tuple(resolved)


def _event_transport(sink: "QueueEventSink") -> "str":
    return {
        "ChannelsQueueEventSink": "channels",
        "CompositeQueueEventSink": "composite",
        "InMemoryQueueEventSink": "memory",
        "NoopQueueEventSink": "none",
    }.get(type(sink).__name__, "custom")

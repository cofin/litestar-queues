from collections.abc import Sequence

import pytest

from litestar_queues import QueueConfig
from litestar_queues.events import (
    EventBufferConfig,
    EventDeliveryConfig,
    EventHistoryConfig,
    EventStreamConfig,
    InMemoryQueueEventSink,
    QueueEvent,
    QueueEventsConfig,
)
from litestar_queues.exceptions import QueueConfigurationError


class _RecordingSink:
    def __init__(self, name: str, calls: list[str], *, fail: bool = False) -> None:
        self.name = name
        self.calls = calls
        self.fail = fail

    async def publish(self, event: QueueEvent, *, channels: Sequence[str]) -> None:
        del event, channels
        self.calls.append(self.name)
        if self.fail:
            msg = self.name
            raise RuntimeError(msg)


class _BatchRecordingSink:
    def __init__(self, name: str, calls: list[str], *, fail: bool = False) -> None:
        self.name = name
        self.calls = calls
        self.fail = fail

    async def publish(self, event: QueueEvent, *, channels: Sequence[str]) -> None:
        del event, channels

    async def publish_many(self, batch: Sequence[tuple[QueueEvent, Sequence[str]]]) -> None:
        self.calls.extend(self.name for _ in batch)
        if self.fail:
            msg = self.name
            raise RuntimeError(msg)


def test_event_capabilities_are_presence_enabled_and_validate_empty_groups() -> None:
    assert QueueConfig(events=QueueEventsConfig(delivery=EventDeliveryConfig(sinks=(InMemoryQueueEventSink(),))))
    assert QueueConfig(events=QueueEventsConfig(stream=EventStreamConfig(transports={"sse"})))
    assert QueueConfig(events=QueueEventsConfig(history=EventHistoryConfig()))

    with pytest.raises(QueueConfigurationError, match="at least one"):
        QueueEventsConfig()
    with pytest.raises(QueueConfigurationError, match="without delivery or stream"):
        QueueEventsConfig(channels=object())  # type: ignore[arg-type]


def test_event_config_uses_positive_names_and_validates_values() -> None:
    assert EventBufferConfig().batch_size == 20
    assert EventStreamConfig().replay_limit == 0
    assert EventStreamConfig().transports == {"sse", "websocket"}
    assert EventStreamConfig().unauthenticated_access == "warn"
    assert EventHistoryConfig().batch_size == 20
    assert EventHistoryConfig().memory_capacity == 1000

    with pytest.raises(QueueConfigurationError):
        EventStreamConfig(transports=set())
    with pytest.raises(QueueConfigurationError):
        EventStreamConfig(path="queues/events")
    with pytest.raises(QueueConfigurationError):
        EventBufferConfig(batch_size=0)


@pytest.mark.anyio
async def test_delivery_sinks_are_additive_and_non_strict_failures_continue() -> None:
    calls: list[str] = []
    first = _RecordingSink("first", calls, fail=True)
    second = _RecordingSink("second", calls)
    config = QueueConfig(
        events=QueueEventsConfig(
            delivery=EventDeliveryConfig(buffer=EventBufferConfig(batch_size=1), sinks=(first, second))
        )
    )

    from litestar_queues.events import QueueEvent

    await config.get_event_publisher().publish(QueueEvent(type="custom", scope="global"), channels=("events",))

    assert calls == ["first", "second"]


@pytest.mark.anyio
async def test_additive_batch_delivery_finishes_each_sink_before_the_next() -> None:
    from litestar_queues.events import CompositeQueueEventSink, QueueEvent

    calls: list[str] = []
    first = _BatchRecordingSink("channels", calls, fail=True)
    second = _BatchRecordingSink("custom", calls)
    sink = CompositeQueueEventSink((first, second))
    batch = [
        (QueueEvent(type="task.started", scope="task"), ("task:one",)),
        (QueueEvent(type="task.completed", scope="task"), ("task:one",)),
    ]

    await sink.publish_many(batch)

    assert calls == ["channels", "channels", "custom", "custom"]

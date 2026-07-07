from typing import TYPE_CHECKING

import pytest

from litestar_queues import EventConfig, QueueConfig, QueueService
from litestar_queues.events import (
    EventBufferConfig,
    InMemoryQueueEventSink,
    QueueChannels,
    QueueEvent,
    QueueEventPublisher,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

pytestmark = pytest.mark.anyio


async def test_task_handle_log_fans_out_to_task_channel() -> None:
    from litestar_queues.events import QueueEventProducer

    sink = InMemoryQueueEventSink()
    producer = QueueEventProducer(QueueEventPublisher(sink))

    await producer.task("t1").log("hi")

    [event] = sink.events_for(QueueChannels.task("t1"))
    assert event.type == "task.log"
    assert event.scope == "task"
    assert event.task_id == "t1"
    assert event.message == "hi"


async def test_channel_handle_publish_fans_out_to_custom_channel() -> None:
    from litestar_queues.events import QueueEventProducer

    sink = InMemoryQueueEventSink()
    producer = QueueEventProducer(QueueEventPublisher(sink))

    await producer.channel("imports:acme").publish("import.note", payload={"a": 1})

    [event] = sink.events_for(QueueChannels.custom("imports:acme"))
    assert event.type == "import.note"
    assert event.scope == "custom"
    assert event.scope_key == "imports:acme"
    assert event.payload == {"a": 1}


async def test_queue_and_worker_handles_target_their_channels() -> None:
    from litestar_queues.events import QueueEventProducer

    sink = InMemoryQueueEventSink()
    producer = QueueEventProducer(QueueEventPublisher(sink))

    await producer.queue("q").publish("queue.note")
    await producer.worker("w").publish("worker.note")

    [queue_event] = sink.events_for(QueueChannels.queue("q"))
    [worker_event] = sink.events_for(QueueChannels.worker("w"))
    assert queue_event.scope == "queue"
    assert queue_event.scope_key == "q"
    assert worker_event.scope == "worker"
    assert worker_event.worker_id == "w"


async def test_task_handle_progress_percent_derived() -> None:
    from litestar_queues.events import QueueEventProducer

    sink = InMemoryQueueEventSink()
    producer = QueueEventProducer(QueueEventPublisher(sink))

    await producer.task("t1").progress(current=3, total=10)

    [event] = sink.events_for(QueueChannels.task("t1"))
    assert event.type == "task.progress"
    assert event.progress_current == 3
    assert event.progress_total == 10
    assert event.progress_percent == 30.0


async def test_task_handle_publish_targets_task_channel() -> None:
    from litestar_queues.events import QueueEventProducer

    sink = InMemoryQueueEventSink()
    producer = QueueEventProducer(QueueEventPublisher(sink))

    await producer.task("t1").publish("task.operator_note", message="operator note")

    [event] = sink.events_for(QueueChannels.task("t1"))
    assert event.type == "task.operator_note"
    assert event.scope == "task"
    assert event.task_id == "t1"
    assert event.message == "operator note"


async def test_producer_handle_immediate_flushes_prior() -> None:
    from litestar_queues.events import QueueEventProducer

    sink = InMemoryQueueEventSink()
    producer = QueueEventProducer(QueueEventPublisher(sink, buffer_config=EventBufferConfig(buffer_size=10)))

    await producer.task("t1").log("buffered")

    assert sink.events == []

    await producer.task("t1").log("urgent", immediate=True)

    assert [event.message for event in sink.events] == ["buffered", "urgent"]


def test_scope_handles_expose_only_publish() -> None:
    from litestar_queues.events import QueueEventProducer

    producer = QueueEventProducer(QueueEventPublisher(InMemoryQueueEventSink()))
    channel_handle = producer.channel("imports:acme")
    task_handle = producer.task("t1")

    assert hasattr(channel_handle, "publish")
    assert not hasattr(channel_handle, "log")
    assert not hasattr(channel_handle, "progress")
    assert hasattr(task_handle, "publish")
    assert hasattr(task_handle, "log")
    assert hasattr(task_handle, "progress")
    assert hasattr(task_handle, "event")


async def test_producer_does_not_open_or_close_sink() -> None:
    from litestar_queues.events import QueueEventProducer

    sink = _LifecycleCountingSink()
    producer = QueueEventProducer(QueueEventPublisher(sink))

    await producer.task("t1").log("hi")

    assert sink.open_count == 0
    assert sink.close_count == 0
    assert len(sink.published) == 1


async def test_service_get_event_producer_wraps_same_publisher() -> None:
    sink = InMemoryQueueEventSink()
    service = QueueService(QueueConfig(event=EventConfig(sink=sink, buffer=EventBufferConfig(enabled=False))))

    await service.get_event_producer().task("t1").log("from service")

    [event] = sink.events_for(QueueChannels.task("t1"))
    assert event.type == "task.log"
    assert event.message == "from service"


class _LifecycleCountingSink:
    def __init__(self) -> None:
        self.open_count = 0
        self.close_count = 0
        self.published: "list[tuple[QueueEvent, tuple[str, ...]]]" = []

    async def open(self) -> None:
        self.open_count += 1

    async def close(self) -> None:
        self.close_count += 1

    async def publish(self, event: QueueEvent, *, channels: "Sequence[str]") -> None:
        self.published.append((event, tuple(channels)))

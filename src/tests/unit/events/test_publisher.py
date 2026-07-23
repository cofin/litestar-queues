import logging
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest

from litestar_queues.events import (
    EventBufferConfig,
    InMemoryQueueEventSink,
    NoopQueueEventSink,
    QueueChannels,
    QueueEvent,
    QueueEventPublisher,
    QueueEventSink,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

pytestmark = pytest.mark.anyio


class FailingSink:
    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> "None":
        msg = f"publish failed for {event.type}"
        raise RuntimeError(msg)


class FailingEventLog:
    async def publish_event(self, event: "QueueEvent") -> "None":
        msg = f"history failed for {event.type}"
        raise RuntimeError(msg)


class TogglingBatchSink:
    """Batch sink that fails while ``fail`` is set so tests can drive the warn-once dampener."""

    def __init__(self) -> "None":
        self.fail = True

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> "None":
        if self.fail:
            msg = "sink offline"
            raise RuntimeError(msg)


def test_queue_channels_normalize_parts_deterministically() -> "None":
    assert QueueChannels.task("Task 1", topic="progress") == "litestar_queues:task:task_1:progress"
    assert QueueChannels.queue("critical/default") == "litestar_queues:queue:critical_default:events"
    assert QueueChannels.worker("worker@host") == "litestar_queues:worker:worker_host:events"
    assert QueueChannels.global_channel() == "litestar_queues:global:events"
    assert QueueChannels.custom("tenant:acme") == "litestar_queues:custom:tenant:acme:events"


async def test_queue_event_publisher_targets_configured_channels() -> "None":
    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink, publish_queue_channel=True, publish_global_lifecycle=True)
    event = QueueEvent(
        type="task.started",
        scope="task",
        task_id="task-1",
        task_name="tasks.export",
        queue="default",
        worker_id="worker-1",
        execution_backend="local",
        attempt=2,
    )

    await publisher.publish(event, channels=[QueueChannels.custom("external")])

    assert [published.type for published in sink.events] == ["task.started"]
    assert sink.events_for(QueueChannels.task("task-1")) == [event]
    assert sink.events_for(QueueChannels.queue("default")) == [event]
    assert sink.events_for(QueueChannels.global_channel()) == [event]
    assert sink.events_for(QueueChannels.custom("external")) == [event]


async def test_queue_event_publisher_failure_semantics() -> "None":
    event = QueueEvent(type="task.progress", scope="task", task_id="task-1")

    await QueueEventPublisher(NoopQueueEventSink()).publish(event)
    await QueueEventPublisher(FailingSink()).publish(event)

    with pytest.raises(RuntimeError, match="publish failed"):
        await QueueEventPublisher(FailingSink(), strict=True).publish(event)


async def test_absent_buffer_uses_immediate_path() -> "None":
    event_types = ["task.progress", "task.log"]
    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink, buffer_config=None)

    for event_type in event_types:
        await publisher.publish(QueueEvent(type=event_type, scope="task", task_id="task-a"))

    assert [event.type for event in sink.events] == event_types


async def test_buffered_progress_not_delivered_until_flush() -> "None":
    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink, buffer_config=EventBufferConfig(batch_size=10))

    await publisher.publish(QueueEvent(type="task.progress", scope="task", task_id="task-a"))
    await publisher.publish(QueueEvent(type="task.log", scope="task", task_id="task-a"))

    assert sink.events == []

    await publisher.flush_buffer()

    assert [event.type for event in sink.events] == ["task.progress", "task.log"]


async def test_immediate_flushes_task_events_first_in_order() -> "None":
    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink, buffer_config=EventBufferConfig(batch_size=10))

    await publisher.publish(QueueEvent(type="task.progress.1", scope="task", task_id="task-a"))
    await publisher.publish(QueueEvent(type="task.progress.2", scope="task", task_id="task-a"))
    await publisher.publish(QueueEvent(type="task.log", scope="task", task_id="task-a"), immediate=True)

    assert [event.type for event in sink.events] == ["task.progress.1", "task.progress.2", "task.log"]


async def test_terminal_event_flushes_buffered_first() -> "None":
    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink, buffer_config=EventBufferConfig(batch_size=10))

    await publisher.publish(QueueEvent(type="task.progress", scope="task", task_id="task-a"))
    await publisher.publish(QueueEvent(type="task.completed", scope="task", task_id="task-a"))

    assert [event.type for event in sink.events] == ["task.progress", "task.completed"]


async def test_terminal_direct_publish_also_flushes() -> "None":
    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink, buffer_config=EventBufferConfig(batch_size=10))

    await publisher.publish(QueueEvent(type="task.progress", scope="task", task_id="task-a"))
    await publisher.publish(QueueEvent(type="task.stale_failed", scope="task", task_id="task-a"))

    assert [event.type for event in sink.events] == ["task.progress", "task.stale_failed"]


async def test_strict_event_log_failure_prevents_buffering() -> "None":
    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(
        sink, event_log=FailingEventLog(), event_log_strict=True, buffer_config=EventBufferConfig(batch_size=10)
    )

    with pytest.raises(RuntimeError, match="history failed"):
        await publisher.publish(QueueEvent(type="task.progress", scope="task", task_id="task-a"))

    await publisher.flush_buffer()

    assert sink.events == []


async def test_batch_delivery_failure_warn_once_dampener(caplog: "pytest.LogCaptureFixture") -> "None":
    sink = TogglingBatchSink()
    publisher = QueueEventPublisher(sink)
    event = QueueEvent(type="task.progress", scope="task", task_id="task-a")
    batch = [(event, ("litestar_queues:task:task_a:progress",))]

    with caplog.at_level(logging.DEBUG, logger="litestar_queues.events.publisher"):
        await publisher._deliver_live_many(batch)
        await publisher._deliver_live_many(batch)
        await publisher._deliver_live_many(batch)

    records = [record for record in caplog.records if record.message == "Queue event batch publish failed"]
    assert [record.levelno for record in records] == [logging.WARNING, logging.DEBUG, logging.DEBUG]
    assert records[0].exc_info is not None
    assert records[1].exc_info is None
    assert records[2].exc_info is None

    caplog.clear()
    sink.fail = False
    with caplog.at_level(logging.DEBUG, logger="litestar_queues.events.publisher"):
        await publisher._deliver_live_many(batch)  # success resets the dampener
    assert publisher._live_failure_signature is None

    sink.fail = True
    with caplog.at_level(logging.DEBUG, logger="litestar_queues.events.publisher"):
        await publisher._deliver_live_many(batch)  # first failure after reset warns again
    reopened = [record for record in caplog.records if record.message == "Queue event batch publish failed"]
    assert reopened[-1].levelno == logging.WARNING
    assert reopened[-1].exc_info is not None


async def test_event_sink_fixture_is_parametrized_over_both_sinks(event_sink: "QueueEventSink") -> "None":
    """The unit-tier `event_sink` fixture exposes both InMemory and Noop sinks."""
    assert isinstance(event_sink, (InMemoryQueueEventSink, NoopQueueEventSink))


def test_event_imports_do_not_load_optional_driver_modules() -> "None":
    code = """
import sys
import litestar_queues.events
optional_roots = {
    "advanced_alchemy",
    "asyncmy",
    "asyncpg",
    "google.cloud.run",
    "mysql",
    "oracledb",
    "psycopg",
    "redis",
    "sqlspec",
    "valkey",
}
loaded = sorted(optional_roots.intersection(sys.modules))
print(",".join(loaded))
raise SystemExit(1 if loaded else 0)
"""

    result = subprocess.run([sys.executable, "-c", code], check=False, capture_output=True, text=True)

    assert result.returncode == 0, result.stdout

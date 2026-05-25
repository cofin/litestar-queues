import subprocess
import sys
from collections.abc import Sequence

import pytest

from litestar_queues.events import (
    InMemoryQueueEventSink,
    NoopQueueEventSink,
    QueueChannels,
    QueueEvent,
    QueueEventPublisher,
    QueueEventSink,
)

pytestmark = pytest.mark.anyio


class FailingSink:
    async def publish(self, event: QueueEvent, *, channels: Sequence[str]) -> None:
        msg = f"publish failed for {event.type}"
        raise RuntimeError(msg)


def test_queue_channels_normalize_parts_deterministically() -> None:
    assert QueueChannels.task("Task 1", topic="progress") == "litestar_queues:task:task_1:progress"
    assert QueueChannels.queue("critical/default") == "litestar_queues:queue:critical_default:events"
    assert QueueChannels.worker("worker@host") == "litestar_queues:worker:worker_host:events"
    assert QueueChannels.global_channel() == "litestar_queues:global:events"
    assert QueueChannels.custom("tenant:acme") == "litestar_queues:custom:tenant:acme:events"


async def test_queue_event_publisher_targets_configured_channels() -> None:
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


async def test_queue_event_publisher_failure_semantics() -> None:
    event = QueueEvent(type="task.progress", scope="task", task_id="task-1")

    await QueueEventPublisher(NoopQueueEventSink()).publish(event)
    await QueueEventPublisher(FailingSink()).publish(event)

    with pytest.raises(RuntimeError, match="publish failed"):
        await QueueEventPublisher(FailingSink(), strict=True).publish(event)


async def test_event_sink_fixture_is_parametrized_over_both_sinks(event_sink: QueueEventSink) -> None:
    """The unit-tier `event_sink` fixture exposes both InMemory and Noop sinks."""
    assert isinstance(event_sink, (InMemoryQueueEventSink, NoopQueueEventSink))


def test_event_imports_do_not_load_optional_driver_modules() -> None:
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
    "pymysql",
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

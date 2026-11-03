"""Using litestar_queues.events standalone with bind_task_context, bind_beat_sink, and create_event_producer."""

from litestar_queues import QueueConfig, WorkerConfig
from litestar_queues.events import (
    EventDeliveryConfig,
    InMemoryQueueEventSink,
    QueueEventActor,
    QueueEventsConfig,
    TaskExecutionContext,
    beat,
    bind_beat_sink,
    bind_task_context,
    create_event_producer,
    publish_task_progress,
)


class RecordingBeatSink:
    """In-memory TaskBeatSink implementation capturing last-value-wins beat details."""

    __slots__ = ("beats",)

    def __init__(self) -> None:
        self.beats: dict[str, str | None] = {}

    def record_beat(self, task_id: str, detail: str | None) -> None:
        """Store the latest beat detail for task_id."""
        self.beats[task_id] = detail


async def main() -> None:
    """Verify standalone TaskExecutionContext binding and create_event_producer publishing."""
    sink = InMemoryQueueEventSink()
    config = QueueConfig(
        queue_backend="memory",
        worker=WorkerConfig(placement="external"),
        events=QueueEventsConfig(delivery=EventDeliveryConfig(sinks=(sink,), buffer=None)),
    )
    beat_sink = RecordingBeatSink()
    publisher = config.get_event_publisher()
    context = TaskExecutionContext(
        task_id="standalone-1",
        task_name="cli.migrate",
        queue="default",
        worker_id="cli-worker",
        execution_backend="local",
        execution_profile=None,
        attempt=1,
        event_publisher=publisher,
        actor=QueueEventActor(type="user", id="cody"),
    )

    with bind_beat_sink(beat_sink), bind_task_context(context):
        beat("reading-schema")
        await publish_task_progress(current=1, total=1, message="Migration finished", strict=True)

    assert beat_sink.beats["standalone-1"] == "reading-schema"
    assert len(sink.events) == 1
    assert sink.events[0].actor == QueueEventActor(type="user", id="cody")

    async with create_event_producer(config) as producer:
        await producer.channel("deployments").publish("release.completed", message="v0.14.0 deployed")

    assert any(event.type == "release.completed" for event in sink.events)

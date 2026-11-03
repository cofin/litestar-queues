"""Unit testing task events deterministically with InMemoryQueueEventSink and immediate execution."""

from litestar_queues import QueueConfig, QueueService, TaskExecutionContext, WorkerConfig, task
from litestar_queues.events import EventDeliveryConfig, InMemoryQueueEventSink, QueueEventsConfig


@task("exports.generate_csv", queue="exports")
async def generate_csv(report_name: str, _task_context: TaskExecutionContext) -> str:
    """Emit progress and custom events during CSV generation."""
    await _task_context.progress(current=1, total=2, message="Querying rows")
    await _task_context.event("export.rows_fetched", payload={"count": 250})
    await _task_context.progress(current=2, total=2, message="Writing CSV")
    return f"{report_name}.csv"


async def main() -> None:
    """Capture and assert task events synchronously with buffer=None and immediate execution."""
    sink = InMemoryQueueEventSink()
    config = QueueConfig(
        queue_backend="memory",
        execution_backend="immediate",
        worker=WorkerConfig(placement="external"),
        events=QueueEventsConfig(delivery=EventDeliveryConfig(sinks=(sink,), buffer=None, strict=True)),
    )
    async with QueueService(config=config) as service:
        handle = await service.enqueue(generate_csv, "q4-revenue")
        assert handle.status == "completed"

    event_types = [event.type for event in sink.events]
    assert event_types == ["task.started", "task.progress", "export.rows_fetched", "task.progress", "task.completed"]

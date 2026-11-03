"""Publishing progress, logs, custom events, and heartbeat details from a task."""

from litestar_queues import QueueConfig, QueueService, TaskExecutionContext, WorkerConfig, task
from litestar_queues.events import (
    EventDeliveryConfig,
    InMemoryQueueEventSink,
    QueueEventsConfig,
    publish_task_event,
    publish_task_log,
    publish_task_progress,
)

EXPECTED_CUSTOM_EVENTS = 3


async def emit_helper_signals() -> None:
    """Publish progress and logs via module-level helpers that safely no-op when unbound."""
    await publish_task_progress(current=3, total=6, message="3/6 pages", payload={"page": 3})
    await publish_task_log("Checkpoint reached", level="info", payload={"step": "checkpoint"})
    await publish_task_event("crawl.page_discovered", payload={"url": "https://litestar.dev"}, immediate=False)


@task("crawler.index_site", queue="crawler")
async def index_site(domain: str, _task_context: TaskExecutionContext) -> dict[str, str]:
    """Execute a task that receives TaskExecutionContext via _task_context injection."""
    _task_context.beat(f"starting:{domain}")
    await _task_context.progress(current=1, total=6, message="1/6 pages", payload={"page": 1})
    await emit_helper_signals()
    await _task_context.progress(current=6, total=6, message="6/6 pages", payload={"page": 6})
    return {"domain": domain, "status": "indexed"}


async def main() -> None:
    """Verify _task_context injection and module-level helper no-op behavior outside a task."""
    await emit_helper_signals()

    sink = InMemoryQueueEventSink()
    config = QueueConfig(
        queue_backend="memory",
        execution_backend="immediate",
        worker=WorkerConfig(placement="external"),
        events=QueueEventsConfig(delivery=EventDeliveryConfig(sinks=(sink,), buffer=None)),
    )
    async with QueueService(config=config) as service:
        handle = await service.enqueue(index_site, "litestar.dev")
        await handle.refresh()
        assert handle.status == "completed"
        progress_events = [e for e in sink.events if e.type == "task.progress"]
        assert len(progress_events) == EXPECTED_CUSTOM_EVENTS
        discovered = [e for e in sink.events if e.type == "crawl.page_discovered"]
        assert len(discovered) == 1

"""Persisting and querying durable event history with stage summaries and actor filters."""

from litestar_queues import QueueConfig, QueueService, TaskExecutionContext, WorkerConfig, task
from litestar_queues.events import (
    EventHistoryConfig,
    QueueEventActor,
    QueueEventEntityRef,
    QueueEventQuery,
    QueueEventsConfig,
)

EXPECTED_STAGE_COUNT = 2


@task("etl.import_catalog", queue="etl", actor=QueueEventActor(type="service", id="catalog-sync"))
async def import_catalog(catalog_id: str, _task_context: TaskExecutionContext) -> str:
    """Emit staged progress events with entity and actor attribution."""
    _task_context.entity = QueueEventEntityRef(type="catalog", id=catalog_id)
    _task_context.scope_key = "tenant:acme"
    await _task_context.progress(
        current=1, total=2, message="Extracted feed", payload={"stage": "extract", "duration_ms": 42.5}
    )
    await _task_context.progress(
        current=2, total=2, message="Loaded catalog rows", payload={"stage": "load", "duration_ms": 85.0}
    )
    return f"imported:{catalog_id}"


async def main() -> None:
    """Verify durable event history queries and per-stage aggregation."""
    config = QueueConfig(
        queue_backend="memory",
        execution_backend="immediate",
        worker=WorkerConfig(placement="external"),
        events=QueueEventsConfig(history=EventHistoryConfig(batch_size=10, flush_interval=0.1)),
    )
    async with QueueService(config=config) as service:
        handle = await service.enqueue(import_catalog, "cat-900")
        event_log = service.get_event_log()
        assert event_log is not None
        await event_log.flush_events()

        page = await event_log.query_events(
            QueueEventQuery(task_id=str(handle.id), order="asc"), extra={"actor_id": "catalog-sync"}
        )
        assert page.total >= EXPECTED_STAGE_COUNT

        summaries = await event_log.summarize_stages(QueueEventQuery(task_id=str(handle.id)))
        stages = {summary.stage for summary in summaries if summary.stage is not None}
        assert stages == {"extract", "load"}

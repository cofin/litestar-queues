"""Configuring EventHistoryExtraColumn for indexed tenant-scoped event history queries."""

from pathlib import Path
from tempfile import TemporaryDirectory

from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues import QueueConfig, QueueService, TaskExecutionContext, WorkerConfig, task
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend, SQLSpecWorkerWakeupConfig
from litestar_queues.events import EventHistoryConfig, EventHistoryExtraColumn, QueueEventQuery, QueueEventsConfig


@task("billing.sync_tenant_ledger", queue="billing")
async def sync_tenant_ledger(tenant_id: str, _task_context: TaskExecutionContext) -> str:
    """Publish an event carrying tenant_id in the payload for extra-column extraction."""
    await _task_context.event(
        "ledger.synced", message="Synchronized tenant ledger", payload={"tenant_id": tenant_id, "entries": 42}
    )
    return f"synced:{tenant_id}"


async def main() -> None:
    """Verify EventHistoryExtraColumn extraction and extra filter querying on SQLSpec."""
    extra_columns = (EventHistoryExtraColumn(name="tenant_id", source="tenant_id", indexed=True),)
    with TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "history_extra.sqlite3"
        backend_config = SQLSpecBackendConfig(
            sqlspec_config=AiosqliteConfig(connection_config={"database": str(db_path)}),
            event_history_extra_columns=extra_columns,
            worker_wakeups=SQLSpecWorkerWakeupConfig(transport="polling"),
        )
        config = QueueConfig(
            queue_backend=backend_config,
            execution_backend="immediate",
            worker=WorkerConfig(placement="external"),
            events=QueueEventsConfig(
                history=EventHistoryConfig(extra_columns=extra_columns, batch_size=5, flush_interval=0.1)
            ),
        )
        async with QueueService(config=config) as service:
            backend = service.get_queue_backend()
            assert isinstance(backend, SQLSpecQueueBackend)
            await backend.create_schema()

            await service.enqueue(sync_tenant_ledger, "tenant-alpha")
            await service.enqueue(sync_tenant_ledger, "tenant-beta")

            event_log = service.get_event_log()
            assert event_log is not None
            await event_log.flush_events()

            alpha_page = await event_log.query_events(
                QueueEventQuery(event_type="ledger.synced"), extra={"tenant_id": "tenant-alpha"}
            )
            assert alpha_page.total == 1
            assert alpha_page.items[0].extra["tenant_id"] == "tenant-alpha"

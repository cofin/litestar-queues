"""Configuring SQLSpecBackendConfig with AiosqliteConfig, schema creation, and terminal_ttl."""

from pathlib import Path
from tempfile import TemporaryDirectory

from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend, SQLSpecWorkerWakeupConfig


@task("reports.archive_monthly", queue="reports")
async def archive_monthly(month: str) -> str:
    """Archive a monthly report record in SQLSpec storage."""
    return f"archived:{month}"


async def main() -> None:
    """Create the SQLSpec schema in a temporary SQLite database and execute a task."""
    with TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "queues.sqlite3"
        sqlspec_config = AiosqliteConfig(connection_config={"database": str(db_path)})
        backend_config = SQLSpecBackendConfig(
            sqlspec_config=sqlspec_config,
            manage_schema=True,
            terminal_ttl=86_400.0,
            worker_wakeups=SQLSpecWorkerWakeupConfig(transport="polling"),
        )
        config = QueueConfig(
            queue_backend=backend_config, execution_backend="immediate", worker=WorkerConfig(placement="external")
        )
        async with QueueService(config=config) as service:
            backend = service.get_queue_backend()
            assert isinstance(backend, SQLSpecQueueBackend)
            await backend.create_schema()

            handle = await service.enqueue(archive_monthly, "2026-11")
            await handle.refresh()
            assert handle.status == "completed"
            assert handle.result == "archived:2026-11"

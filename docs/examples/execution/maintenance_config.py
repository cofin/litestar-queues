"""Configuring and running bounded QueueMaintenanceService passes."""

from pathlib import Path
from tempfile import TemporaryDirectory

from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues import QueueConfig, QueueMaintenanceConfig, QueueMaintenanceService, QueueService, WorkerConfig
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend, SQLSpecWorkerWakeupConfig
from litestar_queues.events import EventHistoryConfig, QueueEventQuery, QueueEventRetentionRule, QueueEventsConfig


def build_maintenance_config() -> QueueMaintenanceConfig:
    """Construct a bounded QueueMaintenanceConfig with explicit retention thresholds and OIDC route protection."""
    return QueueMaintenanceConfig(
        time_budget=120.0,
        coordination_timeout=180.0,
        expire_limit=500,
        external_limit=100,
        stale_after=90.0,
        stale_limit=100,
        schedules_limit=50,
        terminal_retention=86_400.0,
        terminal_limit=1000,
        event_retention_rules=(QueueEventRetentionRule(max_age=604_800.0, match=QueueEventQuery(scope="task")),),
        event_limit=1000,
        mount_route=True,
        verify_oidc=True,
        oidc_audience="https://worker-service-uc.a.run.app",
        oidc_service_account_email="scheduler-invoker@my-gcp-project.iam.gserviceaccount.com",
    )


async def main() -> None:
    """Execute a finite 6-phase maintenance pass against a persistent SQLite SQLSpec backend."""
    maintenance_config = build_maintenance_config()
    with TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "maintenance.sqlite3"
        backend_config = SQLSpecBackendConfig(
            sqlspec_config=AiosqliteConfig(connection_config={"database": str(db_path)}),
            worker_wakeups=SQLSpecWorkerWakeupConfig(transport="polling"),
        )
        config = QueueConfig(
            queue_backend=backend_config,
            execution_backend="local",
            worker=WorkerConfig(placement="external"),
            maintenance=maintenance_config,
            events=QueueEventsConfig(history=EventHistoryConfig()),
        )
        async with QueueService(config=config) as service:
            backend = service.get_queue_backend()
            assert isinstance(backend, SQLSpecQueueBackend)
            await backend.create_schema()

            runner = QueueMaintenanceService(service, maintenance_config)
            summary = await runner.run()
            assert summary.outcome == "completed"
            assert summary.acquired is True
            assert tuple(p.phase for p in summary.phases) == (
                "expire",
                "external",
                "stale",
                "schedules",
                "terminal",
                "events",
            )

"""Explicit SQLSpec schema setup helpers for integration tests."""

from inspect import isawaitable
from typing import TYPE_CHECKING

from litestar_queues import QueueConfig
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend
from litestar_queues.backends.sqlspec.extension import configure_queue_migration_extension
from litestar_queues.events import EventLogConfig

if TYPE_CHECKING:
    from litestar_queues.backends.sqlspec._typing import SQLSpecConfig


async def bootstrap_queue_schema(
    backend_config: "SQLSpecBackendConfig", *, event_log_enabled: "bool" = False
) -> "None":
    """Use the queue backend's explicit direct-DDL fallback for a test database."""
    queue_config = QueueConfig(
        queue_backend=backend_config, event_log=EventLogConfig(enabled=True) if event_log_enabled else None
    )
    backend = SQLSpecQueueBackend(config=queue_config, backend_config=backend_config)
    await backend.open()
    try:
        await backend.create_schema()
    finally:
        await backend.close()


async def run_queue_migrations(
    sqlspec_config: "SQLSpecConfig",
    *,
    queue_table_name: "str" = "litestar_queue_task",
    event_log_enabled: "bool" = False,
    event_log_table_name: "str | None" = None,
) -> "None":
    """Register and run the queue migration through SQLSpec itself."""
    configure_queue_migration_extension(
        sqlspec_config,
        queue_table_name=queue_table_name,
        event_log_enabled=event_log_enabled,
        event_log_table_name=event_log_table_name,
    )
    result = sqlspec_config.migrate_up(echo=False)
    if isawaitable(result):
        await result

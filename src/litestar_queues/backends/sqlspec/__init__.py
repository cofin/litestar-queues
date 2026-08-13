"""SQLSpec queue backend."""

from litestar_queues.backends.sqlspec.backend import SQLSpecQueueBackend
from litestar_queues.backends.sqlspec.config import SQLSpecBackendConfig, SQLSpecWorkerWakeupConfig
from litestar_queues.backends.sqlspec.extension import configure_queue_migration_extension
from litestar_queues.backends.sqlspec.schema import EventHistoryExtraColumn

__all__ = (
    "EventHistoryExtraColumn",
    "SQLSpecBackendConfig",
    "SQLSpecQueueBackend",
    "SQLSpecWorkerWakeupConfig",
    "configure_queue_migration_extension",
)

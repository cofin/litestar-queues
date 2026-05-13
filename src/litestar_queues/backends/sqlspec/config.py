"""SQLSpec backend configuration."""

from dataclasses import dataclass, field
from typing import Any

from sqlspec import SQLSpec
from sqlspec.extensions.events import AsyncEventChannel

__all__ = ("DEFAULT_NOTIFICATION_CHANNEL", "SQLSpecBackendConfig")

DEFAULT_NOTIFICATION_CHANNEL = "litestar_queues_tasks"


@dataclass(slots=True)
class SQLSpecBackendConfig:
    """Configuration values for the SQLSpec queue backend."""

    sqlspec: SQLSpec | None = None
    sqlspec_config: Any | None = None
    heartbeat_pool_config: Any | None = None
    table_name: str | None = None
    create_schema: bool | None = None
    run_migrations: bool | None = None
    event_channel: AsyncEventChannel | None = None
    notifications: bool | None = None
    notification_channel: str | None = None
    event_backend: str | None = None
    event_queue_table: str | None = None
    event_poll_interval: float | None = None
    event_settings: dict[str, Any] = field(default_factory=dict)

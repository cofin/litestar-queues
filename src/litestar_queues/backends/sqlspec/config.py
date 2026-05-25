"""SQLSpec backend configuration."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from sqlspec import SQLSpec
from sqlspec.extensions.events import AsyncEventChannel

from litestar_queues.backends.sqlspec.schema import validate_column_map, validate_native_json_columns

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
    column_map: Mapping[str, str] = field(default_factory=dict)
    native_json_columns: frozenset[str] = field(default_factory=frozenset)
    manage_schema: bool = True

    def __post_init__(self) -> None:
        """Validate adopter-owned table configuration."""
        self.column_map = validate_column_map(self.column_map)
        self.native_json_columns = validate_native_json_columns(frozenset(self.native_json_columns))

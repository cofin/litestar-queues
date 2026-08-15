"""SQLSpec backend configuration."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, cast

from litestar_queues.backends.sqlspec.schema import resolve_column_map, validate_table_name
from litestar_queues.events import EventHistoryExtraColumn, validate_event_history_extra_columns

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlspec import SQLSpec

    from litestar_queues.backends.sqlspec._typing import SQLSpecConfig, SQLSpecStoreConfig
    from litestar_queues.config import QueueConfig

__all__ = (
    "DEFAULT_CONTROL_CHANNEL",
    "DEFAULT_EVENT_HISTORY_TABLE_SUFFIX",
    "DEFAULT_MAINTENANCE_TABLE_SUFFIX",
    "DEFAULT_TABLE_NAME",
    "DEFAULT_TASK_RESERVATION_TABLE_SUFFIX",
    "DEFAULT_WAKEUP_CHANNEL",
    "SQLSpecBackendConfig",
    "SQLSpecWorkerWakeupConfig",
)

DEFAULT_TABLE_NAME = "queue_task"
DEFAULT_EVENT_HISTORY_TABLE_SUFFIX = "_event_history"
DEFAULT_MAINTENANCE_TABLE_SUFFIX = "_maintenance"
DEFAULT_TASK_RESERVATION_TABLE_SUFFIX = "_reservation"
DEFAULT_WAKEUP_CHANNEL = "litestar_queues_wakeups"
DEFAULT_CONTROL_CHANNEL = "litestar_queues_control"


@dataclass
class SQLSpecWorkerWakeupConfig:
    """Configuration for push-based worker wakeup notifications."""

    enabled: "bool" = True
    """Whether push notifications wake idle workers immediately."""

    channel_name: "str" = DEFAULT_WAKEUP_CHANNEL
    """Event channel used for worker wakeup broadcasts."""


@dataclass
class SQLSpecBackendConfig:
    """Configuration for the SQLSpec queue backend."""

    is_async: "ClassVar[bool]" = True
    """Backend execution mode."""

    sqlspec: "SQLSpec | None" = None
    """Dedicated SQLSpec client instance for queue storage."""

    config_name: "str | None" = None
    """Named database configuration from the application SQLSpec plugin."""

    table_name: "str" = DEFAULT_TABLE_NAME
    """Base table name for queue tasks."""

    event_history_table_name: "str | None" = None
    """Table name for queue event history records."""

    event_history_extra_columns: "tuple[EventHistoryExtraColumn, ...]" = field(default_factory=tuple)
    """Adopter-declared scoping columns on the event-history table."""

    maintenance_table_name: "str | None" = None
    """Table name for distributed queue maintenance metadata."""

    task_reservation_table_name: "str | None" = None
    """Table name for forever-uniqueness task reservations."""

    column_map: "Mapping[str, str] | None" = None
    """Mapping of canonical column names to custom database column names."""

    store_config: "SQLSpecStoreConfig | None" = None
    """Driver-specific configuration overrides for the queue store."""

    event_log_store_config: "SQLSpecStoreConfig | None" = None
    """Driver-specific configuration overrides for the event log store."""

    maintenance_store_config: "SQLSpecStoreConfig | None" = None
    """Driver-specific configuration overrides for the maintenance store."""

    task_reservation_store_config: "SQLSpecStoreConfig | None" = None
    """Driver-specific configuration overrides for the task reservation store."""

    worker_wakeups: "SQLSpecWorkerWakeupConfig" = field(default_factory=SQLSpecWorkerWakeupConfig)
    """Configuration for push-based worker wakeup notifications."""

    heartbeat_pool_config: "dict[str, Any] | None" = None
    """Dedicated connection pool configuration for the worker heartbeat loop."""

    def __post_init__(self) -> "None":
        """Validate and normalize SQLSpec backend configuration."""
        self.table_name = validate_table_name(self.table_name)
        if self.event_history_table_name is not None:
            self.event_history_table_name = validate_table_name(self.event_history_table_name)
        if self.maintenance_table_name is not None:
            self.maintenance_table_name = validate_table_name(self.maintenance_table_name)
        if self.task_reservation_table_name is not None:
            self.task_reservation_table_name = validate_table_name(self.task_reservation_table_name)
        if self.column_map is not None:
            self.column_map = resolve_column_map(self.column_map)
        self.event_history_extra_columns = validate_event_history_extra_columns(self.event_history_extra_columns)

    def get_store_config(self, parent_config: "QueueConfig") -> "SQLSpecConfig":
        """Resolve the effective SQLSpec configuration for the queue store."""
        if self.sqlspec is not None:
            return self.sqlspec.config
        if self.config_name is not None:
            return self.config_name
        return cast("SQLSpecConfig", parent_config.signature_namespace.get("sqlspec_config"))

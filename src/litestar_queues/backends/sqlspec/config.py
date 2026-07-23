"""SQLSpec backend configuration."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from litestar_queues.backends.sqlspec.schema import (
    resolve_column_map,
    validate_native_json_columns,
    validate_table_name,
)
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlspec import SQLSpec
    from sqlspec.extensions.events import AsyncEventChannel

    from litestar_queues.backends.sqlspec._typing import SQLSpecStoreConfig

__all__ = ("DEFAULT_WAKEUP_CHANNEL", "WAKEUP_TRANSPORTS", "SQLSpecBackendConfig", "SQLSpecWorkerWakeupConfig")

DEFAULT_WAKEUP_CHANNEL = "litestar_queues_tasks"

WAKEUP_TRANSPORTS: "frozenset[str]" = frozenset({"aq", "notify", "notify_queue", "poll_queue", "polling", "txeventq"})
"""Valid worker-wakeup transports for :attr:`SQLSpecWorkerWakeupConfig.transport`.

``notify`` uses native push wakeups, ``notify_queue`` uses native push wakeups
with a durable queue fallback, ``poll_queue`` uses the durable events table,
``aq`` and ``txeventq`` use Oracle Advanced Queuing backends, and ``polling``
disables push wakeups so workers fall back to interval polling.
"""


@dataclass(slots=True)
class SQLSpecWorkerWakeupConfig:
    """SQLSpec worker-wakeup channel and transport configuration."""

    channel: "AsyncEventChannel | None" = None
    """Explicit SQLSpec event channel; ``None`` constructs one from configuration."""

    transport: "str | None" = None
    """Explicit wakeup transport; ``None`` selects the adapter capability."""

    channel_name: "str | None" = None
    """Logical worker-wakeup channel name; ``None`` uses the package default."""

    queue_table_name: "str | None" = None
    """Durable SQLSpec event-queue table; ``None`` derives it from the adapter."""

    poll_interval: "float | None" = None
    """SQLSpec event-store poll interval in seconds; ``None`` uses SQLSpec defaults."""

    settings: "dict[str, Any]" = field(default_factory=dict)
    """Additional SQLSpec events-extension settings."""

    def __post_init__(self) -> "None":
        """Validate wakeup transport, table name, and poll interval."""
        if self.transport is not None and self.transport not in WAKEUP_TRANSPORTS:
            valid = ", ".join(sorted(WAKEUP_TRANSPORTS))
            msg = f"Invalid SQLSpec worker wakeup transport {self.transport!r}; expected one of: {valid}."
            raise QueueConfigurationError(msg)
        if self.queue_table_name is not None:
            self.queue_table_name = validate_table_name(self.queue_table_name)
        if self.poll_interval is not None and self.poll_interval <= 0:
            msg = "SQLSpecWorkerWakeupConfig.poll_interval must be greater than 0."
            raise QueueConfigurationError(msg)


@dataclass(slots=True)
class SQLSpecBackendConfig:
    """Configuration values for the SQLSpec queue backend."""

    backend_name: "ClassVar[str]" = "sqlspec"
    sqlspec: "SQLSpec | None" = None
    """Injected SQLSpec manager; ``None`` creates a manager owned by the queue backend."""

    sqlspec_config: "SQLSpecStoreConfig | None" = None
    """SQLSpec adapter configuration used for queue operations; ``None`` resolves or creates one."""

    heartbeat_pool_config: "SQLSpecStoreConfig | None" = None
    """Dedicated heartbeat adapter configuration; ``None`` reuses normal queue operations."""

    queue_table_name: "str | None" = None
    """Queue-task table name; ``None`` uses ``queue_task`` or SQLSpec extension settings."""

    worker_wakeups: "SQLSpecWorkerWakeupConfig | None" = field(default_factory=SQLSpecWorkerWakeupConfig)
    """Worker wakeup transport configuration; ``None`` disables wakeups."""

    event_history_table_name: "str | None" = None
    """Task-event history table name; ``None`` derives it from the queue-task table."""

    maintenance_table_name: "str | None" = None
    """Maintenance coordination table name; ``None`` derives the package default."""

    task_reservation_table_name: "str | None" = None
    """Permanent task-reservation table name; ``None`` derives it from the queue-task table."""

    column_map: "Mapping[str, str]" = field(default_factory=dict)
    """Overrides mapping logical queue fields to adopter-owned database columns."""

    native_json_columns: "frozenset[str]" = field(default_factory=frozenset)
    """Logical queue fields stored in database-native JSON columns."""

    manage_schema: "bool" = True
    """Whether backend startup and migrations may create package-owned queue tables."""

    def __post_init__(self) -> "None":
        """Validate adopter-owned table and wakeup-transport configuration."""
        if self.queue_table_name is not None:
            self.queue_table_name = validate_table_name(self.queue_table_name)
        if self.event_history_table_name is not None:
            self.event_history_table_name = validate_table_name(self.event_history_table_name)
        if self.maintenance_table_name is not None:
            self.maintenance_table_name = validate_table_name(self.maintenance_table_name)
        if self.task_reservation_table_name is not None:
            self.task_reservation_table_name = validate_table_name(self.task_reservation_table_name)
        self.column_map = resolve_column_map(self.column_map)
        self.native_json_columns = validate_native_json_columns(frozenset(self.native_json_columns))

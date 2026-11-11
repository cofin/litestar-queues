"""SQLSpec backend configuration."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, cast

from litestar_queues.backends.sqlspec.extension import (
    configure_events_migration_extension,
    configure_queue_migration_extension,
    validate_migration_mode,
)
from litestar_queues.backends.sqlspec.schema import (
    DEFAULT_TABLE_NAME,
    resolve_column_map,
    validate_native_json_columns,
    validate_table_name,
)
from litestar_queues.events import EventHistoryExtraColumn, validate_event_history_extra_columns
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlspec import SQLSpec
    from sqlspec.extensions.events import AsyncEventChannel

    from litestar_queues.backends.sqlspec._typing import ExtensionMigrationMode, SQLSpecConfig, SQLSpecStoreConfig
    from litestar_queues.config import QueueConfig

__all__ = (
    "DEFAULT_CONTROL_CHANNEL",
    "DEFAULT_WAKEUP_CHANNEL",
    "WAKEUP_TRANSPORTS",
    "SQLSpecBackendConfig",
    "SQLSpecWorkerWakeupConfig",
)

DEFAULT_WAKEUP_CHANNEL = "litestar_queues_tasks"

DEFAULT_CONTROL_CHANNEL = "litestar_queues_worker_control"

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
    """Registered queue storage backend identifier (``"sqlspec"``)."""

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

    event_history_extra_columns: "tuple[EventHistoryExtraColumn, ...]" = ()
    """Adopter-declared extra scoping columns on the event-history table."""

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

    migration_mode: "ExtensionMigrationMode" = "versioned"
    """SQLSpec extension migration strategy: ``'versioned'`` or ``'reconcile'``."""

    terminal_ttl: "float | None" = None
    """Optional retention window in seconds for terminal task rows.

    On databases that support filtered row-level expiration (CockroachDB and
    Google Cloud Spanner), ``0001_create_queue_tasks.py`` and ``create_statements()``
    embed a native row-level TTL policy scoped to terminal rows. When
    ``WorkerConfig.terminal_retention`` is ``None``, workers also inherit this
    retention window for periodic ``cleanup_terminal()`` sweeps across all SQL
    dialects.
    """

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
        if self.terminal_ttl is not None and self.terminal_ttl <= 0:
            msg = "SQLSpecBackendConfig.terminal_ttl must be greater than 0."
            raise QueueConfigurationError(msg)
        self.migration_mode = validate_migration_mode(self.migration_mode)
        self.event_history_extra_columns = validate_event_history_extra_columns(self.event_history_extra_columns)
        self.column_map = resolve_column_map(self.column_map)
        self.native_json_columns = validate_native_json_columns(frozenset(self.native_json_columns))

    def configure_migrations(self, config: "QueueConfig") -> "None":
        """Register the queue and durable-events migrations with the SQLSpec config.

        Implements ``MigrationConfiguringBackend``, so ``QueuePlugin`` reaches
        this without importing the SQLSpec backend.
        """
        from litestar_queues.backends.sqlspec.backend import resolve_events_migration_backend

        sqlspec_config = self.sqlspec_config
        if sqlspec_config is None and self.sqlspec is not None:
            registered_configs = tuple(self.sqlspec.configs.values())
            if len(registered_configs) == 1:
                sqlspec_config = registered_configs[0]
        if sqlspec_config is None:
            return

        configure_events_migration_extension(
            cast("SQLSpecConfig", sqlspec_config),
            backend=resolve_events_migration_backend(self, cast("SQLSpecConfig", sqlspec_config)),
            queue_table=(self.worker_wakeups.queue_table_name if self.worker_wakeups is not None else None),
            manage_schema=self.manage_schema,
            migration_mode=self.migration_mode,
        )

        extension_config = sqlspec_config.extension_config or {}
        queue_settings = dict(extension_config.get("litestar_queues", {}) or {})
        queue_table_name = self.queue_table_name or queue_settings.get("queue_table_name") or DEFAULT_TABLE_NAME
        raw_terminal_ttl = self.terminal_ttl if self.terminal_ttl is not None else queue_settings.get("terminal_ttl")
        terminal_ttl = (
            float(raw_terminal_ttl)
            if isinstance(raw_terminal_ttl, (int, float)) and not isinstance(raw_terminal_ttl, bool)
            else None
        )
        event_log_config = config.events.history if config.events is not None else None
        configure_queue_migration_extension(
            cast("SQLSpecConfig", sqlspec_config),
            queue_table_name=str(queue_table_name),
            event_history_enabled=event_log_config is not None,
            event_history_table_name=self.event_history_table_name,
            event_history_extra_columns=self.event_history_extra_columns,
            maintenance_table_name=self.maintenance_table_name,
            task_reservation_table_name=self.task_reservation_table_name,
            column_map=self.column_map,
            manage_schema=self.manage_schema,
            terminal_ttl=terminal_ttl,
            migration_mode=self.migration_mode,
        )

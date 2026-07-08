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

__all__ = ("DEFAULT_NOTIFICATION_CHANNEL", "NOTIFY_TRANSPORTS", "SQLSpecBackendConfig")

DEFAULT_NOTIFICATION_CHANNEL = "litestar_queues_tasks"

NOTIFY_TRANSPORTS: "frozenset[str]" = frozenset({
    "aq",
    "listen_notify",
    "listen_notify_durable",
    "polling",
    "table_queue",
    "txeventq",
})
"""Valid worker-wakeup transports for :attr:`SQLSpecBackendConfig.notify_transport`.

``listen_notify``/``listen_notify_durable`` push wakeups through native
LISTEN/NOTIFY, ``table_queue`` uses the durable events table, ``aq`` and
``txeventq`` use Oracle Advanced Queuing backends, and ``polling`` disables
push wakeups so workers fall back to interval polling.
"""


@dataclass(slots=True)
class SQLSpecBackendConfig:
    """Configuration values for the SQLSpec queue backend."""

    backend_name: "ClassVar[str]" = "sqlspec"
    sqlspec: "SQLSpec | None" = None
    config: "SQLSpecStoreConfig | None" = None
    heartbeat_pool_config: "SQLSpecStoreConfig | None" = None
    table_name: "str | None" = None
    create_schema: "bool | None" = None
    run_migrations: "bool | None" = None
    event_channel: "AsyncEventChannel | None" = None
    notifications: "bool | None" = None
    notification_channel: "str | None" = None
    notify_transport: "str | None" = None
    event_log_table_name: "str | None" = None
    event_backend: "str | None" = None
    event_queue_table: "str | None" = None
    event_poll_interval: "float | None" = None
    event_settings: "dict[str, Any]" = field(default_factory=dict)
    queue_observability: "bool" = True
    column_map: "Mapping[str, str]" = field(default_factory=dict)
    native_json_columns: "frozenset[str]" = field(default_factory=frozenset)
    manage_schema: "bool" = True

    def __post_init__(self) -> "None":
        """Validate adopter-owned table and wakeup-transport configuration."""
        if self.table_name is not None:
            self.table_name = validate_table_name(self.table_name)
        if self.event_log_table_name is not None:
            self.event_log_table_name = validate_table_name(self.event_log_table_name)
        self.column_map = resolve_column_map(self.column_map)
        self.native_json_columns = validate_native_json_columns(frozenset(self.native_json_columns))
        if self.notify_transport is not None and self.notify_transport not in NOTIFY_TRANSPORTS:
            valid = ", ".join(sorted(NOTIFY_TRANSPORTS))
            msg = f"Invalid notify_transport {self.notify_transport!r}; expected one of: {valid}."
            raise QueueConfigurationError(msg)

"""Create all Litestar Queues tables."""

from typing import TYPE_CHECKING, Any, cast

from sqlspec.exceptions import SQLSpecError

from litestar_queues.backends.sqlspec.event_log import create_event_log_store
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.maintenance import create_maintenance_store
from litestar_queues.backends.sqlspec.reservation import create_task_reservation_store
from litestar_queues.backends.sqlspec.schema import DEFAULT_TABLE_NAME, validate_table_name
from litestar_queues.backends.sqlspec.stores.factory import create_queue_store

if TYPE_CHECKING:
    from sqlspec.migrations.context import MigrationContext

    from litestar_queues.backends.sqlspec.event_log import SQLSpecQueueEventLogStore
    from litestar_queues.backends.sqlspec.maintenance import SQLSpecMaintenanceStore
    from litestar_queues.backends.sqlspec.reservation import SQLSpecTaskReservationStore
    from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("down", "up")


async def up(context: "MigrationContext | None" = None) -> "list[str]":
    """Return SQL statements that provision every queue-owned table."""
    queue_store = _load_queue_store(context)
    event_log_store = _load_event_log_store(context)
    statements = queue_store.create_statements()
    if event_log_store is not None:
        statements.extend(event_log_store.create_statements())
    statements.extend(_load_maintenance_store(context).create_statements())
    statements.extend(_load_reservation_store(context).create_statements())
    return statements


async def down(context: "MigrationContext | None" = None) -> "list[str]":
    """Return SQL statements that drop every queue-owned table."""
    queue_store = _load_queue_store(context)
    event_log_store = _load_event_log_store(context)
    statements = _load_reservation_store(context).drop_statements()
    statements.extend(_load_maintenance_store(context).drop_statements())
    if event_log_store is not None:
        statements.extend(event_log_store.drop_statements())
    statements.extend(queue_store.drop_statements())
    return statements


def _load_queue_store(context: "MigrationContext | None") -> "SQLSpecQueueStore":
    if context is None or context.config is None:
        msg = "Migration context with SQLSpec adapter configuration is required"
        raise SQLSpecError(msg)
    config = cast("Any", context.config)
    return create_queue_store(config, manage_schema=bool(getattr(config, "manage_schema", True)))


def _load_event_log_store(context: "MigrationContext | None") -> "SQLSpecQueueEventLogStore | None":
    config, queue_settings, queue_table_name = _migration_settings(context)
    if not bool(queue_settings.get("event_history_enabled")):
        return None
    configured_event_table = queue_settings.get("event_history_table_name")
    event_history_table_name = str(configured_event_table) if configured_event_table is not None else None
    return create_event_log_store(
        config,
        queue_table_name=queue_table_name,
        event_history_table_name=event_history_table_name,
        manage_schema=bool(getattr(config, "manage_schema", True)),
    )


def _load_maintenance_store(context: "MigrationContext | None") -> "SQLSpecMaintenanceStore":
    config, queue_settings, queue_table_name = _migration_settings(context)
    configured = queue_settings.get("maintenance_table_name")
    maintenance_table_name = str(configured) if configured is not None else None
    return create_maintenance_store(
        config,
        queue_table_name=queue_table_name,
        maintenance_table_name=maintenance_table_name,
        manage_schema=bool(getattr(config, "manage_schema", True)),
    )


def _load_reservation_store(context: "MigrationContext | None") -> "SQLSpecTaskReservationStore":
    config, queue_settings, queue_table_name = _migration_settings(context)
    configured = queue_settings.get("task_reservation_table_name")
    task_reservation_table_name = str(configured) if configured is not None else None
    return create_task_reservation_store(
        config,
        queue_table_name=queue_table_name,
        task_reservation_table_name=task_reservation_table_name,
        manage_schema=bool(getattr(config, "manage_schema", True)),
    )


def _migration_settings(context: "MigrationContext | None") -> "tuple[Any, dict[str, Any], str]":
    if context is None or context.config is None:
        msg = "Migration context with SQLSpec adapter configuration is required"
        raise SQLSpecError(msg)
    config = cast("Any", context.config)
    extension_config = config.extension_config or {}
    queue_settings = dict(extension_config.get(QUEUE_EXTENSION_NAME, {}) or {})
    queue_table_name = validate_table_name(str(queue_settings.get("table_name", DEFAULT_TABLE_NAME)))
    return config, queue_settings, queue_table_name

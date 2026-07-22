"""Create the Litestar queue task table."""

from typing import TYPE_CHECKING, Any, cast

from sqlspec.exceptions import SQLSpecError

from litestar_queues.backends.sqlspec.event_log import create_event_log_store
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.maintenance_lease import create_maintenance_lease_store
from litestar_queues.backends.sqlspec.schema import DEFAULT_TABLE_NAME, validate_table_name
from litestar_queues.backends.sqlspec.stores.factory import create_queue_store

if TYPE_CHECKING:
    from sqlspec.migrations.context import MigrationContext

    from litestar_queues.backends.sqlspec.event_log import SQLSpecQueueEventLogStore
    from litestar_queues.backends.sqlspec.maintenance_lease import SQLSpecMaintenanceLeaseStore
    from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("down", "up")


async def up(context: "MigrationContext | None" = None) -> "list[str]":
    """Return SQL statements that provision the queue, event-log, and lease tables."""
    queue_store = _load_queue_store(context)
    event_log_store = _load_event_log_store(context)
    lease_store = _load_lease_store(context)
    statements = queue_store.create_statements()
    if event_log_store is not None:
        statements.extend(event_log_store.create_statements())
    statements.extend(lease_store.create_statements())
    return statements


async def down(context: "MigrationContext | None" = None) -> "list[str]":
    """Return SQL statements that drop the lease, event-log, and queue tables."""
    queue_store = _load_queue_store(context)
    event_log_store = _load_event_log_store(context)
    lease_store = _load_lease_store(context)
    statements = lease_store.drop_statements()
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
    if context is None or context.config is None:
        msg = "Migration context with SQLSpec adapter configuration is required"
        raise SQLSpecError(msg)
    config = cast("Any", context.config)
    extension_config = config.extension_config or {}
    queue_settings = dict(extension_config.get(QUEUE_EXTENSION_NAME, {}) or {})
    if not bool(queue_settings.get("event_log_enabled")):
        return None
    queue_table_name = validate_table_name(str(queue_settings.get("table_name", DEFAULT_TABLE_NAME)))
    configured_event_table = queue_settings.get("event_log_table_name")
    event_log_table_name = str(configured_event_table) if configured_event_table is not None else None
    return create_event_log_store(
        config,
        queue_table_name=queue_table_name,
        event_log_table_name=event_log_table_name,
        manage_schema=bool(getattr(config, "manage_schema", True)),
    )


def _load_lease_store(context: "MigrationContext | None") -> "SQLSpecMaintenanceLeaseStore":
    if context is None or context.config is None:
        msg = "Migration context with SQLSpec adapter configuration is required"
        raise SQLSpecError(msg)
    config = cast("Any", context.config)
    extension_config = config.extension_config or {}
    queue_settings = dict(extension_config.get(QUEUE_EXTENSION_NAME, {}) or {})
    queue_table_name = validate_table_name(str(queue_settings.get("table_name", DEFAULT_TABLE_NAME)))
    configured_lease_table = queue_settings.get("maintenance_lease_table_name")
    lease_table_name = str(configured_lease_table) if configured_lease_table is not None else None
    return create_maintenance_lease_store(
        config,
        queue_table_name=queue_table_name,
        maintenance_lease_table_name=lease_table_name,
        manage_schema=bool(getattr(config, "manage_schema", True)),
    )

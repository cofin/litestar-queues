"""Create the Litestar queue distributed maintenance-lease table."""

from typing import TYPE_CHECKING, Any, cast

from sqlspec.exceptions import SQLSpecError

from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.maintenance_lease import create_maintenance_lease_store
from litestar_queues.backends.sqlspec.schema import DEFAULT_TABLE_NAME, validate_table_name

if TYPE_CHECKING:
    from sqlspec.migrations.context import MigrationContext

    from litestar_queues.backends.sqlspec.maintenance_lease import SQLSpecMaintenanceLeaseStore

__all__ = ("down", "up")


async def up(context: "MigrationContext | None" = None) -> "list[str]":
    """Return SQL statements that provision the maintenance-lease table."""
    return _load_lease_store(context).create_statements()


async def down(context: "MigrationContext | None" = None) -> "list[str]":
    """Return SQL statements that drop the maintenance-lease table."""
    return _load_lease_store(context).drop_statements()


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

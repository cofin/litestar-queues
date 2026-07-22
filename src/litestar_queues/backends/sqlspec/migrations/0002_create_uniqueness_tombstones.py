"""Create the Litestar queue forever-uniqueness tombstone table."""

from typing import TYPE_CHECKING, Any, cast

from sqlspec.exceptions import SQLSpecError

from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.schema import DEFAULT_TABLE_NAME, validate_table_name
from litestar_queues.backends.sqlspec.uniqueness import create_tombstone_store

if TYPE_CHECKING:
    from sqlspec.migrations.context import MigrationContext

    from litestar_queues.backends.sqlspec.uniqueness import SQLSpecQueueTombstoneStore

__all__ = ("down", "up")


async def up(context: "MigrationContext | None" = None) -> "list[str]":
    """Return SQL statements that provision the forever-uniqueness tombstone table."""
    return _load_tombstone_store(context).create_statements()


async def down(context: "MigrationContext | None" = None) -> "list[str]":
    """Return SQL statements that drop the forever-uniqueness tombstone table."""
    return _load_tombstone_store(context).drop_statements()


def _load_tombstone_store(context: "MigrationContext | None") -> "SQLSpecQueueTombstoneStore":
    if context is None or context.config is None:
        msg = "Migration context with SQLSpec adapter configuration is required"
        raise SQLSpecError(msg)
    config = cast("Any", context.config)
    extension_config = config.extension_config or {}
    queue_settings = dict(extension_config.get(QUEUE_EXTENSION_NAME, {}) or {})
    queue_table_name = validate_table_name(str(queue_settings.get("table_name", DEFAULT_TABLE_NAME)))
    configured = queue_settings.get("uniqueness_table_name")
    uniqueness_table_name = str(configured) if configured is not None else None
    return create_tombstone_store(
        config,
        queue_table_name=queue_table_name,
        uniqueness_table_name=uniqueness_table_name,
        manage_schema=bool(getattr(config, "manage_schema", True)),
    )

"""Create the Litestar queue event history table."""

from typing import TYPE_CHECKING, Any, cast

from sqlspec.exceptions import SQLSpecError

from litestar_queues.backends.sqlspec.event_log import create_event_log_store
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.schema import DEFAULT_TABLE_NAME, validate_table_name

if TYPE_CHECKING:
    from sqlspec.migrations.context import MigrationContext

    from litestar_queues.backends.sqlspec.event_log import SQLSpecQueueEventLogStore

__all__ = ("down", "up")


async def up(context: "MigrationContext | None" = None) -> "list[str]":
    """Return SQL statements that provision the queue event history table."""
    store = _load_store(context)
    return [] if store is None else store.create_statements()


async def down(context: "MigrationContext | None" = None) -> "list[str]":
    """Return SQL statements that drop the queue event history table."""
    store = _load_store(context)
    return [] if store is None else store.drop_statements()


def _load_store(context: "MigrationContext | None") -> "SQLSpecQueueEventLogStore | None":
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

"""Add the pending-job expiration deadline."""

from typing import TYPE_CHECKING, Any, cast

from sqlspec.exceptions import SQLSpecError

from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.schema import DEFAULT_TABLE_NAME, validate_table_name
from litestar_queues.backends.sqlspec.stores.factory import create_queue_store

if TYPE_CHECKING:
    from sqlspec.migrations.context import MigrationContext

__all__ = ("down", "up")


async def up(context: "MigrationContext | None" = None) -> "list[str]":
    """Return the statement adding ``expires_at`` to the queue task table."""
    store = _load_store(context)
    return [
        f"ALTER TABLE {store._quoted_table_name()} ADD COLUMN {store._quoted_col('expires_at')} {store._timestamp_type()}"  # noqa: SLF001
    ]


async def down(context: "MigrationContext | None" = None) -> "list[str]":
    """Return the statement removing ``expires_at`` from the queue task table."""
    store = _load_store(context)
    return [
        f"ALTER TABLE {store._quoted_table_name()} DROP COLUMN {store._quoted_col('expires_at')}"  # noqa: SLF001
    ]


def _load_store(context: "MigrationContext | None") -> "Any":
    if context is None or context.config is None:
        msg = "Migration context with SQLSpec adapter configuration is required"
        raise SQLSpecError(msg)
    config = cast("Any", context.config)
    extension_config = config.extension_config or {}
    settings = dict(extension_config.get(QUEUE_EXTENSION_NAME, {}) or {})
    table_name = validate_table_name(str(settings.get("table_name", DEFAULT_TABLE_NAME)))
    return create_queue_store(
        config,
        table_name=table_name,
        column_map=settings.get("column_map"),
        native_json_columns=frozenset(settings.get("native_json_columns", ())),
        manage_schema=bool(getattr(config, "manage_schema", True)),
    )

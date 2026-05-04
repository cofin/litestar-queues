"""Create the Litestar queue task table."""

from typing import TYPE_CHECKING, Any, cast

from sqlspec.exceptions import SQLSpecError

from litestar_queues.backends.sqlspec.store import SQLSpecQueueStore

if TYPE_CHECKING:
    from sqlspec.migrations.context import MigrationContext

__all__ = ("down", "up")


async def up(context: "MigrationContext | None" = None) -> list[str]:
    """Return SQL statements that provision the queue table and indexes."""
    return _load_store(context).create_statements()


async def down(context: "MigrationContext | None" = None) -> list[str]:
    """Return SQL statements that drop the queue table."""
    return _load_store(context).drop_statements()


def _load_store(context: "MigrationContext | None") -> SQLSpecQueueStore:
    if context is None or context.config is None:
        msg = "Migration context with SQLSpec adapter configuration is required"
        raise SQLSpecError(msg)
    return SQLSpecQueueStore(cast("Any", context.config))

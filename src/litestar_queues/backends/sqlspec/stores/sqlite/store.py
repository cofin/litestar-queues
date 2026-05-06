"""sqlite SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("SqliteQueueStore",)


class SqliteQueueStore(SQLSpecQueueStore):
    """sqlite-specific SQLSpec queue statement store."""

    __slots__ = ()

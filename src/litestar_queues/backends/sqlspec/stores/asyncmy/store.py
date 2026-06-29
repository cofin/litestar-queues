"""asyncmy SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores._families import MySQLQueueStore

__all__ = ("AsyncmyQueueStore",)


class AsyncmyQueueStore(MySQLQueueStore):
    """asyncmy-specific SQLSpec queue statement store."""

    __slots__ = ()

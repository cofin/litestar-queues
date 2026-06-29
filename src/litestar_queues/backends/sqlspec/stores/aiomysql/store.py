"""aiomysql SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores._families import MySQLQueueStore

__all__ = ("AiomysqlQueueStore",)


class AiomysqlQueueStore(MySQLQueueStore):
    """aiomysql-specific SQLSpec queue statement store."""

    __slots__ = ()

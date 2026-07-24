"""pymysql SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores._families import MySQLQueueStore

__all__ = ("PymysqlQueueStore",)


class PymysqlQueueStore(MySQLQueueStore):
    """pymysql-specific SQLSpec queue statement store."""

    __slots__ = ()

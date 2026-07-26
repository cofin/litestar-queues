"""mysqlconnector SQLSpec queue stores."""

from litestar_queues.backends.sqlspec.stores._dialects import MySQLQueueStore

__all__ = ("MysqlConnectorAsyncQueueStore", "MysqlConnectorSyncQueueStore")


class MysqlConnectorSyncQueueStore(MySQLQueueStore):
    """mysqlconnector sync SQLSpec queue statement store."""

    __slots__ = ()


class MysqlConnectorAsyncQueueStore(MySQLQueueStore):
    """mysqlconnector async SQLSpec queue statement store."""

    __slots__ = ()

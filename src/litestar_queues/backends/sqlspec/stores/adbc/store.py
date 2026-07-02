"""ADBC SQLite SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores.sqlite import SqliteQueueStore

__all__ = ("AdbcSqliteQueueStore",)


class AdbcSqliteQueueStore(SqliteQueueStore):
    """ADBC SQLite-specific SQLSpec queue statement store."""

    __slots__ = ()

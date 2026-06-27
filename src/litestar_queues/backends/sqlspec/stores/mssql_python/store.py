"""mssql-python SQLSpec queue stores."""

from litestar_queues.backends.sqlspec.stores._families import SQLServerQueueStore

__all__ = ("MssqlPythonAsyncQueueStore", "MssqlPythonQueueStore", "MssqlPythonSyncQueueStore")


class MssqlPythonQueueStore(SQLServerQueueStore):
    """mssql-python sync SQLSpec queue statement store."""

    __slots__ = ()


class MssqlPythonAsyncQueueStore(SQLServerQueueStore):
    """mssql-python async SQLSpec queue statement store."""

    __slots__ = ()


MssqlPythonSyncQueueStore = MssqlPythonQueueStore

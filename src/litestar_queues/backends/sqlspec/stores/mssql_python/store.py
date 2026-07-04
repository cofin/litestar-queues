"""mssql-python SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores._families import MssqlQueueStore

__all__ = ("MssqlPythonQueueStore",)


class MssqlPythonQueueStore(MssqlQueueStore):
    """mssql-python SQLSpec queue statement store."""

    __slots__ = ()

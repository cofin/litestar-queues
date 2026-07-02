"""pymssql SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores._families import MssqlQueueStore

__all__ = ("PymssqlQueueStore",)


class PymssqlQueueStore(MssqlQueueStore):
    """pymssql SQLSpec queue statement store."""

    __slots__ = ()

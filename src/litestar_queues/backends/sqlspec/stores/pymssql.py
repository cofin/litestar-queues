"""pymssql SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores._dialects import MssqlQueueStore

__all__ = ("PymssqlQueueStore",)


class PymssqlQueueStore(MssqlQueueStore):
    """pymssql SQLSpec queue statement store."""

    __slots__ = ()

    skip_explicit_begin = True

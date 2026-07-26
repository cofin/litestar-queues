"""cockroach_asyncpg SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores._dialects import CockroachQueueStore

__all__ = ("CockroachAsyncpgQueueStore",)


class CockroachAsyncpgQueueStore(CockroachQueueStore):
    """cockroach_asyncpg SQLSpec queue statement store."""

    __slots__ = ()

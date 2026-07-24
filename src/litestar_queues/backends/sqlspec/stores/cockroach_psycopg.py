"""cockroach_psycopg SQLSpec queue stores."""

from litestar_queues.backends.sqlspec.stores._families import CockroachQueueStore

__all__ = ("CockroachPsycopgAsyncQueueStore", "CockroachPsycopgSyncQueueStore")


class CockroachPsycopgSyncQueueStore(CockroachQueueStore):
    """cockroach_psycopg sync SQLSpec queue statement store."""

    __slots__ = ()


class CockroachPsycopgAsyncQueueStore(CockroachQueueStore):
    """cockroach_psycopg async SQLSpec queue statement store."""

    __slots__ = ()

"""cockroach_psycopg SQLSpec queue stores."""

from litestar_queues.backends.sqlspec.stores._families import PostgresQueueStore

__all__ = ("CockroachPsycopgAsyncQueueStore", "CockroachPsycopgSyncQueueStore")


class CockroachPsycopgSyncQueueStore(PostgresQueueStore):
    """cockroach_psycopg sync SQLSpec queue statement store."""

    __slots__ = ()

    data_dictionary_dialect = "cockroachdb"


class CockroachPsycopgAsyncQueueStore(PostgresQueueStore):
    """cockroach_psycopg async SQLSpec queue statement store."""

    __slots__ = ()

    data_dictionary_dialect = "cockroachdb"

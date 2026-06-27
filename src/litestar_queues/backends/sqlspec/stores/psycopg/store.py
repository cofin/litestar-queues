"""psycopg SQLSpec queue stores."""

from litestar_queues.backends.sqlspec.stores._families import PostgresQueueStore

__all__ = ("PsycopgAsyncQueueStore", "PsycopgSyncQueueStore")


class PsycopgSyncQueueStore(PostgresQueueStore):
    """psycopg sync SQLSpec queue statement store."""

    __slots__ = ()

    table_storage_parameters = True


class PsycopgAsyncQueueStore(PostgresQueueStore):
    """psycopg async SQLSpec queue statement store."""

    __slots__ = ()

    table_storage_parameters = True

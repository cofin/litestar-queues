"""asyncpg SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores._families import PostgresQueueStore

__all__ = ("AsyncpgQueueStore",)


class AsyncpgQueueStore(PostgresQueueStore):
    """asyncpg-specific SQLSpec queue statement store."""

    __slots__ = ()

    table_storage_parameters = True

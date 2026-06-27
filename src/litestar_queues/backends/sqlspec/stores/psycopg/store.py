"""psycopg SQLSpec queue stores."""

from typing import ClassVar

from litestar_queues.backends.sqlspec.stores._families import PostgresQueueStore

__all__ = ("PsycopgAsyncQueueStore", "PsycopgSyncQueueStore")


class PsycopgSyncQueueStore(PostgresQueueStore):
    """psycopg sync SQLSpec queue statement store."""

    __slots__ = ()

    table_storage_parameters: ClassVar[bool] = True


class PsycopgAsyncQueueStore(PostgresQueueStore):
    """psycopg async SQLSpec queue statement store."""

    __slots__ = ()

    table_storage_parameters: ClassVar[bool] = True

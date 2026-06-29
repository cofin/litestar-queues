"""asyncpg SQLSpec queue store."""

from typing import ClassVar

from litestar_queues.backends.sqlspec.stores._families import PostgresQueueStore

__all__ = ("AsyncpgQueueStore",)


class AsyncpgQueueStore(PostgresQueueStore):
    """asyncpg-specific SQLSpec queue statement store."""

    __slots__ = ()

    table_storage_parameters: "ClassVar[bool]" = True

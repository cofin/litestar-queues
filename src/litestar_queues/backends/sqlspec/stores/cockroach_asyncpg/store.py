"""cockroach_asyncpg SQLSpec queue store."""

from typing import ClassVar

from litestar_queues.backends.sqlspec.stores._families import PostgresQueueStore

__all__ = ("CockroachAsyncpgQueueStore",)


class CockroachAsyncpgQueueStore(PostgresQueueStore):
    """cockroach_asyncpg-specific SQLSpec queue statement store."""

    __slots__ = ()

    data_dictionary_dialect: ClassVar[str | None] = "cockroachdb"

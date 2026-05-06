"""cockroach_asyncpg SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores.cockroach_asyncpg.store import CockroachAsyncpgQueueStore

__all__ = ("CockroachAsyncpgQueueStore",)

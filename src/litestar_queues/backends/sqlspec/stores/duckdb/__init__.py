"""duckdb SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores.duckdb.store import DuckDBQueueStore

__all__ = ("DuckDBQueueStore",)

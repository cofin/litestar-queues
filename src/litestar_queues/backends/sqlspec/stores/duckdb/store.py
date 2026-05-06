"""duckdb SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("DuckDBQueueStore",)


class DuckDBQueueStore(SQLSpecQueueStore):
    """duckdb-specific SQLSpec queue statement store."""

    __slots__ = ()

    id_type = "VARCHAR"
    text_type = "VARCHAR"
    indexed_text_type = "VARCHAR"
    json_type = "JSON"
    timestamp_type = "TIMESTAMP"
    error_type = "VARCHAR"

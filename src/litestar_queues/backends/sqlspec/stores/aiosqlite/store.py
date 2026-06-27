"""aiosqlite SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("AiosqliteQueueStore",)


class AiosqliteQueueStore(SQLSpecQueueStore):
    """aiosqlite-specific SQLSpec queue statement store."""

    __slots__ = ()

    data_dictionary_dialect = "sqlite"

    def _json_type(self) -> str:
        return "TEXT"

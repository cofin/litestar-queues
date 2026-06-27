"""sqlite SQLSpec queue store."""

from typing import ClassVar

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("SqliteQueueStore",)


class SqliteQueueStore(SQLSpecQueueStore):
    """sqlite-specific SQLSpec queue statement store."""

    __slots__ = ()

    data_dictionary_dialect: ClassVar[str | None] = "sqlite"

    def _json_type(self) -> str:
        return "TEXT"

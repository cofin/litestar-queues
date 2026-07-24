"""aiosqlite SQLSpec queue store."""

from typing import ClassVar

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("AiosqliteQueueStore",)


class AiosqliteQueueStore(SQLSpecQueueStore):
    """aiosqlite-specific SQLSpec queue statement store."""

    __slots__ = ()

    bind_datetime_as_text: "ClassVar[bool]" = True
    data_dictionary_dialect: "ClassVar[str | None]" = "sqlite"

    def _json_type(self) -> "str":
        return "TEXT"

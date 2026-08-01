"""psycopg SQLSpec queue stores."""

from typing import ClassVar

from litestar_queues.backends.sqlspec.stores._dialects import PostgresQueueStore

__all__ = ("PsycopgAsyncQueueStore", "PsycopgSyncQueueStore")


class PsycopgSyncQueueStore(PostgresQueueStore):
    """psycopg sync SQLSpec queue statement store."""

    __slots__ = ()

    table_storage_parameters: "ClassVar[bool]" = True

    @property
    def supports_native_bulk_ingest(self) -> "bool":
        """Disable native ingest until SQLSpec issue 663 supports JSON values."""
        return False


class PsycopgAsyncQueueStore(PostgresQueueStore):
    """psycopg async SQLSpec queue statement store."""

    __slots__ = ()

    table_storage_parameters: "ClassVar[bool]" = True

    @property
    def supports_native_bulk_ingest(self) -> "bool":
        """Disable native ingest until SQLSpec issue 663 supports JSON values."""
        return False

"""Declaring adapter_name and queue_store_class on a third-party SQLSpec adapter configuration."""

from typing import ClassVar

from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend
from litestar_queues.backends.sqlspec.stores.aiosqlite import AiosqliteQueueStore
from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore
from litestar_queues.backends.sqlspec.stores.factory import create_queue_store


class CustomSqliteQueueStore(AiosqliteQueueStore):
    """Custom queue store subclass for a third-party or wrapped SQLite adapter."""

    __slots__ = ()


class CustomSqliteAdapterConfig(AiosqliteConfig):
    """Third-party SQLSpec adapter configuration advertising explicit store hooks."""

    adapter_name: ClassVar[str] = "custom_sqlite"
    queue_store_class: ClassVar[type[SQLSpecQueueStore]] = CustomSqliteQueueStore


async def main() -> None:
    """Verify create_queue_store and SQLSpecQueueBackend resolve queue_store_class directly."""
    adapter_config = CustomSqliteAdapterConfig(connection_config={"database": ":memory:"})
    store = create_queue_store(adapter_config, terminal_ttl=3600.0)
    assert isinstance(store, CustomSqliteQueueStore)

    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(sqlspec_config=adapter_config, terminal_ttl=3600.0)
    )
    assert isinstance(backend, SQLSpecQueueBackend)

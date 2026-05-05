"""SQLSpec queue store factory."""

from typing import Any

from litestar_queues.backends.sqlspec.stores.adbc import AdbcQueueStore
from litestar_queues.backends.sqlspec.stores.aiomysql import AiomysqlQueueStore
from litestar_queues.backends.sqlspec.stores.aiosqlite import AiosqliteQueueStore
from litestar_queues.backends.sqlspec.stores.asyncmy import AsyncmyQueueStore
from litestar_queues.backends.sqlspec.stores.asyncpg import AsyncpgQueueStore
from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore, adapter_name
from litestar_queues.backends.sqlspec.stores.bigquery import BigQueryQueueStore
from litestar_queues.backends.sqlspec.stores.cockroach_asyncpg import CockroachAsyncpgQueueStore
from litestar_queues.backends.sqlspec.stores.cockroach_psycopg import (
    CockroachPsycopgAsyncQueueStore,
    CockroachPsycopgSyncQueueStore,
)
from litestar_queues.backends.sqlspec.stores.duckdb import DuckDBQueueStore
from litestar_queues.backends.sqlspec.stores.mysqlconnector import (
    MysqlConnectorAsyncQueueStore,
    MysqlConnectorSyncQueueStore,
)
from litestar_queues.backends.sqlspec.stores.oracledb import OracledbAsyncQueueStore, OracledbSyncQueueStore
from litestar_queues.backends.sqlspec.stores.psqlpy import PsqlpyQueueStore
from litestar_queues.backends.sqlspec.stores.psycopg import PsycopgAsyncQueueStore, PsycopgSyncQueueStore
from litestar_queues.backends.sqlspec.stores.pymysql import PymysqlQueueStore
from litestar_queues.backends.sqlspec.stores.spanner import SpannerQueueStore
from litestar_queues.backends.sqlspec.stores.sqlite import SqliteQueueStore

__all__ = ("create_queue_store",)

_ADAPTER_STORE_TYPES: dict[str, type[SQLSpecQueueStore]] = {
    "adbc": AdbcQueueStore,
    "aiomysql": AiomysqlQueueStore,
    "aiosqlite": AiosqliteQueueStore,
    "asyncmy": AsyncmyQueueStore,
    "asyncpg": AsyncpgQueueStore,
    "bigquery": BigQueryQueueStore,
    "cockroach_asyncpg": CockroachAsyncpgQueueStore,
    "duckdb": DuckDBQueueStore,
    "psqlpy": PsqlpyQueueStore,
    "pymysql": PymysqlQueueStore,
    "spanner": SpannerQueueStore,
    "sqlite": SqliteQueueStore,
}


def create_queue_store(config: Any, *, table_name: str | None = None) -> SQLSpecQueueStore:
    """Create a queue store for a SQLSpec adapter configuration.

    Returns:
        The queue store implementation for the SQLSpec adapter.
    """
    store_type = _adapter_store_type(config)
    return store_type(config, table_name=table_name)


def _adapter_store_type(config: Any) -> type[SQLSpecQueueStore]:
    name = adapter_name(config)
    if name == "cockroach_psycopg":
        return _async_or_sync_store_type(
            config,
            async_store_type=CockroachPsycopgAsyncQueueStore,
            sync_store_type=CockroachPsycopgSyncQueueStore,
        )
    if name == "mysqlconnector":
        return _async_or_sync_store_type(
            config,
            async_store_type=MysqlConnectorAsyncQueueStore,
            sync_store_type=MysqlConnectorSyncQueueStore,
        )
    if name == "oracledb":
        return _async_or_sync_store_type(
            config,
            async_store_type=OracledbAsyncQueueStore,
            sync_store_type=OracledbSyncQueueStore,
        )
    if name == "psycopg":
        return _async_or_sync_store_type(
            config,
            async_store_type=PsycopgAsyncQueueStore,
            sync_store_type=PsycopgSyncQueueStore,
        )
    return _ADAPTER_STORE_TYPES.get(name, SQLSpecQueueStore)


def _async_or_sync_store_type(
    config: Any,
    *,
    async_store_type: type[SQLSpecQueueStore],
    sync_store_type: type[SQLSpecQueueStore],
) -> type[SQLSpecQueueStore]:
    config_type_name = type(config).__name__.lower()
    if "async" in config_type_name:
        return async_store_type
    return sync_store_type

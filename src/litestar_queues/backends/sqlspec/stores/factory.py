"""SQLSpec queue store factory."""

from typing import TYPE_CHECKING, Any

from litestar_queues.backends.sqlspec.stores.aiomysql import AiomysqlQueueStore
from litestar_queues.backends.sqlspec.stores.aiosqlite import AiosqliteQueueStore
from litestar_queues.backends.sqlspec.stores.asyncmy import AsyncmyQueueStore
from litestar_queues.backends.sqlspec.stores.asyncpg import AsyncpgQueueStore
from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore, _adapter_name
from litestar_queues.backends.sqlspec.stores.cockroach_asyncpg import CockroachAsyncpgQueueStore
from litestar_queues.backends.sqlspec.stores.cockroach_psycopg import (
    CockroachPsycopgAsyncQueueStore,
    CockroachPsycopgSyncQueueStore,
)
from litestar_queues.backends.sqlspec.stores.duckdb import DuckDBQueueStore
from litestar_queues.backends.sqlspec.stores.mssql_python import MssqlPythonQueueStore
from litestar_queues.backends.sqlspec.stores.mysqlconnector import (
    MysqlConnectorAsyncQueueStore,
    MysqlConnectorSyncQueueStore,
)
from litestar_queues.backends.sqlspec.stores.oracledb import OracledbAsyncQueueStore, OracledbSyncQueueStore
from litestar_queues.backends.sqlspec.stores.psqlpy import PsqlpyQueueStore
from litestar_queues.backends.sqlspec.stores.psycopg import PsycopgAsyncQueueStore, PsycopgSyncQueueStore
from litestar_queues.backends.sqlspec.stores.pymssql import PymssqlQueueStore
from litestar_queues.backends.sqlspec.stores.pymysql import PymysqlQueueStore
from litestar_queues.backends.sqlspec.stores.spanner import SpannerQueueStore
from litestar_queues.backends.sqlspec.stores.sqlite import SqliteQueueStore
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ("create_queue_store",)

_ADAPTER_STORE_TYPES: "dict[str, type[SQLSpecQueueStore]]" = {
    "aiomysql": AiomysqlQueueStore,
    "aiosqlite": AiosqliteQueueStore,
    "asyncmy": AsyncmyQueueStore,
    "asyncpg": AsyncpgQueueStore,
    "cockroach_asyncpg": CockroachAsyncpgQueueStore,
    "duckdb": DuckDBQueueStore,
    "mssql_python": MssqlPythonQueueStore,
    "psqlpy": PsqlpyQueueStore,
    "pymssql": PymssqlQueueStore,
    "pymysql": PymysqlQueueStore,
    "spanner": SpannerQueueStore,
    "sqlite": SqliteQueueStore,
}
_ASYNC_OR_SYNC_ADAPTER_NAMES = frozenset({"cockroach_psycopg", "mysqlconnector", "oracledb", "psycopg"})
_SUPPORTED_ADAPTER_NAMES = frozenset(_ADAPTER_STORE_TYPES) | _ASYNC_OR_SYNC_ADAPTER_NAMES


def create_queue_store(
    config: "Any",
    *,
    table_name: "str | None" = None,
    column_map: "Mapping[str, str] | None" = None,
    native_json_columns: "frozenset[str] | None" = None,
    manage_schema: "bool" = True,
) -> "SQLSpecQueueStore":
    """Create a queue store for a SQLSpec adapter configuration.

    Returns:
        The queue store implementation for the SQLSpec adapter.
    """
    store_type = _adapter_store_type(config)
    return store_type(
        config,
        table_name=table_name,
        column_map=column_map,
        native_json_columns=native_json_columns or None,
        manage_schema=manage_schema,
    )


def _adapter_store_type(config: "Any") -> "type[SQLSpecQueueStore]":
    name = _adapter_name(config)
    if name == "mysqlconnector":
        return _async_or_sync_store_type(
            config, async_store_type=MysqlConnectorAsyncQueueStore, sync_store_type=MysqlConnectorSyncQueueStore
        )
    if name == "oracledb":
        return _async_or_sync_store_type(
            config, async_store_type=OracledbAsyncQueueStore, sync_store_type=OracledbSyncQueueStore
        )
    if name == "psycopg":
        return _async_or_sync_store_type(
            config, async_store_type=PsycopgAsyncQueueStore, sync_store_type=PsycopgSyncQueueStore
        )
    if name == "cockroach_psycopg":
        return _async_or_sync_store_type(
            config, async_store_type=CockroachPsycopgAsyncQueueStore, sync_store_type=CockroachPsycopgSyncQueueStore
        )
    if name in _ADAPTER_STORE_TYPES:
        return _ADAPTER_STORE_TYPES[name]
    if name:
        supported = ", ".join(sorted(_SUPPORTED_ADAPTER_NAMES))
        msg = f"SQLSpec adapter {name!r} is not supported by this queue backend. Supported adapters: {supported}."
        raise QueueConfigurationError(msg)
    return SQLSpecQueueStore


def _async_or_sync_store_type(
    config: "Any", *, async_store_type: "type[SQLSpecQueueStore]", sync_store_type: "type[SQLSpecQueueStore]"
) -> "type[SQLSpecQueueStore]":
    config_type_name = type(config).__name__.lower()
    if "async" in config_type_name:
        return async_store_type
    return sync_store_type

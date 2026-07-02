"""SQLSpec queue store implementations."""

from litestar_queues.backends.sqlspec.stores.aiomysql import AiomysqlQueueStore
from litestar_queues.backends.sqlspec.stores.aiosqlite import AiosqliteQueueStore
from litestar_queues.backends.sqlspec.stores.asyncmy import AsyncmyQueueStore
from litestar_queues.backends.sqlspec.stores.asyncpg import AsyncpgQueueStore
from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore
from litestar_queues.backends.sqlspec.stores.cockroach_asyncpg import CockroachAsyncpgQueueStore
from litestar_queues.backends.sqlspec.stores.cockroach_psycopg import (
    CockroachPsycopgAsyncQueueStore,
    CockroachPsycopgSyncQueueStore,
)
from litestar_queues.backends.sqlspec.stores.duckdb import DuckDBQueueStore
from litestar_queues.backends.sqlspec.stores.factory import create_queue_store
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
from litestar_queues.backends.sqlspec.stores.sqlite import SqliteQueueStore

__all__ = (
    "AiomysqlQueueStore",
    "AiosqliteQueueStore",
    "AsyncmyQueueStore",
    "AsyncpgQueueStore",
    "CockroachAsyncpgQueueStore",
    "CockroachPsycopgAsyncQueueStore",
    "CockroachPsycopgSyncQueueStore",
    "DuckDBQueueStore",
    "MssqlPythonQueueStore",
    "MysqlConnectorAsyncQueueStore",
    "MysqlConnectorSyncQueueStore",
    "OracledbAsyncQueueStore",
    "OracledbSyncQueueStore",
    "PsqlpyQueueStore",
    "PsycopgAsyncQueueStore",
    "PsycopgSyncQueueStore",
    "PymssqlQueueStore",
    "PymysqlQueueStore",
    "SQLSpecQueueStore",
    "SqliteQueueStore",
    "create_queue_store",
)

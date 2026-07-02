"""Integration-tier backend registry.

Single source of truth for queue-backend parametrize ids, optional-extra gating,
and async construction. Each ``BackendCase`` knows how to build its backend
from a ``FixtureCtx`` (pytest-databases service handles + ``tmp_path``).

The integration ``conftest.py`` consumes ``QUEUE_BACKENDS`` from ``pytest_generate_tests``
so any test that asks for the ``queue_backend`` fixture is auto-parametrized
across the registry. Per-adapter behavior gating uses ``case.capabilities``.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from pathlib import Path

    from litestar_queues.backends import BaseQueueBackend


class PostgresService(Protocol):
    """pytest-databases Postgres service attributes used by backend builders."""

    host: "str"
    port: "int"
    user: "str"
    password: "str"
    database: "str"


class MySQLService(Protocol):
    """pytest-databases MySQL service attributes used by backend builders."""

    host: "str"
    port: "int"
    user: "str"
    password: "str"
    db: "str"


class OracleService(Protocol):
    """pytest-databases Oracle service attributes used by backend builders."""

    host: "str"
    port: "int"
    user: "str"
    password: "str"
    service_name: "str"


class CockroachService(Protocol):
    """pytest-databases Cockroach service attributes used by backend builders."""

    host: "str"
    port: "int"
    database: "str"
    driver_opts: "dict[str, str]"


@dataclass(frozen=True, slots=True)
class FixtureCtx:
    """Per-test fixture context handed to a BackendCase builder."""

    tmp_path: "Path"
    service: "object | None" = None
    table_name: "str | None" = None


@dataclass(frozen=True, slots=True)
class BackendCase:
    """One row in the parametrize matrix."""

    name: "str"
    extras: "frozenset[str]"
    service_attr: "str | None"
    build: 'Callable[[FixtureCtx], Awaitable["BaseQueueBackend"]]'
    capabilities: "frozenset[str]"


# ---------------------------------------------------------------------------
# Builders. Each builder is async and returns a constructed-but-unopened
# backend. The fixture owns the open()/close() lifecycle.
# ---------------------------------------------------------------------------


async def _build_memory(ctx: "FixtureCtx") -> "BaseQueueBackend":
    from litestar_queues.backends import InMemoryQueueBackend

    return InMemoryQueueBackend()


def _sqlspec_backend(sqlspec_config: "object", *, table_name: "str | None" = None) -> "BaseQueueBackend":
    """Return a SQLSpec backend configured through the typed config object.

    ``table_name`` is set per-case by the fixture so adapters sharing the
    same Docker database (the Postgres/MySQL/Oracle service containers)
    each own a dedicated queue table and cannot pollute one another.
    """
    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend

    return SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(config=sqlspec_config, table_name=table_name))


async def _build_aiosqlite(ctx: "FixtureCtx") -> "BaseQueueBackend":
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    return _sqlspec_backend(
        AiosqliteConfig(connection_config={"database": str(ctx.tmp_path / "queue-aiosqlite.db")}),
        table_name=ctx.table_name,
    )


async def _build_sqlite(ctx: "FixtureCtx") -> "BaseQueueBackend":
    from sqlspec.adapters.sqlite import SqliteConfig

    return _sqlspec_backend(
        SqliteConfig(connection_config={"database": str(ctx.tmp_path / "queue-sqlite.db")}), table_name=ctx.table_name
    )


async def _build_duckdb(ctx: "FixtureCtx") -> "BaseQueueBackend":
    from sqlspec.adapters.duckdb import DuckDBConfig

    return _sqlspec_backend(
        DuckDBConfig(connection_config={"database": str(ctx.tmp_path / "queue-duckdb.db")}), table_name=ctx.table_name
    )


async def _build_postgres_asyncpg(ctx: "FixtureCtx") -> "BaseQueueBackend":
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    svc = cast("PostgresService", ctx.service)
    assert svc is not None
    return _sqlspec_backend(
        AsyncpgConfig(
            connection_config={
                "host": svc.host,
                "port": svc.port,
                "user": svc.user,
                "password": svc.password,
                "database": svc.database,
            }
        ),
        table_name=ctx.table_name,
    )


async def _build_postgres_psycopg(ctx: "FixtureCtx") -> "BaseQueueBackend":
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig

    svc = cast("PostgresService", ctx.service)
    assert svc is not None
    return _sqlspec_backend(
        PsycopgAsyncConfig(
            connection_config={
                "host": svc.host,
                "port": svc.port,
                "user": svc.user,
                "password": svc.password,
                "dbname": svc.database,
            }
        ),
        table_name=ctx.table_name,
    )


async def _build_postgres_psqlpy(ctx: "FixtureCtx") -> "BaseQueueBackend":
    from sqlspec.adapters.psqlpy import PsqlpyConfig

    svc = cast("PostgresService", ctx.service)
    assert svc is not None
    return _sqlspec_backend(
        PsqlpyConfig(
            connection_config={
                "host": svc.host,
                "port": svc.port,
                "username": svc.user,
                "password": svc.password,
                "db_name": svc.database,
            }
        ),
        table_name=ctx.table_name,
    )


async def _build_cockroach_asyncpg(ctx: "FixtureCtx") -> "BaseQueueBackend":
    from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig

    svc = cast("CockroachService", ctx.service)
    assert svc is not None
    return _sqlspec_backend(
        CockroachAsyncpgConfig(
            connection_config={
                "host": svc.host,
                "port": svc.port,
                "user": "root",
                "password": "",
                "database": svc.database,
                "ssl": False,
            }
        ),
        table_name=ctx.table_name,
    )


async def _build_cockroach_psycopg(ctx: "FixtureCtx") -> "BaseQueueBackend":
    from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncConfig

    svc = cast("CockroachService", ctx.service)
    assert svc is not None
    conninfo = f"postgresql://root@{svc.host}:{svc.port}/{svc.database}?sslmode=disable"
    return _sqlspec_backend(
        CockroachPsycopgAsyncConfig(connection_config={"conninfo": conninfo}), table_name=ctx.table_name
    )


async def _build_mysql_asyncmy(ctx: "FixtureCtx") -> "BaseQueueBackend":
    from sqlspec.adapters.asyncmy import AsyncmyConfig

    svc = cast("MySQLService", ctx.service)
    assert svc is not None
    return _sqlspec_backend(
        AsyncmyConfig(
            connection_config={
                "host": svc.host,
                "port": svc.port,
                "user": svc.user,
                "password": svc.password,
                "database": svc.db,
            }
        ),
        table_name=ctx.table_name,
    )


async def _build_mysql_aiomysql(ctx: "FixtureCtx") -> "BaseQueueBackend":
    from sqlspec.adapters.aiomysql import AiomysqlConfig

    svc = cast("MySQLService", ctx.service)
    assert svc is not None
    return _sqlspec_backend(
        AiomysqlConfig(
            connection_config={
                "host": svc.host,
                "port": svc.port,
                "user": svc.user,
                "password": svc.password,
                "db": svc.db,
            }
        ),
        table_name=ctx.table_name,
    )


async def _build_mysql_mysqlconnector(ctx: "FixtureCtx") -> "BaseQueueBackend":
    from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncConfig

    svc = cast("MySQLService", ctx.service)
    assert svc is not None
    return _sqlspec_backend(
        MysqlConnectorAsyncConfig(
            connection_config={
                "host": svc.host,
                "port": svc.port,
                "user": svc.user,
                "password": svc.password,
                "database": svc.db,
            }
        ),
        table_name=ctx.table_name,
    )


async def _build_oracle_oracledb(ctx: "FixtureCtx") -> "BaseQueueBackend":
    from sqlspec.adapters.oracledb import OracleAsyncConfig

    svc = cast("OracleService", ctx.service)
    assert svc is not None
    return _sqlspec_backend(
        OracleAsyncConfig(
            connection_config={
                "host": svc.host,
                "port": svc.port,
                "user": svc.user,
                "password": svc.password,
                "service_name": svc.service_name,
            }
        ),
        table_name=ctx.table_name,
    )


QUEUE_BACKENDS: "tuple[BackendCase, ...]" = (
    BackendCase("memory", frozenset(), None, _build_memory, frozenset({"in-process", "notify-direct"})),
    BackendCase(
        "aiosqlite",
        frozenset({"aiosqlite", "sqlspec"}),
        None,
        _build_aiosqlite,
        frozenset({"in-process", "polling-only", "json-text"}),
    ),
    BackendCase(
        "sqlite",
        frozenset({"sqlspec"}),
        None,
        _build_sqlite,
        frozenset({"in-process", "polling-only", "json-text", "sync-driver"}),
    ),
    BackendCase(
        "duckdb",
        frozenset({"duckdb", "sqlspec"}),
        None,
        _build_duckdb,
        frozenset({"in-process", "polling-only", "json-column", "sync-driver"}),
    ),
    BackendCase(
        "postgres-asyncpg",
        frozenset({"asyncpg", "sqlspec"}),
        "postgres_service",
        _build_postgres_asyncpg,
        frozenset({"listen-notify", "json-column"}),
    ),
    BackendCase(
        "postgres-psycopg",
        frozenset({"psycopg", "sqlspec"}),
        "postgres_service",
        _build_postgres_psycopg,
        frozenset({"listen-notify", "json-column"}),
    ),
    BackendCase(
        "postgres-psqlpy",
        frozenset({"psqlpy", "sqlspec"}),
        "postgres_service",
        _build_postgres_psqlpy,
        frozenset({"listen-notify", "json-column"}),
    ),
    BackendCase(
        "cockroach-asyncpg",
        frozenset({"asyncpg", "sqlspec"}),
        "cockroachdb_service",
        _build_cockroach_asyncpg,
        frozenset({"polling-only", "json-column"}),
    ),
    BackendCase(
        "cockroach-psycopg",
        frozenset({"psycopg", "sqlspec"}),
        "cockroachdb_service",
        _build_cockroach_psycopg,
        frozenset({"polling-only", "json-column"}),
    ),
    BackendCase(
        "mysql-asyncmy",
        frozenset({"asyncmy", "sqlspec"}),
        "mysql_service",
        _build_mysql_asyncmy,
        frozenset({"polling-only", "json-column"}),
    ),
    BackendCase(
        "mysql-aiomysql",
        frozenset({"aiomysql", "sqlspec"}),
        "mysql_service",
        _build_mysql_aiomysql,
        frozenset({"polling-only", "json-column"}),
    ),
    BackendCase(
        "mysql-mysqlconnector",
        frozenset({"mysql.connector", "sqlspec"}),
        "mysql_service",
        _build_mysql_mysqlconnector,
        frozenset({"polling-only", "json-column"}),
    ),
    BackendCase(
        "oracle-oracledb",
        frozenset({"oracledb", "sqlspec"}),
        "oracle_service",
        _build_oracle_oracledb,
        frozenset({"polling-only", "json-blob-checked", "blob-storage", "inmemory-capable"}),
    ),
)

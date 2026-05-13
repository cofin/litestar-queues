"""Integration-tier backend registry.

Single source of truth for queue-backend parametrize ids, optional-extra gating,
and async construction. Each ``BackendCase`` knows how to build its backend
from a ``FixtureCtx`` (pytest-databases service handles + ``tmp_path``).

The integration ``conftest.py`` consumes ``QUEUE_BACKENDS`` from ``pytest_generate_tests``
so any test that asks for the ``queue_backend`` fixture is auto-parametrized
across the registry. Per-adapter behavior gating uses ``case.capabilities``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from litestar_queues.backends import BaseQueueBackend


@dataclass(frozen=True, slots=True)
class FixtureCtx:
    """Per-test fixture context handed to a BackendCase builder."""

    tmp_path: Path
    postgres_service: Any | None = None
    mysql_service: Any | None = None
    mariadb_service: Any | None = None
    oracle_service: Any | None = None


@dataclass(frozen=True, slots=True)
class BackendCase:
    """One row in the parametrize matrix."""

    name: str
    extras: frozenset[str]
    service_attr: str | None
    build: Callable[[FixtureCtx], Awaitable["BaseQueueBackend"]]
    capabilities: frozenset[str]


# ---------------------------------------------------------------------------
# Builders. Each builder is async and returns a constructed-but-unopened
# backend. The fixture owns the open()/close() lifecycle.
# ---------------------------------------------------------------------------


async def _build_memory(ctx: FixtureCtx) -> "BaseQueueBackend":
    from litestar_queues.backends import InMemoryQueueBackend

    return InMemoryQueueBackend()


async def _build_aiosqlite(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    return SQLSpecQueueBackend(
        sqlspec_config=AiosqliteConfig(connection_config={"database": str(ctx.tmp_path / "queue-aiosqlite.db")})
    )


async def _build_sqlite(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.sqlite import SqliteConfig

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    return SQLSpecQueueBackend(
        sqlspec_config=SqliteConfig(connection_config={"database": str(ctx.tmp_path / "queue-sqlite.db")})
    )


async def _build_duckdb(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.duckdb import DuckDBConfig

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    return SQLSpecQueueBackend(
        sqlspec_config=DuckDBConfig(connection_config={"database": str(ctx.tmp_path / "queue-duckdb.db")})
    )


async def _build_adbc_sqlite(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.adbc import AdbcConfig

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    return SQLSpecQueueBackend(
        sqlspec_config=AdbcConfig(
            connection_config={"driver_name": "sqlite", "uri": str(ctx.tmp_path / "queue-adbc.db")}
        )
    )


async def _build_postgres_asyncpg(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    svc = ctx.postgres_service
    assert svc is not None
    return SQLSpecQueueBackend(
        sqlspec_config=AsyncpgConfig(
            connection_config={
                "host": svc.host,
                "port": svc.port,
                "user": svc.user,
                "password": svc.password,
                "database": svc.database,
            }
        )
    )


async def _build_postgres_psycopg(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    svc = ctx.postgres_service
    assert svc is not None
    return SQLSpecQueueBackend(
        sqlspec_config=PsycopgAsyncConfig(
            connection_config={
                "host": svc.host,
                "port": svc.port,
                "user": svc.user,
                "password": svc.password,
                "dbname": svc.database,
            }
        )
    )


async def _build_postgres_psqlpy(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.psqlpy import PsqlpyConfig

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    svc = ctx.postgres_service
    assert svc is not None
    return SQLSpecQueueBackend(
        sqlspec_config=PsqlpyConfig(
            connection_config={
                "host": svc.host,
                "port": svc.port,
                "username": svc.user,
                "password": svc.password,
                "db_name": svc.database,
            }
        )
    )


async def _build_mysql_asyncmy(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.asyncmy import AsyncmyConfig

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    svc = ctx.mysql_service
    assert svc is not None
    return SQLSpecQueueBackend(
        sqlspec_config=AsyncmyConfig(
            connection_config={
                "host": svc.host,
                "port": svc.port,
                "user": svc.user,
                "password": svc.password,
                "database": svc.db,
            }
        )
    )


async def _build_mysql_aiomysql(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.aiomysql import AiomysqlConfig

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    svc = ctx.mysql_service
    assert svc is not None
    return SQLSpecQueueBackend(
        sqlspec_config=AiomysqlConfig(
            connection_config={
                "host": svc.host,
                "port": svc.port,
                "user": svc.user,
                "password": svc.password,
                "db": svc.db,
            }
        )
    )


async def _build_mysql_pymysql(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.pymysql import PyMysqlConfig

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    svc = ctx.mysql_service
    assert svc is not None
    return SQLSpecQueueBackend(
        sqlspec_config=PyMysqlConfig(
            connection_config={
                "host": svc.host,
                "port": svc.port,
                "user": svc.user,
                "password": svc.password,
                "database": svc.db,
            }
        )
    )


async def _build_mysql_mysqlconnector(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncConfig

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    svc = ctx.mysql_service
    assert svc is not None
    return SQLSpecQueueBackend(
        sqlspec_config=MysqlConnectorAsyncConfig(
            connection_config={
                "host": svc.host,
                "port": svc.port,
                "user": svc.user,
                "password": svc.password,
                "database": svc.db,
            }
        )
    )


async def _build_oracle_oracledb(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.oracledb import OracleAsyncConfig

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    svc = ctx.oracle_service
    assert svc is not None
    return SQLSpecQueueBackend(
        sqlspec_config=OracleAsyncConfig(
            connection_config={
                "host": svc.host,
                "port": svc.port,
                "user": svc.user,
                "password": svc.password,
                "service_name": svc.service_name,
            }
        )
    )


# ``xfail-upstream`` is read by the integration conftest to mark cases that
# regress against known upstream issues (see bead litestar-queues-27b).
# When the upstream fixes land, drop the capability from the BackendCase to flip
# the case back to a hard-pass requirement.
QUEUE_BACKENDS: tuple[BackendCase, ...] = (
    BackendCase(
        "memory",
        frozenset(),
        None,
        _build_memory,
        frozenset({"in-process", "notify-direct"}),
    ),
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
        frozenset({"in-process", "polling-only", "json-column", "sync-driver", "xfail-upstream"}),
    ),
    BackendCase(
        "adbc-sqlite",
        frozenset({"adbc_driver_sqlite", "sqlspec"}),
        None,
        _build_adbc_sqlite,
        frozenset({"in-process", "polling-only", "json-text", "sync-driver", "xfail-upstream"}),
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
        "mysql-pymysql",
        frozenset({"pymysql", "sqlspec"}),
        "mysql_service",
        _build_mysql_pymysql,
        frozenset({"polling-only", "json-column", "sync-driver"}),
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

"""Integration-tier backend registry.

Single source of truth for queue-backend parametrize ids, optional-extra gating,
and async construction. Each ``BackendCase`` knows how to build its backend
from a ``FixtureCtx`` (pytest-databases service handles + ``tmp_path``).

The integration ``conftest.py`` consumes ``QUEUE_BACKENDS`` from ``pytest_generate_tests``
so any test that asks for the ``queue_backend`` fixture is auto-parametrized
across the registry. Per-adapter behavior gating uses ``case.capabilities``.
"""

from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    from litestar_queues.backends import BaseQueueBackend


class PostgresService(Protocol):
    """pytest-databases Postgres service attributes used by backend builders."""

    host: str
    port: int
    user: str
    password: str
    database: str


class MySQLService(Protocol):
    """pytest-databases MySQL service attributes used by backend builders."""

    host: str
    port: int
    user: str
    password: str
    db: str


class OracleService(Protocol):
    """pytest-databases Oracle service attributes used by backend builders."""

    host: str
    port: int
    user: str
    password: str
    service_name: str


class BigQueryService(Protocol):
    """pytest-databases BigQuery service attributes used by backend builders."""

    project: str
    dataset: str
    credentials: object
    client_options: object


class SpannerService(Protocol):
    """pytest-databases Spanner service attributes used by backend builders."""

    project: str
    instance_name: str
    database_name: str
    credentials: object
    client_options: object


class SpannerOperation(Protocol):
    """Synchronous Spanner emulator operation result used by the test bootstrap."""

    def result(self, timeout: int) -> object:
        """Wait for the operation to complete."""


class SpannerDatabase(Protocol):
    """Spanner database methods used by the test bootstrap."""

    def exists(self) -> bool:
        """Return whether the database exists."""

    def create(self) -> SpannerOperation:
        """Create the database."""


class SpannerInstance(Protocol):
    """Spanner instance methods used by the test bootstrap."""

    def exists(self) -> bool:
        """Return whether the instance exists."""

    def create(self) -> SpannerOperation:
        """Create the instance."""

    def database(self, database_name: str) -> SpannerDatabase:
        """Return a database handle."""


class SpannerClient(Protocol):
    """Spanner client methods used by the test bootstrap."""

    def instance(self, instance_id: str, *, configuration_name: str, display_name: str) -> SpannerInstance:
        """Return an instance handle."""

    def close(self) -> None:
        """Close the client."""


@dataclass(frozen=True, slots=True)
class FixtureCtx:
    """Per-test fixture context handed to a BackendCase builder."""

    tmp_path: Path
    service: object | None = None


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


def _sqlspec_backend(sqlspec_config: object) -> "BaseQueueBackend":
    """Return a SQLSpec backend configured through the typed config object."""
    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend

    return SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(sqlspec_config=sqlspec_config))


async def _build_aiosqlite(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    return _sqlspec_backend(AiosqliteConfig(connection_config={"database": str(ctx.tmp_path / "queue-aiosqlite.db")}))


async def _build_sqlite(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.sqlite import SqliteConfig

    return _sqlspec_backend(SqliteConfig(connection_config={"database": str(ctx.tmp_path / "queue-sqlite.db")}))


async def _build_duckdb(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.duckdb import DuckDBConfig

    return _sqlspec_backend(DuckDBConfig(connection_config={"database": str(ctx.tmp_path / "queue-duckdb.db")}))


async def _build_adbc_sqlite(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.adbc import AdbcConfig

    return _sqlspec_backend(
        AdbcConfig(connection_config={"driver_name": "sqlite", "uri": str(ctx.tmp_path / "queue-adbc.db")})
    )


async def _build_postgres_asyncpg(ctx: FixtureCtx) -> "BaseQueueBackend":
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
        )
    )


async def _build_postgres_psycopg(ctx: FixtureCtx) -> "BaseQueueBackend":
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
        )
    )


async def _build_postgres_psqlpy(ctx: FixtureCtx) -> "BaseQueueBackend":
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
        )
    )


async def _build_mysql_asyncmy(ctx: FixtureCtx) -> "BaseQueueBackend":
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
        )
    )


async def _build_mysql_aiomysql(ctx: FixtureCtx) -> "BaseQueueBackend":
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
        )
    )


async def _build_mysql_pymysql(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.pymysql import PyMysqlConfig

    svc = cast("MySQLService", ctx.service)
    assert svc is not None
    return _sqlspec_backend(
        PyMysqlConfig(
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
        )
    )


async def _build_oracle_oracledb(ctx: FixtureCtx) -> "BaseQueueBackend":
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
        )
    )


async def _build_bigquery(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.bigquery import BigQueryConfig

    svc = cast("BigQueryService", ctx.service)
    assert svc is not None
    return _sqlspec_backend(
        BigQueryConfig(
            connection_config={
                "project": svc.project,
                "dataset_id": svc.dataset,
                "credentials": svc.credentials,
                "client_options": svc.client_options,
                "use_query_cache": False,
            }
        )
    )


async def _build_spanner(ctx: FixtureCtx) -> "BaseQueueBackend":
    from sqlspec.adapters.spanner import SpannerSyncConfig

    svc = cast("SpannerService", ctx.service)
    assert svc is not None
    _ensure_spanner_database(svc)
    return _sqlspec_backend(
        SpannerSyncConfig(
            connection_config={
                "project": svc.project,
                "instance_id": svc.instance_name,
                "database_id": svc.database_name,
                "credentials": svc.credentials,
                "client_options": svc.client_options,
                "min_sessions": 1,
                "max_sessions": 2,
            }
        )
    )


def _ensure_spanner_database(service: SpannerService) -> None:
    """Create the Spanner emulator instance and database used by SQLSpec tests."""
    from google.api_core.exceptions import AlreadyExists
    from google.cloud.spanner import Client

    client = cast(
        "SpannerClient",
        Client(project=service.project, credentials=service.credentials, client_options=service.client_options),
    )
    try:
        instance = client.instance(
            service.instance_name, configuration_name="emulator-config", display_name=service.instance_name
        )
        if not instance.exists():
            with suppress(AlreadyExists):
                instance.create().result(timeout=30)
        database = instance.database(service.database_name)
        if not database.exists():
            with suppress(AlreadyExists):
                database.create().result(timeout=30)
    finally:
        client.close()


# ``skip-upstream`` / ``xfail-upstream`` are read by the integration conftest
# to mark cases that regress against known upstream issues (see bead
# litestar-queues-27b).
# When the upstream fixes land, drop the capability from the BackendCase to flip
# the case back to a hard-pass requirement.
QUEUE_BACKENDS: tuple[BackendCase, ...] = (
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
    BackendCase(
        "bigquery",
        frozenset({"google.cloud.bigquery", "sqlspec.adapters.bigquery"}),
        "bigquery_service",
        _build_bigquery,
        frozenset({"emulator", "polling-only", "json-column", "sync-driver", "skip-upstream"}),
    ),
    BackendCase(
        "spanner",
        frozenset({"google.cloud.spanner", "google.cloud.spanner_v1", "sqlspec.adapters.spanner"}),
        "spanner_service",
        _build_spanner,
        frozenset({"emulator", "polling-only", "json-column", "sync-driver", "skip-upstream"}),
    ),
)

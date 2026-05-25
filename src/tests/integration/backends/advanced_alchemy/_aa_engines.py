"""Advanced Alchemy engine registry for integration tests.

Mirrors ``tests.integration._backends`` but for ``SQLAlchemyAsyncConfig``
factories. Each ``AAEngineCase`` knows how to build a config from a
``FixtureCtx``; the subdir conftest wraps that config in
``AdvancedAlchemyBackendConfig(create_schema=True)`` and owns lifecycle.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from tests.integration._backends import FixtureCtx, MySQLService, OracleService, PostgresService

if TYPE_CHECKING:
    from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig


@dataclass(frozen=True, slots=True)
class AAEngineCase:
    """One Advanced Alchemy engine row in the parametrize matrix."""

    name: str
    extras: frozenset[str]
    service_attr: str | None
    build_config: Callable[[FixtureCtx], "SQLAlchemyAsyncConfig"]
    capabilities: frozenset[str]


# ---------------------------------------------------------------------------
# Config factories. Each returns an unopened SQLAlchemyAsyncConfig; the
# fixture wraps it in AdvancedAlchemyBackendConfig(create_schema=True) and
# runs open()/close().
# ---------------------------------------------------------------------------


def _config_aiosqlite(ctx: FixtureCtx) -> "SQLAlchemyAsyncConfig":
    from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig

    return SQLAlchemyAsyncConfig(
        connection_string=f"sqlite+aiosqlite:///{ctx.tmp_path / 'queue-aa-aiosqlite.db'}",
    )


def _config_postgres_asyncpg(ctx: FixtureCtx) -> "SQLAlchemyAsyncConfig":
    from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig

    svc = cast("PostgresService", ctx.service)
    assert svc is not None
    return SQLAlchemyAsyncConfig(
        connection_string=f"postgresql+asyncpg://{svc.user}:{svc.password}@{svc.host}:{svc.port}/{svc.database}",
    )


def _config_mysql_asyncmy(ctx: FixtureCtx) -> "SQLAlchemyAsyncConfig":
    from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig

    svc = cast("MySQLService", ctx.service)
    assert svc is not None
    return SQLAlchemyAsyncConfig(
        connection_string=f"mysql+asyncmy://{svc.user}:{svc.password}@{svc.host}:{svc.port}/{svc.db}",
    )


def _config_oracle_oracledb(ctx: FixtureCtx) -> "SQLAlchemyAsyncConfig":
    from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig

    svc = cast("OracleService", ctx.service)
    assert svc is not None
    return SQLAlchemyAsyncConfig(
        connection_string=(
            f"oracle+oracledb_async://{svc.user}:{svc.password}@{svc.host}:{svc.port}/?service_name={svc.service_name}"
        ),
    )


# ``xfail-upstream`` is honored by the integration conftest the same way
# Ch.3 honors it: cases tagged with the capability are wrapped in
# pytest.param(..., marks=xfail). When upstream fixes land, drop the flag.
AA_ENGINES: tuple[AAEngineCase, ...] = (
    AAEngineCase(
        "aa-aiosqlite",
        frozenset({"aiosqlite", "sqlalchemy", "advanced_alchemy"}),
        None,
        _config_aiosqlite,
        frozenset({"in-process", "json-text"}),
    ),
    AAEngineCase(
        "aa-postgres-asyncpg",
        frozenset({"asyncpg", "sqlalchemy", "advanced_alchemy"}),
        "postgres_service",
        _config_postgres_asyncpg,
        frozenset({"json-column"}),
    ),
    AAEngineCase(
        "aa-mysql-asyncmy",
        frozenset({"asyncmy", "sqlalchemy", "advanced_alchemy"}),
        "mysql_service",
        _config_mysql_asyncmy,
        frozenset({"json-column"}),
    ),
    AAEngineCase(
        "aa-oracle-oracledb",
        frozenset({"oracledb", "sqlalchemy", "advanced_alchemy"}),
        "oracle_service",
        _config_oracle_oracledb,
        frozenset({"json-blob-checked", "blob-storage"}),
    ),
)


__all__ = ("AA_ENGINES", "AAEngineCase")

"""SQLSpec backend configuration."""

from dataclasses import dataclass

from litestar_queues.backends.sqlspec._typing import SQLFileLoaderT, SQLSpecConfigT, SQLSpecPluginT, SQLSpecT
from litestar_queues.backends.sqlspec.schema import DEFAULT_TABLE_NAME

__all__ = ("SQLSpecBackendConfig",)


@dataclass(slots=True)
class SQLSpecBackendConfig:
    """Configuration values for SQLSpec queue storage."""

    sqlspec: SQLSpecT | None = None
    sqlspec_config: SQLSpecConfigT | None = None
    sqlspec_plugin: SQLSpecPluginT | None = None
    table_name: str = DEFAULT_TABLE_NAME
    create_schema: bool = True
    run_migrations: bool = False
    register_plugin: bool = False
    loader: SQLFileLoaderT | None = None

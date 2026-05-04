"""SQLSpec backend configuration."""

from dataclasses import dataclass

from litestar_queues.backends.sqlspec.schema import DEFAULT_TABLE_NAME

__all__ = ("SQLSpecBackendConfig",)


@dataclass(slots=True)
class SQLSpecBackendConfig:
    """Configuration values for the SQLSpec queue backend."""

    table_name: str = DEFAULT_TABLE_NAME
    create_schema: bool = True
    run_migrations: bool = False

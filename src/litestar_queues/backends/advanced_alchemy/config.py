"""Advanced Alchemy backend configuration."""

from dataclasses import dataclass
from importlib.resources import files
from typing import Any

from litestar_queues.backends.advanced_alchemy._typing import AsyncSessionMakerT, SQLAlchemyAsyncConfigT
from litestar_queues.exceptions import QueueConfigurationError

__all__ = (
    "DEFAULT_TABLE_NAME",
    "AdvancedAlchemyBackendConfig",
    "build_alembic_config",
    "migration_script_location",
    "validate_table_name",
)

DEFAULT_TABLE_NAME = "litestar_queue_tasks"


@dataclass(slots=True)
class AdvancedAlchemyBackendConfig:
    """Configuration values for the Advanced Alchemy queue backend."""

    sqlalchemy_config: SQLAlchemyAsyncConfigT | None = None
    session_maker: AsyncSessionMakerT | None = None
    table_name: str = DEFAULT_TABLE_NAME
    create_schema: bool = False
    run_migrations: bool = False


def validate_table_name(table_name: str) -> str:
    """Validate a simple SQL table identifier."""
    if not table_name.replace("_", "").isalnum() or table_name[0].isdigit():
        msg = f"Invalid Advanced Alchemy queue table name: {table_name!r}"
        raise QueueConfigurationError(msg)
    return table_name


def migration_script_location() -> str:
    """Return the packaged Advanced Alchemy migration script location."""
    return str(files("litestar_queues.backends.advanced_alchemy").joinpath("migrations"))


def build_alembic_config(script_config: str = "alembic.ini") -> Any:
    """Build an Advanced Alchemy Alembic config pointing at packaged queue migrations."""
    try:
        from advanced_alchemy.extensions.litestar import AlembicAsyncConfig
    except ModuleNotFoundError as exc:
        from litestar_queues.backends.advanced_alchemy._typing import missing_advanced_alchemy_error

        raise missing_advanced_alchemy_error(exc) from exc
    return AlembicAsyncConfig(script_config=script_config, script_location=migration_script_location())

"""Native SQLAlchemy schema helpers for Advanced Alchemy integration tests."""

from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig
    from sqlalchemy import Table


class MappedModel(Protocol):
    """Minimal protocol for a mapped model with a SQLAlchemy table."""

    __table__: "Table"


async def create_tables(config: "SQLAlchemyAsyncConfig", *models: "type[object]") -> "None":
    """Create selected tables through SQLAlchemy's native table lifecycle."""
    engine = config.get_engine()
    async with engine.begin() as connection:
        for model in models:
            table = cast("MappedModel", model).__table__
            await connection.run_sync(table.create, checkfirst=True)

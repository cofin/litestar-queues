"""Advanced Alchemy backend configuration."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from advanced_alchemy.config.asyncio import SQLAlchemyAsyncConfig
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

__all__ = ("AdvancedAlchemyBackendConfig",)


@dataclass(slots=True)
class AdvancedAlchemyBackendConfig:
    """Configuration values for the Advanced Alchemy queue backend."""

    backend_name: "ClassVar[str]" = "advanced-alchemy"
    sqlalchemy_config: "SQLAlchemyAsyncConfig | None" = None
    heartbeat_session_maker: "async_sessionmaker[AsyncSession] | None" = None
    model_class: "type[Any] | None" = None
    create_schema: "bool" = False

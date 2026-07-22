"""Advanced Alchemy backend configuration."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from advanced_alchemy.config.asyncio import SQLAlchemyAsyncConfig
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

__all__ = ("SQLAlchemyBackendConfig",)


def _default_model_class() -> "type[object]":
    from litestar_queues.backends.advanced_alchemy.models import QueueTaskModel

    return QueueTaskModel


def _default_event_log_model_class() -> "type[object]":
    from litestar_queues.backends.advanced_alchemy.models import QueueEventLogModel

    return QueueEventLogModel


def _default_maintenance_lease_model_class() -> "type[object]":
    from litestar_queues.backends.advanced_alchemy.models import QueueMaintenanceLeaseModel

    return QueueMaintenanceLeaseModel


@dataclass(slots=True)
class SQLAlchemyBackendConfig:
    """Configuration values for the SQLAlchemy queue backend."""

    backend_name: "ClassVar[str]" = "advanced-alchemy"
    sqlalchemy_config: "SQLAlchemyAsyncConfig | None" = None
    heartbeat_session_maker: "async_sessionmaker[AsyncSession] | None" = None
    model_class: "type[object] | None" = field(default_factory=_default_model_class)
    event_log_model_class: "type[object] | None" = field(default_factory=_default_event_log_model_class)
    maintenance_lease_model_class: "type[object] | None" = field(default_factory=_default_maintenance_lease_model_class)
    notifications: "bool" = False
    notification_channel: "str" = "litestar_queues_tasks"
    event_poll_interval: "float | None" = None

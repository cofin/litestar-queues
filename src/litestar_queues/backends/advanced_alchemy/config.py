"""Advanced Alchemy backend configuration."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

from litestar_queues.backends.advanced_alchemy.models import (
    QueueEventHistoryModel,
    QueueMaintenanceModel,
    QueueTaskModel,
    QueueTaskReservationModel,
)

if TYPE_CHECKING:
    from advanced_alchemy.config.asyncio import SQLAlchemyAsyncConfig
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

__all__ = ("SQLAlchemyBackendConfig",)


def _default_model_class() -> "type[object]":
    return QueueTaskModel


def _default_event_history_model_class() -> "type[object]":
    return QueueEventHistoryModel


def _default_maintenance_model_class() -> "type[object]":
    return QueueMaintenanceModel


def _default_task_reservation_model_class() -> "type[object]":
    return QueueTaskReservationModel


@dataclass(slots=True)
class SQLAlchemyBackendConfig:
    """Configuration values for the SQLAlchemy queue backend."""

    backend_name: "ClassVar[str]" = "advanced-alchemy"
    sqlalchemy_config: "SQLAlchemyAsyncConfig | None" = None
    """Advanced Alchemy async configuration; ``None`` requires an explicit session maker."""

    heartbeat_session_maker: "async_sessionmaker[AsyncSession] | None" = None
    """Dedicated heartbeat session factory; ``None`` reuses the configured database path."""

    model_class: "type[object] | None" = field(default_factory=_default_model_class)
    """Queue-task ORM model; ``None`` disables package-managed task persistence."""

    event_history_model_class: "type[object] | None" = field(default_factory=_default_event_history_model_class)
    """Task-event history ORM model; ``None`` disables history support."""

    maintenance_model_class: "type[object] | None" = field(default_factory=_default_maintenance_model_class)
    """Maintenance coordination ORM model; ``None`` disables maintenance support."""

    task_reservation_model_class: "type[object] | None" = field(default_factory=_default_task_reservation_model_class)
    """Permanent task-reservation ORM model; ``None`` disables durable reservations."""

    worker_wakeups: "bool" = False
    """Whether workers listen for database wakeup hints between polling passes."""

    wakeup_channel: "str" = "litestar_queues_tasks"
    """Database notification channel used for worker wakeup hints."""

    wakeup_poll_interval: "float | None" = None
    """Wakeup-listener fallback poll interval in seconds; ``None`` uses the backend default."""

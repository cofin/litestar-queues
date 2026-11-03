"""Composing custom Advanced Alchemy queue ORM models using model mixins."""

from advanced_alchemy.base import UUIDAuditBase
from advanced_alchemy.config.asyncio import SQLAlchemyAsyncConfig

from litestar_queues import QueueConfig, WorkerConfig
from litestar_queues.backends.advanced_alchemy import (
    QueueEventHistoryModelMixin,
    QueueMaintenanceModelMixin,
    QueueTaskModelMixin,
    QueueTaskReservationModelMixin,
    SQLAlchemyBackendConfig,
)


class AppQueueTask(UUIDAuditBase, QueueTaskModelMixin):
    """Custom queue task table integrated into the application's declarative base."""

    __tablename__ = "app_queue_task"


class AppQueueEventHistory(UUIDAuditBase, QueueEventHistoryModelMixin):
    """Custom durable event-history table."""

    __tablename__ = "app_queue_task_event_history"


class AppQueueMaintenance(UUIDAuditBase, QueueMaintenanceModelMixin):
    """Custom distributed maintenance lease table."""

    __tablename__ = "app_queue_maintenance"


class AppQueueTaskReservation(UUIDAuditBase, QueueTaskReservationModelMixin):
    """Custom permanent forever-uniqueness reservation table."""

    __tablename__ = "app_queue_task_reservation"


async def main() -> None:
    """Verify SQLAlchemyBackendConfig binds custom ORM model classes."""
    alchemy_config = SQLAlchemyAsyncConfig(connection_string="sqlite+aiosqlite:///:memory:")
    backend_config = SQLAlchemyBackendConfig(
        sqlalchemy_config=alchemy_config,
        model_class=AppQueueTask,
        event_history_model_class=AppQueueEventHistory,
        maintenance_model_class=AppQueueMaintenance,
        task_reservation_model_class=AppQueueTaskReservation,
        worker_wakeups=False,
    )
    config = QueueConfig(
        queue_backend=backend_config, execution_backend="local", worker=WorkerConfig(placement="external")
    )
    assert backend_config.model_class is AppQueueTask
    assert config.worker.placement == "external"

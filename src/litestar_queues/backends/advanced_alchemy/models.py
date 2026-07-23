"""Advanced Alchemy queue models."""

from advanced_alchemy.base import UUIDAuditBase

from litestar_queues.backends.advanced_alchemy.mixins import (
    QueueEventHistoryModelMixin,
    QueueMaintenanceModelMixin,
    QueueTaskModelMixin,
    QueueTaskReservationModelMixin,
)

__all__ = ("QueueEventHistoryModel", "QueueMaintenanceModel", "QueueTaskModel", "QueueTaskReservationModel")


class QueueTaskModel(UUIDAuditBase, QueueTaskModelMixin):
    """Default queue task model for the Advanced Alchemy backend."""

    __tablename__ = "queue_task"


class QueueEventHistoryModel(UUIDAuditBase, QueueEventHistoryModelMixin):
    """Default queue event-history model for the Advanced Alchemy backend."""

    __tablename__ = "queue_task_event_history"


class QueueMaintenanceModel(UUIDAuditBase, QueueMaintenanceModelMixin):
    """Default distributed maintenance coordination model for the Advanced Alchemy backend."""

    __tablename__ = "queue_maintenance"


class QueueTaskReservationModel(UUIDAuditBase, QueueTaskReservationModelMixin):
    """Default forever-uniqueness reservation model for the Advanced Alchemy backend."""

    __tablename__ = "queue_task_reservation"

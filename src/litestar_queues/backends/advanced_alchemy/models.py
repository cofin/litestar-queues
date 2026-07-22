"""Advanced Alchemy queue models."""

from advanced_alchemy.base import UUIDAuditBase

from litestar_queues.backends.advanced_alchemy.mixins import (
    QueueEventLogModelMixin,
    QueueMaintenanceLeaseModelMixin,
    QueueTaskModelMixin,
    QueueUniquenessModelMixin,
)

__all__ = ("QueueEventLogModel", "QueueMaintenanceLeaseModel", "QueueTaskModel", "QueueUniquenessModel")


class QueueTaskModel(UUIDAuditBase, QueueTaskModelMixin):
    """Default queue task model for the Advanced Alchemy backend."""

    __tablename__ = "litestar_queue_task"


class QueueEventLogModel(UUIDAuditBase, QueueEventLogModelMixin):
    """Default queue event-history model for the Advanced Alchemy backend."""

    __tablename__ = "litestar_queue_task_event_log"


class QueueMaintenanceLeaseModel(UUIDAuditBase, QueueMaintenanceLeaseModelMixin):
    """Default distributed maintenance-lease model for the Advanced Alchemy backend."""

    __tablename__ = "litestar_queue_maintenance_lease"


class QueueUniquenessModel(UUIDAuditBase, QueueUniquenessModelMixin):
    """Default forever-uniqueness tombstone model for the Advanced Alchemy backend."""

    __tablename__ = "litestar_queue_task_uniqueness"

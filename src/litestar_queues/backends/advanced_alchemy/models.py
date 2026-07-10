"""Advanced Alchemy queue models."""

from advanced_alchemy.base import UUIDAuditBase

from litestar_queues.backends.advanced_alchemy.mixins import QueueEventLogModelMixin, QueueTaskModelMixin

__all__ = ("QueueEventLogModel", "QueueTaskModel")


class QueueTaskModel(UUIDAuditBase, QueueTaskModelMixin):
    """Default queue task model for the Advanced Alchemy backend."""

    __tablename__ = "litestar_queue_task"


class QueueEventLogModel(UUIDAuditBase, QueueEventLogModelMixin):
    """Default queue event-history model for the Advanced Alchemy backend."""

    __tablename__ = "litestar_queue_task_event_log"

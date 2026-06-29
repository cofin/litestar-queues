"""Advanced Alchemy queue models."""

from advanced_alchemy.base import UUIDAuditBase

from litestar_queues.backends.advanced_alchemy.mixins import QueueTaskModelMixin

__all__ = ("QueueTaskModel",)


class QueueTaskModel(UUIDAuditBase, QueueTaskModelMixin):
    """Default queue task model for the Advanced Alchemy backend."""

    __tablename__ = "litestar_queue_task"

"""Advanced Alchemy queue task repository."""

from advanced_alchemy.repository import SQLAlchemyAsyncRepository

from litestar_queues.backends.advanced_alchemy.models import QueueTaskModel

__all__ = ("QueueTaskRepository",)


class QueueTaskRepository(SQLAlchemyAsyncRepository[QueueTaskModel]):
    """Repository for queue task records."""

    model_type = QueueTaskModel

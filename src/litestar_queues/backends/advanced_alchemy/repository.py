"""Advanced Alchemy queue task repository."""

from litestar_queues.backends.advanced_alchemy._typing import missing_advanced_alchemy_error
from litestar_queues.backends.advanced_alchemy.models import QueueTaskModel

try:
    from advanced_alchemy.repository import SQLAlchemyAsyncRepository
except ModuleNotFoundError as exc:
    raise missing_advanced_alchemy_error(exc) from exc

__all__ = ("QueueTaskRepository",)


class QueueTaskRepository(SQLAlchemyAsyncRepository[QueueTaskModel]):
    """Repository for queue task records."""

    model_type = QueueTaskModel

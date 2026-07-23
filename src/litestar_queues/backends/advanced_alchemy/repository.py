"""Advanced Alchemy queue task repository."""

from typing import TYPE_CHECKING, Any, cast

from advanced_alchemy.repository import SQLAlchemyAsyncRepository

if TYPE_CHECKING:
    from litestar_queues.backends.advanced_alchemy.mixins import (
        QueueEventHistoryModelMixin,
        QueueTaskModelMixin,
        QueueTaskReservationModelMixin,
    )

__all__ = ("QueueEventLogRepository", "QueueTaskRepository", "QueueTaskReservationRepository")


class QueueTaskRepository(SQLAlchemyAsyncRepository[Any]):
    """Repository for queue task records."""

    @classmethod
    def for_model(cls, model_class: "type[QueueTaskModelMixin]") -> 'type["QueueTaskRepository"]':
        """Return a repository subclass bound to ``model_class``."""
        return cast(
            "type[QueueTaskRepository]",
            type(f"QueueTaskRepositoryFor{model_class.__name__}", (cls,), {"model_type": model_class}),
        )


class QueueEventLogRepository(SQLAlchemyAsyncRepository[Any]):
    """Repository for queue event-history records."""

    @classmethod
    def for_model(cls, model_class: "type[QueueEventHistoryModelMixin]") -> 'type["QueueEventLogRepository"]':
        """Return a repository subclass bound to ``model_class``."""
        return cast(
            "type[QueueEventLogRepository]",
            type(f"QueueEventLogRepositoryFor{model_class.__name__}", (cls,), {"model_type": model_class}),
        )


class QueueTaskReservationRepository(SQLAlchemyAsyncRepository[Any]):
    """Repository for forever-uniqueness reservations."""

    @classmethod
    def for_model(cls, model_class: "type[QueueTaskReservationModelMixin]") -> 'type["QueueTaskReservationRepository"]':
        """Return a repository subclass bound to ``model_class``."""
        return cast(
            "type[QueueTaskReservationRepository]",
            type(f"QueueTaskReservationRepositoryFor{model_class.__name__}", (cls,), {"model_type": model_class}),
        )

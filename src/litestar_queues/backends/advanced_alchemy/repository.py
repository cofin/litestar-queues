"""Advanced Alchemy queue task repository."""

from typing import TYPE_CHECKING, Any, cast

from advanced_alchemy.repository import SQLAlchemyAsyncRepository

if TYPE_CHECKING:
    from litestar_queues.backends.advanced_alchemy.mixins import (
        QueueEventLogModelMixin,
        QueueTaskModelMixin,
        QueueUniquenessModelMixin,
    )

__all__ = ("QueueEventLogRepository", "QueueTaskRepository", "QueueUniquenessRepository")


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
    def for_model(cls, model_class: "type[QueueEventLogModelMixin]") -> 'type["QueueEventLogRepository"]':
        """Return a repository subclass bound to ``model_class``."""
        return cast(
            "type[QueueEventLogRepository]",
            type(f"QueueEventLogRepositoryFor{model_class.__name__}", (cls,), {"model_type": model_class}),
        )


class QueueUniquenessRepository(SQLAlchemyAsyncRepository[Any]):
    """Repository for forever-uniqueness tombstones."""

    @classmethod
    def for_model(cls, model_class: "type[QueueUniquenessModelMixin]") -> 'type["QueueUniquenessRepository"]':
        """Return a repository subclass bound to ``model_class``."""
        return cast(
            "type[QueueUniquenessRepository]",
            type(f"QueueUniquenessRepositoryFor{model_class.__name__}", (cls,), {"model_type": model_class}),
        )

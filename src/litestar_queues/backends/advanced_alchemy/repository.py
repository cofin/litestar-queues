"""Advanced Alchemy queue task repository."""

from typing import TYPE_CHECKING, Any, cast

from advanced_alchemy.repository import SQLAlchemyAsyncRepository

if TYPE_CHECKING:
    from litestar_queues.backends.advanced_alchemy.mixins import QueueTaskModelMixin

__all__ = ("QueueTaskRepository",)


class QueueTaskRepository(SQLAlchemyAsyncRepository[Any]):
    """Repository for queue task records."""

    @classmethod
    def for_model(cls, model_class: "type[QueueTaskModelMixin]") -> 'type["QueueTaskRepository"]':
        """Return a repository subclass bound to ``model_class``."""
        return cast(
            "type[QueueTaskRepository]",
            type(f"QueueTaskRepositoryFor{model_class.__name__}", (cls,), {"model_type": model_class}),
        )

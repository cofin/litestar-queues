"""Advanced Alchemy queue task repository."""

from typing import Any, cast

from advanced_alchemy.repository import SQLAlchemyAsyncRepository

__all__ = ("QueueTaskRepository",)


class QueueTaskRepository(SQLAlchemyAsyncRepository[Any]):
    """Repository for queue task records."""

    @classmethod
    def for_model(cls, model_class: type[Any]) -> type["QueueTaskRepository"]:
        """Return a repository subclass bound to ``model_class``."""
        return cast(
            "type[QueueTaskRepository]",
            type(f"QueueTaskRepositoryFor{model_class.__name__}", (cls,), {"model_type": model_class}),
        )

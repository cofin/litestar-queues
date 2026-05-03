from typing import TYPE_CHECKING

from typing_extensions import Self

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig

__all__ = ("BaseExecutionBackend",)


class BaseExecutionBackend:
    """Base class for queue execution backends."""

    __slots__ = ("config",)

    def __init__(self, config: "QueueConfig | None" = None) -> None:
        """Initialize the execution backend."""
        self.config = config

    async def open(self) -> bool:
        """Open execution resources.

        Returns:
            True when resources are ready.
        """
        return True

    async def close(self) -> None:
        """Close execution resources."""

    async def execute(self, task_name: str, *args: object, **kwargs: object) -> object:
        """Execute a task.

        Full execution behavior is implemented in later chapters.
        """
        message = "Queue execution behavior lands in Chapter 2."
        raise NotImplementedError(message)

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        await self.close()

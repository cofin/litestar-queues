from typing import TYPE_CHECKING

from typing_extensions import Self

if TYPE_CHECKING:
    from litestar_queues.backends import BaseStorageBackend
    from litestar_queues.config import QueueConfig
    from litestar_queues.execution import BaseExecutionBackend

__all__ = ("QueueService",)


class QueueService:
    """High-level facade for queue storage and execution backends."""

    __slots__ = ("_config", "_execution_backend", "_storage_backend")

    def __init__(self, config: "QueueConfig") -> None:
        """Initialize the queue service.

        Args:
            config: Queue configuration.
        """
        self._config = config
        self._storage_backend: BaseStorageBackend | None = None
        self._execution_backend: BaseExecutionBackend | None = None

    @property
    def config(self) -> "QueueConfig":
        """Return the queue configuration."""
        return self._config

    def get_storage_backend(self) -> "BaseStorageBackend":
        """Return the configured storage backend."""
        if self._storage_backend is not None:
            return self._storage_backend
        return self._config.get_storage_backend()

    def get_execution_backend(self) -> "BaseExecutionBackend":
        """Return the configured execution backend."""
        if self._execution_backend is not None:
            return self._execution_backend
        return self._config.get_execution_backend()

    async def enqueue(self, task_name: str, *args: object, **kwargs: object) -> object:
        """Enqueue a task.

        Full queue runtime behavior is intentionally deferred to Chapter 2.
        """
        message = "Queue runtime behavior lands in Chapter 2."
        raise NotImplementedError(message)

    async def __aenter__(self) -> Self:
        self._storage_backend = self._config.get_storage_backend()
        self._execution_backend = self._config.get_execution_backend()
        await self._storage_backend.open()
        await self._execution_backend.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if self._execution_backend is not None:
            await self._execution_backend.close()
            self._execution_backend = None
        if self._storage_backend is not None:
            await self._storage_backend.close()
            self._storage_backend = None

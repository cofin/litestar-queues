from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from litestar.datastructures import State

    from litestar_queues.backends import BaseStorageBackend
    from litestar_queues.execution import BaseExecutionBackend
    from litestar_queues.service import QueueService

__all__ = (
    "AsyncServiceProvider",
    "ExecutionBackendConfig",
    "QueueConfig",
    "StorageBackendConfig",
)

StorageBackendConfig = str
"""Type alias for storage backend configuration values."""

ExecutionBackendConfig = str
"""Type alias for execution backend configuration values."""


class AsyncServiceProvider:
    """Provides QueueService as an async context manager."""

    __slots__ = ("_config", "_service")

    def __init__(self, config: "QueueConfig") -> None:
        """Initialize the service provider.

        Args:
            config: Queue configuration.
        """
        self._config = config
        self._service: QueueService | None = None

    async def __aenter__(self) -> "QueueService":
        """Enter the async context and return a QueueService.

        Returns:
            A managed QueueService instance.
        """
        from litestar_queues.service import QueueService

        self._service = QueueService(self._config)
        await self._service.__aenter__()
        return self._service

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Exit the async context and close the QueueService."""
        if self._service is not None:
            await self._service.__aexit__(exc_type, exc_val, exc_tb)
            self._service = None

    async def __aiter__(self) -> AsyncIterator["QueueService"]:
        """Yield a managed QueueService for Litestar dependency injection."""
        async with self as service:
            yield service


@dataclass(slots=True)
class QueueConfig:
    """Configuration for QueuePlugin.

    Chapter 1 keeps runtime behavior intentionally small while preserving the
    extension points used by later storage and execution backends.
    """

    storage_backend: StorageBackendConfig = "memory"
    execution_backend: ExecutionBackendConfig = "immediate"
    start_worker: bool = False
    queue_service_dependency_key: str = "queue_service"
    queue_service_state_key: str = "queue_service"
    queue_worker_state_key: str = "queue_worker"
    task_modules: tuple[str, ...] = ()
    initialize_schedules: bool = True
    worker_batch_size: int = 10
    worker_poll_interval: float = 0.1

    @property
    def signature_namespace(self) -> dict[str, Any]:
        """Return names added to Litestar's signature namespace."""
        from litestar_queues.backends import BaseStorageBackend, InMemoryStorageBackend
        from litestar_queues.execution import BaseExecutionBackend, ImmediateExecutionBackend, LocalExecutionBackend
        from litestar_queues.models import QueuedTaskRecord
        from litestar_queues.service import QueueService
        from litestar_queues.task import ScheduleConfig, Task, TaskResult
        from litestar_queues.worker import Worker

        return {
            "BaseExecutionBackend": BaseExecutionBackend,
            "BaseStorageBackend": BaseStorageBackend,
            "ImmediateExecutionBackend": ImmediateExecutionBackend,
            "InMemoryStorageBackend": InMemoryStorageBackend,
            "LocalExecutionBackend": LocalExecutionBackend,
            "QueueConfig": QueueConfig,
            "QueuedTaskRecord": QueuedTaskRecord,
            "QueueService": QueueService,
            "ScheduleConfig": ScheduleConfig,
            "Task": Task,
            "TaskResult": TaskResult,
            "Worker": Worker,
        }

    @property
    def dependencies(self) -> dict[str, Any]:
        """Return dependency providers for Litestar's DI system."""
        from litestar.di import Provide

        return {self.queue_service_dependency_key: Provide(self.provide_service, sync_to_thread=False)}

    def get_service(self, state: "State | None" = None) -> "QueueService":
        """Return a QueueService for this configuration."""
        from litestar_queues.service import QueueService

        if state is not None and self.queue_service_state_key in state:
            cached = state[self.queue_service_state_key]
            if isinstance(cached, QueueService):
                return cached
            if isinstance(cached, QueueConfig):
                return QueueService(cached)

        return QueueService(self)

    def get_storage_backend(self) -> "BaseStorageBackend":
        """Return a configured storage backend instance."""
        from litestar_queues.backends import get_storage_backend

        return get_storage_backend(self.storage_backend, config=self)

    def get_execution_backend(self) -> "BaseExecutionBackend":
        """Return a configured execution backend instance."""
        from litestar_queues.execution import get_execution_backend

        return get_execution_backend(self.execution_backend, config=self)

    def provide_service(self) -> AsyncServiceProvider:
        """Provide a QueueService instance as an async context manager.

        Returns:
            An async service provider.
        """
        return AsyncServiceProvider(self)

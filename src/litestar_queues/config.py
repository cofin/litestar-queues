from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from litestar_queues.events import QueueEventConfig

if TYPE_CHECKING:
    from litestar.datastructures import State

    from litestar_queues.backends import BaseQueueBackend
    from litestar_queues.events import QueueEventPublisher
    from litestar_queues.execution import BaseExecutionBackend
    from litestar_queues.service import QueueService

__all__ = (
    "AsyncServiceProvider",
    "ExecutionBackendConfig",
    "QueueBackendConfig",
    "QueueConfig",
    "QueueEventConfig",
)

QueueBackendConfig = str
"""Type alias for queue backend configuration values."""

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
    extension points used by later queue and execution backends.
    """

    queue_backend: QueueBackendConfig = "memory"
    queue_backend_config: dict[str, Any] = field(default_factory=dict)
    execution_backend: ExecutionBackendConfig = "immediate"
    execution_backend_config: dict[str, Any] = field(default_factory=dict)
    start_worker: bool = False
    queue_service_dependency_key: str = "queue_service"
    queue_service_state_key: str = "queue_service"
    queue_worker_state_key: str = "queue_worker"
    queue_event_publisher_state_key: str = "queue_event_publisher"
    queue_event_channels_backend_state_key: str = "queue_event_channels_backend"
    event_config: QueueEventConfig = field(default_factory=QueueEventConfig)
    task_modules: tuple[str, ...] = ()
    initialize_schedules: bool = True
    worker_batch_size: int = 10
    worker_poll_interval: float = 0.1
    worker_max_concurrency: int = 1
    worker_heartbeat_interval: float = 30
    worker_reconcile_interval: float = 30
    worker_stale_after: float | None = None
    worker_graceful_shutdown_timeout: float = 30
    worker_final_cancel_timeout: float = 5

    @property
    def signature_namespace(self) -> dict[str, Any]:
        """Return names added to Litestar's signature namespace."""
        from litestar_queues.backends import (
            AdvancedAlchemyQueueBackend,
            BaseQueueBackend,
            InMemoryQueueBackend,
            SQLSpecQueueBackend,
        )
        from litestar_queues.events import (
            InMemoryQueueEventSink,
            NoopQueueEventSink,
            QueueChannels,
            QueueEvent,
            QueueEventActor,
            QueueEventConfig,
            QueueEventEntityRef,
            QueueEventPublisher,
            TaskExecutionContext,
        )
        from litestar_queues.exceptions import NonRetryableError, non_retryable
        from litestar_queues.execution import (
            BaseExecutionBackend,
            CloudRunExecutionBackend,
            CloudRunExecutionConfig,
            CloudRunExecutionStatus,
            ImmediateExecutionBackend,
            LocalExecutionBackend,
        )
        from litestar_queues.models import QueueBackendCapabilities, QueuedTaskRecord, QueueStatistics
        from litestar_queues.service import QueueService
        from litestar_queues.task import ScheduleConfig, Task, TaskResult
        from litestar_queues.worker import Worker

        return {
            "BaseExecutionBackend": BaseExecutionBackend,
            "BaseQueueBackend": BaseQueueBackend,
            "AdvancedAlchemyQueueBackend": AdvancedAlchemyQueueBackend,
            "CloudRunExecutionBackend": CloudRunExecutionBackend,
            "CloudRunExecutionConfig": CloudRunExecutionConfig,
            "CloudRunExecutionStatus": CloudRunExecutionStatus,
            "ImmediateExecutionBackend": ImmediateExecutionBackend,
            "InMemoryQueueBackend": InMemoryQueueBackend,
            "LocalExecutionBackend": LocalExecutionBackend,
            "NonRetryableError": NonRetryableError,
            "NoopQueueEventSink": NoopQueueEventSink,
            "InMemoryQueueEventSink": InMemoryQueueEventSink,
            "QueueChannels": QueueChannels,
            "QueueConfig": QueueConfig,
            "QueueBackendCapabilities": QueueBackendCapabilities,
            "QueueEvent": QueueEvent,
            "QueueEventActor": QueueEventActor,
            "QueueEventConfig": QueueEventConfig,
            "QueueEventEntityRef": QueueEventEntityRef,
            "QueueEventPublisher": QueueEventPublisher,
            "QueuedTaskRecord": QueuedTaskRecord,
            "QueueService": QueueService,
            "QueueStatistics": QueueStatistics,
            "ScheduleConfig": ScheduleConfig,
            "SQLSpecQueueBackend": SQLSpecQueueBackend,
            "Task": Task,
            "TaskExecutionContext": TaskExecutionContext,
            "TaskResult": TaskResult,
            "Worker": Worker,
            "non_retryable": non_retryable,
        }

    @property
    def dependencies(self) -> dict[str, Any]:
        """Return dependency providers for Litestar's DI system."""
        from litestar.di import Provide

        return {self.queue_service_dependency_key: Provide(self.provide_service_dependency)}

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

    def get_queue_backend(self) -> "BaseQueueBackend":
        """Return a configured queue backend instance."""
        from litestar_queues.backends import get_queue_backend

        return get_queue_backend(self.queue_backend, config=self)

    def get_execution_backend(self) -> "BaseExecutionBackend":
        """Return a configured execution backend instance."""
        from litestar_queues.execution import get_execution_backend

        return get_execution_backend(self.execution_backend, config=self)

    def get_event_publisher(self) -> "QueueEventPublisher":
        """Return a configured queue event publisher."""
        from litestar_queues.events import (
            ChannelsQueueEventSink,
            NoopQueueEventSink,
            QueueEventPublisher,
            QueueEventSink,
        )

        event_config = self.event_config
        sink: QueueEventSink
        if not event_config.enabled:
            sink = NoopQueueEventSink()
        elif event_config.sink is not None:
            sink = event_config.sink
        elif event_config.channels_backend is not None:
            sink = ChannelsQueueEventSink(event_config.channels_backend)
        else:
            sink = NoopQueueEventSink()
        return QueueEventPublisher(
            sink,
            strict=event_config.strict,
            publish_task_channel=event_config.publish_task_channel,
            publish_queue_channel=event_config.publish_queue_channel,
            publish_global_lifecycle=event_config.publish_global_lifecycle,
        )

    def provide_service(self) -> AsyncServiceProvider:
        """Provide a QueueService instance as an async context manager.

        Returns:
            An async service provider.
        """
        return AsyncServiceProvider(self)

    async def provide_service_dependency(self) -> AsyncIterator["QueueService"]:
        """Yield a managed QueueService for Litestar dependency injection."""
        async with self.provide_service() as service:
            yield service

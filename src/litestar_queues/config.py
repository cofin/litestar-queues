from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

from litestar.di import Provide

from litestar_queues.events import QueueEventConfig

logger = getLogger(__name__)

if TYPE_CHECKING:
    from types import TracebackType

    from litestar.datastructures import State

    from litestar_queues.backends import BaseQueueBackend
    from litestar_queues.events import QueueEventPublisher, TaskExecutionContext
    from litestar_queues.execution import BaseExecutionBackend
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.service import QueueService
    from litestar_queues.task import Task

__all__ = (
    "AsyncServiceProvider",
    "ExecutionBackendConfig",
    "ExecutionBackendConfigProtocol",
    "QueueBackendConfig",
    "QueueBackendConfigProtocol",
    "QueueConfig",
    "QueueEventConfig",
    "TaskDependencyResolver",
    "execution_backend_name",
    "queue_backend_name",
)


class QueueBackendConfigProtocol(Protocol):
    """Protocol for typed queue backend configuration objects."""

    backend_name: "ClassVar[str]"


class ExecutionBackendConfigProtocol(Protocol):
    """Protocol for typed execution backend configuration objects."""

    backend_name: "ClassVar[str]"


QueueBackendConfig = str | QueueBackendConfigProtocol
"""Type alias for queue backend selectors."""

ExecutionBackendConfig = str | ExecutionBackendConfigProtocol
"""Type alias for execution backend selectors."""


def queue_backend_name(backend: "QueueBackendConfig") -> "str":
    """Return the registered queue backend name for a selector."""
    return backend if isinstance(backend, str) else backend.backend_name


def execution_backend_name(backend: "ExecutionBackendConfig") -> "str":
    """Return the registered execution backend name for a selector."""
    return backend if isinstance(backend, str) else backend.backend_name


TaskDependencyResolver = Callable[
    ["Task[..., object]", "QueuedTaskRecord", "TaskExecutionContext"], Awaitable[Mapping[str, object]]
]
"""User-supplied callable that resolves extra kwargs for a task before execution."""


class AsyncServiceProvider:
    """Provides QueueService as an async context manager."""

    __slots__ = ("_config", "_service")

    def __init__(self, config: "QueueConfig") -> "None":
        """Initialize the service provider.

        Args:
            config: Queue configuration.
        """
        self._config = config
        self._service: "QueueService | None" = None

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
        exc_type: "type[BaseException] | None",  # noqa: PYI036
        exc_val: "BaseException | None",  # noqa: PYI036
        exc_tb: "TracebackType | None",  # noqa: PYI036
    ) -> "None":
        """Exit the async context and close the QueueService."""
        if self._service is not None:
            await self._service.__aexit__(exc_type, exc_val, exc_tb)
            self._service = None

    async def __aiter__(self) -> 'AsyncIterator["QueueService"]':
        """Yield a managed QueueService for Litestar dependency injection.

        Yields:
            Managed queue service instance.
        """
        service = await self.__aenter__()
        try:
            yield service
        except BaseException as exc:
            await self.__aexit__(type(exc), exc, exc.__traceback__)
            raise
        else:
            await self.__aexit__(None, None, None)


@dataclass(slots=True)
class QueueConfig:
    """Configuration for QueuePlugin."""

    queue_backend: "QueueBackendConfig" = "memory"
    execution_backend: "ExecutionBackendConfig" = "local"
    task_dependency_resolver: "TaskDependencyResolver | None" = None
    in_app_worker: "bool" = True
    queue_service_dependency_key: "str" = "queue_service"
    queue_service_state_key: "str" = "queue_service"
    queue_worker_state_key: "str" = "queue_worker"
    queue_event_publisher_state_key: "str" = "queue_event_publisher"
    queue_event_channels_backend_state_key: "str" = "queue_event_channels_backend"
    event_config: "QueueEventConfig" = field(default_factory=QueueEventConfig)
    task_modules: "tuple[str, ...]" = ()
    initialize_schedules: "bool" = True
    worker_batch_size: "int" = 10
    worker_poll_interval: "float" = 0.1
    worker_max_concurrency: "int" = 1
    worker_heartbeat_interval: "float" = 30
    worker_reconcile_interval: "float" = 30
    worker_stale_after: "float | None" = None
    worker_stale_check_interval: "float" = 60.0
    worker_graceful_shutdown_timeout: "float" = 30
    worker_final_cancel_timeout: "float" = 5
    worker_queues: "tuple[str, ...]" = ()
    sync_executor_max_workers: "int | None" = None
    sync_executor_thread_name_prefix: "str" = "litestar-queues"
    scheduler_canary_task: "str" = "scheduler.heartbeat"

    @property
    def signature_namespace(self) -> "dict[str, Any]":
        """Names added to Litestar's signature namespace.

        Optional backends (advanced_alchemy, sqlspec, redis, valkey) are added
        only when their driver extra is installed; missing extras silently drop
        the corresponding entries.
        """
        from litestar.di import NamedDependency

        from litestar_queues.backends import BaseQueueBackend, InMemoryQueueBackend
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
        from litestar_queues.models import (
            QueueBackendCapabilities,
            QueuedTaskRecord,
            QueueStatistics,
            StaleTaskRecoveryResult,
        )
        from litestar_queues.service import QueueService
        from litestar_queues.task import ScheduleConfig, Task, TaskResult
        from litestar_queues.worker import Worker

        namespace: "dict[str, Any]" = {
            "BaseExecutionBackend": BaseExecutionBackend,
            "BaseQueueBackend": BaseQueueBackend,
            "CloudRunExecutionBackend": CloudRunExecutionBackend,
            "CloudRunExecutionConfig": CloudRunExecutionConfig,
            "CloudRunExecutionStatus": CloudRunExecutionStatus,
            "ImmediateExecutionBackend": ImmediateExecutionBackend,
            "InMemoryQueueBackend": InMemoryQueueBackend,
            "LocalExecutionBackend": LocalExecutionBackend,
            "NamedDependency": NamedDependency,
            "NonRetryableError": NonRetryableError,
            "NoopQueueEventSink": NoopQueueEventSink,
            "InMemoryQueueEventSink": InMemoryQueueEventSink,
            "ExecutionBackendConfig": ExecutionBackendConfig,
            "QueueChannels": QueueChannels,
            "QueueConfig": QueueConfig,
            "QueueBackendCapabilities": QueueBackendCapabilities,
            "QueueBackendConfig": QueueBackendConfig,
            "QueueBackendConfigProtocol": QueueBackendConfigProtocol,
            "QueueEvent": QueueEvent,
            "QueueEventActor": QueueEventActor,
            "QueueEventConfig": QueueEventConfig,
            "QueueEventEntityRef": QueueEventEntityRef,
            "QueueEventPublisher": QueueEventPublisher,
            "QueuedTaskRecord": QueuedTaskRecord,
            "QueueService": QueueService,
            "QueueStatistics": QueueStatistics,
            "ScheduleConfig": ScheduleConfig,
            "StaleTaskRecoveryResult": StaleTaskRecoveryResult,
            "Task": Task,
            "TaskDependencyResolver": TaskDependencyResolver,
            "ExecutionBackendConfigProtocol": ExecutionBackendConfigProtocol,
            "TaskExecutionContext": TaskExecutionContext,
            "TaskResult": TaskResult,
            "Worker": Worker,
            "non_retryable": non_retryable,
        }
        with suppress(ImportError):
            from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyQueueBackend

            namespace["AdvancedAlchemyQueueBackend"] = AdvancedAlchemyQueueBackend
        with suppress(ImportError):
            from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

            namespace["SQLSpecQueueBackend"] = SQLSpecQueueBackend
        with suppress(ImportError):
            from litestar_queues.backends.redis import RedisBackendConfig, RedisQueueBackend

            namespace["RedisBackendConfig"] = RedisBackendConfig
            namespace["RedisQueueBackend"] = RedisQueueBackend
        with suppress(ImportError):
            from litestar_queues.backends.valkey import ValkeyBackendConfig, ValkeyQueueBackend

            namespace["ValkeyBackendConfig"] = ValkeyBackendConfig
            namespace["ValkeyQueueBackend"] = ValkeyQueueBackend
        return namespace

    @property
    def dependencies(self) -> "dict[str, Any]":
        """Dependency providers for Litestar's DI system."""
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
        sink: "QueueEventSink"
        if not event_config.enabled:
            if event_config.sink is not None or event_config.channels_backend is not None:
                logger.warning(
                    "Queue event sink configured while event publishing is disabled; "
                    "set QueueEventConfig(enabled=True) to publish queue events."
                )
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

    def provide_service(self) -> "AsyncServiceProvider":
        """Provide a QueueService instance as an async context manager.

        Returns:
            An async service provider.
        """
        return AsyncServiceProvider(self)

    async def provide_service_dependency(self, state: "State") -> 'AsyncIterator["QueueService"]':
        """Yield the application-scoped QueueService for Litestar dependency injection."""
        yield self.get_service(state)

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

from litestar_queues.events import EventConfig, EventLogConfig, EventStreamConfig, QueueEventProducer

logger = getLogger(__name__)

if TYPE_CHECKING:
    from types import TracebackType

    from litestar.datastructures import State

    from litestar_queues.backends import BaseQueueBackend
    from litestar_queues.events import QueueEventPublisher, TaskExecutionContext
    from litestar_queues.execution import BaseExecutionBackend
    from litestar_queues.maintenance import QueueMaintenanceConfig
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.observability import ObservabilityConfig
    from litestar_queues.service import QueueService
    from litestar_queues.task import Task
    from litestar_queues.typing import ChannelsLike

__all__ = (
    "AsyncServiceProvider",
    "EventConfig",
    "EventLogConfig",
    "EventStreamConfig",
    "ExecutionBackendConfig",
    "ExecutionBackendConfigProtocol",
    "QueueBackendConfig",
    "QueueBackendConfigProtocol",
    "QueueConfig",
    "TaskDependencyResolver",
    "TaskErrorSanitizer",
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

TaskErrorSanitizer = Callable[["BaseException", "QueuedTaskRecord"], str]
"""User-supplied callable that converts task exceptions into persisted error messages."""


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
    error_sanitizer: "TaskErrorSanitizer | None" = None
    in_app_worker: "bool" = True
    queue_service_dependency_key: "str" = "queue_service"
    queue_events_dependency_key: "str" = "queue_events"
    queue_service_state_key: "str" = "queue_service"
    queue_worker_state_key: "str" = "queue_worker"
    queue_event_publisher_state_key: "str" = "queue_event_publisher"
    queue_event_channels_backend_state_key: "str" = "queue_event_channels_backend"
    queue_event_stream_state_key: "str" = "queue_event_stream"
    queue_observability_runtime_state_key: "str" = "queue_observability_runtime"
    event: "EventConfig | None" = None
    event_stream: "EventStreamConfig | None" = None
    observability: "ObservabilityConfig | None" = None
    task_modules: "tuple[str, ...]" = ()
    initialize_schedules: "bool" = True
    quiet_success: "bool" = True
    worker_batch_size: "int" = 10
    worker_poll_interval: "float" = 0.1
    worker_poll_backoff_max: "float | None" = 30.0
    worker_poll_backoff_multiplier: "float" = 2.0
    worker_poll_jitter: "float" = 0.15
    worker_max_concurrency: "int" = 1
    worker_heartbeat_interval: "float" = 30
    worker_heartbeat_miss_threshold: "int" = 2
    worker_reconcile_interval: "float" = 30
    worker_stale_after: "float | None" = None
    worker_stale_check_interval: "float" = 60.0
    worker_graceful_shutdown_timeout: "float" = 30
    worker_final_cancel_timeout: "float" = 5
    worker_queues: "tuple[str, ...]" = ()
    sync_executor_max_workers: "int | None" = None
    sync_executor_thread_name_prefix: "str" = "litestar-queues"
    scheduler_canary_task: "str" = "scheduler.heartbeat"
    event_log: "EventLogConfig | None" = None
    maintenance: "QueueMaintenanceConfig | None" = None

    def __post_init__(self) -> "None":
        """Validate adaptive polling backoff settings before backend/worker startup.

        Raises:
            QueueConfigurationError: If a backoff field is set to an invalid value.
        """
        from litestar_queues.exceptions import QueueConfigurationError

        if self.worker_poll_backoff_max is not None:
            if self.worker_poll_backoff_max <= 0:
                msg = "QueueConfig.worker_poll_backoff_max must be greater than 0."
                raise QueueConfigurationError(msg)
            if self.worker_poll_backoff_max < self.worker_poll_interval:
                msg = "QueueConfig.worker_poll_backoff_max must be greater than or equal to worker_poll_interval."
                raise QueueConfigurationError(msg)
        if self.worker_poll_backoff_multiplier < 1.0:
            msg = "QueueConfig.worker_poll_backoff_multiplier must be greater than or equal to 1.0."
            raise QueueConfigurationError(msg)
        if not 0.0 <= self.worker_poll_jitter <= 1.0:
            msg = "QueueConfig.worker_poll_jitter must be between 0.0 and 1.0, inclusive."
            raise QueueConfigurationError(msg)

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
            EventBufferConfig,
            EventConfig,
            EventLogConfig,
            EventStreamConfig,
            InMemoryQueueEventSink,
            NoopQueueEventSink,
            QueueChannels,
            QueueEvent,
            QueueEventActor,
            QueueEventEntityRef,
            QueueEventLog,
            QueueEventLogRecord,
            QueueEventProducer,
            QueueEventPublisher,
            QueueEventStageSummary,
            TaskExecutionContext,
        )
        from litestar_queues.exceptions import JobCancelledError, NonRetryableError, job_cancelled, non_retryable
        from litestar_queues.execution import (
            BaseExecutionBackend,
            CloudRunExecutionBackend,
            CloudRunExecutionConfig,
            CloudRunExecutionStatus,
            ImmediateExecutionBackend,
            LocalExecutionBackend,
        )
        from litestar_queues.maintenance import (
            QueueMaintenanceConfig,
            QueueMaintenancePhaseResult,
            QueueMaintenanceService,
            QueueMaintenanceSummary,
        )
        from litestar_queues.models import (
            QueueBackendCapabilities,
            QueuedTaskRecord,
            QueueStatistics,
            StaleTaskRecoveryResult,
        )
        from litestar_queues.observability import ObservabilityConfig
        from litestar_queues.service import QueueService
        from litestar_queues.task import ScheduleConfig, Task, TaskResult
        from litestar_queues.worker import Worker

        namespace: "dict[str, Any]" = {
            "BaseExecutionBackend": BaseExecutionBackend,
            "BaseQueueBackend": BaseQueueBackend,
            "CloudRunExecutionBackend": CloudRunExecutionBackend,
            "CloudRunExecutionConfig": CloudRunExecutionConfig,
            "CloudRunExecutionStatus": CloudRunExecutionStatus,
            "EventConfig": EventConfig,
            "EventBufferConfig": EventBufferConfig,
            "EventLogConfig": EventLogConfig,
            "EventStreamConfig": EventStreamConfig,
            "ImmediateExecutionBackend": ImmediateExecutionBackend,
            "InMemoryQueueBackend": InMemoryQueueBackend,
            "LocalExecutionBackend": LocalExecutionBackend,
            "NamedDependency": NamedDependency,
            "ObservabilityConfig": ObservabilityConfig,
            "JobCancelledError": JobCancelledError,
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
            "QueueEventEntityRef": QueueEventEntityRef,
            "QueueEventLog": QueueEventLog,
            "QueueEventLogRecord": QueueEventLogRecord,
            "QueueEventProducer": QueueEventProducer,
            "QueueEventPublisher": QueueEventPublisher,
            "QueueEventStageSummary": QueueEventStageSummary,
            "QueueMaintenanceConfig": QueueMaintenanceConfig,
            "QueueMaintenancePhaseResult": QueueMaintenancePhaseResult,
            "QueueMaintenanceService": QueueMaintenanceService,
            "QueueMaintenanceSummary": QueueMaintenanceSummary,
            "QueuedTaskRecord": QueuedTaskRecord,
            "QueueService": QueueService,
            "QueueStatistics": QueueStatistics,
            "ScheduleConfig": ScheduleConfig,
            "StaleTaskRecoveryResult": StaleTaskRecoveryResult,
            "Task": Task,
            "TaskDependencyResolver": TaskDependencyResolver,
            "TaskErrorSanitizer": TaskErrorSanitizer,
            "ExecutionBackendConfigProtocol": ExecutionBackendConfigProtocol,
            "TaskExecutionContext": TaskExecutionContext,
            "TaskResult": TaskResult,
            "Worker": Worker,
            "job_cancelled": job_cancelled,
            "non_retryable": non_retryable,
        }
        with suppress(ImportError):
            from litestar_queues.backends.advanced_alchemy import SQLAlchemyBackend

            namespace["SQLAlchemyBackend"] = SQLAlchemyBackend
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
        from litestar.di import Provide

        return {
            self.queue_service_dependency_key: Provide(self.provide_service_dependency),
            self.queue_events_dependency_key: Provide(self.provide_event_producer_dependency),
        }

    def get_service(self, state: "State | None" = None) -> "QueueService":
        """Return a QueueService for this configuration."""
        from litestar_queues.service import QueueService

        if state is None:
            return QueueService(self)

        if self.queue_service_state_key not in state:
            msg = (
                f"QueueService is not available in app state under {self.queue_service_state_key!r}; "
                "ensure QueuePlugin startup has completed before resolving the queue service."
            )
            raise RuntimeError(msg)

        cached = state[self.queue_service_state_key]
        if isinstance(cached, QueueService):
            return cached

        msg = (
            f"QueueService has not been opened in app state under {self.queue_service_state_key!r}; "
            f"found {type(cached).__name__}."
        )
        raise RuntimeError(msg)

    def get_queue_backend(self) -> "BaseQueueBackend":
        """Return a configured queue backend instance."""
        from litestar_queues.backends import get_queue_backend

        return get_queue_backend(self.queue_backend, config=self)

    def get_execution_backend(self) -> "BaseExecutionBackend":
        """Return a configured execution backend instance."""
        from litestar_queues.execution import get_execution_backend

        return get_execution_backend(self.execution_backend, config=self)

    def get_event_publisher(self, *, channels_backend: "ChannelsLike | None" = None) -> "QueueEventPublisher":
        """Return a configured queue event publisher.

        Args:
            channels_backend: Fallback live sink target used only when
                ``EventConfig.channels_backend`` is unset. ``QueuePlugin`` passes the
                app's registered ``ChannelsPlugin`` here so ``EventConfig(enabled=True)``
                needs no manual channel wiring.
        """
        from litestar_queues.events import (
            ChannelsQueueEventSink,
            NoopQueueEventSink,
            QueueEventPublisher,
            QueueEventSink,
        )

        event_config = self.event
        sink: "QueueEventSink"
        if event_config is None:
            sink = NoopQueueEventSink()
            return QueueEventPublisher(sink)
        if not event_config.enabled:
            if event_config.sink is not None or event_config.channels_backend is not None:
                logger.warning(
                    "Queue event sink configured while event publishing is explicitly disabled; "
                    "omit enabled=False to publish queue events."
                )
            sink = NoopQueueEventSink()
        elif event_config.sink is not None:
            sink = event_config.sink
        else:
            live_backend = event_config.channels_backend
            if live_backend is None:
                live_backend = channels_backend
            if live_backend is not None:
                sink = ChannelsQueueEventSink(
                    live_backend,
                    max_payload_bytes=event_config.max_payload_bytes,
                    payload_size_estimator=event_config.payload_size_estimator,
                )
            else:
                sink = NoopQueueEventSink()
        return QueueEventPublisher(
            sink,
            buffer_config=event_config.buffer,
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

    async def provide_event_producer_dependency(self, state: "State") -> "AsyncIterator[QueueEventProducer]":
        """Yield the application-scoped QueueEventProducer for Litestar dependency injection."""
        from litestar_queues.events import QueueEventProducer

        if self.queue_event_publisher_state_key not in state:
            msg = (
                "Queue event publisher is not available in app state under "
                f"{self.queue_event_publisher_state_key!r}; ensure QueuePlugin startup has completed before "
                "resolving queue events."
            )
            raise RuntimeError(msg)
        yield QueueEventProducer(state[self.queue_event_publisher_state_key])

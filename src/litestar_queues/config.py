from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol, get_args, runtime_checkable

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from types import TracebackType

    from litestar.datastructures import State

    from litestar_queues.backends import BaseQueueBackend
    from litestar_queues.events import QueueEventProducer, QueueEventPublisher, QueueEventsConfig, TaskExecutionContext
    from litestar_queues.execution import BaseExecutionBackend
    from litestar_queues.maintenance import QueueMaintenanceConfig
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.observability import ObservabilityConfig
    from litestar_queues.service import QueueService
    from litestar_queues.task import Task
    from litestar_queues.typing import ChannelsLike

__all__ = (
    "AsyncServiceProvider",
    "ExecutionBackendConfig",
    "ExecutionBackendConfigProtocol",
    "MigrationConfiguringBackend",
    "QueueBackendConfig",
    "QueueBackendConfigProtocol",
    "QueueConfig",
    "TaskDependencyResolver",
    "TaskErrorSanitizer",
    "WorkerConfig",
    "WorkerPlacement",
    "execution_backend_name",
    "queue_backend_name",
)

logger = getLogger(__name__)

_SERVICE_STATE_KEY = "queue_service"
_WORKER_STATE_KEY = "queue_worker"
_EVENT_PUBLISHER_STATE_KEY = "queue_event_publisher"
_EVENT_CHANNELS_STATE_KEY = "queue_event_channels"
_OBSERVABILITY_RUNTIME_STATE_KEY = "queue_observability_runtime"


class QueueBackendConfigProtocol(Protocol):
    """Protocol for typed queue backend configuration objects."""

    backend_name: "ClassVar[str]"


@runtime_checkable
class MigrationConfiguringBackend(Protocol):
    """Backend config that registers its own migrations during plugin init.

    A backend owns whatever application wiring its storage needs. ``QueuePlugin``
    only asks whether the configured backend provides this hook, so selecting one
    backend never imports another backend's package -- or requires its extra to be
    installed.
    """

    def configure_migrations(self, config: "QueueConfig") -> "None":
        """Register backend-owned migrations with the application."""
        ...


class ExecutionBackendConfigProtocol(Protocol):
    """Protocol for typed execution backend configuration objects."""

    backend_name: "ClassVar[str]"


QueueBackendConfig = str | QueueBackendConfigProtocol
"""Type alias for queue backend selectors."""

ExecutionBackendConfig = str | ExecutionBackendConfigProtocol
"""Type alias for execution backend selectors."""

WorkerPlacement = Literal["server", "asgi", "external"]
"""Which process owns the queue worker.

``server``
    The Litestar CLI server lifespan owns exactly one fresh worker process per
    ``litestar run`` invocation. This is the default.
``asgi``
    Each ASGI worker owns one queue worker inside its own application lifespan.
    Deliberately multiplicative with the web-worker count.
``external``
    Nothing is started automatically; a separate process manager runs
    ``litestar queues run``, or the caller executes tasks inline.
"""

_PLACEMENTS: "tuple[str, ...]" = get_args(WorkerPlacement)
_MANAGED_PLACEMENTS = ("server", "asgi")


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
class WorkerConfig:
    """Configuration shared by in-app and standalone workers."""

    placement: "WorkerPlacement" = "server"
    """Which process owns this worker; see :data:`WorkerPlacement`."""

    id: "str | None" = None
    """Explicit worker identity; ``None`` uses a process-derived identifier."""

    batch_size: "int" = 10
    """Maximum task records claimed in one worker iteration."""

    poll_interval: "float" = 0.1
    """Base worker polling interval in seconds."""

    poll_backoff_max: "float | None" = 30.0
    """Maximum adaptive polling interval in seconds; ``None`` disables backoff."""

    poll_backoff_multiplier: "float" = 2.0
    """Multiplier applied after an empty polling iteration."""

    poll_jitter: "float" = 0.15
    """Symmetric polling jitter ratio from zero through one."""

    max_concurrency: "int" = 1
    """Maximum number of tasks executed concurrently."""

    heartbeat_interval: "float" = 30
    """Interval between bulk heartbeat writes in seconds."""

    heartbeat_miss_threshold: "int" = 2
    """Consecutive heartbeat misses tolerated before claim loss."""

    reconcile_interval: "float" = 30
    """Interval between external-execution reconciliation passes in seconds."""

    stale_after: "float | None" = None
    """Running-task age threshold in seconds; ``None`` disables stale recovery."""

    stale_check_interval: "float" = 60.0
    """Interval between stale-task recovery passes in seconds."""

    graceful_shutdown_timeout: "float" = 30
    """Maximum graceful drain time in seconds."""

    final_cancel_timeout: "float" = 5
    """Maximum post-cancellation drain time in seconds."""

    startup_timeout: "float" = 30
    """Maximum time to wait for worker startup readiness in seconds."""

    queues: "tuple[str, ...]" = ()
    """Queue names claimed by this worker; empty claims every queue."""

    def __post_init__(self) -> "None":
        """Validate worker placement, concurrency, intervals, and adaptive polling."""
        if self.placement not in _PLACEMENTS:
            msg = f"WorkerConfig.placement must be one of {_PLACEMENTS}, not {self.placement!r}."
            raise QueueConfigurationError(msg)
        positive = {
            "batch_size": self.batch_size,
            "poll_interval": self.poll_interval,
            "max_concurrency": self.max_concurrency,
            "heartbeat_interval": self.heartbeat_interval,
            "heartbeat_miss_threshold": self.heartbeat_miss_threshold,
            "reconcile_interval": self.reconcile_interval,
            "stale_check_interval": self.stale_check_interval,
            "graceful_shutdown_timeout": self.graceful_shutdown_timeout,
            "final_cancel_timeout": self.final_cancel_timeout,
            "startup_timeout": self.startup_timeout,
        }
        for name, value in positive.items():
            if value <= 0:
                msg = f"WorkerConfig.{name} must be greater than 0."
                raise QueueConfigurationError(msg)
        if self.poll_backoff_max is not None and self.poll_backoff_max < self.poll_interval:
            msg = "WorkerConfig.poll_backoff_max must be greater than or equal to poll_interval."
            raise QueueConfigurationError(msg)
        if self.poll_backoff_multiplier < 1.0:
            msg = "WorkerConfig.poll_backoff_multiplier must be greater than or equal to 1.0."
            raise QueueConfigurationError(msg)
        if not 0.0 <= self.poll_jitter <= 1.0:
            msg = "WorkerConfig.poll_jitter must be between 0.0 and 1.0, inclusive."
            raise QueueConfigurationError(msg)
        if self.stale_after is not None and self.stale_after <= 0:
            msg = "WorkerConfig.stale_after must be greater than 0 when set."
            raise QueueConfigurationError(msg)


@dataclass(slots=True)
class QueueConfig:
    """Configuration for QueuePlugin."""

    queue_backend: "QueueBackendConfig" = "ephemeral"
    """Queue-record persistence backend selector or typed backend configuration."""

    execution_backend: "ExecutionBackendConfig" = "local"
    """Default placement backend used to execute claimed tasks."""

    task_dependency_resolver: "TaskDependencyResolver | None" = None
    """Per-attempt dependency resolver; ``None`` injects no additional task keyword arguments."""

    error_sanitizer: "TaskErrorSanitizer | None" = None
    """Persisted task-error formatter; ``None`` stores the default exception representation."""

    worker: "WorkerConfig" = field(default_factory=WorkerConfig)
    """Shared configuration for in-app and standalone workers."""

    service_dependency_key: "str" = "queue_service"
    """Litestar dependency key for the injected queue service."""

    events_dependency_key: "str" = "queue_events"
    """Litestar dependency key for the injected queue event producer."""

    events: "QueueEventsConfig | None" = None
    """Task-event capabilities; ``None`` disables delivery, streams, and history."""

    observability: "ObservabilityConfig | None" = None
    """Package telemetry configuration; ``None`` disables the observability runtime."""

    task_modules: "tuple[str, ...]" = ()
    """Module names imported during startup to register decorated tasks."""

    initialize_schedules: "bool" = True
    """Whether application startup synchronizes registered recurring schedules."""

    log_success: "bool" = False
    """Whether successful task completion emits an informational log by default."""

    sync_thread_pool_size: "int" = 40
    """Maximum threads running synchronous tasks concurrently.

    Matches anyio's default thread limiter. Threads are created on demand, so this is a
    ceiling rather than a startup cost.
    """

    sync_thread_name_prefix: "str" = "litestar-queues"
    """Thread-name prefix for the synchronous task thread pool."""

    scheduler_canary_task: "str" = "scheduler.heartbeat"
    """Registered task name used by the scheduler-health command."""

    maintenance: "QueueMaintenanceConfig | None" = None
    """Automatic maintenance policy; ``None`` disables the maintenance loop."""

    max_argument_identity_bytes: "int | None" = None
    """Maximum canonical argument-identity size in bytes; ``None`` disables the bound."""

    def __post_init__(self) -> "None":
        """Validate the synchronous task thread pool and the placement matrix."""
        if self.sync_thread_pool_size <= 0:
            msg = "QueueConfig.sync_thread_pool_size must be greater than 0."
            raise QueueConfigurationError(msg)
        self._validate_placement()

    def _validate_placement(self) -> "None":
        """Reject storage, execution, and placement combinations that cannot work.

        Selectors are compared by registered name so validating a configuration
        never imports an optional backend or its driver extra.

        Raises:
            QueueConfigurationError: If the combination has no coherent owner.
        """
        backend = queue_backend_name(self.queue_backend)
        execution = execution_backend_name(self.execution_backend)
        placement = self.worker.placement

        if placement in _MANAGED_PLACEMENTS and execution == "immediate":
            msg = (
                f"execution_backend='immediate' runs tasks inline at enqueue time, so "
                f"placement={placement!r} would start a worker with nothing to claim. "
                f"Use execution_backend='local', or placement='external'."
            )
            raise QueueConfigurationError(msg)
        if backend == "ephemeral" and placement != "server":
            msg = (
                f"queue_backend='ephemeral' is created and removed by the Litestar CLI server "
                f"lifespan, so it is only available to placement='server', not {placement!r}. "
                f"Configure a persistent backend for {placement!r}."
            )
            raise QueueConfigurationError(msg)
        if backend == "ephemeral" and execution != "local":
            msg = f"queue_backend='ephemeral' supports execution_backend='local' only, not {execution!r}."
            raise QueueConfigurationError(msg)
        if backend == "memory" and placement == "server":
            msg = (
                "queue_backend='memory' is process-local, so a separate server-owned worker "
                "process could never see its records. Use the default queue_backend='ephemeral' "
                "for zero-setup background work, or a persistent backend."
            )
            raise QueueConfigurationError(msg)
        # memory + external is deliberately allowed: it describes an application
        # that enqueues into a process-local queue and starts nothing itself,
        # which is what inline callers and direct Worker tests do. Pointing the
        # standalone 'litestar queues run' command at it is the incoherent case,
        # and that command rejects process-local storage itself.

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
            ChannelAuthorizer,
            CompositeQueueEventSink,
            EventBufferConfig,
            EventDeliveryConfig,
            EventHistoryConfig,
            EventStreamConfig,
            EventStreamTransport,
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
            QueueEventsConfig,
            QueueEventScope,
            QueueEventSink,
            QueueEventStageSummary,
            QueueEventType,
            TaskExecutionContext,
            UnauthenticatedAccess,
        )
        from litestar_queues.exceptions import (
            JobCancelledError,
            NonRetryableError,
            TaskIdentityError,
            TaskIdentityTooLargeError,
            job_cancelled,
            non_retryable,
        )
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
            HeartbeatTouch,
            HeartbeatTouchResult,
            QueueBackendCapabilities,
            QueuedTaskRecord,
            QueueStatistics,
            StaleTaskRecoveryResult,
            TaskRequest,
            TaskReservation,
            TaskStatus,
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
            "ChannelAuthorizer": ChannelAuthorizer,
            "CompositeQueueEventSink": CompositeQueueEventSink,
            "EventDeliveryConfig": EventDeliveryConfig,
            "EventBufferConfig": EventBufferConfig,
            "EventHistoryConfig": EventHistoryConfig,
            "EventStreamConfig": EventStreamConfig,
            "EventStreamTransport": EventStreamTransport,
            "HeartbeatTouch": HeartbeatTouch,
            "HeartbeatTouchResult": HeartbeatTouchResult,
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
            "QueueEventScope": QueueEventScope,
            "QueueEventSink": QueueEventSink,
            "QueueEventStageSummary": QueueEventStageSummary,
            "QueueEventType": QueueEventType,
            "QueueEventsConfig": QueueEventsConfig,
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
            "TaskIdentityError": TaskIdentityError,
            "TaskIdentityTooLargeError": TaskIdentityTooLargeError,
            "TaskRequest": TaskRequest,
            "TaskReservation": TaskReservation,
            "TaskStatus": TaskStatus,
            "TaskDependencyResolver": TaskDependencyResolver,
            "TaskErrorSanitizer": TaskErrorSanitizer,
            "ExecutionBackendConfigProtocol": ExecutionBackendConfigProtocol,
            "TaskExecutionContext": TaskExecutionContext,
            "TaskResult": TaskResult,
            "Worker": Worker,
            "WorkerConfig": WorkerConfig,
            "WorkerPlacement": WorkerPlacement,
            "UnauthenticatedAccess": UnauthenticatedAccess,
            "job_cancelled": job_cancelled,
            "non_retryable": non_retryable,
        }
        with suppress(ImportError):
            from litestar_queues.backends.advanced_alchemy import SQLAlchemyBackend, SQLAlchemyBackendConfig

            namespace["SQLAlchemyBackend"] = SQLAlchemyBackend
            namespace["SQLAlchemyBackendConfig"] = SQLAlchemyBackendConfig
        with suppress(ImportError):
            from litestar_queues.backends.sqlspec import (
                SQLSpecBackendConfig,
                SQLSpecQueueBackend,
                SQLSpecWorkerWakeupConfig,
            )

            namespace["SQLSpecBackendConfig"] = SQLSpecBackendConfig
            namespace["SQLSpecQueueBackend"] = SQLSpecQueueBackend
            namespace["SQLSpecWorkerWakeupConfig"] = SQLSpecWorkerWakeupConfig
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
            self.service_dependency_key: Provide(self.provide_service_dependency),
            self.events_dependency_key: Provide(self.provide_event_producer_dependency),
        }

    def get_service(self, state: "State | None" = None) -> "QueueService":
        """Return a QueueService for this configuration."""
        from litestar_queues.service import QueueService

        if state is None:
            return QueueService(self)

        if _SERVICE_STATE_KEY not in state:
            msg = (
                f"QueueService is not available in app state under {_SERVICE_STATE_KEY!r}; "
                "ensure QueuePlugin startup has completed before resolving the queue service."
            )
            raise RuntimeError(msg)

        cached = state[_SERVICE_STATE_KEY]
        if isinstance(cached, QueueService):
            return cached

        msg = (
            f"QueueService has not been opened in app state under {_SERVICE_STATE_KEY!r}; "
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

    def get_event_publisher(
        self, *, channels_backend: "ChannelsLike | None" = None, manage_channels_lifecycle: "bool" = False
    ) -> "QueueEventPublisher":
        """Return a configured queue event publisher.

        Args:
            channels_backend: Fallback live sink target used only when
                ``QueueEventsConfig.channels`` is unset. ``QueuePlugin`` passes the
                app's registered ``ChannelsPlugin`` here so event delivery
                needs no manual channel wiring.
            manage_channels_lifecycle: Whether the publisher owns the resolved
                Channels target lifecycle.
        """
        from litestar_queues.events import (
            ChannelsQueueEventSink,
            CompositeQueueEventSink,
            NoopQueueEventSink,
            QueueEventPublisher,
            QueueEventSink,
        )

        events_config = self.events
        if events_config is None or events_config.delivery is None:
            sink: "QueueEventSink" = NoopQueueEventSink()
            return QueueEventPublisher(sink)
        delivery = events_config.delivery
        sinks: "list[QueueEventSink]" = []
        live_backend = events_config.channels if events_config.channels is not None else channels_backend
        if live_backend is not None:
            sinks.append(
                ChannelsQueueEventSink(
                    live_backend,
                    manage_lifecycle=manage_channels_lifecycle,
                    max_payload_bytes=delivery.max_payload_bytes,
                    payload_size_estimator=delivery.payload_size_estimator,
                )
            )
        sinks.extend(delivery.sinks)
        if not sinks:
            msg = "EventDeliveryConfig requires events.channels, an app ChannelsPlugin, or at least one custom sink."
            raise QueueConfigurationError(msg)
        sink = sinks[0] if len(sinks) == 1 else CompositeQueueEventSink(sinks, strict=delivery.strict)
        return QueueEventPublisher(
            sink,
            buffer_config=delivery.buffer,
            strict=delivery.strict,
            publish_task_channel=delivery.publish_task_channel,
            publish_queue_channel=delivery.publish_queue_channel,
            publish_global_lifecycle=delivery.publish_global_lifecycle,
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

        if _EVENT_PUBLISHER_STATE_KEY not in state:
            msg = (
                "Queue event publisher is not available in app state under "
                f"{_EVENT_PUBLISHER_STATE_KEY!r}; ensure QueuePlugin startup has completed before "
                "resolving queue events."
            )
            raise RuntimeError(msg)
        yield QueueEventProducer(state[_EVENT_PUBLISHER_STATE_KEY])

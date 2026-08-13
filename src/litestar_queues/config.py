import os
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol, cast, get_args, runtime_checkable

from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.namespace import DEFAULT_NAMESPACE, QueueNamespace

if TYPE_CHECKING:
    from litestar.datastructures import State

    from litestar_queues.backends import BaseQueueBackend
    from litestar_queues.events import QueueEventProducer, QueueEventPublisher, QueueEventsConfig, TaskExecutionContext
    from litestar_queues.events.typing import ChannelsLike
    from litestar_queues.execution import BaseExecutionBackend
    from litestar_queues.maintenance import QueueMaintenanceConfig
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.observability import ObservabilityConfig
    from litestar_queues.service import QueueService
    from litestar_queues.task import Task

__all__ = (
    "STALE_REQUEUE_PRIORITY",
    "ExecutionBackendConfig",
    "ExecutionBackendConfigProtocol",
    "MigrationConfiguringBackend",
    "QueueBackendConfig",
    "QueueBackendConfigProtocol",
    "QueueConfig",
    "QueueNamespace",
    "StaleRequeuePriority",
    "TaskDependencyResolver",
    "TaskErrorSanitizer",
    "WorkerConfig",
    "WorkerPlacement",
    "execution_backend_name",
    "queue_backend_name",
)

logger = getLogger(__name__)

STALE_REQUEUE_PRIORITY: "StaleRequeuePriority" = "preserve"
"""Default stale-requeue policy: recovered work keeps the priority it was enqueued with."""

_SERVICE_STATE_KEY = "queue_service"
_WORKER_STATE_KEY = "queue_worker"
_EVENT_PUBLISHER_STATE_KEY = "queue_event_publisher"
_EVENT_CHANNELS_STATE_KEY = "queue_event_channels"
_OBSERVABILITY_RUNTIME_STATE_KEY = "queue_observability_runtime"


def _cgroup_cpu_limit(
    *,
    cpu_max: "Path" = Path("/sys/fs/cgroup/cpu.max"),
    cpu_quota: "Path" = Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"),
    cpu_period: "Path" = Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us"),
) -> "int | None":
    """Return the rounded-up cgroup v2 or v1 CPU quota when available."""
    with suppress(OSError, ValueError):
        quota_text, period_text = cpu_max.read_text(encoding="utf-8").split()
        if quota_text != "max":
            quota = int(quota_text)
            period = int(period_text)
            if quota > 0 and period > 0:
                return max(1, (quota + period - 1) // period)
    with suppress(OSError, ValueError):
        quota = int(cpu_quota.read_text(encoding="utf-8"))
        period = int(cpu_period.read_text(encoding="utf-8"))
        if quota > 0 and period > 0:
            return max(1, (quota + period - 1) // period)
    return None


def _effective_cpu_count() -> "int":
    """Return CPUs usable by this process, including a Linux cgroup quota."""
    counts: "list[int]" = []
    process_cpu_count = cast("Callable[[], int | None] | None", getattr(os, "process_cpu_count", None))
    if process_cpu_count is not None:
        with suppress(OSError):
            count = process_cpu_count()
            if count is not None and count > 0:
                counts.append(count)
    sched_getaffinity = cast("Callable[[int], set[int]] | None", getattr(os, "sched_getaffinity", None))
    if sched_getaffinity is not None:
        with suppress(OSError):
            affinity_count = len(sched_getaffinity(0))
            if affinity_count > 0:
                counts.append(affinity_count)
    host_count = os.cpu_count()
    if host_count is not None and host_count > 0:
        counts.append(host_count)
    cgroup_count = _cgroup_cpu_limit()
    if cgroup_count is not None:
        counts.append(cgroup_count)
    return max(1, min(counts, default=1))


def _default_sync_thread_pool_size() -> "int":
    """Match the modern Python executor default using process-usable CPUs."""
    return min(32, _effective_cpu_count() + 4)


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

StaleRequeuePriority = Literal["preserve"] | int | Callable[[int], int]
"""Priority policy applied to work recovered by the stale sweep.

``"preserve"`` keeps the original priority, an ``int`` is a ceiling clamp, and a
callable maps the current priority to the recovered one.
"""


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

    queue_concurrency: "Mapping[str, int]" = field(default_factory=dict)
    """Per-worker concurrency caps for named queues."""

    heartbeat_interval: "float" = 30
    """Interval between bulk heartbeat writes in seconds."""

    heartbeat_jitter_fraction: "float" = 0.1
    """Positive heartbeat delay jitter ratio from zero through one."""

    heartbeat_miss_threshold: "int" = 2
    """Consecutive heartbeat misses tolerated before claim loss."""

    cancellation_poll_interval: "float" = 1.0
    """Interval between durable running-cancellation reconciliation passes."""

    reconcile_interval: "float" = 30
    """Interval between external-execution reconciliation passes in seconds."""

    stale_after: "float | None" = None
    """Running-task age threshold in seconds; ``None`` disables stale recovery."""

    stale_check_interval: "float" = 60.0
    """Interval between stale-task recovery passes in seconds."""

    expiry_check_interval: "float | None" = 60.0
    """Interval between pending-job expiration passes; ``None`` disables sweeps."""

    graceful_shutdown_timeout: "float" = 30
    """Maximum graceful drain time in seconds."""

    final_cancel_timeout: "float" = 5
    """Maximum post-cancellation drain time in seconds."""

    hard_exit_timeout: "float | None" = 10.0
    """Wall-clock budget from forced shutdown to process exit; ``None`` disables the watchdog."""

    requeue_on_shutdown: "bool" = False
    """Whether cancelled executions are requeued after shutdown drain timeout."""

    max_interruptions: "int" = 3
    """Shutdown requeues an attempt may absorb before interruptions consume the retry budget."""

    startup_timeout: "float" = 30
    """Maximum time to wait for worker startup readiness in seconds."""

    queues: "tuple[str, ...]" = ()
    """Queue names claimed by this worker; empty claims every queue."""

    def __post_init__(self) -> "None":  # noqa: C901
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
            "cancellation_poll_interval": self.cancellation_poll_interval,
            "reconcile_interval": self.reconcile_interval,
            "stale_check_interval": self.stale_check_interval,
            "graceful_shutdown_timeout": self.graceful_shutdown_timeout,
            "final_cancel_timeout": self.final_cancel_timeout,
            "max_interruptions": self.max_interruptions,
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
        if not 0.0 <= self.heartbeat_jitter_fraction <= 1.0:
            msg = "WorkerConfig.heartbeat_jitter_fraction must be between 0.0 and 1.0, inclusive."
            raise QueueConfigurationError(msg)
        if self.hard_exit_timeout is not None and self.hard_exit_timeout <= 0:
            msg = "WorkerConfig.hard_exit_timeout must be greater than 0 when set."
            raise QueueConfigurationError(msg)
        if self.stale_after is not None and self.stale_after <= 0:
            msg = "WorkerConfig.stale_after must be greater than 0 when set."
            raise QueueConfigurationError(msg)
        if self.expiry_check_interval is not None and self.expiry_check_interval < 0:
            msg = "WorkerConfig.expiry_check_interval must be greater than or equal to 0 when set."
            raise QueueConfigurationError(msg)
        for queue, cap in self.queue_concurrency.items():
            if not queue:
                msg = "WorkerConfig.queue_concurrency queue names must not be empty."
                raise QueueConfigurationError(msg)
            if cap <= 0:
                msg = "WorkerConfig.queue_concurrency values must be greater than 0."
                raise QueueConfigurationError(msg)
            if self.queues and queue not in self.queues:
                msg = f"WorkerConfig.queue_concurrency contains {queue!r}, which is not in WorkerConfig.queues."
                raise QueueConfigurationError(msg)


@dataclass(slots=True)
class QueueConfig:
    """Configuration for QueuePlugin."""

    namespace: "str" = DEFAULT_NAMESPACE
    """Root used to derive package-owned runtime identifiers."""

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

    service_dependency_key: "str | None" = None
    """Litestar dependency key for the injected queue service; ``None`` derives it from the namespace."""

    events_dependency_key: "str | None" = None
    """Litestar dependency key for the event producer; ``None`` derives it from the namespace."""

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

    sync_thread_pool_size: "int" = field(default_factory=_default_sync_thread_pool_size)
    """Maximum threads running synchronous tasks concurrently.

    Defaults to the cgroup-aware effective CPU count plus four, capped at 32.
    Threads are created on demand, so this is a ceiling rather than a startup cost.
    """

    sync_thread_name_prefix: "str | None" = None
    """Thread-name prefix for synchronous tasks; ``None`` derives it from the namespace."""

    scheduler_canary_task: "str | None" = None
    """Scheduler-health task name; ``None`` derives the package-owned default."""

    maintenance: "QueueMaintenanceConfig | None" = None
    """Automatic maintenance policy; ``None`` disables the maintenance loop."""

    max_argument_identity_bytes: "int | None" = None
    """Maximum canonical argument-identity size in bytes; ``None`` disables the bound."""

    stale_requeue_priority: "StaleRequeuePriority" = STALE_REQUEUE_PRIORITY
    """Priority applied to stale-recovered work; see :data:`StaleRequeuePriority`."""

    names: "QueueNamespace" = field(init=False)
    """Validated format-specific runtime-name renderer."""

    def __post_init__(self) -> "None":
        """Validate the synchronous task thread pool and the placement matrix."""
        self.names = QueueNamespace(self.namespace)
        if self.service_dependency_key is None:
            self.service_dependency_key = self.names.registration("service")
        if self.events_dependency_key is None:
            self.events_dependency_key = self.names.registration("events")
        if self.sync_thread_name_prefix is None:
            self.sync_thread_name_prefix = self.names.resource()
        if self.scheduler_canary_task is None:
            self.scheduler_canary_task = self.names.package_task("scheduler", "heartbeat")
        if self.sync_thread_pool_size <= 0:
            msg = "QueueConfig.sync_thread_pool_size must be greater than 0."
            raise QueueConfigurationError(msg)
        self._validate_stale_requeue_priority()
        self._validate_placement()

    def _validate_stale_requeue_priority(self) -> "None":
        """Reject a stale-requeue policy that is neither ``"preserve"``, a priority, nor a mapper.

        Raises:
            QueueConfigurationError: If the configured policy cannot be applied.
        """
        policy = self.stale_requeue_priority
        if callable(policy) or policy == "preserve":
            return
        if isinstance(policy, bool) or not isinstance(policy, int) or policy < 0:
            msg = (
                "QueueConfig.stale_requeue_priority must be 'preserve', a non-negative priority ceiling, "
                f"or a callable mapping the old priority to the new one, not {policy!r}."
            )
            raise QueueConfigurationError(msg)

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
        if execution == "cloudtasks":
            if isinstance(self.execution_backend, str):
                msg = (
                    "execution_backend='cloudtasks' has no defaults for the project, queue, "
                    "delivery target, or audience. Configure it as "
                    "execution_backend=CloudTasksExecutionConfig(...)."
                )
                raise QueueConfigurationError(msg)
            if placement != "external":
                msg = (
                    "execution_backend='cloudtasks' schedules work itself and requires "
                    "WorkerConfig(placement='external')."
                )
                raise QueueConfigurationError(msg)
            if backend in {"memory", "ephemeral"}:
                msg = (
                    "execution_backend='cloudtasks' delivers each record to a separate process, "
                    "which cannot reach process-local storage. Use a shared persistent queue backend."
                )
                raise QueueConfigurationError(msg)
        # Ephemeral storage is deliberately unconstrained by placement. Placement
        # does not decide whether the private database exists: whatever entered
        # EphemeralServerContext created it, and the backend refuses to open when
        # that environment is absent. Restating that check as a placement rule
        # only blocked embedders that create the database themselves.
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
        """Names Litestar must resolve that an application cannot supply itself.

        This carries only the types named in this package's own dependency
        providers. ``provide_service_dependency`` and
        ``provide_event_producer_dependency`` annotate their return types as
        strings while importing those types under ``TYPE_CHECKING``, so Litestar
        needs them here to resolve the injected ``queue_service`` and
        ``queue_events`` dependencies.

        Nothing else belongs here. Config, backend, worker, and event types are
        named in application setup code, not in handler signatures, and a
        handler that does annotate one has already imported it. Registering the
        whole public API instead made ``QueuePlugin.on_app_init`` import every
        installed adapter on every application startup, which defeated the
        package's lazy-import boundary and charged applications for extras they
        never selected.
        """
        from litestar.di import NamedDependency

        from litestar_queues.events import QueueEventProducer
        from litestar_queues.service import QueueService

        return {
            "NamedDependency": NamedDependency,
            "QueueEventProducer": QueueEventProducer,
            "QueueService": QueueService,
        }

    @property
    def dependencies(self) -> "dict[str, Any]":
        """Dependency providers for Litestar's DI system."""
        from litestar.di import Provide

        return {
            cast("str", self.service_dependency_key): Provide(self.provide_service_dependency),
            cast("str", self.events_dependency_key): Provide(self.provide_event_producer_dependency),
        }

    @property
    def service_state_key(self) -> "str":
        """Litestar state key holding the queue service."""
        return self.names.registration("service")

    @property
    def worker_state_key(self) -> "str":
        """Litestar state key holding the in-process worker."""
        return self.names.registration("worker")

    @property
    def event_publisher_state_key(self) -> "str":
        """Litestar state key holding the event publisher."""
        return self.names.registration("event", "publisher")

    @property
    def event_channels_state_key(self) -> "str":
        """Litestar state key holding the resolved channels backend."""
        return self.names.registration("event", "channels")

    @property
    def observability_runtime_state_key(self) -> "str":
        """Litestar state key holding the observability runtime."""
        return self.names.registration("observability", "runtime")

    @property
    def maintenance_name(self) -> "str":
        """Distributed maintenance coordination name."""
        return self.names.coordination("maintenance")

    def get_service(self, state: "State | None" = None) -> "QueueService":
        """Return a QueueService for this configuration."""
        from litestar_queues.service import QueueService

        if state is None:
            return QueueService(self)

        state_key = self.service_state_key
        if state_key not in state:
            msg = (
                f"QueueService is not available in app state under {state_key!r}; "
                "ensure QueuePlugin startup has completed before resolving the queue service."
            )
            raise RuntimeError(msg)

        cached = state[state_key]
        if isinstance(cached, QueueService):
            return cached

        msg = f"QueueService has not been opened in app state under {state_key!r}; found {type(cached).__name__}."
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
            return QueueEventPublisher(sink, namespace=self.names)
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
        sink = (
            sinks[0]
            if len(sinks) == 1
            else CompositeQueueEventSink(sinks, strict=delivery.strict, namespace=self.names)
        )
        return QueueEventPublisher(
            sink,
            buffer_config=delivery.buffer,
            strict=delivery.strict,
            publish_task_channel=delivery.publish_task_channel,
            publish_queue_channel=delivery.publish_queue_channel,
            publish_global_lifecycle=delivery.publish_global_lifecycle,
            namespace=self.names,
        )

    async def provide_service_dependency(self, state: "State") -> 'AsyncIterator["QueueService"]':
        """Yield the application-scoped QueueService for Litestar dependency injection."""
        yield self.get_service(state)

    async def provide_event_producer_dependency(self, state: "State") -> "AsyncIterator[QueueEventProducer]":
        """Yield the application-scoped QueueEventProducer for Litestar dependency injection."""
        from litestar_queues.events import QueueEventProducer

        state_key = self.event_publisher_state_key
        if state_key not in state:
            msg = (
                "Queue event publisher is not available in app state under "
                f"{state_key!r}; ensure QueuePlugin startup has completed before "
                "resolving queue events."
            )
            raise RuntimeError(msg)
        yield QueueEventProducer(state[state_key])

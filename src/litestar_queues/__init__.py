from typing import TYPE_CHECKING

from litestar_queues.backends import (
    BaseQueueBackend,
    InMemoryQueueBackend,
    get_queue_backend,
    get_queue_backend_class,
    list_queue_backends,
    queue_backend,
)
from litestar_queues.config import (
    AsyncServiceProvider,
    ExecutionBackendConfig,
    QueueBackendConfig,
    QueueConfig,
    QueueEventConfig,
    TaskDependencyResolver,
)
from litestar_queues.events import (
    InMemoryQueueEventSink,
    NoopQueueEventSink,
    QueueChannels,
    QueueEvent,
    QueueEventActor,
    QueueEventEntityRef,
    QueueEventPublisher,
    TaskExecutionContext,
    get_current_task_context,
    publish_task_event,
    publish_task_log,
    publish_task_progress,
    require_current_task_context,
)
from litestar_queues.exceptions import (
    MissingDependencyError,
    NonRetryableError,
    QueueConfigurationError,
    QueueError,
    non_retryable,
)
from litestar_queues.execution import (
    BaseExecutionBackend,
    CloudRunExecutionBackend,
    CloudRunExecutionConfig,
    CloudRunExecutionStatus,
    ImmediateExecutionBackend,
    LocalExecutionBackend,
    execution_backend,
    get_execution_backend,
    get_execution_backend_class,
    list_execution_backends,
)
from litestar_queues.models import QueueBackendCapabilities, QueuedTaskRecord, QueueStatistics, TaskStatus
from litestar_queues.service import QueueService
from litestar_queues.task import (
    ScheduleConfig,
    Task,
    TaskResult,
    discover_tasks,
    get_scheduled_tasks,
    get_task_registry,
    load_task_modules,
    task,
)
from litestar_queues.worker import Worker

if TYPE_CHECKING:
    from litestar_queues.plugin import QueuePlugin

__all__ = (
    "AsyncServiceProvider",
    "BaseExecutionBackend",
    "BaseQueueBackend",
    "CloudRunExecutionBackend",
    "CloudRunExecutionConfig",
    "CloudRunExecutionStatus",
    "ExecutionBackendConfig",
    "ImmediateExecutionBackend",
    "InMemoryQueueBackend",
    "InMemoryQueueEventSink",
    "LocalExecutionBackend",
    "MissingDependencyError",
    "NonRetryableError",
    "NoopQueueEventSink",
    "QueueBackendCapabilities",
    "QueueBackendConfig",
    "QueueChannels",
    "QueueConfig",
    "QueueConfigurationError",
    "QueueError",
    "QueueEvent",
    "QueueEventActor",
    "QueueEventConfig",
    "QueueEventEntityRef",
    "QueueEventPublisher",
    "QueuePlugin",
    "QueueService",
    "QueueStatistics",
    "QueuedTaskRecord",
    "ScheduleConfig",
    "Task",
    "TaskDependencyResolver",
    "TaskExecutionContext",
    "TaskResult",
    "TaskStatus",
    "Worker",
    "discover_tasks",
    "execution_backend",
    "get_current_task_context",
    "get_execution_backend",
    "get_execution_backend_class",
    "get_queue_backend",
    "get_queue_backend_class",
    "get_scheduled_tasks",
    "get_task_registry",
    "list_execution_backends",
    "list_queue_backends",
    "load_task_modules",
    "non_retryable",
    "publish_task_event",
    "publish_task_log",
    "publish_task_progress",
    "queue_backend",
    "require_current_task_context",
    "task",
)


def __getattr__(name: str) -> object:
    """Resolve heavier public exports lazily.

    Returns:
        The requested public export.

    Raises:
        AttributeError: If the export name is unknown.
    """
    if name == "QueuePlugin":
        from litestar_queues.plugin import QueuePlugin

        return QueuePlugin
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)

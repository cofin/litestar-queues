"""Public package exports for Litestar Queues."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from litestar_queues.task import task

if TYPE_CHECKING:
    from litestar_queues.backends import (
        BaseQueueBackend,
        InMemoryQueueBackend,
        get_queue_backend,
        get_queue_backend_class,
        list_queue_backends,
        queue_backend,
    )
    from litestar_queues.background import QueuedBackgroundTask
    from litestar_queues.config import (
        AsyncServiceProvider,
        ExecutionBackendConfig,
        ExecutionBackendConfigProtocol,
        QueueBackendConfig,
        QueueBackendConfigProtocol,
        QueueConfig,
        TaskDependencyResolver,
        TaskErrorSanitizer,
    )
    from litestar_queues.events import (
        InMemoryQueueEventSink,
        NoopQueueEventSink,
        QueueChannels,
        QueueEvent,
        QueueEventActor,
        QueueEventConfig,
        QueueEventEntityRef,
        QueueEventLog,
        QueueEventLogConfig,
        QueueEventLogRecord,
        QueueEventPublisher,
        QueueEventStageSummary,
        TaskExecutionContext,
        get_current_task_context,
        publish_task_event,
        publish_task_log,
        publish_task_progress,
        require_current_task_context,
    )
    from litestar_queues.exceptions import (
        JobCancelledError,
        MissingDependencyError,
        NonRetryableError,
        QueueConfigurationError,
        QueueError,
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
        execution_backend,
        get_execution_backend,
        get_execution_backend_class,
        list_execution_backends,
    )
    from litestar_queues.models import (
        EnqueueSpec,
        QueueBackendCapabilities,
        QueuedTaskRecord,
        QueueStatistics,
        StaleTaskRecoveryResult,
        TaskStatus,
    )
    from litestar_queues.plugin import QueuePlugin
    from litestar_queues.service import QueueService
    from litestar_queues.task import (
        ScheduleConfig,
        Task,
        TaskResult,
        discover_tasks,
        get_scheduled_tasks,
        get_task_registry,
        load_task_modules,
    )
    from litestar_queues.worker import Worker

_EXPORTS = {
    "AsyncServiceProvider": "litestar_queues.config",
    "BaseExecutionBackend": "litestar_queues.execution",
    "BaseQueueBackend": "litestar_queues.backends",
    "CloudRunExecutionBackend": "litestar_queues.execution",
    "CloudRunExecutionConfig": "litestar_queues.execution",
    "CloudRunExecutionStatus": "litestar_queues.execution",
    "EnqueueSpec": "litestar_queues.models",
    "ExecutionBackendConfig": "litestar_queues.config",
    "ExecutionBackendConfigProtocol": "litestar_queues.config",
    "ImmediateExecutionBackend": "litestar_queues.execution",
    "InMemoryQueueBackend": "litestar_queues.backends",
    "InMemoryQueueEventSink": "litestar_queues.events",
    "LocalExecutionBackend": "litestar_queues.execution",
    "MissingDependencyError": "litestar_queues.exceptions",
    "JobCancelledError": "litestar_queues.exceptions",
    "NonRetryableError": "litestar_queues.exceptions",
    "NoopQueueEventSink": "litestar_queues.events",
    "QueueBackendCapabilities": "litestar_queues.models",
    "QueueBackendConfig": "litestar_queues.config",
    "QueueBackendConfigProtocol": "litestar_queues.config",
    "QueueChannels": "litestar_queues.events",
    "QueueConfig": "litestar_queues.config",
    "QueueConfigurationError": "litestar_queues.exceptions",
    "QueueError": "litestar_queues.exceptions",
    "QueueEvent": "litestar_queues.events",
    "QueueEventActor": "litestar_queues.events",
    "QueueEventConfig": "litestar_queues.events",
    "QueueEventEntityRef": "litestar_queues.events",
    "QueueEventLog": "litestar_queues.events",
    "QueueEventLogConfig": "litestar_queues.events",
    "QueueEventLogRecord": "litestar_queues.events",
    "QueueEventPublisher": "litestar_queues.events",
    "QueueEventStageSummary": "litestar_queues.events",
    "QueuePlugin": "litestar_queues.plugin",
    "QueueService": "litestar_queues.service",
    "QueueStatistics": "litestar_queues.models",
    "QueuedBackgroundTask": "litestar_queues.background",
    "QueuedTaskRecord": "litestar_queues.models",
    "ScheduleConfig": "litestar_queues.task",
    "StaleTaskRecoveryResult": "litestar_queues.models",
    "Task": "litestar_queues.task",
    "TaskDependencyResolver": "litestar_queues.config",
    "TaskErrorSanitizer": "litestar_queues.config",
    "TaskExecutionContext": "litestar_queues.events",
    "TaskResult": "litestar_queues.task",
    "TaskStatus": "litestar_queues.models",
    "Worker": "litestar_queues.worker",
    "discover_tasks": "litestar_queues.task",
    "execution_backend": "litestar_queues.execution",
    "get_current_task_context": "litestar_queues.events",
    "get_execution_backend": "litestar_queues.execution",
    "get_execution_backend_class": "litestar_queues.execution",
    "get_queue_backend": "litestar_queues.backends",
    "get_queue_backend_class": "litestar_queues.backends",
    "get_scheduled_tasks": "litestar_queues.task",
    "get_task_registry": "litestar_queues.task",
    "list_execution_backends": "litestar_queues.execution",
    "list_queue_backends": "litestar_queues.backends",
    "load_task_modules": "litestar_queues.task",
    "job_cancelled": "litestar_queues.exceptions",
    "non_retryable": "litestar_queues.exceptions",
    "publish_task_event": "litestar_queues.events",
    "publish_task_log": "litestar_queues.events",
    "publish_task_progress": "litestar_queues.events",
    "queue_backend": "litestar_queues.backends",
    "require_current_task_context": "litestar_queues.events",
    "task": "litestar_queues.task",
}

__all__ = (
    "AsyncServiceProvider",
    "BaseExecutionBackend",
    "BaseQueueBackend",
    "CloudRunExecutionBackend",
    "CloudRunExecutionConfig",
    "CloudRunExecutionStatus",
    "EnqueueSpec",
    "ExecutionBackendConfig",
    "ExecutionBackendConfigProtocol",
    "ImmediateExecutionBackend",
    "InMemoryQueueBackend",
    "InMemoryQueueEventSink",
    "JobCancelledError",
    "LocalExecutionBackend",
    "MissingDependencyError",
    "NonRetryableError",
    "NoopQueueEventSink",
    "QueueBackendCapabilities",
    "QueueBackendConfig",
    "QueueBackendConfigProtocol",
    "QueueChannels",
    "QueueConfig",
    "QueueConfigurationError",
    "QueueError",
    "QueueEvent",
    "QueueEventActor",
    "QueueEventConfig",
    "QueueEventEntityRef",
    "QueueEventLog",
    "QueueEventLogConfig",
    "QueueEventLogRecord",
    "QueueEventPublisher",
    "QueueEventStageSummary",
    "QueuePlugin",
    "QueueService",
    "QueueStatistics",
    "QueuedBackgroundTask",
    "QueuedTaskRecord",
    "ScheduleConfig",
    "StaleTaskRecoveryResult",
    "Task",
    "TaskDependencyResolver",
    "TaskErrorSanitizer",
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
    "job_cancelled",
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


def __getattr__(name: "str") -> "Any":
    """Lazily load package-root exports.

    Returns:
        The requested package-root export.
    """
    module_name = _EXPORTS.get(name)
    if module_name is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> "list[str]":
    """Return the package root export names for interactive use."""
    return sorted((*globals(), *__all__))

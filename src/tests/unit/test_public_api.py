def test_public_exports() -> None:
    """Test that the package exposes the public queue API.

    Optional backends (advanced_alchemy, sqlspec, redis, valkey) are NOT in the
    top-level public API and must be imported explicitly from their submodules.
    The factory imports them lazily on first lookup.
    """
    import litestar_queues
    from litestar_queues import (
        AsyncServiceProvider,
        CloudRunExecutionBackend,
        CloudRunExecutionConfig,
        CloudRunExecutionStatus,
        ExecutionBackendConfig,
        ExecutionBackendConfigProtocol,
        ImmediateExecutionBackend,
        InMemoryQueueBackend,
        LocalExecutionBackend,
        QueueBackendConfig,
        QueueBackendConfigProtocol,
        QueueConfig,
        QueuedTaskRecord,
        QueueError,
        QueuePlugin,
        QueueService,
        ScheduleConfig,
        StaleTaskRecoveryResult,
        Task,
        TaskResult,
        Worker,
        get_execution_backend_class,
        get_queue_backend_class,
        get_scheduled_tasks,
        get_task_registry,
        list_execution_backends,
        list_queue_backends,
        task,
    )

    expected_exports = {
        "AsyncServiceProvider",
        "BaseExecutionBackend",
        "BaseQueueBackend",
        "CloudRunExecutionBackend",
        "CloudRunExecutionConfig",
        "CloudRunExecutionStatus",
        "ExecutionBackendConfig",
        "ExecutionBackendConfigProtocol",
        "ImmediateExecutionBackend",
        "InMemoryQueueBackend",
        "LocalExecutionBackend",
        "QueueBackendConfig",
        "QueueBackendConfigProtocol",
        "QueuedTaskRecord",
        "QueueConfig",
        "QueueError",
        "QueuePlugin",
        "QueueService",
        "ScheduleConfig",
        "StaleTaskRecoveryResult",
        "Task",
        "TaskResult",
        "Worker",
        "discover_tasks",
        "get_execution_backend_class",
        "get_queue_backend_class",
        "get_scheduled_tasks",
        "get_task_registry",
        "list_execution_backends",
        "list_queue_backends",
        "task",
    }
    forbidden_exports = {
        "AdvancedAlchemyQueueBackend",
        "RedisBackendConfig",
        "RedisQueueBackend",
        "SQLSpecQueueBackend",
        "ValkeyBackendConfig",
        "ValkeyQueueBackend",
    }

    assert expected_exports.issubset(set(litestar_queues.__all__))
    assert forbidden_exports.isdisjoint(set(litestar_queues.__all__))
    assert QueueConfig().queue_backend == "memory"
    assert QueueBackendConfig is not None
    assert ExecutionBackendConfig is not None
    assert QueueBackendConfigProtocol is not None
    assert ExecutionBackendConfigProtocol is not None
    assert get_queue_backend_class("memory") is InMemoryQueueBackend
    assert {"advanced-alchemy", "memory", "redis", "sqlspec", "valkey"}.issubset(set(list_queue_backends()))
    assert get_execution_backend_class("cloudrun") is CloudRunExecutionBackend
    assert get_execution_backend_class("immediate") is ImmediateExecutionBackend
    assert get_execution_backend_class("local") is LocalExecutionBackend
    assert {"cloudrun", "immediate", "local"}.issubset(set(list_execution_backends()))
    assert QueuePlugin().config.queue_backend == "memory"
    assert QueueService(QueueConfig()).config.queue_backend == "memory"
    assert issubclass(QueueError, Exception)
    assert AsyncServiceProvider(QueueConfig()) is not None
    assert ScheduleConfig(task_name="example", interval=1).task_name == "example"
    assert StaleTaskRecoveryResult().requeued == 0
    assert QueuedTaskRecord(task_name="example").task_name == "example"
    assert CloudRunExecutionBackend is not None
    assert CloudRunExecutionConfig(project_id="example", job_name="worker").resolve_job_name() == "worker"
    assert CloudRunExecutionStatus().running is True
    assert Task is not None
    assert TaskResult is not None
    assert Worker is not None
    assert get_task_registry() == {}
    assert get_scheduled_tasks() == {}
    assert callable(task)


def test_optional_backends_resolve_lazily_via_factory() -> None:
    """Optional backends are not top-level exports but are resolvable by name through the factory."""
    from litestar_queues import get_queue_backend_class
    from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyQueueBackend
    from litestar_queues.backends.redis import RedisQueueBackend
    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend
    from litestar_queues.backends.valkey import ValkeyQueueBackend

    assert get_queue_backend_class("advanced-alchemy") is AdvancedAlchemyQueueBackend
    assert get_queue_backend_class("redis") is RedisQueueBackend
    assert get_queue_backend_class("sqlspec") is SQLSpecQueueBackend
    assert get_queue_backend_class("valkey") is ValkeyQueueBackend


def test_optional_backend_configs_live_on_submodules() -> None:
    """Backend-specific config dataclasses are not top-level exports."""
    from litestar_queues.backends.redis import RedisBackendConfig
    from litestar_queues.backends.valkey import ValkeyBackendConfig

    assert RedisBackendConfig(url="redis://example").url == "redis://example"
    assert ValkeyBackendConfig(url="valkey://example").url == "valkey://example"


def test_task_dependency_resolver_is_re_exported_from_package_root() -> None:
    """TaskDependencyResolver is part of the package root surface."""
    import litestar_queues
    from litestar_queues import TaskDependencyResolver
    from litestar_queues.config import TaskDependencyResolver as ConfigTaskDependencyResolver

    assert TaskDependencyResolver is ConfigTaskDependencyResolver
    assert "TaskDependencyResolver" in litestar_queues.__all__


def test_task_dependency_resolver_config_surface() -> None:
    """The TaskDependencyResolver alias and config field are part of the config module surface."""
    from litestar_queues import config as config_module
    from litestar_queues.config import QueueConfig, TaskDependencyResolver

    assert "TaskDependencyResolver" in config_module.__all__
    assert TaskDependencyResolver is not None
    instance = QueueConfig()
    assert instance.task_dependency_resolver is None
    assert instance.signature_namespace["TaskDependencyResolver"] is TaskDependencyResolver

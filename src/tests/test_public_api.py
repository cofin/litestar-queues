def test_public_exports() -> None:
    """Test that the scaffold exposes the public queue API."""
    import litestar_queues
    from litestar_queues import (
        AdvancedAlchemyQueueBackend,
        AsyncServiceProvider,
        CloudRunExecutionBackend,
        CloudRunExecutionConfig,
        CloudRunExecutionStatus,
        ImmediateExecutionBackend,
        InMemoryQueueBackend,
        LocalExecutionBackend,
        QueueBackendConfig,
        QueueConfig,
        QueuedTaskRecord,
        QueueError,
        QueuePlugin,
        QueueService,
        RedisBackendConfig,
        RedisQueueBackend,
        ScheduleConfig,
        SQLSpecQueueBackend,
        Task,
        TaskResult,
        ValkeyBackendConfig,
        ValkeyQueueBackend,
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
        "AdvancedAlchemyQueueBackend",
        "AsyncServiceProvider",
        "BaseExecutionBackend",
        "BaseQueueBackend",
        "CloudRunExecutionBackend",
        "CloudRunExecutionConfig",
        "CloudRunExecutionStatus",
        "ImmediateExecutionBackend",
        "InMemoryQueueBackend",
        "LocalExecutionBackend",
        "QueueBackendConfig",
        "QueuedTaskRecord",
        "QueueConfig",
        "QueueError",
        "QueuePlugin",
        "QueueService",
        "RedisBackendConfig",
        "RedisQueueBackend",
        "ScheduleConfig",
        "SQLSpecQueueBackend",
        "Task",
        "TaskResult",
        "ValkeyBackendConfig",
        "ValkeyQueueBackend",
        "Worker",
        "get_execution_backend_class",
        "get_queue_backend_class",
        "get_scheduled_tasks",
        "get_task_registry",
        "list_execution_backends",
        "list_queue_backends",
        "task",
    }

    assert expected_exports.issubset(set(litestar_queues.__all__))
    assert QueueConfig().queue_backend == "memory"
    assert QueueBackendConfig is str
    assert get_queue_backend_class("advanced-alchemy") is AdvancedAlchemyQueueBackend
    assert get_queue_backend_class("memory") is InMemoryQueueBackend
    assert get_queue_backend_class("redis") is RedisQueueBackend
    assert get_queue_backend_class("sqlspec") is SQLSpecQueueBackend
    assert get_queue_backend_class("valkey") is ValkeyQueueBackend
    assert list_queue_backends() == ["advanced-alchemy", "memory", "redis", "sqlspec", "valkey"]
    assert get_execution_backend_class("cloudrun") is CloudRunExecutionBackend
    assert get_execution_backend_class("immediate") is ImmediateExecutionBackend
    assert get_execution_backend_class("local") is LocalExecutionBackend
    assert list_execution_backends() == ["cloudrun", "immediate", "local"]
    assert QueuePlugin().config.queue_backend == "memory"
    assert QueueService(QueueConfig()).config.queue_backend == "memory"
    assert RedisBackendConfig(url="redis://example").url == "redis://example"
    assert issubclass(QueueError, Exception)
    assert AsyncServiceProvider(QueueConfig()) is not None
    assert ScheduleConfig(task_name="example", interval=1).task_name == "example"
    assert QueuedTaskRecord(task_name="example").task_name == "example"
    assert CloudRunExecutionBackend is not None
    assert CloudRunExecutionConfig(project_id="example", job_name="worker").resolve_job_name() == "worker"
    assert CloudRunExecutionStatus().running is True
    assert ValkeyBackendConfig(url="valkey://example").url == "valkey://example"
    assert Task is not None
    assert TaskResult is not None
    assert Worker is not None
    assert get_task_registry() == {}
    assert get_scheduled_tasks() == {}
    assert callable(task)

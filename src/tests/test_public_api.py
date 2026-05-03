def test_public_exports() -> None:
    """Test that the scaffold exposes the public queue API."""
    import litestar_queues
    from litestar_queues import (
        AsyncServiceProvider,
        ImmediateExecutionBackend,
        InMemoryStorageBackend,
        LocalExecutionBackend,
        QueueConfig,
        QueuedTaskRecord,
        QueueError,
        QueuePlugin,
        QueueService,
        ScheduleConfig,
        SQLSpecStorageBackend,
        StorageBackendConfig,
        Task,
        TaskResult,
        Worker,
        get_execution_backend_class,
        get_scheduled_tasks,
        get_storage_backend_class,
        get_task_registry,
        list_execution_backends,
        list_storage_backends,
        task,
    )

    expected_exports = {
        "AsyncServiceProvider",
        "BaseExecutionBackend",
        "BaseStorageBackend",
        "ImmediateExecutionBackend",
        "InMemoryStorageBackend",
        "LocalExecutionBackend",
        "QueuedTaskRecord",
        "QueueConfig",
        "QueueError",
        "QueuePlugin",
        "QueueService",
        "ScheduleConfig",
        "SQLSpecStorageBackend",
        "StorageBackendConfig",
        "Task",
        "TaskResult",
        "Worker",
        "get_execution_backend_class",
        "get_scheduled_tasks",
        "get_storage_backend_class",
        "get_task_registry",
        "list_execution_backends",
        "list_storage_backends",
        "task",
    }

    assert expected_exports.issubset(set(litestar_queues.__all__))
    assert QueueConfig().storage_backend == "memory"
    assert StorageBackendConfig is str
    assert get_storage_backend_class("memory") is InMemoryStorageBackend
    assert get_storage_backend_class("sqlspec") is SQLSpecStorageBackend
    assert list_storage_backends() == ["memory", "sqlspec"]
    assert get_execution_backend_class("immediate") is ImmediateExecutionBackend
    assert get_execution_backend_class("local") is LocalExecutionBackend
    assert list_execution_backends() == ["immediate", "local"]
    assert QueuePlugin().config.storage_backend == "memory"
    assert QueueService(QueueConfig()).config.storage_backend == "memory"
    assert issubclass(QueueError, Exception)
    assert AsyncServiceProvider(QueueConfig()) is not None
    assert ScheduleConfig(task_name="example", interval=1).task_name == "example"
    assert QueuedTaskRecord(task_name="example").task_name == "example"
    assert Task is not None
    assert TaskResult is not None
    assert Worker is not None
    assert get_task_registry() == {}
    assert get_scheduled_tasks() == {}
    assert callable(task)

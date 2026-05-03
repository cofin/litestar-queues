def test_public_exports() -> None:
    """Test that the scaffold exposes the public queue API."""
    import litestar_queues
    from litestar_queues import (
        AsyncServiceProvider,
        ImmediateExecutionBackend,
        InMemoryStorageBackend,
        LocalExecutionBackend,
        QueueConfig,
        QueueError,
        QueuePlugin,
        QueueService,
        StorageBackendConfig,
        get_execution_backend_class,
        get_storage_backend_class,
        list_execution_backends,
        list_storage_backends,
    )

    expected_exports = {
        "AsyncServiceProvider",
        "BaseExecutionBackend",
        "BaseStorageBackend",
        "ImmediateExecutionBackend",
        "InMemoryStorageBackend",
        "LocalExecutionBackend",
        "QueueConfig",
        "QueueError",
        "QueuePlugin",
        "QueueService",
        "StorageBackendConfig",
        "get_execution_backend_class",
        "get_storage_backend_class",
        "list_execution_backends",
        "list_storage_backends",
    }

    assert expected_exports.issubset(set(litestar_queues.__all__))
    assert QueueConfig().storage_backend == "memory"
    assert StorageBackendConfig is str
    assert get_storage_backend_class("memory") is InMemoryStorageBackend
    assert list_storage_backends() == ["memory"]
    assert get_execution_backend_class("immediate") is ImmediateExecutionBackend
    assert get_execution_backend_class("local") is LocalExecutionBackend
    assert list_execution_backends() == ["immediate", "local"]
    assert QueuePlugin().config.storage_backend == "memory"
    assert QueueService(QueueConfig()).config.storage_backend == "memory"
    assert issubclass(QueueError, Exception)
    assert AsyncServiceProvider(QueueConfig()) is not None

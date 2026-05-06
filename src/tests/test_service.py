import pytest

from litestar_queues import QueueConfig, QueueService

pytestmark = pytest.mark.anyio


async def test_service_context_manager_returns_service() -> None:
    """Test that the service can be used as an async context manager."""
    config = QueueConfig()

    async with config.provide_service() as service:
        assert isinstance(service, QueueService)
        assert service.config is config


async def test_service_placeholder_enqueue_reports_unimplemented() -> None:
    """Test that service enqueue runs through the immediate backend."""
    from litestar_queues import task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    @task("example")
    async def example() -> str:
        return "ok"

    service = QueueService(QueueConfig())

    async with service:
        result = await service.enqueue("example")

    assert result.status == "completed"
    assert result.result == "ok"

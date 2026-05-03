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
    """Test that runtime behavior is deliberately deferred beyond scaffold."""
    service = QueueService(QueueConfig())

    with pytest.raises(NotImplementedError, match="Chapter 2"):
        await service.enqueue("example")

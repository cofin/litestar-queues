from typing import TYPE_CHECKING, cast

import pytest
from litestar import Litestar, Response, post
from litestar.background_tasks import BackgroundTask
from litestar.testing import AsyncTestClient

from litestar_queues import QueueConfig, QueuedBackgroundTask, QueuePlugin, QueueService, task
from litestar_queues.task import get_default_service

if TYPE_CHECKING:
    from litestar.di import NamedDependency

    from litestar_queues.models import QueuedTaskRecord

pytestmark = pytest.mark.anyio


def _records(service: "QueueService") -> "dict[str, QueuedTaskRecord]":
    backend = service.get_queue_backend()
    return cast("dict[str, QueuedTaskRecord]", backend._records)  # type: ignore[attr-defined]


def _build_plugin() -> "QueuePlugin":
    return QueuePlugin(QueueConfig(queue_backend="memory", execution_backend="immediate", initialize_schedules=False))


async def test_background_task_native_integration() -> "None":
    """Native BackgroundTask should call task.enqueue after the response is sent."""

    @task("tasks.background_test")
    async def sample_task(val: "int", name: "str") -> "str":
        return f"{name}-{val}"

    @post("/test-native")
    async def native_handler() -> "Response[dict[str, str]]":
        return Response({"status": "ok"}, background=BackgroundTask(sample_task.enqueue, 42, name="test-run"))

    app = Litestar(route_handlers=[native_handler], plugins=[_build_plugin()])

    async with AsyncTestClient(app=app) as client:
        response = await client.post("/test-native")

    assert response.status_code == 201
    assert response.json() == {"status": "ok"}

    records = _records(app.state.queue_service)
    assert len(records) == 1
    record = next(iter(records.values()))
    assert record.task_name == "tasks.background_test"
    assert record.args == (42,)
    assert record.kwargs == {"name": "test-run"}


async def test_queued_background_task_helper() -> "None":
    """QueuedBackgroundTask should resolve the active default service and enqueue post-response."""

    @task("tasks.helper_test")
    async def helper_task(val: "int") -> "None":
        return None

    @post("/test-helper")
    async def helper_handler() -> "Response[dict[str, str]]":
        return Response({"status": "ok"}, background=QueuedBackgroundTask(helper_task, 99))

    app = Litestar(route_handlers=[helper_handler], plugins=[_build_plugin()])

    async with AsyncTestClient(app=app) as client:
        response = await client.post("/test-helper")

    assert response.status_code == 201
    records = _records(app.state.queue_service)
    assert len(records) == 1
    record = next(iter(records.values()))
    assert record.task_name == "tasks.helper_test"
    assert record.args == (99,)


async def test_queued_background_task_custom_service() -> "None":
    """QueuedBackgroundTask should honour an explicitly passed QueueService."""

    @task("tasks.custom_service_test")
    async def custom_task(val: "int") -> "None":
        return None

    @post("/test-custom")
    async def custom_handler(queue_service: "NamedDependency[QueueService]") -> "Response[dict[str, str]]":
        return Response({"status": "ok"}, background=QueuedBackgroundTask(custom_task, 88, service=queue_service))

    app = Litestar(route_handlers=[custom_handler], plugins=[_build_plugin()])

    async with AsyncTestClient(app=app) as client:
        response = await client.post("/test-custom")

    assert response.status_code == 201
    records = _records(app.state.queue_service)
    assert len(records) == 1
    record = next(iter(records.values()))
    assert record.task_name == "tasks.custom_service_test"
    assert record.args == (88,)


async def test_di_provides_application_scoped_service() -> "None":
    """The injected QueueService should be the same app-scoped instance on every request."""
    injected: "list[QueueService]" = []

    @post("/test-di")
    async def di_handler(queue_service: "NamedDependency[QueueService]") -> "dict[str, str]":
        injected.append(queue_service)
        return {"status": "ok"}

    app = Litestar(route_handlers=[di_handler], plugins=[_build_plugin()])

    async with AsyncTestClient(app=app) as client:
        await client.post("/test-di")
        await client.post("/test-di")

    assert len(injected) == 2
    app_scoped = app.state.queue_service
    assert injected[0] is app_scoped
    assert injected[1] is app_scoped


async def test_queued_background_task_without_plugin_raises() -> "None":
    """Constructing QueuedBackgroundTask outside an app lifespan should raise RuntimeError."""

    @task("tasks.no_plugin")
    async def standalone_task() -> "None":
        return None

    assert get_default_service() is None

    with pytest.raises(RuntimeError, match="No active QueueService"):
        QueuedBackgroundTask(standalone_task)


async def test_default_service_is_cleared_after_shutdown() -> "None":
    """The global default service registry must be cleared when the plugin shuts down."""

    @post("/test-shutdown")
    async def shutdown_handler() -> "dict[str, str]":
        assert get_default_service() is not None
        return {"status": "ok"}

    app = Litestar(route_handlers=[shutdown_handler], plugins=[_build_plugin()])

    async with AsyncTestClient(app=app) as client:
        await client.post("/test-shutdown")
        assert get_default_service() is not None

    assert get_default_service() is None

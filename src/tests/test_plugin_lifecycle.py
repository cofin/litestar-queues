import pytest
from litestar import Litestar
from litestar.config.app import AppConfig
from litestar.testing import AsyncTestClient

from litestar_queues import QueueConfig, QueuePlugin, QueueService, Worker, get_scheduled_tasks, get_task_registry
from litestar_queues.backends import BaseStorageBackend, storage_backend
from litestar_queues.backends.factory import _storage_backend_registry
from litestar_queues.task import clear_task_registry

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def clean_task_registry() -> None:
    clear_task_registry()


async def test_plugin_startup_loads_task_modules_and_initializes_schedules() -> None:
    plugin = QueuePlugin(
        QueueConfig(
            execution_backend="local",
            task_modules=("tests.support.queue_tasks",),
        )
    )
    app = Litestar(plugins=[plugin])

    async with AsyncTestClient(app=app):
        service = app.state[plugin.config.queue_service_state_key]
        assert isinstance(service, QueueService)
        assert "support_ping" in get_task_registry()
        assert "support_ping" in get_scheduled_tasks()
        scheduled = await service.get_storage_backend().get_task_by_key("scheduled:support_ping")

    assert scheduled is not None
    assert scheduled.status == "scheduled"


async def test_plugin_start_worker_creates_and_cleans_up_worker() -> None:
    plugin = QueuePlugin(
        QueueConfig(
            execution_backend="local",
            start_worker=True,
            worker_poll_interval=0.01,
        )
    )
    app = Litestar(plugins=[plugin])

    async with AsyncTestClient(app=app):
        worker = app.state[plugin.config.queue_worker_state_key]
        assert isinstance(worker, Worker)
        assert worker.is_running

    assert not worker.is_running


async def test_plugin_calls_storage_backend_app_init_hook_once() -> None:
    class HookedStorageBackend(BaseStorageBackend):
        __slots__ = ()

        def on_app_init(self, app_config: AppConfig) -> AppConfig:
            app_config.state.update({"queue_storage_app_init": self})
            return app_config

    try:
        storage_backend("hooked-app-init")(HookedStorageBackend)
        plugin = QueuePlugin(QueueConfig(storage_backend="hooked-app-init"))
        app_config = plugin.on_app_init(AppConfig())

        assert app_config.state["queue_storage_app_init"] is plugin.get_service(app_config.state).get_storage_backend()
    finally:
        _storage_backend_registry.pop("hooked-app-init", None)

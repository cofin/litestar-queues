import subprocess
import sys
import textwrap

import pytest
from litestar import Litestar
from litestar.config.app import AppConfig
from litestar.testing import AsyncTestClient

from litestar_queues import QueueConfig, QueuePlugin, QueueService, Worker, get_scheduled_tasks, get_task_registry
from litestar_queues.backends import BaseQueueBackend, queue_backend
from litestar_queues.backends.factory import _queue_backend_registry
from litestar_queues.task import clear_task_registry

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def clean_task_registry() -> None:
    clear_task_registry()


async def test_plugin_startup_loads_task_modules_and_initializes_schedules() -> None:
    plugin = QueuePlugin(
        QueueConfig(
            execution_backend="local",
            task_modules=("tests._factories.queue_tasks",),
        )
    )
    app = Litestar(plugins=[plugin])

    async with AsyncTestClient(app=app):
        service = app.state[plugin.config.queue_service_state_key]
        assert isinstance(service, QueueService)
        assert "support_ping" in get_task_registry()
        assert "support_ping" in get_scheduled_tasks()
        scheduled = await service.get_queue_backend().get_task_by_key("scheduled:support_ping")

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


def test_importing_litestar_queues_does_not_load_click() -> None:
    """``click`` is only pulled in when the CLI is actually invoked.

    Verified in a fresh subprocess so prior in-process imports cannot leak
    ``click`` into the test process's ``sys.modules``.
    """
    code = textwrap.dedent(
        """
        import sys
        import litestar_queues  # noqa: F401
        from litestar_queues import QueueConfig, QueuePlugin, discover_tasks  # noqa: F401
        assert "click" not in sys.modules, sorted(m for m in sys.modules if "click" in m)
        """
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, check=False, timeout=20)
    assert result.returncode == 0, result.stderr.decode()


async def test_plugin_uses_registered_queue_backend_instance() -> None:
    class CustomQueueBackend(BaseQueueBackend):
        __slots__ = ()

    try:
        queue_backend("custom-plugin")(CustomQueueBackend)
        plugin = QueuePlugin(QueueConfig(queue_backend="custom-plugin"))
        app_config = plugin.on_app_init(AppConfig())

        assert isinstance(plugin.get_service(app_config.state).get_queue_backend(), CustomQueueBackend)
    finally:
        _queue_backend_registry.pop("custom-plugin", None)

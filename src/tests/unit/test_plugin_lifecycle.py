import asyncio
import logging
import subprocess
import sys
import textwrap
from typing import TYPE_CHECKING

import pytest
from litestar import Litestar
from litestar.channels import ChannelsPlugin
from litestar.channels.backends.memory import MemoryChannelsBackend
from litestar.config.app import AppConfig
from litestar.testing import AsyncTestClient

from litestar_queues import QueueConfig, QueuePlugin, QueueService, Worker, get_scheduled_tasks, get_task_registry, task
from litestar_queues.backends import BaseQueueBackend, queue_backend
from litestar_queues.backends.factory import _queue_backend_registry
from litestar_queues.events import EventBufferConfig, EventConfig, QueueChannels, publish_task_progress
from litestar_queues.events.litestar import ChannelsQueueEventSink
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.task import clear_task_registry

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def clean_task_registry() -> "None":
    clear_task_registry()


async def test_plugin_startup_loads_task_modules_and_initializes_schedules() -> "None":
    plugin = QueuePlugin(QueueConfig(execution_backend="local", task_modules=("tests._factories.queue_tasks",)))
    app = Litestar(plugins=[plugin])

    async with AsyncTestClient(app=app):
        service = app.state[plugin.config.queue_service_state_key]
        assert isinstance(service, QueueService)
        assert "support_ping" in get_task_registry()
        assert "support_ping" in get_scheduled_tasks()
        scheduled = await service.get_queue_backend().get_task_by_key("scheduled:support_ping")

    assert scheduled is not None
    assert scheduled.status == "scheduled"


async def test_plugin_in_app_worker_creates_and_cleans_up_worker() -> "None":
    plugin = QueuePlugin(QueueConfig(in_app_worker=True, worker_poll_interval=0.01))
    app = Litestar(plugins=[plugin])

    async with AsyncTestClient(app=app):
        worker = app.state[plugin.config.queue_worker_state_key]
        assert isinstance(worker, Worker)
        assert worker.is_running

    assert not worker.is_running


def test_importing_litestar_queues_does_not_load_click() -> "None":
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


def test_importing_consumer_core_does_not_load_click() -> "None":
    code = "import sys; import litestar_queues._consumer; assert 'click' not in sys.modules"
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, check=False, timeout=20)
    assert result.returncode == 0, result.stderr.decode()


def test_queues_execute_is_discoverable_without_an_app(tmp_path: "Path") -> "None":
    """The ``litestar.commands`` entry point exposes ``queues execute`` with no app present."""
    import os
    import shutil
    from pathlib import Path

    candidate = Path(sys.executable).with_name("litestar")
    litestar_bin = str(candidate) if candidate.exists() else (shutil.which("litestar") or "")
    if not litestar_bin:
        pytest.skip("litestar console entry point is not available")

    env = {key: value for key, value in os.environ.items() if key != "LITESTAR_APP"}
    result = subprocess.run(
        [litestar_bin, "queues", "execute", "--help"],
        cwd=tmp_path,
        capture_output=True,
        check=False,
        timeout=30,
        env=env,
    )

    assert result.returncode == 0, result.stderr.decode()
    assert "dispatch envelope" in result.stdout.decode().lower()


class _RecordingMemoryChannelsBackend(MemoryChannelsBackend):
    """Records teardown order so tests can assert the worker drains before channels close."""

    def __init__(self, order: "list[str]", **kwargs: "object") -> "None":
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self._order = order

    async def on_shutdown(self) -> "None":
        self._order.append("channels_shutdown")
        await super().on_shutdown()

    async def publish(self, data: "bytes", channels: "Iterable[str]") -> "None":
        # A publish after on_shutdown raises "Backend not yet initialized"; record it so a
        # misordered teardown is visible even if the RuntimeError is swallowed as best-effort.
        if self._queue is None:
            self._order.append("publish_after_shutdown")
        await super().publish(data, channels)


async def test_channels_before_queues_drains_worker_before_channels_close(caplog: "pytest.LogCaptureFixture") -> "None":
    """Regression: ChannelsPlugin registered before QueuePlugin must drain the worker first.

    Litestar runs on_startup/on_shutdown hooks around lifespan context managers, so a
    hook-based QueuePlugin drained its worker after the Channels backend had already torn
    down, flushing events into a dead sink ("Backend not yet initialized"). With both
    plugins as lifespan managers, LIFO exit drains the worker before channels close.
    """
    order: "list[str]" = []

    @task("tests.slow_publisher")
    async def slow_publisher() -> "str":
        try:
            for _ in range(10_000):
                await publish_task_progress(percent=1)
                await asyncio.sleep(0.02)
        except asyncio.CancelledError:
            order.append("worker_task_cancelled")
            raise
        return "done"

    channels = ChannelsPlugin(
        backend=_RecordingMemoryChannelsBackend(order, history=50), arbitrary_channels_allowed=True
    )
    plugin = QueuePlugin(
        QueueConfig(
            in_app_worker=True,
            worker_poll_interval=0.01,
            worker_graceful_shutdown_timeout=1,
            event=EventConfig(
                enabled=True, channels_backend=channels, buffer=EventBufferConfig(buffer_size=4, flush_interval=0.05)
            ),
        )
    )
    app = Litestar(plugins=[channels, plugin])

    with caplog.at_level(logging.DEBUG, logger="litestar_queues.events.publisher"):
        async with AsyncTestClient(app=app) as client:
            service = client.app.state[plugin.config.queue_service_state_key]
            publisher = client.app.state[plugin.config.queue_event_publisher_state_key]
            result = await service.enqueue(slow_publisher)
            for _ in range(200):
                await result.refresh()
                if result.status == "running":
                    break
                await asyncio.sleep(0.01)
            assert result.status == "running"
            # Let the running task publish for a moment before shutting down mid-run.
            await asyncio.sleep(0.1)

    assert "Backend not yet initialized" not in caplog.text
    assert "publish_after_shutdown" not in order
    assert publisher._live_failure_signature is None
    assert order.index("worker_task_cancelled") < order.index("channels_shutdown")


async def test_queues_before_channels_raises_configuration_error_at_startup() -> "None":
    """Misordered registration (QueuePlugin before ChannelsPlugin) fails fast at lifespan enter."""
    channels = ChannelsPlugin(backend=MemoryChannelsBackend(), arbitrary_channels_allowed=True)
    plugin = QueuePlugin(QueueConfig(event=EventConfig(enabled=True, channels_backend=channels)))
    app = Litestar(plugins=[plugin, channels])

    # Litestar's event emitter task group wraps startup failures in ExceptionGroups.
    def _find_configuration_error(exc: "BaseException") -> "QueueConfigurationError | None":
        if isinstance(exc, QueueConfigurationError):
            return exc
        for sub_exc in getattr(exc, "exceptions", ()):
            found = _find_configuration_error(sub_exc)
            if found is not None:
                return found
        return None

    with pytest.raises(BaseException) as exc_info:
        async with app.lifespan():
            pass

    error = _find_configuration_error(exc_info.value)
    assert error is not None
    assert "ChannelsPlugin must be registered before QueuePlugin" in str(error)
    assert "plugins=[channels, QueuePlugin(config)]" in str(error)


async def test_event_config_auto_resolves_registered_channels_plugin() -> "None":
    """EventConfig(enabled=True) without channels_backend targets the registered ChannelsPlugin."""

    @task("tests.auto_resolved")
    async def auto_resolved() -> "str":
        await publish_task_progress(percent=50)
        return "ok"

    channels = ChannelsPlugin(backend=MemoryChannelsBackend(), arbitrary_channels_allowed=True)
    config = QueueConfig(
        in_app_worker=True,
        worker_poll_interval=0.01,
        event=EventConfig(enabled=True, buffer=EventBufferConfig(enabled=False)),
    )
    plugin = QueuePlugin(config)
    app = Litestar(plugins=[channels, plugin])

    assert config.event is not None
    assert config.event.channels_backend is None  # auto-resolve must not mutate the config

    async with AsyncTestClient(app=app) as client:
        assert client.app.state[plugin.config.queue_event_channels_backend_state_key] is channels
        publisher = client.app.state[plugin.config.queue_event_publisher_state_key]
        assert isinstance(publisher.sink, ChannelsQueueEventSink)
        assert publisher.sink.channels_backend is channels

        async with channels.start_subscription(QueueChannels.queue("default")) as subscriber:
            service = client.app.state[plugin.config.queue_service_state_key]
            result = await service.enqueue(auto_resolved)
            received: "list[bytes]" = []

            async def collect() -> "None":
                async for payload in subscriber.iter_events():
                    received.append(payload)
                    if len(received) >= 3:
                        return

            await asyncio.wait_for(collect(), timeout=5)
            await result.refresh()

    assert result.status == "completed"
    assert any(b"task.progress" in payload for payload in received)
    assert config.event.channels_backend is None


async def test_plugin_uses_registered_queue_backend_instance() -> "None":
    class CustomQueueBackend(BaseQueueBackend):
        __slots__ = ()

    try:
        queue_backend("custom-plugin")(CustomQueueBackend)
        plugin = QueuePlugin(QueueConfig(queue_backend="custom-plugin"))
        app_config = plugin.on_app_init(AppConfig())

        assert isinstance(plugin.get_service(app_config.state).get_queue_backend(), CustomQueueBackend)
    finally:
        _queue_backend_registry.pop("custom-plugin", None)

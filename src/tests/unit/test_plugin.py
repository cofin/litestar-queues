from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from litestar import Litestar

    from litestar_queues import QueueConfig, QueuePlugin

pytestmark = pytest.mark.anyio


def test_plugin_instantiation_with_defaults() -> "None":
    """Test that the plugin can be instantiated with default configuration."""
    from litestar_queues import QueuePlugin

    plugin = QueuePlugin()
    assert plugin.config.queue_backend == "memory"
    assert plugin.config.execution_backend == "local"
    assert plugin.config.in_app_worker is True


def test_plugin_instantiation_with_config(queue_config: "QueueConfig") -> "None":
    """Test that the plugin can be instantiated with custom configuration."""
    from litestar_queues import QueuePlugin

    plugin = QueuePlugin(config=queue_config)
    assert plugin.config.queue_backend == "memory"
    assert plugin.config.in_app_worker is False


def test_config_defaults() -> "None":
    """Test that the configuration has sensible defaults."""
    from litestar_queues import QueueConfig

    config = QueueConfig()
    assert config.queue_backend == "memory"
    assert config.execution_backend == "local"
    assert config.queue_service_dependency_key == "queue_service"
    assert config.queue_service_state_key == "queue_service"
    assert config.queue_worker_state_key == "queue_worker"
    assert config.in_app_worker is True
    assert config.quiet_success is True
    assert config.scheduler_canary_task == "scheduler.heartbeat"


def test_in_app_worker_controls_plugin_worker_startup() -> "None":
    """The in_app_worker setting should control plugin worker startup."""
    from litestar_queues import QueueConfig

    config = QueueConfig(in_app_worker=False)
    assert config.in_app_worker is False


def test_scheduler_canary_task_is_overridable() -> "None":
    """Operators can override the canary task name used by scheduler-health."""
    from litestar_queues import QueueConfig

    config = QueueConfig(scheduler_canary_task="ops.healthcheck")
    assert config.scheduler_canary_task == "ops.healthcheck"


def test_plugin_with_litestar_app(app: "Litestar", queue_plugin: "QueuePlugin") -> "None":
    """Test that the plugin integrates with a Litestar application."""
    assert queue_plugin in app.plugins
    assert queue_plugin.config.queue_backend == "memory"


def test_queue_plugin_is_detected_as_cli_plugin(queue_plugin: "QueuePlugin") -> "None":
    """``QueuePlugin`` satisfies ``CLIPluginProtocol`` and is registered on ``app.plugins.cli``."""
    from litestar import Litestar
    from litestar.plugins import CLIPluginProtocol, InitPlugin

    app = Litestar(plugins=[queue_plugin])
    assert isinstance(queue_plugin, InitPlugin)
    assert isinstance(queue_plugin, CLIPluginProtocol)
    assert any(p is queue_plugin for p in app.plugins.cli)


def test_plugin_registers_dependencies_and_state() -> "None":
    """Test that the plugin registers dependencies, state, and signature namespace."""
    from litestar.config.app import AppConfig

    from litestar_queues import QueueConfig, QueuePlugin

    config = QueueConfig()
    plugin = QueuePlugin(config=config)
    app_config = AppConfig()

    plugin.on_app_init(app_config)

    assert config.queue_service_dependency_key in app_config.dependencies
    assert config.queue_service_state_key in app_config.state
    assert app_config.state[config.queue_service_state_key] is config
    assert "QueueService" in app_config.signature_namespace
    assert plugin.get_service(app_config.state).config is config


def test_queue_config_get_service_requires_opened_app_state_service() -> "None":
    """QueueConfig state lookup should not create an unopened app service."""
    from litestar.config.app import AppConfig

    from litestar_queues import QueueConfig, QueueService

    config = QueueConfig()
    app_config = AppConfig()
    service = QueueService(config)

    assert isinstance(config.get_service(), QueueService)

    with pytest.raises(RuntimeError, match="QueueService is not available"):
        config.get_service(app_config.state)

    app_config.state[config.queue_service_state_key] = config
    with pytest.raises(RuntimeError, match="QueueService has not been opened"):
        config.get_service(app_config.state)

    app_config.state[config.queue_service_state_key] = service
    assert config.get_service(app_config.state) is service

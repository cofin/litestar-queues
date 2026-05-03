from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from litestar import Litestar

    from litestar_queues import QueueConfig, QueuePlugin

pytestmark = pytest.mark.anyio


def test_plugin_instantiation_with_defaults() -> None:
    """Test that the plugin can be instantiated with default configuration."""
    from litestar_queues import QueuePlugin

    plugin = QueuePlugin()
    assert plugin.config.storage_backend == "memory"
    assert plugin.config.execution_backend == "immediate"
    assert plugin.config.start_worker is False


def test_plugin_instantiation_with_config(queue_config: "QueueConfig") -> None:
    """Test that the plugin can be instantiated with custom configuration."""
    from litestar_queues import QueuePlugin

    plugin = QueuePlugin(config=queue_config)
    assert plugin.config.storage_backend == "memory"
    assert plugin.config.start_worker is False


def test_config_defaults() -> None:
    """Test that the configuration has sensible defaults."""
    from litestar_queues import QueueConfig

    config = QueueConfig()
    assert config.storage_backend == "memory"
    assert config.execution_backend == "immediate"
    assert config.queue_service_dependency_key == "queue_service"
    assert config.queue_service_state_key == "queue_service"
    assert config.start_worker is False


def test_plugin_with_litestar_app(app: "Litestar", queue_plugin: "QueuePlugin") -> None:
    """Test that the plugin integrates with a Litestar application."""
    assert queue_plugin in app.plugins
    assert queue_plugin.config.storage_backend == "memory"


def test_plugin_registers_dependencies_and_state() -> None:
    """Test that the plugin registers dependencies, state, and signature namespace."""
    from litestar.config.app import AppConfig

    from litestar_queues import QueueConfig, QueuePlugin, QueueService

    config = QueueConfig()
    plugin = QueuePlugin(config=config)
    app_config = AppConfig()

    plugin.on_app_init(app_config)

    assert config.queue_service_dependency_key in app_config.dependencies
    assert config.queue_service_state_key in app_config.state
    assert app_config.state[config.queue_service_state_key] is config
    assert "QueueService" in app_config.signature_namespace
    assert isinstance(plugin.get_service(app_config.state), QueueService)

from typing import TYPE_CHECKING

from litestar.plugins import InitPluginProtocol

if TYPE_CHECKING:
    from litestar.config.app import AppConfig
    from litestar.datastructures import State

    from litestar_queues.config import QueueConfig
    from litestar_queues.service import QueueService

__all__ = ("QueuePlugin",)


class QueuePlugin(InitPluginProtocol):
    """Litestar plugin for queue service dependency registration."""

    __slots__ = ("_config",)

    def __init__(self, config: "QueueConfig | None" = None) -> None:
        """Initialize the queue plugin.

        Args:
            config: Optional queue configuration.
        """
        from litestar_queues.config import QueueConfig

        self._config = config or QueueConfig()

    @property
    def config(self) -> "QueueConfig":
        """Return the plugin configuration."""
        return self._config

    def get_service(self, state: "State | None" = None) -> "QueueService":
        """Return a QueueService for this plugin."""
        return self._config.get_service(state)

    def on_app_init(self, app_config: "AppConfig") -> "AppConfig":
        """Register queue dependencies, signature namespace, and app state.

        Returns:
            The updated application configuration.
        """
        app_config.dependencies.update(self._config.dependencies)
        app_config.signature_namespace.update(self._config.signature_namespace)
        app_config.state.update({self._config.queue_service_state_key: self._config})
        return app_config

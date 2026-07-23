import asyncio
import contextlib
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, cast

from litestar.channels import ChannelsPlugin
from litestar.plugins import InitPlugin

from litestar_queues.config import (
    _EVENT_CHANNELS_STATE_KEY,
    _EVENT_PUBLISHER_STATE_KEY,
    _OBSERVABILITY_RUNTIME_STATE_KEY,
    _SERVICE_STATE_KEY,
    _WORKER_STATE_KEY,
    QueueConfig,
)
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.service import QueueService
from litestar_queues.task import load_task_modules, set_default_service
from litestar_queues.worker import Worker

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable

    from click import Group as ClickGroup
    from litestar import Litestar
    from litestar.config.app import AppConfig
    from litestar.datastructures import State

    from litestar_queues.backends import BaseQueueBackend
    from litestar_queues.backends.sqlspec._typing import SQLSpecConfig
    from litestar_queues.events import QueueEventPublisher
    from litestar_queues.typing import ChannelsLike

__all__ = ("QueuePlugin",)

logger = logging.getLogger(__name__)

_UNKNOWN = object()


def _find_registered_channels_plugin(plugins: "Iterable[object]") -> "ChannelsLike | None":
    return next((plugin for plugin in plugins if isinstance(plugin, ChannelsPlugin)), None)


class QueuePlugin(InitPlugin):
    """Litestar plugin for queue service dependency registration and lifecycle."""

    __slots__ = (
        "_auto_channels_backend",
        "_config",
        "_event_publisher",
        "_queue_backend",
        "_service",
        "_worker",
        "_worker_task",
    )

    def __init__(self, config: "QueueConfig | None" = None) -> "None":
        """Initialize the queue plugin."""
        self._config = config or QueueConfig()
        self._service: "QueueService | None" = None
        self._queue_backend: "BaseQueueBackend | None" = None
        self._event_publisher: "QueueEventPublisher | None" = None
        self._auto_channels_backend: "ChannelsLike | None" = None
        self._worker: "Worker | None" = None
        self._worker_task: "asyncio.Task[None] | None" = None

    @property
    def config(self) -> "QueueConfig":
        """Plugin configuration."""
        return self._config

    def get_service(self, state: "State | None" = None) -> "QueueService":
        """Return a QueueService for this plugin."""
        if self._service is not None:
            return self._service
        return QueueService(self._config, queue_backend=self._queue_backend, event_publisher=self._event_publisher)

    def _configure_sqlspec_migrations(self) -> "None":
        """Register queue migrations with the application's SQLSpec config."""
        from litestar_queues.backends.sqlspec import SQLSpecBackendConfig

        backend_config = self._config.queue_backend
        if not isinstance(backend_config, SQLSpecBackendConfig):
            return

        sqlspec_config = backend_config.sqlspec_config
        if sqlspec_config is None and backend_config.sqlspec is not None:
            registered_configs = tuple(backend_config.sqlspec.configs.values())
            if len(registered_configs) == 1:
                sqlspec_config = registered_configs[0]
        if sqlspec_config is None:
            return

        from litestar_queues.backends.sqlspec.backend import resolve_events_migration_backend
        from litestar_queues.backends.sqlspec.extension import (
            configure_events_migration_extension,
            configure_queue_migration_extension,
        )
        from litestar_queues.backends.sqlspec.schema import DEFAULT_TABLE_NAME

        # Register the durable events queue migration first: a capability-native
        # adapter (asyncpg/psycopg/psqlpy notify_queue, DuckDB poll_queue) needs
        # its events table provisioned on migrate-up so zero-config native wakeups
        # work on a fresh database.
        events_backend = resolve_events_migration_backend(backend_config, cast("SQLSpecConfig", sqlspec_config))
        if events_backend is not None:
            configure_events_migration_extension(
                cast("SQLSpecConfig", sqlspec_config),
                backend=events_backend,
                queue_table=(
                    backend_config.worker_wakeups.queue_table_name
                    if backend_config.worker_wakeups is not None
                    else None
                ),
            )

        extension_config = sqlspec_config.extension_config or {}
        queue_settings = dict(extension_config.get("litestar_queues", {}) or {})
        queue_table_name = backend_config.queue_table_name or queue_settings.get("table_name") or DEFAULT_TABLE_NAME
        event_log_config = self._config.events.history if self._config.events is not None else None
        configure_queue_migration_extension(
            cast("SQLSpecConfig", sqlspec_config),
            queue_table_name=str(queue_table_name),
            event_history_enabled=event_log_config is not None,
            event_history_table_name=backend_config.event_history_table_name,
            maintenance_table_name=backend_config.maintenance_table_name,
            task_reservation_table_name=backend_config.task_reservation_table_name,
        )

    def on_app_init(self, app_config: "AppConfig") -> "AppConfig":
        """Register queue dependencies, signature namespace, state, and the lifespan manager.

        Returns:
            The updated application configuration.
        """
        self._configure_sqlspec_migrations()
        self._queue_backend = self._config.get_queue_backend()
        event_config = self._config.events
        if (
            event_config is not None
            and (event_config.delivery is not None or event_config.stream is not None)
            and event_config.channels is None
        ):
            # Zero-wiring live delivery or streaming without explicit channels resolves
            # the app's registered ChannelsPlugin. The config
            # object is never mutated so a QueueConfig shared across apps cannot leak
            # one app's ChannelsPlugin into another's publisher.
            self._auto_channels_backend = _find_registered_channels_plugin(app_config.plugins)
        self._event_publisher = self._config.get_event_publisher(channels_backend=self._auto_channels_backend)
        app_config.dependencies.update(self._config.dependencies)
        app_config.signature_namespace.update(self._config.signature_namespace)
        state = {_SERVICE_STATE_KEY: self._config, _EVENT_PUBLISHER_STATE_KEY: self._event_publisher}
        if self._config.events is not None and self._effective_channels_backend() is not None:
            state[_EVENT_CHANNELS_STATE_KEY] = self._effective_channels_backend()
        stream_config = self._config.events.stream if self._config.events is not None else None
        if stream_config is not None:
            from litestar_queues.events.streaming import build_stream_router

            self._verify_stream_channels_source(app_config)
            if (
                not app_config.guards
                and not stream_config.guards
                and stream_config.channel_authorizer is None
                and stream_config.unauthenticated_access != "allow"
            ):
                message = (
                    "Queue event streams have no configured authorization. Set a guard or channel_authorizer, "
                    "or explicitly set unauthenticated_access='allow'. See docs/usage/event-streams.rst."
                )
                if stream_config.unauthenticated_access == "error":
                    raise QueueConfigurationError(message)
                logger.warning(message)
            app_config.route_handlers.append(
                build_stream_router(self._config, stream_config, channels_backend=self._effective_channels_backend())
            )
        app_config.state.update(state)
        # Register lifecycle as a lifespan context manager (not on_startup/on_shutdown
        # hooks): Litestar runs on_shutdown hooks AFTER exiting every lifespan manager,
        # so a hook-based worker drain would flush events into an already-closed
        # ChannelsPlugin backend. As a lifespan manager appended after channels, exit is
        # LIFO, so the worker drains before channels tears down.
        app_config.lifespan.append(self._lifespan)
        return app_config

    def _effective_channels_backend(self) -> "ChannelsLike | None":
        if self._config.events is not None and self._config.events.channels is not None:
            return self._config.events.channels
        return self._auto_channels_backend

    def _validate_channels_shutdown_order(self, app: "Litestar") -> "None":
        """Fail fast at startup when a live-sink ChannelsPlugin is registered after this plugin.

        Litestar exits lifespan managers in LIFO order, so a ChannelsPlugin listed after
        ``QueuePlugin`` tears its backend down before the queue worker drains and every
        event published during the graceful-drain window hits a dead sink. Registration
        order cannot be fixed from ``on_app_init`` (a later ChannelsPlugin has not
        appended its lifespan manager yet), so misordering is rejected here instead.

        Raises:
            QueueConfigurationError: If the ChannelsPlugin targeted by the live event
                sink is registered after this plugin.
        """
        event_config = self._config.events
        if event_config is None or event_config.delivery is None or event_config.delivery.sinks:
            return
        target = self._effective_channels_backend()
        if target is None:
            return
        try:
            registered: "object | None" = app.plugins.get(ChannelsPlugin)
        except KeyError:
            # PluginRegistry.get keys by exact type; fall back for ChannelsPlugin subclasses.
            registered = _find_registered_channels_plugin(app.plugins)
        if registered is None or registered is not target:
            return
        # ChannelsPlugin._on_startup creates _pub_queue; it is None before startup and
        # after shutdown. A missing attribute (renamed litestar internals) degrades to
        # "unknown" and skips validation rather than crashing.
        if getattr(registered, "_pub_queue", _UNKNOWN) is not None:
            return

        msg = (
            "ChannelsPlugin must be registered before QueuePlugin so the queue worker "
            "drains before the channels backend closes on shutdown: "
            "plugins=[channels, QueuePlugin(config)]"
        )
        raise QueueConfigurationError(msg)

    def _verify_stream_channels_source(self, app_config: "AppConfig") -> "None":
        source: "object | None" = None
        if self._config.events is not None:
            source = self._config.events.channels
        if source is None:
            source = next((plugin for plugin in app_config.plugins if type(plugin).__name__ == "ChannelsPlugin"), None)
        if source is None or type(source).__name__ != "ChannelsPlugin":
            return
        if getattr(source, "_arbitrary_channels_allowed", False):
            return

        msg = (
            "Queue event streaming requires a ChannelsPlugin created with "
            "arbitrary_channels_allowed=True because queue channel names are dynamic "
            "(litestar_queues:task:<id>:events, ...). Reconstruct the plugin as "
            "ChannelsPlugin(backend=..., arbitrary_channels_allowed=True)."
        )
        raise QueueConfigurationError(msg)

    def on_cli_init(self, cli: "ClickGroup") -> "None":
        """Attach the ``queues`` subcommand group to the Litestar CLI.

        Args:
            cli: The root ``click.Group`` of the Litestar CLI.
        """
        self._configure_sqlspec_migrations()

        from litestar_queues._cli import register

        register(cli)

    @asynccontextmanager
    async def _lifespan(self, app: "Litestar") -> "AsyncIterator[None]":
        self._validate_channels_shutdown_order(app)
        if self._config.task_modules:
            load_task_modules(self._config.task_modules)

        observability_runtime = None
        observability_config = self._config.observability
        if observability_config is not None:
            from litestar_queues.observability import create_observability_runtime

            observability_runtime = create_observability_runtime(observability_config, app=app)
        if observability_runtime is not None:
            app.state[_OBSERVABILITY_RUNTIME_STATE_KEY] = observability_runtime

        self._service = QueueService(
            self._config,
            queue_backend=self._queue_backend,
            event_publisher=self._event_publisher,
            observability_runtime=observability_runtime,
        )
        await self._service.open()
        set_default_service(self._service)
        app.state[_SERVICE_STATE_KEY] = self._service
        app.state[_EVENT_PUBLISHER_STATE_KEY] = self._service.get_event_publisher()
        effective_channels = self._effective_channels_backend()
        if self._config.events is not None and effective_channels is not None:
            app.state[_EVENT_CHANNELS_STATE_KEY] = effective_channels

        if self._config.initialize_schedules:
            await self._service.initialize_schedules()

        if self._config.worker.run_in_app:
            self._worker = Worker(self._service, self._config.worker)
            self._worker_task = asyncio.create_task(self._worker.start())
            self._worker_task.add_done_callback(self._log_worker_task_result)
            await asyncio.sleep(0)
            app.state[_WORKER_STATE_KEY] = self._worker

        try:
            yield
        finally:
            if self._worker is not None:
                await self._worker.stop()
            if self._worker_task is not None:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._worker_task
                self._worker_task = None
            if self._service is not None:
                set_default_service(None)
                await self._service.close()
                self._service = None

    def _log_worker_task_result(self, task: "asyncio.Task[None]") -> "None":
        if task.cancelled():
            return
        exception = task.exception()
        if exception is None:
            return
        logger.error(
            "In-app queue worker stopped unexpectedly", exc_info=(type(exception), exception, exception.__traceback__)
        )

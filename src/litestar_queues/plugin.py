import asyncio
import contextlib
import logging
import os
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING

from litestar.channels import ChannelsPlugin
from litestar.plugins import CLIPlugin, InitPlugin

from litestar_queues.config import (
    _EVENT_CHANNELS_STATE_KEY,
    _EVENT_PUBLISHER_STATE_KEY,
    _OBSERVABILITY_RUNTIME_STATE_KEY,
    _SERVICE_STATE_KEY,
    _WORKER_STATE_KEY,
    MigrationConfiguringBackend,
    QueueConfig,
    execution_backend_name,
    queue_backend_name,
)
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.service import QueueService
from litestar_queues.task import load_task_modules, set_default_service
from litestar_queues.worker import Worker

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Generator, Iterable
    from contextlib import AbstractContextManager

    from click import Group as ClickGroup
    from litestar import Litestar
    from litestar.config.app import AppConfig
    from litestar.datastructures import State

    from litestar_queues.backends import BaseQueueBackend
    from litestar_queues.events import QueueEventPublisher
    from litestar_queues.typing import ChannelsLike

__all__ = ("QueuePlugin",)

logger = logging.getLogger(__name__)

_UNKNOWN = object()
_APP_PATH_ENV_VAR = "LITESTAR_APP"
_PROCESS_LOCAL_CHANNELS_BACKENDS = frozenset({"MemoryChannelsBackend"})

_MISSING_SERVER_CONTEXT_ERROR = (
    "WorkerConfig(placement='server') requires the Litestar CLI server lifecycle. Start the "
    "application with 'litestar run', or choose placement='asgi' to run one worker inside each "
    "ASGI process, or placement='external' to run 'litestar queues run' separately."
)
_PROCESS_LOCAL_CHANNELS_ERROR = (
    "WorkerConfig(placement='server') runs the worker in its own process, but live event "
    "delivery is configured against a process-local Channels backend ({backend}). Events "
    "published by the worker would never reach this application's subscribers. Use a "
    "cross-process Channels backend (for example RedisChannelsStreamBackend), or choose "
    "placement='asgi' to keep the worker in this process."
)
_MISSING_APP_PATH_ERROR = (
    "WorkerConfig(placement='server') loads a fresh application in the worker process, which "
    "requires an explicit app path. Start with 'litestar --app module:app run' or set the "
    "LITESTAR_APP environment variable; application autodiscovery is not supported for server "
    "placement."
)


def _find_registered_channels_plugin(plugins: "Iterable[object]") -> "ChannelsLike | None":
    return next((plugin for plugin in plugins if isinstance(plugin, ChannelsPlugin)), None)


def _process_local_channels_backend(channels: "object | None") -> "str | None":
    """Return the backend class name when it cannot cross a process boundary.

    Matched by name, like the other Channels checks here, so this never imports
    a Channels backend module that the application did not choose itself.

    Returns:
        The offending backend class name, or ``None`` when events can be shared.
    """
    backend = getattr(channels, "_backend", None)
    name = type(backend).__name__ if backend is not None else None
    return name if name in _PROCESS_LOCAL_CHANNELS_BACKENDS else None


class QueuePlugin(InitPlugin, CLIPlugin):
    """Litestar plugin for queue service dependency registration and lifecycle.

    Inheriting the concrete :class:`~litestar.plugins.CLIPlugin` is what makes
    Litestar register :meth:`server_lifespan`; satisfying ``CLIPluginProtocol``
    structurally is not enough.
    """

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

    def create_worker_service(self) -> "QueueService":
        """Create a fresh service that owns its process-local event resources."""
        return QueueService(
            self._config,
            queue_backend=self._config.get_queue_backend(),
            event_publisher=self._config.get_event_publisher(
                channels_backend=self._auto_channels_backend, manage_channels_lifecycle=True
            ),
        )

    def _configure_backend_migrations(self) -> "None":
        """Let the configured backend register whatever migrations it owns."""
        backend_config = self._config.queue_backend
        if isinstance(backend_config, MigrationConfiguringBackend):
            backend_config.configure_migrations(self._config)

    def on_app_init(self, app_config: "AppConfig") -> "AppConfig":
        """Register queue dependencies, signature namespace, state, and the lifespan manager.

        Returns:
            The updated application configuration.
        """
        self._configure_backend_migrations()
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
            from litestar_queues.events.streaming import _build_stream_router

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
                _build_stream_router(self._config, stream_config, channels_backend=self._effective_channels_backend())
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

        Only ASGI placement has a drain to order. Server and external workers
        publish events from their own process and lifecycle, so their teardown
        is unaffected by this application's plugin registration order.

        Raises:
            QueueConfigurationError: If the ChannelsPlugin targeted by the live event
                sink is registered after this plugin.
        """
        if self._config.worker.placement != "asgi":
            return
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
        from litestar_queues._cli import register

        self._configure_backend_migrations()
        register(cli)

    def _validate_server_placement(self) -> "None":
        """Reject a server invocation that cannot own a fresh worker process.

        This runs before any marker, database, or child process exists, so a
        misconfigured launch fails without leaving anything behind.

        Raises:
            QueueConfigurationError: If storage is process-local, execution is
                inline, or no explicit application path was selected.
        """
        backend = queue_backend_name(self._config.queue_backend)
        if backend == "memory":
            msg = (
                "queue_backend='memory' is process-local and cannot be shared with a server-owned "
                "worker process. Use the default queue_backend='ephemeral' or a persistent backend."
            )
            raise QueueConfigurationError(msg)
        if execution_backend_name(self._config.execution_backend) == "immediate":
            msg = (
                "execution_backend='immediate' runs tasks inline at enqueue time, so a server-owned "
                "worker would have nothing to claim. Use execution_backend='local'."
            )
            raise QueueConfigurationError(msg)
        if not os.environ.get(_APP_PATH_ENV_VAR):
            raise QueueConfigurationError(_MISSING_APP_PATH_ERROR)
        events = self._config.events
        if events is not None and events.delivery is not None:
            backend_name = _process_local_channels_backend(self._effective_channels_backend())
            if backend_name is not None:
                raise QueueConfigurationError(_PROCESS_LOCAL_CHANNELS_ERROR.format(backend=backend_name))

    def _storage_context(self, nonce: "str") -> "AbstractContextManager[object]":
        """Return the storage lifecycle this invocation owns.

        Returns:
            The private ephemeral database context, or a null context when the
            configured backend already persists outside this invocation.
        """
        if queue_backend_name(self._config.queue_backend) != "ephemeral":
            return contextlib.nullcontext()
        from litestar_queues.backends.ephemeral.server import EphemeralServerContext

        return EphemeralServerContext(nonce=nonce)

    @contextmanager
    def server_lifespan(self, app: "Litestar") -> "Generator[None]":
        """Own exactly one queue worker for the lifetime of a ``litestar run`` invocation.

        Litestar enters this once, around the whole server command, for both its
        direct Uvicorn call and its multi-worker/reload subprocess path. Any
        alternative run-command plugin enters the same hook, so there is
        deliberately no server-specific detection or flag parsing here.

        Yields:
            None: with the invocation marker, storage, and worker child active.
        """
        del app
        if self._config.worker.placement != "server":
            yield
            return
        self._validate_server_placement()
        from litestar_queues.worker import supervisor
        from litestar_queues.worker.invocation import console_break_unwinds, server_context

        with console_break_unwinds(), server_context() as nonce, self._storage_context(nonce):
            server_worker = supervisor.ServerWorkerSupervisor.from_plugin(self)
            server_worker.start()
            try:
                yield
            finally:
                server_worker.close()

    @asynccontextmanager
    async def _lifespan(self, app: "Litestar") -> "AsyncIterator[None]":
        placement = self._config.worker.placement
        if placement == "server":
            # Fail before the service opens so a raw ASGI launch never accepts
            # traffic against a queue whose worker was never started.
            from litestar_queues.worker.invocation import server_context_active

            if not server_context_active():
                raise QueueConfigurationError(_MISSING_SERVER_CONTEXT_ERROR)
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

        # Schedules belong to whichever process owns a worker. Server and
        # external placements initialize them from that owner instead, so an
        # enqueue-only ASGI process never writes schedule records.
        if placement == "asgi":
            if self._config.initialize_schedules:
                await self._service.initialize_schedules()
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

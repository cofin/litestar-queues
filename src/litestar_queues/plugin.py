import asyncio
import contextlib
import logging
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import TYPE_CHECKING

from litestar.plugins import InitPlugin

from litestar_queues.config import QueueConfig
from litestar_queues.service import QueueService
from litestar_queues.task import load_task_modules, set_default_service
from litestar_queues.worker import Worker

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from click import Group as ClickGroup
    from litestar import Litestar
    from litestar.config.app import AppConfig
    from litestar.datastructures import State

    from litestar_queues.backends import BaseQueueBackend
    from litestar_queues.events import QueueEventPublisher

__all__ = ("QueuePlugin",)

logger = logging.getLogger(__name__)


class QueuePlugin(InitPlugin):
    """Litestar plugin for queue service dependency registration and lifecycle."""

    __slots__ = ("_config", "_event_publisher", "_queue_backend", "_service", "_worker", "_worker_task")

    def __init__(self, config: "QueueConfig | None" = None) -> "None":
        """Initialize the queue plugin."""
        self._config = config or QueueConfig()
        self._service: "QueueService | None" = None
        self._queue_backend: "BaseQueueBackend | None" = None
        self._event_publisher: "QueueEventPublisher | None" = None
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

    def on_app_init(self, app_config: "AppConfig") -> "AppConfig":
        """Register queue dependencies, signature namespace, state, and the lifespan manager.

        Returns:
            The updated application configuration.
        """
        self._queue_backend = self._config.get_queue_backend()
        self._event_publisher = self._config.get_event_publisher()
        app_config.dependencies.update(self._config.dependencies)
        app_config.signature_namespace.update(self._config.signature_namespace)
        state = {
            self._config.queue_service_state_key: self._config,
            self._config.queue_event_publisher_state_key: self._event_publisher,
        }
        if self._config.event is not None and self._config.event.channels_backend is not None:
            state[self._config.queue_event_channels_backend_state_key] = self._config.event.channels_backend
        stream_config = self._config.event_stream
        if stream_config is not None and stream_config.enabled:
            from litestar_queues.events.streaming import build_stream_router

            self._verify_stream_channels_source(app_config)
            if not stream_config.guards and stream_config.channel_authorizer is None:
                logger.warning(
                    "Queue event streaming is enabled without guards or a channel_authorizer; "
                    "task, queue, and worker metadata will be served to unauthenticated clients. "
                    "Set EventStreamConfig(guards=..., channel_authorizer=..., scopes=...) "
                    "to restrict access. See docs/usage/events.rst."
                )
            app_config.route_handlers.append(build_stream_router(self._config, stream_config))
            state[self._config.queue_event_stream_state_key] = stream_config
        app_config.state.update(state)
        # Register lifecycle as a lifespan context manager (not on_startup/on_shutdown
        # hooks): Litestar runs on_shutdown hooks AFTER exiting every lifespan manager,
        # so a hook-based worker drain would flush events into an already-closed
        # ChannelsPlugin backend. As a lifespan manager appended after channels, exit is
        # LIFO, so the worker drains before channels tears down.
        app_config.lifespan.append(self._lifespan)
        return app_config

    def _verify_stream_channels_source(self, app_config: "AppConfig") -> "None":
        source: "object | None" = None
        if self._config.event is not None:
            source = self._config.event.channels_backend
        if source is None:
            source = next((plugin for plugin in app_config.plugins if type(plugin).__name__ == "ChannelsPlugin"), None)
        if source is None or type(source).__name__ != "ChannelsPlugin":
            return
        if getattr(source, "_arbitrary_channels_allowed", False):
            return

        from litestar_queues.exceptions import QueueConfigurationError

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

        register(cli)

    @asynccontextmanager
    async def _lifespan(self, app: "Litestar") -> "AsyncIterator[None]":
        if self._config.task_modules:
            load_task_modules(self._config.task_modules)

        observability_runtime = None
        observability_config = self._config.observability
        if observability_config is not None:
            from litestar_queues.observability import create_observability_runtime

            observability_runtime = create_observability_runtime(observability_config, app=app)
        if observability_runtime is not None:
            app.state[self._config.queue_observability_runtime_state_key] = observability_runtime

        self._service = QueueService(
            self._config,
            queue_backend=self._queue_backend,
            event_publisher=self._event_publisher,
            observability_runtime=observability_runtime,
        )
        await self._service.open()
        set_default_service(self._service)
        app.state[self._config.queue_service_state_key] = self._service
        app.state[self._config.queue_event_publisher_state_key] = self._service.get_event_publisher()
        if self._config.event is not None and self._config.event.channels_backend is not None:
            app.state[self._config.queue_event_channels_backend_state_key] = self._config.event.channels_backend

        if self._config.initialize_schedules:
            await self._service.initialize_schedules()

        if self._config.in_app_worker:
            self._worker = Worker(
                self._service,
                batch_size=self._config.worker_batch_size,
                poll_interval=self._config.worker_poll_interval,
                max_concurrency=self._config.worker_max_concurrency,
                heartbeat_interval=self._config.worker_heartbeat_interval,
                heartbeat_miss_threshold=self._config.worker_heartbeat_miss_threshold,
                reconcile_interval=self._config.worker_reconcile_interval,
                stale_after=(
                    timedelta(seconds=self._config.worker_stale_after)
                    if self._config.worker_stale_after is not None
                    else None
                ),
                stale_check_interval=self._config.worker_stale_check_interval,
                graceful_shutdown_timeout=self._config.worker_graceful_shutdown_timeout,
                final_cancel_timeout=self._config.worker_final_cancel_timeout,
                queues=self._config.worker_queues,
            )
            self._worker_task = asyncio.create_task(self._worker.start())
            self._worker_task.add_done_callback(self._log_worker_task_result)
            await asyncio.sleep(0)
            app.state[self._config.queue_worker_state_key] = self._worker

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

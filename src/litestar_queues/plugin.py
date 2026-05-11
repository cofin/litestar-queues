import asyncio
import contextlib
from datetime import timedelta
from typing import TYPE_CHECKING

from litestar_queues.service import QueueService
from litestar_queues.task import load_task_modules
from litestar_queues.worker import Worker

if TYPE_CHECKING:
    from litestar import Litestar
    from litestar.config.app import AppConfig
    from litestar.datastructures import State

    from litestar_queues.backends import BaseQueueBackend
    from litestar_queues.config import QueueConfig
    from litestar_queues.events import QueueEventPublisher

__all__ = ("QueuePlugin",)


class QueuePlugin:
    """Litestar plugin for queue service dependency registration and lifecycle."""

    __slots__ = ("_config", "_event_publisher", "_queue_backend", "_service", "_worker", "_worker_task")

    def __init__(self, config: "QueueConfig | None" = None) -> None:
        """Initialize the queue plugin."""
        from litestar_queues.config import QueueConfig

        self._config = config or QueueConfig()
        self._service: QueueService | None = None
        self._queue_backend: "BaseQueueBackend | None" = None
        self._event_publisher: "QueueEventPublisher | None" = None
        self._worker: Worker | None = None
        self._worker_task: asyncio.Task[None] | None = None

    @property
    def config(self) -> "QueueConfig":
        """Return the plugin configuration."""
        return self._config

    def get_service(self, state: "State | None" = None) -> QueueService:
        """Return a QueueService for this plugin."""
        if self._service is not None:
            return self._service
        return QueueService(self._config, queue_backend=self._queue_backend, event_publisher=self._event_publisher)

    def on_app_init(self, app_config: "AppConfig") -> "AppConfig":
        """Register queue dependencies, signature namespace, state, and lifecycle hooks.

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
        if self._config.event_config.channels_backend is not None:
            state[self._config.queue_event_channels_backend_state_key] = self._config.event_config.channels_backend
        app_config.state.update(state)
        app_config.on_startup.append(self._on_startup)
        app_config.on_shutdown.append(self._on_shutdown)
        return app_config

    async def _on_startup(self, app: "Litestar") -> None:
        if self._config.task_modules:
            load_task_modules(self._config.task_modules)

        self._service = QueueService(
            self._config,
            queue_backend=self._queue_backend,
            event_publisher=self._event_publisher,
        )
        await self._service.open()
        app.state[self._config.queue_service_state_key] = self._service
        app.state[self._config.queue_event_publisher_state_key] = self._service.get_event_publisher()
        if self._config.event_config.channels_backend is not None:
            app.state[self._config.queue_event_channels_backend_state_key] = self._config.event_config.channels_backend

        if self._config.initialize_schedules:
            await self._service.initialize_schedules()

        if self._config.start_worker:
            self._worker = Worker(
                self._service,
                batch_size=self._config.worker_batch_size,
                poll_interval=self._config.worker_poll_interval,
                max_concurrency=self._config.worker_max_concurrency,
                heartbeat_interval=self._config.worker_heartbeat_interval,
                reconcile_interval=self._config.worker_reconcile_interval,
                stale_after=(
                    timedelta(seconds=self._config.worker_stale_after)
                    if self._config.worker_stale_after is not None
                    else None
                ),
                stale_check_interval=self._config.worker_stale_check_interval,
                graceful_shutdown_timeout=self._config.worker_graceful_shutdown_timeout,
                final_cancel_timeout=self._config.worker_final_cancel_timeout,
            )
            self._worker_task = asyncio.create_task(self._worker.start())
            await asyncio.sleep(0)
            app.state[self._config.queue_worker_state_key] = self._worker

    async def _on_shutdown(self, app: "Litestar") -> None:
        if self._worker is not None:
            await self._worker.stop()
        if self._worker_task is not None:
            self._worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._worker_task
            self._worker_task = None
        if self._service is not None:
            await self._service.close()
            self._service = None

"""SQLSpec queue backend."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, timezone
from inspect import isawaitable
from logging import getLogger
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from sqlspec import SQLSpec
from sqlspec.extensions.events import normalize_event_channel_name
from sqlspec.utils.sync_tools import ensure_async_, with_ensure_async_

from litestar_queues.backends.base import BaseQueueBackend
from litestar_queues.backends.sqlspec.config import DEFAULT_NOTIFICATION_CHANNEL, SQLSpecBackendConfig
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME, configure_queue_migration_extension
from litestar_queues.backends.sqlspec.schema import (
    DEFAULT_TABLE_NAME,
    validate_column_map,
    validate_native_json_columns,
    validate_table_name,
)
from litestar_queues.backends.sqlspec.stores.factory import create_queue_store
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.models import QueueBackendCapabilities, QueuedTaskRecord, QueueStatistics, TaskStatus

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig

__all__ = ("SQLSpecQueueBackend",)

_DUE_STATUSES = ("pending", "scheduled")
_DURABLE_NOTIFICATION_BACKENDS = frozenset({"advanced_queue", "listen_notify_durable", "table_queue"})
_EVENT_EXTENSION_NAME = "events"
_QUEUE_SETTING_EVENT_SETTINGS = ("event_settings", "events")


class SQLSpecQueueBackend(BaseQueueBackend):
    """SQLSpec-backed queue backend."""

    __slots__ = (
        "_column_map",
        "_create_schema",
        "_event_backend",
        "_event_channel",
        "_event_poll_interval",
        "_event_queue_table",
        "_event_settings",
        "_heartbeat_pool_config",
        "_heartbeat_pool_enabled",
        "_heartbeat_pool_registered",
        "_manage_schema",
        "_native_json_columns",
        "_notification_backend",
        "_notification_channel",
        "_notifications_enabled",
        "_notifications_requested",
        "_opened",
        "_owns_event_channel",
        "_owns_sqlspec",
        "_run_migrations",
        "_sqlspec",
        "_sqlspec_config",
        "_store",
        "_table_name",
    )

    def __init__(
        self,
        config: "QueueConfig | None" = None,
        *,
        backend_config: SQLSpecBackendConfig | None = None,
    ) -> None:
        super().__init__(config=config)
        backend_config = backend_config or SQLSpecBackendConfig()
        self._column_map = validate_column_map(backend_config.column_map)
        self._native_json_columns = validate_native_json_columns(frozenset(backend_config.native_json_columns))
        self._manage_schema = backend_config.manage_schema
        self._sqlspec = backend_config.sqlspec
        self._sqlspec_config = backend_config.sqlspec_config
        self._heartbeat_pool_config = backend_config.heartbeat_pool_config
        self._heartbeat_pool_enabled = self._heartbeat_pool_config is not None
        self._heartbeat_pool_registered = False
        self._owns_sqlspec = self._sqlspec is None
        self._table_name = (
            validate_table_name(backend_config.table_name) if backend_config.table_name is not None else None
        )
        self._create_schema = backend_config.create_schema
        self._run_migrations = backend_config.run_migrations
        self._event_channel = backend_config.event_channel
        self._owns_event_channel = self._event_channel is None
        self._notifications_requested = backend_config.notifications
        self._notification_channel = backend_config.notification_channel
        self._event_backend = backend_config.event_backend
        self._event_queue_table = backend_config.event_queue_table
        self._event_poll_interval = backend_config.event_poll_interval
        self._event_settings = dict(backend_config.event_settings)
        self._notification_backend: str | None = getattr(self._event_channel, "_backend_name", None)
        self._notifications_enabled = self._event_channel is not None
        self._store: Any | None = None
        self._opened = False

    async def open(self) -> bool:
        """Open SQLSpec resources.

        Returns:
            True when SQLSpec resources are ready.
        """
        if self._opened:
            return True

        self._get_or_create_sqlspec()
        self._resolve_table_name()
        self._configure_notifications()
        self._register_heartbeat_pool()
        self._opened = True
        if self._resolve_run_migrations():
            await self.run_migrations()
        if self._resolve_create_schema():
            await self.create_schema()
        return True

    async def close(self) -> None:
        """Close SQLSpec resources."""
        await self._close_heartbeat_pool()
        if self._owns_event_channel and self._event_channel is not None:
            await self._event_channel.shutdown()
            self._event_channel = None
        if self._owns_sqlspec and self._sqlspec is not None:
            await self._sqlspec.close_all_pools()
            self._sqlspec = None
        self._opened = False

    @property
    def capabilities(self) -> QueueBackendCapabilities:
        """Return backend behavior capabilities."""
        notification_backend = self._notification_backend
        return QueueBackendCapabilities(
            supports_notifications=self._notifications_enabled,
            notification_backend=notification_backend,
            notifications_durable=notification_backend in _DURABLE_NOTIFICATION_BACKENDS,
        )

    async def create_schema(self) -> None:
        """Create the SQLSpec queue table and indexes."""
        if not self._manage_schema:
            return
        async with self._session() as driver:
            for statement in self._get_store().create_statements():
                await driver.execute_script(statement)
            await driver.commit()

    async def run_migrations(self) -> None:
        """Apply packaged SQLSpec migrations."""
        if not self._manage_schema:
            return
        sqlspec_config = self._get_sqlspec_config()
        configure_queue_migration_extension(sqlspec_config, table_name=self._resolve_table_name())
        await sqlspec_config.migrate_up(echo=False)

    async def enqueue(
        self,
        task_name: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        queue: str = "default",
        priority: int = 0,
        max_retries: int = 0,
        scheduled_at: datetime | None = None,
        key: str | None = None,
        execution_backend: str = "local",
        execution_profile: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> QueuedTaskRecord:
        async with self._session() as driver:
            await driver.begin()
            try:
                if key is not None:
                    existing_row = await self._select_task_by_key(driver, key)
                    if existing_row is not None:
                        existing = self._record_from_row(existing_row)
                        if not existing.is_terminal:
                            await driver.rollback()
                            return existing
                        await self._clear_key(driver, existing.id)

                now = _utc_now()
                record = QueuedTaskRecord(
                    task_name=task_name,
                    args=args,
                    kwargs=dict(kwargs or {}),
                    queue=queue,
                    execution_backend=execution_backend,
                    execution_profile=execution_profile,
                    status="scheduled" if scheduled_at is not None and scheduled_at > now else "pending",
                    priority=priority,
                    max_retries=max_retries,
                    scheduled_at=scheduled_at,
                    key=key,
                    metadata=dict(metadata or {}),
                )
                await driver.execute(self._get_store().insert_task(self._params_from_record(record)))
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        await self.notify_new_task(record)
        return record

    async def get_task(self, task_id: UUID) -> QueuedTaskRecord | None:
        async with self._session() as driver:
            row = await self._select_task(driver, task_id)
        return self._record_from_row(row) if row is not None else None

    async def get_task_by_key(self, key: str) -> QueuedTaskRecord | None:
        async with self._session() as driver:
            row = await self._select_task_by_key(driver, key)
        return self._record_from_row(row) if row is not None else None

    async def list_pending(
        self,
        *,
        limit: int = 1,
        queue: str | None = None,
        execution_backend: str | None = None,
    ) -> list[QueuedTaskRecord]:
        rows = await self._select_pending_rows(limit=limit, queue=queue, execution_backend=execution_backend)
        return [self._record_from_row(row) for row in rows]

    async def claim_task(self, task_id: UUID) -> QueuedTaskRecord | None:
        async with self._session() as driver:
            await driver.begin()
            try:
                row = await self._select_task(driver, task_id)
                if row is None:
                    await driver.rollback()
                    return None

                record = self._record_from_row(row)
                if record.status not in _DUE_STATUSES or not record.is_due:
                    await driver.rollback()
                    return None

                now = _utc_now()
                result = await driver.execute(
                    self._get_store().claim_task(
                        task_id=str(task_id),
                        due_at=_serialize_datetime(now),
                        heartbeat_at=_serialize_datetime(now),
                        started_at=_serialize_datetime(now),
                    ),
                )
                if result.rows_affected == 0:
                    await driver.rollback()
                    return None

                updated_row = await self._select_task(driver, task_id)
                if updated_row is None or self._record_from_row(updated_row).status != "running":
                    await driver.rollback()
                    return None
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        return self._record_from_row(updated_row) if updated_row is not None else None

    async def claim_next(
        self,
        *,
        queue: str | None = None,
        execution_backend: str | None = None,
    ) -> QueuedTaskRecord | None:
        rows = await self._select_pending_rows(limit=10, queue=queue, execution_backend=execution_backend)
        for row in rows:
            task_id = UUID(str(row["id"]))
            claimed = await self.claim_task(task_id)
            if claimed is not None:
                return claimed
        return None

    async def complete_task(self, task_id: UUID, *, result: Any = None) -> QueuedTaskRecord | None:
        now = _utc_now()
        store = self._get_store()
        async with self._session() as driver:
            await driver.begin()
            try:
                updated = await driver.execute(
                    store.complete_task(
                        task_id=str(task_id),
                        completed_at=_serialize_datetime(now),
                        heartbeat_at=_serialize_datetime(now),
                        result_json=store.serialize_json_column("result_json", result),
                    ),
                )
                row = await self._select_task(driver, task_id) if updated.rows_affected else None
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        return self._record_from_row(row) if row is not None else None

    async def fail_task(
        self,
        task_id: UUID,
        error: str,
        *,
        retry: bool = True,
    ) -> QueuedTaskRecord | None:
        async with self._session() as driver:
            await driver.begin()
            try:
                row = await self._select_task(driver, task_id)
                if row is None:
                    await driver.rollback()
                    return None

                record = self._record_from_row(row)
                if retry and record.retry_count < record.max_retries:
                    await driver.execute(
                        self._get_store().retry_task(
                            task_id=str(task_id),
                            error=error,
                            retry_count=record.retry_count + 1,
                        ),
                    )
                else:
                    now = _utc_now()
                    await driver.execute(
                        self._get_store().fail_task(
                            task_id=str(task_id),
                            completed_at=_serialize_datetime(now),
                            heartbeat_at=_serialize_datetime(now),
                            error=error,
                        ),
                    )

                updated_row = await self._select_task(driver, task_id)
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        return self._record_from_row(updated_row) if updated_row is not None else None

    async def cancel_task(self, task_id: UUID) -> bool:
        async with self._session() as driver:
            await driver.begin()
            try:
                result = await driver.execute(
                    self._get_store().cancel_task(task_id=str(task_id), completed_at=_serialize_datetime(_utc_now())),
                )
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        return int(result.rows_affected) == 1

    async def touch_heartbeat(self, task_id: UUID) -> None:
        async with self._heartbeat_session() as driver:
            await driver.begin()
            try:
                await driver.execute(
                    self._get_store().touch_heartbeat(
                        task_id=str(task_id),
                        heartbeat_at=_serialize_datetime(_utc_now()),
                    ),
                )
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise

    async def null_heartbeats(self, task_ids: list[UUID]) -> None:
        if not task_ids:
            return
        async with self._heartbeat_session() as driver:
            await driver.begin()
            try:
                await driver.execute(self._get_store().null_heartbeats(task_ids=[str(task_id) for task_id in task_ids]))
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise

    async def requeue_stale_running(self, *, stale_after: timedelta) -> int:
        cutoff = _utc_now() - stale_after
        async with self._session() as driver:
            await driver.begin()
            try:
                result = await driver.execute(self._get_store().requeue_stale(cutoff=_serialize_datetime(cutoff)))
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        return int(result.rows_affected)

    async def set_execution_ref(
        self,
        task_id: UUID,
        execution_backend: str,
        execution_ref: str,
        *,
        execution_profile: str | None = None,
    ) -> QueuedTaskRecord | None:
        async with self._session() as driver:
            await driver.begin()
            try:
                result = await driver.execute(
                    self._get_store().set_execution_ref(
                        task_id=str(task_id),
                        execution_backend=execution_backend,
                        execution_profile=execution_profile,
                        execution_ref=execution_ref,
                    ),
                )
                row = await self._select_task(driver, task_id) if result.rows_affected else None
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        return self._record_from_row(row) if row is not None else None

    async def set_execution_backend(
        self,
        task_id: UUID,
        execution_backend: str,
        *,
        execution_profile: str | None = None,
    ) -> QueuedTaskRecord | None:
        async with self._session() as driver:
            await driver.begin()
            try:
                result = await driver.execute(
                    self._get_store().set_execution_backend(
                        task_id=str(task_id),
                        execution_backend=execution_backend,
                        execution_profile=execution_profile,
                    ),
                )
                row = await self._select_task(driver, task_id) if result.rows_affected else None
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        record = self._record_from_row(row) if row is not None else None
        if record is not None:
            await self.notify_new_task(record)
        return record

    async def list_running_external(self, *, limit: int | None = None) -> list[QueuedTaskRecord]:
        async with self._session() as driver:
            rows = await driver.select(self._get_store().list_running_external(limit=limit))
        return [self._record_from_row(row) for row in cast("list[dict[str, Any]]", rows)]

    async def get_statistics(self) -> QueueStatistics:
        async with self._session() as driver:
            rows = await driver.select(self._get_store().list_all())
        statistics = QueueStatistics()
        for row in cast("list[dict[str, Any]]", rows):
            status = _coerce_status(row["status"])
            setattr(statistics, status, getattr(statistics, status) + 1)
        return statistics

    async def list_completed_by_task(
        self,
        task_name: str,
        *,
        since: datetime | None = None,
        limit: int = 10,
    ) -> list[QueuedTaskRecord]:
        async with self._session() as driver:
            rows = await driver.select(
                self._get_store().list_completed_by_task(
                    task_name=task_name,
                    since=_serialize_datetime(since),
                    limit=limit,
                )
            )
        return [self._record_from_row(row) for row in cast("list[dict[str, Any]]", rows)]

    async def cleanup_terminal(self, before: datetime) -> int:
        async with self._session() as driver:
            await driver.begin()
            try:
                result = await driver.execute(self._get_store().cleanup_terminal(before=_serialize_datetime(before)))
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        return int(result.rows_affected)

    async def notify_new_task(self, record: QueuedTaskRecord) -> None:
        """Publish a SQLSpec event when configured queue work becomes available."""
        if not self._notifications_enabled or self._event_channel is None or record.status not in _DUE_STATUSES:
            return
        await self._event_channel.publish(
            self._resolve_notification_channel(),
            {
                "task_id": str(record.id),
                "task_name": record.task_name,
                "queue": record.queue,
                "execution_backend": record.execution_backend,
            },
            {"event_type": "litestar_queues.task_available"},
        )

    async def wait_for_notifications(self, timeout: float | None = None) -> bool:
        """Wait for a SQLSpec event when queue notifications are configured.

        Returns:
            True when a notification was received.
        """
        if not self._notifications_enabled or self._event_channel is None:
            return await super().wait_for_notifications(timeout=timeout)

        stream = self._event_channel.iter_events(
            self._resolve_notification_channel(),
            poll_interval=self._event_poll_interval if self._event_poll_interval is not None else timeout,
        )
        try:
            if timeout is None:
                event = await anext(stream)
            else:
                event = await asyncio.wait_for(anext(stream), timeout=timeout)
        except TimeoutError:
            return False
        finally:
            await cast("Any", stream).aclose()

        await self._event_channel.ack(event.event_id)
        return True

    @staticmethod
    def _default_sqlspec_config() -> Any:
        from sqlspec.adapters.aiosqlite import AiosqliteConfig

        return AiosqliteConfig()

    def _resolve_table_name(self) -> str:
        if self._table_name is None:
            queue_settings = _queue_extension_settings(self._sqlspec_config)
            configured_table_name = _setting(queue_settings, "table_name") or DEFAULT_TABLE_NAME
            self._table_name = validate_table_name(str(configured_table_name))
        return self._table_name

    def _resolve_create_schema(self) -> bool:
        if not self._manage_schema:
            return False
        return _resolve_bool(
            self._create_schema, _queue_extension_settings(self._sqlspec_config), "create_schema", True
        )

    def _resolve_run_migrations(self) -> bool:
        if not self._manage_schema:
            return False
        return _resolve_bool(
            self._run_migrations,
            _queue_extension_settings(self._sqlspec_config),
            "run_migrations",
            False,
        )

    def _resolve_notification_channel(self) -> str:
        if self._notification_channel is not None:
            self._notification_channel = _normalize_notification_channel(str(self._notification_channel))
        else:
            queue_settings = _queue_extension_settings(self._sqlspec_config)
            configured_channel = _setting(queue_settings, "notification_channel") or DEFAULT_NOTIFICATION_CHANNEL
            self._notification_channel = _normalize_notification_channel(str(configured_channel))
        return self._notification_channel

    def _configure_notifications(self) -> None:
        sqlspec_config = self._get_sqlspec_config()
        queue_settings = _queue_extension_settings(sqlspec_config)
        events_settings = _events_extension_settings(sqlspec_config)
        events_settings = self._configure_notification_overrides(sqlspec_config, queue_settings, events_settings)

        notifications_requested = self._notifications_requested
        if notifications_requested is None and "notifications" in queue_settings:
            notifications_requested = bool(queue_settings["notifications"])
        events_configured = bool(events_settings) or _EVENT_EXTENSION_NAME in cast(
            "dict[str, Any]", getattr(sqlspec_config, "extension_config", {}) or {}
        )
        self._notifications_enabled = bool(
            self._event_channel is not None or notifications_requested is True or events_configured
        )
        if notifications_requested is False:
            self._notifications_enabled = False
        if not self._notifications_enabled:
            self._notification_backend = None
            return

        self._resolve_notification_channel()
        if self._event_channel is None:
            self._event_channel = self._get_or_create_sqlspec().event_channel(sqlspec_config)
            self._owns_event_channel = True
        self._notification_backend = cast("str | None", getattr(self._event_channel, "_backend_name", None))

    def _configure_notification_overrides(
        self,
        sqlspec_config: Any,
        queue_settings: dict[str, Any],
        events_settings: dict[str, Any],
    ) -> dict[str, Any]:
        merged_event_settings = dict(events_settings)
        for name in _QUEUE_SETTING_EVENT_SETTINGS:
            configured_events = queue_settings.get(name)
            if isinstance(configured_events, dict):
                merged_event_settings.update(configured_events)
        merged_event_settings.update(self._event_settings)

        configured_backend = self._event_backend or _setting(queue_settings, "event_backend")
        configured_queue_table = self._event_queue_table or _setting(queue_settings, "event_queue_table")
        configured_poll_interval = self._event_poll_interval
        if configured_poll_interval is None:
            configured_poll_interval = _setting(queue_settings, "event_poll_interval")
        if configured_poll_interval is None and "poll_interval" in merged_event_settings:
            configured_poll_interval = merged_event_settings["poll_interval"]

        if configured_backend is not None:
            merged_event_settings["backend"] = str(configured_backend)
        if configured_queue_table is not None:
            merged_event_settings["queue_table"] = str(configured_queue_table)
        if configured_poll_interval is not None:
            self._event_poll_interval = float(configured_poll_interval)
            merged_event_settings["poll_interval"] = self._event_poll_interval

        notifications_requested = self._notifications_requested
        if notifications_requested is None and "notifications" in queue_settings:
            notifications_requested = bool(queue_settings["notifications"])
        should_store_event_settings = bool(merged_event_settings) or notifications_requested is True
        if not should_store_event_settings:
            return merged_event_settings

        extension_config = dict(cast("dict[str, Any]", getattr(sqlspec_config, "extension_config", {}) or {}))
        extension_config[_EVENT_EXTENSION_NAME] = merged_event_settings
        sqlspec_config.extension_config = extension_config
        migration_config = dict(cast("dict[str, Any]", getattr(sqlspec_config, "migration_config", {}) or {}))
        sqlspec_config.set_migration_config(migration_config)
        return merged_event_settings

    def _get_or_create_sqlspec(self) -> SQLSpec:
        if self._sqlspec is None:
            self._sqlspec = SQLSpec()
        return self._sqlspec

    def _get_sqlspec_config(self) -> Any:
        if self._sqlspec_config is None:
            registered_configs = tuple(cast("dict[int, Any]", self._get_or_create_sqlspec().configs).values())
            if len(registered_configs) == 1:
                self._sqlspec_config = registered_configs[0]
            elif len(registered_configs) > 1:
                msg = (
                    "SQLSpecQueueBackend received a SQLSpec manager with multiple configs; "
                    "pass sqlspec_config to select the queue database."
                )
                raise QueueConfigurationError(msg)
            else:
                self._sqlspec_config = self._default_sqlspec_config()
        return self._sqlspec_config

    def _get_store(self) -> Any:
        if self._store is None:
            self._store = create_queue_store(
                self._get_sqlspec_config(),
                table_name=self._resolve_table_name(),
                column_map=self._column_map,
                native_json_columns=self._native_json_columns,
                manage_schema=self._manage_schema,
            )
        return self._store

    @asynccontextmanager
    async def _session(self) -> AsyncIterator[Any]:
        if not self._opened or self._sqlspec is None:
            msg = "SQLSpecQueueBackend.open() must be called before using the backend."
            raise RuntimeError(msg)
        sqlspec_config = self._get_sqlspec_config()
        async with _bridge_session(self._get_or_create_sqlspec(), sqlspec_config) as driver:
            yield driver

    @asynccontextmanager
    async def _heartbeat_session(self) -> AsyncIterator[Any]:
        """Yield a driver bound to the dedicated heartbeat pool when configured.

        Falls back to the main pool when ``heartbeat_pool_config`` is not set,
        or when the dedicated pool failed to register at ``open()`` time.

        Raises:
            RuntimeError: When ``open()`` has not been called on the backend.
        """
        if not self._opened or self._sqlspec is None:
            msg = "SQLSpecQueueBackend.open() must be called before using the backend."
            raise RuntimeError(msg)
        if self._heartbeat_pool_enabled and self._heartbeat_pool_registered and self._heartbeat_pool_config is not None:
            async with _bridge_session(self._sqlspec, self._heartbeat_pool_config) as driver:
                yield driver
            return
        async with self._session() as driver:
            yield driver

    def _register_heartbeat_pool(self) -> None:
        """Register the dedicated heartbeat pool with the SQLSpec manager.

        Best effort. On failure the backend logs a warning and continues with
        the main pool for heartbeats.
        """
        if not self._heartbeat_pool_enabled or self._heartbeat_pool_config is None:
            return
        if self._heartbeat_pool_registered:
            return
        try:
            self._get_or_create_sqlspec().add_config(self._heartbeat_pool_config)
        except Exception:
            getLogger("litestar_queues").warning(
                "SQLSpecQueueBackend heartbeat pool registration failed; "
                "falling back to main pool for heartbeat writes.",
                exc_info=True,
            )
            self._heartbeat_pool_enabled = False
            self._heartbeat_pool_registered = False
            return
        self._heartbeat_pool_registered = True

    async def _close_heartbeat_pool(self) -> None:
        """Close the dedicated heartbeat pool if the backend opened one."""
        if not self._heartbeat_pool_registered or self._heartbeat_pool_config is None:
            return

        try:
            close_result = self._heartbeat_pool_config.close_pool()
            if isawaitable(close_result):
                await close_result
        except Exception:
            getLogger("litestar_queues").debug("SQLSpecQueueBackend heartbeat pool close failed.", exc_info=True)
        self._heartbeat_pool_registered = False

    async def _select_pending_rows(
        self,
        *,
        limit: int,
        queue: str | None,
        execution_backend: str | None,
    ) -> list[dict[str, Any]]:
        async with self._session() as driver:
            rows = await driver.select(
                self._get_store().list_pending(
                    now=_serialize_datetime(_utc_now()),
                    limit=limit,
                    queue=queue,
                    execution_backend=execution_backend,
                )
            )
        return cast("list[dict[str, Any]]", rows)

    async def _select_task(self, driver: Any, task_id: UUID) -> dict[str, Any] | None:
        row = await driver.select_one_or_none(self._get_store().select_task(str(task_id)))
        return cast("dict[str, Any] | None", row)

    async def _select_task_by_key(self, driver: Any, key: str) -> dict[str, Any] | None:
        row = await driver.select_one_or_none(self._get_store().select_task_by_key(key))
        return cast("dict[str, Any] | None", row)

    async def _clear_key(self, driver: Any, task_id: UUID) -> None:
        await driver.execute(self._get_store().clear_key(task_id=str(task_id)))

    def _params_from_record(self, record: QueuedTaskRecord) -> dict[str, Any]:
        store = self._get_store()
        return {
            "args_json": store.serialize_json_column("args_json", list(record.args)),
            "completed_at": _serialize_datetime(record.completed_at),
            "created_at": _serialize_datetime(record.created_at),
            "error": record.error,
            "execution_backend": record.execution_backend,
            "execution_profile": record.execution_profile,
            "execution_ref": record.execution_ref,
            "heartbeat_at": _serialize_datetime(record.heartbeat_at),
            "id": str(record.id),
            "kwargs_json": store.serialize_json_column("kwargs_json", record.kwargs),
            "max_retries": record.max_retries,
            "metadata_json": store.serialize_json_column("metadata_json", record.metadata),
            "priority": record.priority,
            "queue": record.queue,
            "result_json": store.serialize_json_column("result_json", record.result),
            "retry_count": record.retry_count,
            "scheduled_at": _serialize_datetime(record.scheduled_at),
            "started_at": _serialize_datetime(record.started_at),
            "status": record.status,
            "task_key": record.key,
            "task_name": record.task_name,
        }

    def _record_from_row(self, row: dict[str, Any]) -> QueuedTaskRecord:
        store = self._get_store()
        args = store.deserialize_json(row["args_json"])
        kwargs = store.deserialize_json(row["kwargs_json"])
        metadata = store.deserialize_json(row["metadata_json"])
        return QueuedTaskRecord(
            id=UUID(str(row["id"])),
            task_name=str(row["task_name"]),
            args=tuple(args),
            kwargs=kwargs,
            queue=str(row["queue"]),
            execution_backend=str(row["execution_backend"]),
            execution_profile=cast("str | None", row["execution_profile"]),
            execution_ref=cast("str | None", row["execution_ref"]),
            status=_coerce_status(row["status"]),
            priority=int(row["priority"]),
            max_retries=int(row["max_retries"]),
            retry_count=int(row["retry_count"]),
            scheduled_at=_deserialize_datetime(row["scheduled_at"]),
            created_at=cast("datetime", _deserialize_datetime(row["created_at"])),
            started_at=_deserialize_datetime(row["started_at"]),
            completed_at=_deserialize_datetime(row["completed_at"]),
            heartbeat_at=_deserialize_datetime(row["heartbeat_at"]),
            result=store.deserialize_json(row["result_json"]),
            error=cast("str | None", row["error"]),
            key=cast("str | None", row["task_key"]),
            metadata=metadata,
        )


class _AsyncDriverWrapper:
    """Expose sync SQLSpec driver methods as awaitable for queue backend use.

    The SQLSpec queue backend awaits driver methods like ``execute`` and ``commit``;
    sync drivers return values directly. ``sqlspec.utils.sync_tools.ensure_async_``
    wraps each callable on demand so the backend's ``async with self._session()``
    path is uniform across sync and async configs.
    """

    __slots__ = ("_driver",)

    def __init__(self, driver: Any) -> None:
        self._driver = driver

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._driver, name)
        if callable(attr):
            return ensure_async_(attr)
        return attr


@asynccontextmanager
async def _bridge_session(sqlspec_manager: Any, sqlspec_config: Any) -> "AsyncIterator[Any]":
    """Yield a SQLSpec driver regardless of sync/async config.

    Sync SQLSpec configs (``SqliteConfig``, ``DuckDBConfig``, ``PyMysqlConfig``, etc.)
    return sync context managers; this helper wraps them via
    ``sqlspec.utils.sync_tools.with_ensure_async_`` so ``__aenter__`` works, and
    wraps the yielded driver via :class:`_AsyncDriverWrapper` so individual method
    calls are awaitable.

    Yields:
        A SQLSpec driver whose methods can be awaited regardless of whether the
        underlying config is sync or async.
    """
    session_cm = sqlspec_manager.provide_session(sqlspec_config)
    if sqlspec_config.is_async:
        async with session_cm as driver:
            yield driver
    else:
        async with with_ensure_async_(session_cm) as driver:
            yield _AsyncDriverWrapper(driver)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _serialize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _deserialize_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    value_text = str(value)
    try:
        parsed = datetime.fromisoformat(value_text)
    except ValueError:
        parsed = datetime.strptime(value_text.upper(), "%d-%b-%y").replace(tzinfo=timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_status(value: Any) -> TaskStatus:
    status = str(value)
    if status not in {"cancelled", "completed", "failed", "pending", "running", "scheduled"}:
        msg = f"Unknown queued task status from SQLSpec queue backend: {status!r}"
        raise ValueError(msg)
    return cast("TaskStatus", status)


def _queue_extension_settings(sqlspec_config: Any | None) -> dict[str, Any]:
    if sqlspec_config is None:
        return {}
    extension_config = cast("dict[str, Any]", getattr(sqlspec_config, "extension_config", {}) or {})
    return dict(cast("dict[str, Any]", extension_config.get(QUEUE_EXTENSION_NAME, {}) or {}))


def _events_extension_settings(sqlspec_config: Any | None) -> dict[str, Any]:
    if sqlspec_config is None:
        return {}
    extension_config = cast("dict[str, Any]", getattr(sqlspec_config, "extension_config", {}) or {})
    return dict(cast("dict[str, Any]", extension_config.get(_EVENT_EXTENSION_NAME, {}) or {}))


def _setting(queue_settings: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in queue_settings:
            return queue_settings[name]
    return None


def _resolve_bool(value: bool | None, queue_settings: dict[str, Any], key: str, default: bool) -> bool:
    if value is not None:
        return value
    if key in queue_settings:
        return bool(queue_settings[key])
    return default


def _normalize_notification_channel(channel: str) -> str:
    try:
        return str(normalize_event_channel_name(channel))
    except Exception as exc:
        msg = f"Invalid SQLSpec queue notification channel: {channel!r}"
        raise QueueConfigurationError(msg) from exc

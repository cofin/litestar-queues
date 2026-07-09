"""Advanced Alchemy queue backend."""

import asyncio
from contextlib import asynccontextmanager, suppress
from importlib import import_module
from typing import TYPE_CHECKING, Any, cast

from advanced_alchemy.exceptions import DuplicateKeyError
from sqlalchemy import inspect as sqlalchemy_inspect
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError as SQLAlchemyIntegrityError

from litestar_queues.backends.advanced_alchemy.config import AdvancedAlchemyBackendConfig
from litestar_queues.backends.advanced_alchemy.event_log import AdvancedAlchemyQueueEventLog
from litestar_queues.backends.advanced_alchemy.mixins import QueueEventLogModelMixin, QueueTaskModelMixin
from litestar_queues.backends.advanced_alchemy.service import QueueEventLogService, QueueTaskService
from litestar_queues.backends.base import BaseQueueBackend
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.models import HeartbeatTouchResult, QueueBackendCapabilities

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping, Sequence
    from datetime import datetime, timedelta
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from litestar_queues.config import QueueConfig
    from litestar_queues.models import (
        EnqueueSpec,
        HeartbeatTouch,
        QueuedTaskRecord,
        QueueStatistics,
        StaleTaskRecoveryResult,
    )
    from litestar_queues.observability import QueueObservabilityRuntimeProtocol
    from litestar_queues.events import EventLogConfig, QueueEventLog

__all__ = ("AdvancedAlchemyQueueBackend",)

_POSTGRES_NOTIFY_BACKEND = "postgres-listen-notify"
_POSTGRES_NOTIFY_PAYLOAD = "tasks"


class AdvancedAlchemyQueueBackend(BaseQueueBackend):
    """Advanced Alchemy-backed queue backend."""

    _model_class: "type[QueueTaskModelMixin]"
    _service_class: 'type["QueueTaskService"]'
    _event_log_model_class: "type[QueueEventLogModelMixin]"
    _event_log_service_class: 'type["QueueEventLogService"]'

    __slots__ = (
        "_create_schema",
        "_event_poll_interval",
        "_event_log",
        "_event_log_model_class",
        "_event_log_service_class",
        "_heartbeat_session_maker",
        "_model_class",
        "_notification_channel",
        "_notification_listener",
        "_notifications",
        "_observability_runtime",
        "_opened",
        "_service_class",
        "_sqlalchemy_config",
    )

    def __init__(
        self, config: "QueueConfig | None" = None, *, backend_config: "AdvancedAlchemyBackendConfig | None" = None
    ) -> "None":
        super().__init__(config=config)
        backend_config = backend_config or AdvancedAlchemyBackendConfig()
        self._sqlalchemy_config = backend_config.sqlalchemy_config
        self._heartbeat_session_maker = backend_config.heartbeat_session_maker
        self._model_class, self._service_class = self._resolve_model_classes(backend_config.model_class)
        self._event_log_model_class, self._event_log_service_class = self._resolve_event_log_model_classes(
            backend_config.event_log_model_class
        )
        self._create_schema = backend_config.create_schema
        self._notifications = backend_config.notifications
        self._notification_channel = backend_config.notification_channel
        self._event_poll_interval = backend_config.event_poll_interval
        self._notification_listener: "Any | None" = None
        self._observability_runtime: "QueueObservabilityRuntimeProtocol | None" = None
        self._event_log: "AdvancedAlchemyQueueEventLog | None" = None
        self._opened = False

    @property
    def capabilities(self) -> "QueueBackendCapabilities":
        """Backend behavior capabilities."""
        notifications_enabled = self._notifications_supported()
        return QueueBackendCapabilities(
            supports_notifications=notifications_enabled,
            notification_backend=_POSTGRES_NOTIFY_BACKEND if notifications_enabled else None,
            notifications_durable=False,
            supports_batch_claim=_supports_batch_claim_driver(self._driver_name()),
        )

    async def open(self) -> "bool":
        """Open Advanced Alchemy resources.

        Returns:
            True when resources are ready.
        """
        if self._opened:
            return True
        self._ensure_configured()
        if self._create_schema:
            await self.create_schema()
        self._opened = True
        return True

    async def close(self) -> "None":
        """Close backend-owned resources."""
        if self._notification_listener is not None:
            await self._notification_listener.close()
            self._notification_listener = None
        if self._event_log is not None:
            await self._event_log.flush_events()
        self._opened = False

    async def create_schema(self) -> "None":
        """Create the queue task table and enabled event-history table."""
        self._ensure_configured()
        sqlalchemy_config = self._sqlalchemy_config
        if sqlalchemy_config is None:
            msg = "AdvancedAlchemyQueueBackend requires sqlalchemy_config."
            raise QueueConfigurationError(msg)
        engine = sqlalchemy_config.get_engine()
        table = self._model_class.__dict__["__table__"]
        async with engine.begin() as connection:
            await connection.run_sync(table.create, checkfirst=True)
            if self._event_log_enabled():
                event_log_table = self._event_log_model_class.__dict__["__table__"]
                await connection.run_sync(event_log_table.create, checkfirst=True)

    def get_event_log(self, config: "EventLogConfig") -> "QueueEventLog | None":
        """Return Advanced Alchemy-managed queue event history when enabled."""
        if not config.enabled:
            return None
        if self._event_log is None:
            self._event_log = AdvancedAlchemyQueueEventLog(
                config=config, service_factory=self._event_log_service, transaction_factory=self._event_log_operation
            )
        return self._event_log

    async def enqueue(
        self,
        task_name: "str",
        *,
        args: "tuple[Any, ...]" = (),
        kwargs: "dict[str, Any] | None" = None,
        queue: "str" = "default",
        priority: "int" = 0,
        max_retries: "int" = 0,
        scheduled_at: "datetime | None" = None,
        key: "str | None" = None,
        execution_backend: "str" = "local",
        execution_profile: "str | None" = None,
        metadata: "dict[str, Any] | None" = None,
    ) -> "QueuedTaskRecord":
        try:
            async with self._operation() as service:
                record = await service.enqueue(
                    task_name,
                    args=args,
                    kwargs=dict(kwargs or {}),
                    queue=queue,
                    priority=priority,
                    max_retries=max_retries,
                    scheduled_at=scheduled_at,
                    key=key,
                    execution_backend=execution_backend,
                    execution_profile=execution_profile,
                    metadata=dict(metadata or {}),
                )
        except (DuplicateKeyError, SQLAlchemyIntegrityError):
            if key is None:
                raise
            async with self._service() as service:
                existing = await service.get_task_by_key(key)
            if existing is None:
                raise
            record = existing
        await self.notify_new_task(record)
        return record

    async def enqueue_many(self, specs: "Sequence[EnqueueSpec]") -> "list[QueuedTaskRecord]":
        """Persist multiple queued tasks in one Advanced Alchemy operation.

        Returns:
            Queue task records in input order.
        """
        if not specs:
            return []
        async with self._operation() as service:
            records = await service.enqueue_many(specs)
        self._increment_queue_metric("enqueue", float(len(records)))
        await self.notify_new_tasks(records)
        return records

    async def get_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        async with self._service() as service:
            return await service.get_task(task_id)

    async def get_task_by_key(self, key: "str") -> "QueuedTaskRecord | None":
        async with self._service() as service:
            return await service.get_task_by_key(key)

    async def list_pending(
        self, *, limit: "int" = 1, queue: "str | None" = None, execution_backend: "str | None" = None
    ) -> "list[QueuedTaskRecord]":
        async with self._service() as service:
            return await service.list_pending(limit=limit, queue=queue, execution_backend=execution_backend)

    async def claim_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        async with self._operation() as service:
            return await service.claim_task(task_id)

    async def claim_next(
        self, *, queue: "str | None" = None, execution_backend: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        async with self._operation() as service:
            return await service.claim_next(queue=queue, execution_backend=execution_backend)

    async def claim_many(
        self, *, limit: "int", queue: "str | None" = None, execution_backend: "str | None" = None
    ) -> "list[QueuedTaskRecord]":
        """Claim up to ``limit`` due tasks.

        Returns:
            Claimed task records.
        """
        if limit <= 0:
            return []
        async with self._operation() as service:
            records = await service.claim_many(limit=limit, queue=queue, execution_backend=execution_backend)
        self._increment_queue_metric("claim", float(len(records)))
        return records

    async def complete_task(
        self, task_id: "UUID", *, result: "Any" = None, expected_retry_count: "int | None" = None
    ) -> "QueuedTaskRecord | None":
        async with self._operation() as service:
            return await service.complete_task(task_id, result=result, expected_retry_count=expected_retry_count)

    async def fail_task(
        self, task_id: "UUID", error: "str", *, retry: "bool" = True, expected_retry_count: "int | None" = None
    ) -> "QueuedTaskRecord | None":
        async with self._operation() as service:
            return await service.fail_task(task_id, error, retry=retry, expected_retry_count=expected_retry_count)

    async def cancel_task(self, task_id: "UUID", *, include_running: "bool" = False) -> "bool":
        async with self._operation() as service:
            return await service.cancel_task(task_id, include_running=include_running)

    async def cancel_tasks(
        self,
        *,
        task_name: "str | None" = None,
        queue: "str | None" = None,
        kwargs: "Mapping[str, Any] | None" = None,
        metadata: "Mapping[str, Any] | None" = None,
        include_running: "bool" = False,
    ) -> "int":
        async with self._operation() as service:
            return await service.cancel_tasks(
                task_name=task_name, queue=queue, kwargs=kwargs, metadata=metadata, include_running=include_running
            )

    async def touch_heartbeats(self, touches: "Sequence[HeartbeatTouch]") -> "HeartbeatTouchResult":
        if not touches:
            return HeartbeatTouchResult()
        async with self._heartbeat_operation() as service:
            return await service.touch_heartbeats(touches)

    async def null_heartbeats(self, task_ids: "list[UUID]", *, expected_retry_count: "int | None" = None) -> "None":
        async with self._heartbeat_operation() as service:
            await service.null_heartbeats(task_ids, expected_retry_count=expected_retry_count)

    async def requeue_stale_running(self, *, stale_after: "timedelta") -> "StaleTaskRecoveryResult":
        async with self._operation() as service:
            return await service.requeue_stale_running(stale_after=stale_after)

    async def set_execution_ref(
        self, task_id: "UUID", execution_backend: "str", execution_ref: "str", *, execution_profile: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        async with self._operation() as service:
            return await service.set_execution_ref(
                task_id, execution_backend, execution_ref, execution_profile=execution_profile
            )

    async def set_execution_backend(
        self, task_id: "UUID", execution_backend: "str", *, execution_profile: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        async with self._operation() as service:
            record = await service.set_execution_backend(
                task_id, execution_backend, execution_profile=execution_profile
            )
        if record is not None:
            await self.notify_new_task(record)
        return record

    async def notify_new_task(self, record: "QueuedTaskRecord") -> "None":
        """Publish a PostgreSQL worker wakeup marker when enabled.

        Returns:
            None.
        """
        if not self._notifications_supported() or record.status not in {"pending", "scheduled"} or not record.is_due:
            return
        await self._send_notification_marker()
        self._increment_queue_metric("notify")

    async def notify_new_tasks(self, records: "Sequence[QueuedTaskRecord]") -> "None":
        """Coalesce a batch of task records into at most one wakeup marker.

        Returns:
            None.
        """
        for record in records:
            if record.status in {"pending", "scheduled"} and record.is_due:
                await self.notify_new_task(record)
                return

    async def wait_for_notifications(self, timeout: "float | None" = None) -> "bool":
        """Wait for a PostgreSQL worker wakeup marker when configured.

        Returns:
            True when a wakeup marker or due-row reconciliation is observed.
        """
        if not self._notifications_supported():
            return await super().wait_for_notifications(timeout=timeout)
        listener = self._get_notification_listener()
        await listener.start()
        if await self._has_due_tasks():
            self._increment_queue_metric("poll_fallback")
            return True
        wait_timeout = self._event_poll_interval if self._event_poll_interval is not None else timeout
        notified = await listener.wait(wait_timeout)
        if notified:
            self._increment_queue_metric("listener_wakeup")
        return notified

    async def list_running_external(self, *, limit: "int | None" = None) -> "list[QueuedTaskRecord]":
        async with self._service() as service:
            return await service.list_running_external(limit=limit)

    async def get_statistics(self) -> "QueueStatistics":
        async with self._service() as service:
            return await service.get_statistics()

    async def list_completed_by_task(
        self, task_name: "str", *, since: "datetime | None" = None, limit: "int" = 10
    ) -> "list[QueuedTaskRecord]":
        async with self._service() as service:
            return await service.list_completed_by_task(task_name, since=since, limit=limit)

    async def cleanup_terminal(self, before: "datetime") -> "int":
        async with self._operation() as service:
            return await service.cleanup_terminal(before)

    def _ensure_configured(self) -> "None":
        if self._sqlalchemy_config is None:
            msg = "AdvancedAlchemyQueueBackend requires sqlalchemy_config."
            raise QueueConfigurationError(msg)

    def _ensure_opened(self) -> "None":
        if not self._opened:
            msg = "AdvancedAlchemyQueueBackend.open() must be called before using the backend."
            raise RuntimeError(msg)

    def _driver_name(self) -> "str | None":
        sqlalchemy_config = self._sqlalchemy_config
        if sqlalchemy_config is None or sqlalchemy_config.connection_string is None:
            return None
        return make_url(sqlalchemy_config.connection_string).drivername

    def _notifications_supported(self) -> "bool":
        return self._notifications and self._driver_name() == "postgresql+asyncpg"

    def _get_notification_listener(self) -> "Any":
        if self._notification_listener is None:
            self._notification_listener = self._create_notification_listener()
        return self._notification_listener

    def _create_notification_listener(self) -> "Any":
        sqlalchemy_config = self._sqlalchemy_config
        if sqlalchemy_config is None or sqlalchemy_config.connection_string is None:
            msg = "AdvancedAlchemyQueueBackend requires sqlalchemy_config for PostgreSQL notifications."
            raise QueueConfigurationError(msg)
        url = make_url(sqlalchemy_config.connection_string).set(drivername="postgresql")
        return _AsyncpgNotificationListener(
            dsn=url.render_as_string(hide_password=False), channel=self._notification_channel
        )

    async def _send_notification_marker(self) -> "None":
        sqlalchemy_config = self._sqlalchemy_config
        if sqlalchemy_config is None:
            msg = "AdvancedAlchemyQueueBackend requires sqlalchemy_config for PostgreSQL notifications."
            raise QueueConfigurationError(msg)
        engine = sqlalchemy_config.get_engine()
        async with engine.begin() as connection:
            await connection.execute(
                text("SELECT pg_notify(:channel, :payload)"),
                {"channel": self._notification_channel, "payload": _POSTGRES_NOTIFY_PAYLOAD},
            )

    async def _has_due_tasks(self) -> "bool":
        async with self._service() as service:
            return bool(await service.list_pending(limit=1, queue=None, execution_backend=None))

    def _increment_queue_metric(self, name: "str", amount: "float" = 1.0) -> "None":
        if amount == 0 or self.config is None or self.config.observability is None:
            return
        if self._observability_runtime is None:
            from litestar_queues.observability import create_observability_runtime

            self._observability_runtime = create_observability_runtime(self.config.observability)
        self._observability_runtime.record_counter(
            f"litestar_queues.queue.{name}",
            int(amount),
            attributes={"messaging.system": "litestar_queues", "backend": "advanced-alchemy"},
        )

    def _resolve_model_classes(
        self, model_class: "type[object] | None"
    ) -> 'tuple[type[QueueTaskModelMixin], type["QueueTaskService"]]':
        if model_class is None:
            msg = "AdvancedAlchemyBackendConfig.model_class must inherit QueueTaskModelMixin."
            raise QueueConfigurationError(msg)
        try:
            valid_model = issubclass(model_class, QueueTaskModelMixin)
        except TypeError:
            valid_model = False
        if not valid_model:
            msg = "AdvancedAlchemyBackendConfig.model_class must inherit QueueTaskModelMixin."
            raise QueueConfigurationError(msg)
        if "__tablename__" not in model_class.__dict__:
            msg = "AdvancedAlchemyBackendConfig.model_class must declare __tablename__."
            raise QueueConfigurationError(msg)
        typed_model = cast("type[QueueTaskModelMixin]", model_class)
        mapper = cast("Any", sqlalchemy_inspect(typed_model))
        missing_columns = {
            "id",
            "created_at",
            "task_name",
            "args_json",
            "kwargs_json",
            "queue",
            "execution_backend",
            "execution_profile",
            "execution_ref",
            "status",
            "priority",
            "max_retries",
            "retry_count",
            "scheduled_at",
            "started_at",
            "completed_at",
            "heartbeat_at",
            "result_json",
            "error",
            "task_key",
            "metadata_json",
        } - {property_.key for property_ in mapper.column_attrs}
        if missing_columns:
            columns = ", ".join(sorted(missing_columns))
            msg = f"AdvancedAlchemyBackendConfig.model_class is missing queue columns: {columns}."
            raise QueueConfigurationError(msg)
        return typed_model, QueueTaskService.for_model(typed_model)

    def _resolve_event_log_model_classes(
        self, model_class: "type[object] | None"
    ) -> 'tuple[type[QueueEventLogModelMixin], type["QueueEventLogService"]]':
        if model_class is None:
            msg = "AdvancedAlchemyBackendConfig.event_log_model_class must inherit QueueEventLogModelMixin."
            raise QueueConfigurationError(msg)
        try:
            valid_model = issubclass(model_class, QueueEventLogModelMixin)
        except TypeError:
            valid_model = False
        if not valid_model:
            msg = "AdvancedAlchemyBackendConfig.event_log_model_class must inherit QueueEventLogModelMixin."
            raise QueueConfigurationError(msg)
        if "__tablename__" not in model_class.__dict__:
            msg = "AdvancedAlchemyBackendConfig.event_log_model_class must declare __tablename__."
            raise QueueConfigurationError(msg)
        typed_model = cast("type[QueueEventLogModelMixin]", model_class)
        mapper = cast("Any", sqlalchemy_inspect(typed_model))
        missing_columns = {
            "created_at",
            "event_id",
            "event_type",
            "task_id",
            "task_name",
            "queue",
            "worker_id",
            "execution_backend",
            "execution_profile",
            "level",
            "message",
            "detail_json",
            "progress_current",
            "progress_total",
            "progress_percent",
            "sequence",
            "occurred_at",
        } - {property_.key for property_ in mapper.column_attrs}
        if missing_columns:
            columns = ", ".join(sorted(missing_columns))
            msg = f"AdvancedAlchemyBackendConfig.event_log_model_class is missing event-log columns: {columns}."
            raise QueueConfigurationError(msg)
        return typed_model, QueueEventLogService.for_model(typed_model)

    def _event_log_enabled(self) -> "bool":
        event_log_config = self.config.event_log if self.config is not None else None
        return event_log_config is not None and event_log_config.enabled

    @asynccontextmanager
    async def _session(self) -> "AsyncIterator[AsyncSession]":
        self._ensure_configured()
        sqlalchemy_config = self._sqlalchemy_config
        if sqlalchemy_config is None:
            msg = "AdvancedAlchemyQueueBackend requires sqlalchemy_config."
            raise QueueConfigurationError(msg)
        session_maker = sqlalchemy_config.create_session_maker()
        async with session_maker() as session:
            yield session

    @asynccontextmanager
    async def _service(self) -> 'AsyncIterator["QueueTaskService"]':
        self._ensure_opened()
        async with self._session() as session:
            yield self._service_class(session=session)

    @asynccontextmanager
    async def _operation(self) -> 'AsyncIterator["QueueTaskService"]':
        self._ensure_opened()
        async with self._session() as session, session.begin():
            yield self._service_class(session=session)

    @asynccontextmanager
    async def _event_log_service(self) -> 'AsyncIterator["QueueEventLogService"]':
        self._ensure_opened()
        async with self._session() as session:
            yield self._event_log_service_class(session=session)

    @asynccontextmanager
    async def _event_log_operation(self) -> 'AsyncIterator["QueueEventLogService"]':
        self._ensure_opened()
        async with self._session() as session, session.begin():
            yield self._event_log_service_class(session=session)

    @asynccontextmanager
    async def _heartbeat_operation(self) -> 'AsyncIterator["QueueTaskService"]':
        """Yield a ``QueueTaskService`` bound to the dedicated heartbeat session maker.

        Falls back to :meth:`_operation` when ``heartbeat_session_maker`` is not
        configured. The dedicated engine is supplied and owned by the adopter;
        :meth:`close` does not dispose it.

        Yields:
            Queue task service bound to the heartbeat or default operation.
        """
        self._ensure_opened()
        if self._heartbeat_session_maker is None:
            async with self._operation() as service:
                yield service
        else:
            async with self._heartbeat_session_maker() as session, session.begin():
                yield self._service_class(session=session)


class _AsyncpgNotificationListener:
    """Dedicated asyncpg LISTEN connection for Advanced Alchemy wakeups."""

    __slots__ = ("_channel", "_connection", "_dsn", "_event")

    def __init__(self, *, dsn: "str", channel: "str") -> "None":
        self._dsn = dsn
        self._channel = channel
        self._event = asyncio.Event()
        self._connection: "Any | None" = None

    async def start(self) -> "None":
        connection = self._connection
        if connection is not None and not connection.is_closed():
            return
        await self.close()
        connection = await self._connect()
        await connection.add_listener(self._channel, self._handle_notification)
        self._connection = connection

    async def wait(self, timeout: "float | None") -> "bool":
        await self.start()
        if self._event.is_set():
            self._event.clear()
            return True
        try:
            await asyncio.wait_for(self._event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return False
        self._event.clear()
        return True

    async def close(self) -> "None":
        connection = self._connection
        self._connection = None
        if connection is None:
            return
        with suppress(Exception):
            await connection.remove_listener(self._channel, self._handle_notification)
        with suppress(Exception):
            await connection.close()

    async def _connect(self) -> "Any":
        try:
            asyncpg = import_module("asyncpg")
        except ImportError as exc:
            msg = "AdvancedAlchemyBackendConfig.notifications=True for postgresql+asyncpg requires asyncpg."
            raise QueueConfigurationError(msg) from exc
        return await cast("Any", asyncpg).connect(dsn=self._dsn)

    def _handle_notification(self, _connection: "Any", _pid: "int", _channel: "str", _payload: "str") -> "None":
        self._event.set()


def _supports_batch_claim_driver(driver_name: "str | None") -> "bool":
    return driver_name == "postgresql+asyncpg"

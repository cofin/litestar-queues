"""Advanced Alchemy queue backend."""

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast

from advanced_alchemy.exceptions import DuplicateKeyError
from sqlalchemy.exc import IntegrityError as SQLAlchemyIntegrityError

from litestar_queues.backends.advanced_alchemy.config import AdvancedAlchemyBackendConfig
from litestar_queues.backends.advanced_alchemy.mixins import QueueTaskModelMixin
from litestar_queues.backends.advanced_alchemy.service import QueueTaskService
from litestar_queues.backends.base import BaseQueueBackend
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.models import HeartbeatTouchResult

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping, Sequence
    from datetime import datetime, timedelta
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from litestar_queues.config import QueueConfig
    from litestar_queues.models import HeartbeatTouch, QueuedTaskRecord, QueueStatistics, StaleTaskRecoveryResult

__all__ = ("AdvancedAlchemyQueueBackend",)


class AdvancedAlchemyQueueBackend(BaseQueueBackend):
    """Advanced Alchemy-backed queue backend."""

    _model_class: "type[QueueTaskModelMixin]"
    _service_class: 'type["QueueTaskService"]'

    __slots__ = (
        "_create_schema",
        "_heartbeat_session_maker",
        "_model_class",
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
        self._create_schema = backend_config.create_schema
        self._opened = False

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
        self._opened = False

    async def create_schema(self) -> "None":
        """Create the queue task table and indexes."""
        self._ensure_configured()
        sqlalchemy_config = self._sqlalchemy_config
        if sqlalchemy_config is None:
            msg = "AdvancedAlchemyQueueBackend requires sqlalchemy_config."
            raise QueueConfigurationError(msg)
        engine = sqlalchemy_config.get_engine()
        table = self._model_class.__dict__["__table__"]
        async with engine.begin() as connection:
            await connection.run_sync(table.create, checkfirst=True)

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
        table = typed_model.__dict__["__table__"]
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
        } - {column.name for column in table.columns}
        if missing_columns:
            columns = ", ".join(sorted(missing_columns))
            msg = f"AdvancedAlchemyBackendConfig.model_class is missing queue columns: {columns}."
            raise QueueConfigurationError(msg)
        return typed_model, QueueTaskService.for_model(typed_model)

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

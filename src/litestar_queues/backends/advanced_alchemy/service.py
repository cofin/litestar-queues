"""Advanced Alchemy queue persistence service."""

from datetime import datetime, timedelta, timezone
from typing import Any, cast
from uuid import UUID

from advanced_alchemy import _serialization
from advanced_alchemy.service import SQLAlchemyAsyncRepositoryService
from sqlalchemy import and_, delete, desc, func, or_, select, update

from litestar_queues.backends.advanced_alchemy.repository import QueueTaskRepository
from litestar_queues.models import QueuedTaskRecord, QueueStatistics, TaskStatus

__all__ = ("QueueTaskService",)

_DUE_STATUSES = ("pending", "scheduled")
_TERMINAL_STATUSES = ("completed", "failed", "cancelled")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _serialize_json(value: Any) -> str:
    encode_json = _serialization.encode_json  # pyright: ignore[reportPrivateImportUsage]
    return str(encode_json(value))


def _deserialize_json(value: str) -> Any:
    decode_json = _serialization.decode_json  # pyright: ignore[reportPrivateImportUsage]
    return decode_json(value)


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_status(value: Any) -> TaskStatus:
    status = str(value)
    if status not in {"cancelled", "completed", "failed", "pending", "running", "scheduled"}:
        msg = f"Unknown queued task status from Advanced Alchemy queue backend: {status!r}"
        raise ValueError(msg)
    return cast("TaskStatus", status)


class QueueTaskService(SQLAlchemyAsyncRepositoryService[Any]):  # noqa: PLR0904
    """Persistence operations for Advanced Alchemy queue records."""

    @classmethod
    def for_model(cls, model_class: type[Any]) -> type["QueueTaskService"]:
        """Return a service subclass bound to ``model_class``."""
        repository_type = QueueTaskRepository.for_model(model_class)
        return cast(
            "type[QueueTaskService]",
            type(f"QueueTaskServiceFor{model_class.__name__}", (cls,), {"repository_type": repository_type}),
        )

    async def enqueue(
        self,
        task_name: str,
        *,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        queue: str,
        priority: int,
        max_retries: int,
        scheduled_at: datetime | None,
        key: str | None,
        execution_backend: str,
        execution_profile: str | None,
        metadata: dict[str, Any],
    ) -> QueuedTaskRecord:
        if key is not None:
            existing = await self._select_task_by_key(key)
            if existing is not None:
                existing_record = self.record_from_model(existing)
                if not existing_record.is_terminal:
                    return existing_record
                existing.task_key = None
                await self.repository.session.flush()

        now = _utc_now()
        record = QueuedTaskRecord(
            task_name=task_name,
            args=args,
            kwargs=dict(kwargs),
            queue=queue,
            execution_backend=execution_backend,
            execution_profile=execution_profile,
            status="scheduled" if scheduled_at is not None and scheduled_at > now else "pending",
            priority=priority,
            max_retries=max_retries,
            scheduled_at=scheduled_at,
            key=key,
            metadata=dict(metadata),
        )
        await self.repository.add(self.model_from_record(record), auto_commit=False, auto_refresh=False)
        return record

    async def get_task(self, task_id: UUID) -> QueuedTaskRecord | None:
        model = await self._select_task(task_id)
        return self.record_from_model(model) if model is not None else None

    async def get_task_by_key(self, key: str) -> QueuedTaskRecord | None:
        model = await self._select_task_by_key(key)
        return self.record_from_model(model) if model is not None else None

    async def list_pending(
        self,
        *,
        limit: int,
        queue: str | None,
        execution_backend: str | None,
    ) -> list[QueuedTaskRecord]:
        statement = self._pending_statement(queue=queue, execution_backend=execution_backend).limit(limit)
        models = await self.list(statement=statement)
        return [self.record_from_model(model) for model in models]

    async def claim_task(self, task_id: UUID) -> QueuedTaskRecord | None:
        now = _utc_now()
        model_type = self.model_type
        result = await self.repository.session.execute(
            update(model_type)
            .where(
                model_type.id == task_id,
                model_type.status.in_(_DUE_STATUSES),
                or_(model_type.scheduled_at.is_(None), model_type.scheduled_at <= now),
            )
            .values(status="running", started_at=now, heartbeat_at=now)
        )
        if result.rowcount != 1:
            return None
        model = await self._select_task(task_id)
        return self.record_from_model(model) if model is not None else None

    async def claim_next(
        self,
        *,
        queue: str | None,
        execution_backend: str | None,
    ) -> QueuedTaskRecord | None:
        pending = await self.list_pending(limit=10, queue=queue, execution_backend=execution_backend)
        for record in pending:
            claimed = await self.claim_task(record.id)
            if claimed is not None:
                return claimed
        return None

    async def complete_task(self, task_id: UUID, *, result: Any = None) -> QueuedTaskRecord | None:
        now = _utc_now()
        model_type = self.model_type
        update_result = await self.repository.session.execute(
            update(model_type)
            .where(model_type.id == task_id)
            .values(
                status="completed",
                completed_at=now,
                heartbeat_at=now,
                result_json=_serialize_json(result),
                error=None,
            )
        )
        if update_result.rowcount != 1:
            return None
        model = await self._select_task(task_id)
        return self.record_from_model(model) if model is not None else None

    async def fail_task(self, task_id: UUID, error: str, *, retry: bool) -> QueuedTaskRecord | None:
        model = await self._select_task(task_id)
        if model is None:
            return None
        model_type = self.model_type
        if retry and int(model.retry_count) < int(model.max_retries):
            await self.repository.session.execute(
                update(model_type)
                .where(model_type.id == task_id)
                .values(
                    status="pending",
                    started_at=None,
                    heartbeat_at=None,
                    retry_count=int(model.retry_count) + 1,
                    error=error,
                )
            )
        else:
            now = _utc_now()
            await self.repository.session.execute(
                update(model_type)
                .where(model_type.id == task_id)
                .values(status="failed", completed_at=now, heartbeat_at=now, error=error)
            )
        updated = await self._select_task(task_id)
        return self.record_from_model(updated) if updated is not None else None

    async def cancel_task(self, task_id: UUID) -> bool:
        model_type = self.model_type
        result = await self.repository.session.execute(
            update(model_type)
            .where(model_type.id == task_id, model_type.status.in_(_DUE_STATUSES))
            .values(status="cancelled", completed_at=_utc_now())
        )
        return int(result.rowcount or 0) == 1

    async def touch_heartbeat(self, task_id: UUID) -> None:
        model_type = self.model_type
        await self.repository.session.execute(
            update(model_type)
            .where(model_type.id == task_id, model_type.status == "running")
            .values(heartbeat_at=_utc_now())
        )

    async def null_heartbeats(self, task_ids: list[UUID]) -> None:
        if not task_ids:
            return
        model_type = self.model_type
        await self.repository.session.execute(
            update(model_type).where(model_type.id.in_(task_ids)).values(heartbeat_at=None)
        )

    async def requeue_stale_running(self, *, stale_after: timedelta) -> int:
        cutoff = _utc_now() - stale_after
        model_type = self.model_type
        criteria = [model_type.status == "running"]
        if stale_after > timedelta(0):
            criteria.append(or_(model_type.heartbeat_at.is_(None), model_type.heartbeat_at <= cutoff))
        result = await self.repository.session.execute(
            update(model_type)
            .where(*criteria)
            .values(
                status="pending",
                started_at=None,
                heartbeat_at=None,
                retry_count=model_type.retry_count + 1,
            )
        )
        return int(result.rowcount or 0)

    async def set_execution_ref(
        self,
        task_id: UUID,
        execution_backend: str,
        execution_ref: str,
        *,
        execution_profile: str | None,
    ) -> QueuedTaskRecord | None:
        model_type = self.model_type
        result = await self.repository.session.execute(
            update(model_type)
            .where(model_type.id == task_id)
            .values(
                execution_backend=execution_backend,
                execution_profile=execution_profile,
                execution_ref=execution_ref,
            )
        )
        if result.rowcount != 1:
            return None
        model = await self._select_task(task_id)
        return self.record_from_model(model) if model is not None else None

    async def set_execution_backend(
        self,
        task_id: UUID,
        execution_backend: str,
        *,
        execution_profile: str | None,
    ) -> QueuedTaskRecord | None:
        model_type = self.model_type
        result = await self.repository.session.execute(
            update(model_type)
            .where(model_type.id == task_id)
            .values(
                execution_backend=execution_backend,
                execution_profile=execution_profile,
                execution_ref=None,
            )
        )
        if result.rowcount != 1:
            return None
        model = await self._select_task(task_id)
        return self.record_from_model(model) if model is not None else None

    async def list_running_external(self, *, limit: int | None = None) -> list[QueuedTaskRecord]:
        model_type = self.model_type
        statement = (
            select(model_type)
            .where(
                model_type.status.in_(("pending", "scheduled", "running")),
                model_type.execution_ref.is_not(None),
            )
            .order_by(model_type.started_at, model_type.created_at)
        )
        if limit is not None:
            statement = statement.limit(limit)
        models = await self.list(statement=statement)
        return [self.record_from_model(model) for model in models]

    async def get_statistics(self) -> QueueStatistics:
        model_type = self.model_type
        result = await self.repository.session.execute(
            select(model_type.status, func.count()).group_by(model_type.status)
        )
        statistics = QueueStatistics()
        for status, count in result.all():
            coerced = _coerce_status(status)
            setattr(statistics, coerced, int(count))
        return statistics

    async def list_completed_by_task(
        self,
        task_name: str,
        *,
        since: datetime | None,
        limit: int,
    ) -> list[QueuedTaskRecord]:
        model_type = self.model_type
        criteria = [model_type.task_name == task_name, model_type.status == "completed"]
        if since is not None:
            criteria.append(model_type.completed_at >= since)
        statement = (
            select(model_type).where(and_(*criteria)).order_by(desc(model_type.completed_at)).limit(limit)
        )
        models = await self.list(statement=statement)
        return [self.record_from_model(model) for model in models]

    async def cleanup_terminal(self, before: datetime) -> int:
        model_type = self.model_type
        result = await self.repository.session.execute(
            delete(model_type).where(
                model_type.status.in_(_TERMINAL_STATUSES),
                model_type.completed_at.is_not(None),
                model_type.completed_at < before,
            )
        )
        return int(result.rowcount or 0)

    def model_from_record(self, record: QueuedTaskRecord) -> Any:
        """Convert a backend-neutral record into an Advanced Alchemy model.

        Returns:
            The Advanced Alchemy queue task model.
        """
        return self.model_type(
            id=record.id,
            task_name=record.task_name,
            args_json=_serialize_json(list(record.args)),
            kwargs_json=_serialize_json(record.kwargs),
            queue=record.queue,
            execution_backend=record.execution_backend,
            execution_profile=record.execution_profile,
            execution_ref=record.execution_ref,
            status=record.status,
            priority=record.priority,
            max_retries=record.max_retries,
            retry_count=record.retry_count,
            scheduled_at=record.scheduled_at,
            created_at=record.created_at,
            started_at=record.started_at,
            completed_at=record.completed_at,
            heartbeat_at=record.heartbeat_at,
            result_json=_serialize_json(record.result),
            error=record.error,
            task_key=record.key,
            metadata_json=_serialize_json(record.metadata),
        )

    @staticmethod
    def record_from_model(model: Any) -> QueuedTaskRecord:
        """Convert an Advanced Alchemy model into a backend-neutral record.

        Returns:
            The backend-neutral queued task record.
        """
        args = _deserialize_json(model.args_json)
        kwargs = _deserialize_json(model.kwargs_json)
        metadata = _deserialize_json(model.metadata_json)
        return QueuedTaskRecord(
            id=UUID(str(model.id)),
            task_name=model.task_name,
            args=tuple(args),
            kwargs=kwargs,
            queue=model.queue,
            execution_backend=model.execution_backend,
            execution_profile=model.execution_profile,
            execution_ref=model.execution_ref,
            status=_coerce_status(model.status),
            priority=int(model.priority),
            max_retries=int(model.max_retries),
            retry_count=int(model.retry_count),
            scheduled_at=_coerce_datetime(model.scheduled_at),
            created_at=cast("datetime", _coerce_datetime(model.created_at)),
            started_at=_coerce_datetime(model.started_at),
            completed_at=_coerce_datetime(model.completed_at),
            heartbeat_at=_coerce_datetime(model.heartbeat_at),
            result=_deserialize_json(model.result_json),
            error=model.error,
            key=model.task_key,
            metadata=metadata,
        )

    async def _select_task(self, task_id: UUID) -> Any | None:
        return await self.repository.get_one_or_none(id=task_id)

    async def _select_task_by_key(self, key: str) -> Any | None:
        return await self.repository.get_one_or_none(task_key=key)

    def _pending_statement(self, *, queue: str | None, execution_backend: str | None) -> Any:
        now = _utc_now()
        model_type = self.model_type
        criteria = [
            model_type.status.in_(_DUE_STATUSES),
            or_(model_type.scheduled_at.is_(None), model_type.scheduled_at <= now),
        ]
        if queue is not None:
            criteria.append(model_type.queue == queue)
        if execution_backend is not None:
            criteria.append(model_type.execution_backend == execution_backend)
        return (
            select(model_type)
            .where(and_(*criteria))
            .order_by(desc(model_type.priority), model_type.created_at)
        )

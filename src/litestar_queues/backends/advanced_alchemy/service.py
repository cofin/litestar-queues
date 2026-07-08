"""Advanced Alchemy queue persistence service."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from advanced_alchemy.operations import OnConflictUpsert
from advanced_alchemy.service import SQLAlchemyAsyncRepositoryService
from advanced_alchemy.utils.serialization import decode_json as _decode_json
from advanced_alchemy.utils.serialization import encode_json as _encode_json
from sqlalchemy import and_, case, delete, desc, func, literal, or_, select, update
from sqlalchemy import inspect as sqlalchemy_inspect
from sqlalchemy.orm.exc import UnmappedColumnError

from litestar_queues.backends.advanced_alchemy.repository import QueueTaskRepository
from litestar_queues.backends.base import (
    STALE_HEARTBEAT_ERROR,
    record_matches_filters,
    stale_requeue_error,
    stale_requeue_priority,
)
from litestar_queues.models import (
    HeartbeatTouchResult,
    QueuedTaskRecord,
    QueueStatistics,
    StaleTaskRecoveryResult,
    TaskStatus,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from litestar_queues.backends.advanced_alchemy.mixins import QueueTaskModelMixin
    from litestar_queues.models import HeartbeatTouch

__all__ = ("QueueTaskService",)

_DUE_STATUSES = ("pending", "scheduled")
_TERMINAL_STATUSES = ("completed", "failed", "cancelled")
_SKIP_LOCKED_CLAIM_DIALECTS = frozenset({"oracle", "postgresql"})
_NATIVE_KEYED_ENQUEUE_DIALECTS = frozenset({"mariadb", "mysql", "oracle", "postgresql"})
_ORACLE_CLAIM_CANDIDATE_LIMIT = 10
_CAS_CLAIM_BATCH_SIZE = 10


class QueueTaskService(SQLAlchemyAsyncRepositoryService[Any]):
    """Persistence operations for Advanced Alchemy queue records."""

    @classmethod
    def for_model(cls, model_class: "type[QueueTaskModelMixin]") -> 'type["QueueTaskService"]':
        """Return a service subclass bound to ``model_class``."""
        repository_type = QueueTaskRepository.for_model(model_class)
        return cast(
            "type[QueueTaskService]",
            type(f"QueueTaskServiceFor{model_class.__name__}", (cls,), {"repository_type": repository_type}),
        )

    async def enqueue(
        self,
        task_name: "str",
        *,
        args: "tuple[Any, ...]",
        kwargs: "dict[str, Any]",
        queue: "str",
        priority: "int",
        max_retries: "int",
        scheduled_at: "datetime | None",
        key: "str | None",
        execution_backend: "str",
        execution_profile: "str | None",
        metadata: "dict[str, Any]",
    ) -> "QueuedTaskRecord":
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
        return await self._insert_task_record(record, key=key)

    async def get_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        model = await self._select_task(task_id)
        return self.record_from_model(model) if model is not None else None

    async def get_task_by_key(self, key: "str") -> "QueuedTaskRecord | None":
        model = await self._select_task_by_key(key)
        return self.record_from_model(model) if model is not None else None

    async def list_pending(
        self, *, limit: "int", queue: "str | None", execution_backend: "str | None"
    ) -> "list[QueuedTaskRecord]":
        statement = self._pending_statement(queue=queue, execution_backend=execution_backend).limit(limit)
        models = await self.get_many(statement=statement)
        return [self.record_from_model(model) for model in models]

    async def claim_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        now = _utc_now()
        model_type = self.model_type
        result = await self.repository.session.execute(
            update(model_type)
            .where(
                model_type.id == task_id,
                model_type.status.in_(_DUE_STATUSES),
                or_(model_type.scheduled_at.is_(None), model_type.scheduled_at <= now),
            )
            .values(_update_values(model_type, {"status": "running", "started_at": now, "heartbeat_at": now}, now=now))
        )
        if result.rowcount != 1:
            return None
        model = await self._select_task(task_id)
        return self.record_from_model(model) if model is not None else None

    async def claim_next(self, *, queue: "str | None", execution_backend: "str | None") -> "QueuedTaskRecord | None":
        if _supports_skip_locked_claim(self._dialect_name()):
            return await self._claim_next_skip_locked(queue=queue, execution_backend=execution_backend)

        skipped_ids: "set[UUID]" = set()
        pending_limit = _CAS_CLAIM_BATCH_SIZE
        while True:
            pending = await self.list_pending(limit=pending_limit, queue=queue, execution_backend=execution_backend)
            candidates = [record for record in pending if record.id not in skipped_ids]
            if not candidates:
                return None
            for record in candidates:
                claimed = await self.claim_task(record.id)
                if claimed is not None:
                    return claimed
                skipped_ids.add(record.id)
            if len(pending) < pending_limit:
                return None
            pending_limit += _CAS_CLAIM_BATCH_SIZE

    async def _claim_next_skip_locked(
        self, *, queue: "str | None", execution_backend: "str | None"
    ) -> "QueuedTaskRecord | None":
        now = _utc_now()
        dialect_name = self._dialect_name()
        if dialect_name == "oracle":
            statement = _build_claim_candidate_statement(
                self.model_type,
                queue=queue,
                execution_backend=execution_backend,
                now=now,
                limit=_ORACLE_CLAIM_CANDIDATE_LIMIT,
                skip_locked=False,
            )
            candidates = (await self.repository.session.execute(statement)).scalars().all()
            for candidate in candidates:
                lock_statement = _build_claim_lock_statement(self.model_type, UUID(str(candidate.id)))
                locked = (await self.repository.session.execute(lock_statement)).scalars().first()
                if locked is None:
                    continue
                return await self.claim_task(UUID(str(locked.id)))
            return None

        statement = _build_claim_candidate_statement(
            self.model_type, queue=queue, execution_backend=execution_backend, now=now, limit=1, skip_locked=True
        )
        row = (await self.repository.session.execute(statement)).scalars().first()
        if row is None:
            return None
        return await self.claim_task(UUID(str(row.id)))

    async def complete_task(
        self, task_id: "UUID", *, result: "Any" = None, expected_retry_count: "int | None" = None
    ) -> "QueuedTaskRecord | None":
        now = _utc_now()
        model_type = self.model_type
        criteria = [model_type.id == task_id]
        if expected_retry_count is not None:
            criteria.extend((model_type.status == "running", model_type.retry_count == expected_retry_count))
        update_result = await self.repository.session.execute(
            update(model_type)
            .where(*criteria)
            .values(
                _update_values(
                    model_type,
                    {
                        "status": "completed",
                        "completed_at": now,
                        "heartbeat_at": now,
                        "result_json": _serialize_json(result),
                        "error": None,
                    },
                    now=now,
                )
            )
        )
        if update_result.rowcount != 1:
            return None
        model = await self._select_task(task_id)
        return self.record_from_model(model) if model is not None else None

    async def fail_task(
        self, task_id: "UUID", error: "str", *, retry: "bool", expected_retry_count: "int | None" = None
    ) -> "QueuedTaskRecord | None":
        model = await self._select_task(task_id)
        if model is None:
            return None
        if expected_retry_count is not None and (
            str(model.status) != "running" or int(model.retry_count) != expected_retry_count
        ):
            return None
        if str(model.status) != "running":
            return None
        model_type = self.model_type
        retry_fence = expected_retry_count if expected_retry_count is not None else int(model.retry_count)
        criteria = [model_type.id == task_id, model_type.status == "running", model_type.retry_count == retry_fence]
        if retry and int(model.retry_count) < int(model.max_retries):
            update_result = await self.repository.session.execute(
                update(model_type)
                .where(*criteria)
                .values(
                    _update_values(
                        model_type,
                        {
                            "status": "pending",
                            "started_at": None,
                            "heartbeat_at": None,
                            "retry_count": int(model.retry_count) + 1,
                            "error": error,
                        },
                    )
                )
            )
        else:
            now = _utc_now()
            update_result = await self.repository.session.execute(
                update(model_type)
                .where(*criteria)
                .values(
                    _update_values(
                        model_type,
                        {"status": "failed", "completed_at": now, "heartbeat_at": now, "error": error},
                        now=now,
                    )
                )
            )
        if update_result.rowcount != 1:
            return None
        updated = await self._select_task(task_id)
        return self.record_from_model(updated) if updated is not None else None

    async def cancel_task(self, task_id: "UUID", *, include_running: "bool" = False) -> "bool":
        model_type = self.model_type
        now = _utc_now()
        cancellable_statuses = (*_DUE_STATUSES, "running") if include_running else _DUE_STATUSES
        result = await self.repository.session.execute(
            update(model_type)
            .where(model_type.id == task_id, model_type.status.in_(cancellable_statuses))
            .values(
                _update_values(model_type, {"status": "cancelled", "completed_at": now, "heartbeat_at": None}, now=now)
            )
        )
        return int(result.rowcount or 0) == 1

    async def cancel_tasks(
        self,
        *,
        task_name: "str | None" = None,
        queue: "str | None" = None,
        kwargs: "Mapping[str, Any] | None" = None,
        metadata: "Mapping[str, Any] | None" = None,
        include_running: "bool" = False,
    ) -> "int":
        model_type = self.model_type
        cancellable_statuses = (*_DUE_STATUSES, "running") if include_running else _DUE_STATUSES
        criteria = [model_type.status.in_(cancellable_statuses)]
        if task_name is not None:
            criteria.append(model_type.task_name == task_name)
        if queue is not None:
            criteria.append(model_type.queue == queue)
        models = (await self.repository.session.execute(select(model_type).where(*criteria))).scalars().all()
        cancelled = 0
        for model in models:
            record = self.record_from_model(model)
            if not record_matches_filters(record, task_name=task_name, queue=queue, kwargs=kwargs, metadata=metadata):
                continue
            if await self.cancel_task(record.id, include_running=include_running):
                cancelled += 1
        return cancelled

    async def touch_heartbeats(self, touches: "Sequence[HeartbeatTouch]") -> "HeartbeatTouchResult":
        result = HeartbeatTouchResult()
        if not touches:
            return result

        model_type = self.model_type
        now = _utc_now()
        groups: "dict[int | None, dict[UUID, HeartbeatTouch]]" = {}
        for touch in touches:
            groups.setdefault(touch.expected_retry_count, {})[touch.task_id] = touch

        dialect_name = self._dialect_name()
        for expected_retry_count, grouped_touches in groups.items():
            task_ids = set(grouped_touches)
            criteria = [model_type.id.in_(task_ids), model_type.status == "running"]
            if expected_retry_count is not None:
                criteria.append(model_type.retry_count == expected_retry_count)
            models = (await self.repository.session.execute(select(model_type).where(*criteria))).scalars().all()
            models_by_id = {UUID(str(model.id)): model for model in models}
            touched_task_ids = set(models_by_id)
            result.missed_task_ids.update(task_ids - touched_task_ids)
            if not touched_task_ids:
                continue

            if dialect_name == "oracle" and any(
                grouped_touches[task_id].metadata_patch for task_id in touched_task_ids
            ):
                await self._touch_oracle_heartbeats(
                    grouped_touches=grouped_touches,
                    models_by_id=models_by_id,
                    expected_retry_count=expected_retry_count,
                    now=now,
                    result=result,
                )
                continue

            values: "dict[str, Any]" = {"heartbeat_at": now}
            metadata_column = _mapped_column(model_type, "metadata_json")
            metadata_cases: "list[tuple[Any, Any]]" = []
            for task_id, model in models_by_id.items():
                metadata_patch = grouped_touches[task_id].metadata_patch
                if not metadata_patch:
                    continue
                metadata = dict(_deserialize_json(model.metadata_json) or {})
                metadata.update(metadata_patch)
                metadata_cases.append((
                    model_type.id == task_id,
                    literal(_serialize_json(metadata), type_=metadata_column.type),
                ))
            if metadata_cases:
                values["metadata_json"] = case(*metadata_cases, else_=metadata_column)

            update_criteria = [model_type.id.in_(touched_task_ids), model_type.status == "running"]
            if expected_retry_count is not None:
                update_criteria.append(model_type.retry_count == expected_retry_count)
            execution_result = await self.repository.session.execute(
                update(model_type)
                .where(*update_criteria)
                .values(_update_values(model_type, values, now=now))
                .execution_options(synchronize_session=False)
            )
            rowcount = int(execution_result.rowcount or 0)
            if rowcount == len(touched_task_ids) or rowcount < 0:
                result.touched_task_ids.update(touched_task_ids)
            else:
                result.missed_task_ids.update(touched_task_ids)
        return result

    async def _touch_oracle_heartbeats(
        self,
        *,
        grouped_touches: "dict[UUID, HeartbeatTouch]",
        models_by_id: "dict[UUID, Any]",
        expected_retry_count: "int | None",
        now: "datetime",
        result: "HeartbeatTouchResult",
    ) -> "None":
        """Touch Oracle JsonB metadata without CASE expressions over BLOB JSON."""
        patched_task_ids = {task_id for task_id in models_by_id if grouped_touches[task_id].metadata_patch}
        heartbeat_only_task_ids = set(models_by_id) - patched_task_ids
        if heartbeat_only_task_ids:
            await self._touch_heartbeat_rows(
                heartbeat_only_task_ids,
                values={"heartbeat_at": now},
                expected_retry_count=expected_retry_count,
                now=now,
                result=result,
            )

        for task_id in patched_task_ids:
            model = models_by_id[task_id]
            metadata = dict(_deserialize_json(model.metadata_json) or {})
            metadata_patch = grouped_touches[task_id].metadata_patch
            if metadata_patch:
                metadata.update(metadata_patch)
            await self._touch_heartbeat_rows(
                {task_id},
                values={"heartbeat_at": now, "metadata_json": _serialize_json(metadata)},
                expected_retry_count=expected_retry_count,
                now=now,
                result=result,
            )

    async def _touch_heartbeat_rows(
        self,
        task_ids: "set[UUID]",
        *,
        values: "dict[str, Any]",
        expected_retry_count: "int | None",
        now: "datetime",
        result: "HeartbeatTouchResult",
    ) -> "None":
        if not task_ids:
            return
        model_type = self.model_type
        update_criteria = [model_type.id.in_(task_ids), model_type.status == "running"]
        if expected_retry_count is not None:
            update_criteria.append(model_type.retry_count == expected_retry_count)
        execution_result = await self.repository.session.execute(
            update(model_type)
            .where(*update_criteria)
            .values(_update_values(model_type, values, now=now))
            .execution_options(synchronize_session=False)
        )
        rowcount = int(execution_result.rowcount or 0)
        if rowcount == len(task_ids) or rowcount < 0:
            result.touched_task_ids.update(task_ids)
        else:
            result.missed_task_ids.update(task_ids)

    async def null_heartbeats(self, task_ids: "list[UUID]", *, expected_retry_count: "int | None" = None) -> "None":
        if not task_ids:
            return
        model_type = self.model_type
        criteria = [model_type.id.in_(task_ids)]
        if expected_retry_count is not None:
            criteria.append(model_type.retry_count == expected_retry_count)
        await self.repository.session.execute(
            update(model_type).where(*criteria).values(_update_values(model_type, {"heartbeat_at": None}))
        )

    async def requeue_stale_running(self, *, stale_after: "timedelta") -> "StaleTaskRecoveryResult":
        cutoff = _utc_now() - stale_after
        model_type = self.model_type
        stale_heartbeat = or_(model_type.heartbeat_at.is_(None), model_type.heartbeat_at <= cutoff)
        select_criteria = [model_type.status == "running"]
        use_heartbeat_cutoff = stale_after.total_seconds() > 0
        if use_heartbeat_cutoff:
            select_criteria.append(stale_heartbeat)
        models = (await self.repository.session.execute(select(model_type).where(*select_criteria))).scalars().all()
        result = StaleTaskRecoveryResult()
        for model in models:
            metadata = _deserialize_json(model.metadata_json)
            requeue_on_stale = metadata.get("requeue_on_stale", True) is not False
            update_criteria = [
                model_type.id == model.id,
                model_type.status == "running",
                model_type.retry_count == int(model.retry_count),
            ]
            if use_heartbeat_cutoff:
                update_criteria.append(stale_heartbeat)
            if requeue_on_stale and int(model.retry_count) < int(model.max_retries):
                update_result = await self.repository.session.execute(
                    update(model_type)
                    .where(*update_criteria)
                    .values(
                        _update_values(
                            model_type,
                            {
                                "status": "pending",
                                "started_at": None,
                                "heartbeat_at": None,
                                "retry_count": int(model.retry_count) + 1,
                                "priority": stale_requeue_priority(int(model.priority)),
                                "error": stale_requeue_error(model.error),
                            },
                        )
                    )
                    .execution_options(synchronize_session=False)
                )
                if update_result.rowcount == 1:
                    result.requeued += 1
                else:
                    result.skipped += 1
            else:
                now = _utc_now()
                update_result = await self.repository.session.execute(
                    update(model_type)
                    .where(*update_criteria)
                    .values(
                        _update_values(
                            model_type,
                            {
                                "status": "failed",
                                "completed_at": now,
                                "heartbeat_at": now,
                                "error": STALE_HEARTBEAT_ERROR,
                            },
                            now=now,
                        )
                    )
                    .execution_options(synchronize_session=False)
                )
                if update_result.rowcount == 1:
                    result.failed += 1
                    task_id = UUID(str(model.id))
                    result.failed_task_ids.append(task_id)
                    if not requeue_on_stale:
                        result.handler_needed += 1
                        result.handler_needed_task_ids.append(task_id)
                else:
                    result.skipped += 1
        return result

    async def set_execution_ref(
        self, task_id: "UUID", execution_backend: "str", execution_ref: "str", *, execution_profile: "str | None"
    ) -> "QueuedTaskRecord | None":
        model_type = self.model_type
        result = await self.repository.session.execute(
            update(model_type)
            .where(model_type.id == task_id)
            .values(
                _update_values(
                    model_type,
                    {
                        "execution_backend": execution_backend,
                        "execution_profile": execution_profile,
                        "execution_ref": execution_ref,
                    },
                )
            )
        )
        if result.rowcount != 1:
            return None
        model = await self._select_task(task_id)
        return self.record_from_model(model) if model is not None else None

    async def set_execution_backend(
        self, task_id: "UUID", execution_backend: "str", *, execution_profile: "str | None"
    ) -> "QueuedTaskRecord | None":
        model_type = self.model_type
        result = await self.repository.session.execute(
            update(model_type)
            .where(model_type.id == task_id)
            .values(
                _update_values(
                    model_type,
                    {
                        "execution_backend": execution_backend,
                        "execution_profile": execution_profile,
                        "execution_ref": None,
                    },
                )
            )
        )
        if result.rowcount != 1:
            return None
        model = await self._select_task(task_id)
        return self.record_from_model(model) if model is not None else None

    async def list_running_external(self, *, limit: "int | None" = None) -> "list[QueuedTaskRecord]":
        model_type = self.model_type
        statement = (
            select(model_type)
            .where(model_type.status.in_(("pending", "scheduled", "running")), model_type.execution_ref.is_not(None))
            .order_by(model_type.started_at, model_type.created_at)
        )
        if limit is not None:
            statement = statement.limit(limit)
        models = await self.get_many(statement=statement)
        return [self.record_from_model(model) for model in models]

    async def get_statistics(self) -> "QueueStatistics":
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
        self, task_name: "str", *, since: "datetime | None", limit: "int"
    ) -> "list[QueuedTaskRecord]":
        model_type = self.model_type
        criteria = [model_type.task_name == task_name, model_type.status == "completed"]
        if since is not None:
            criteria.append(model_type.completed_at >= since)
        statement = select(model_type).where(and_(*criteria)).order_by(desc(model_type.completed_at)).limit(limit)
        models = await self.get_many(statement=statement)
        return [self.record_from_model(model) for model in models]

    async def cleanup_terminal(self, before: "datetime") -> "int":
        model_type = self.model_type
        result = await self.repository.session.execute(
            delete(model_type).where(
                model_type.status.in_(_TERMINAL_STATUSES),
                model_type.completed_at.is_not(None),
                model_type.completed_at < before,
            )
        )
        return int(result.rowcount or 0)

    def model_from_record(self, record: "QueuedTaskRecord") -> "Any":
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
    def record_from_model(model: "Any") -> "QueuedTaskRecord":
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

    async def _select_task(self, task_id: "UUID") -> "Any | None":
        return await self.repository.get_one_or_none(id=task_id)

    async def _select_task_by_key(self, key: "str") -> "Any | None":
        return await self.repository.get_one_or_none(task_key=key)

    async def _insert_task_record(self, record: "QueuedTaskRecord", *, key: "str | None") -> "QueuedTaskRecord":
        model = self.model_from_record(record)
        dialect_name = self._dialect_name()
        if key is not None and dialect_name is not None and _supports_native_keyed_enqueue(dialect_name):
            values = _model_insert_values(model, self.model_type)
            statement, params = _build_keyed_enqueue_upsert(
                self.model_type.__table__,
                values,
                dialect_name=dialect_name,
                key_column=_mapped_column(self.model_type, "task_key").name,
            )
            if params:
                await self.repository.session.execute(statement, params)
            else:
                await self.repository.session.execute(statement)
            inserted = await self._select_task(record.id)
            if inserted is not None:
                return self.record_from_model(inserted)
            existing = await self._select_task_by_key(key)
            if existing is not None:
                return self.record_from_model(existing)
            return record

        await self.repository.add(model, auto_commit=False, auto_refresh=False)
        return record

    def _dialect_name(self) -> "str | None":
        bind = self.repository.session.get_bind()
        return bind.dialect.name if bind is not None else None

    def _pending_statement(self, *, queue: "str | None", execution_backend: "str | None") -> "Any":
        return _build_claim_candidate_statement(
            self.model_type,
            queue=queue,
            execution_backend=execution_backend,
            now=_utc_now(),
            limit=None,
            skip_locked=False,
        )


def _supports_skip_locked_claim(dialect_name: "str | None") -> "bool":
    return dialect_name in _SKIP_LOCKED_CLAIM_DIALECTS


def _supports_native_keyed_enqueue(dialect_name: "str | None") -> "bool":
    return dialect_name in _NATIVE_KEYED_ENQUEUE_DIALECTS


def _build_claim_candidate_statement(
    model_type: "type[Any]",
    *,
    queue: "str | None",
    execution_backend: "str | None",
    now: "datetime",
    limit: "int | None",
    skip_locked: "bool",
) -> "Any":
    criteria = [
        model_type.status.in_(_DUE_STATUSES),
        or_(model_type.scheduled_at.is_(None), model_type.scheduled_at <= now),
    ]
    if queue is not None:
        criteria.append(model_type.queue == queue)
    if execution_backend is not None:
        criteria.append(model_type.execution_backend == execution_backend)
    statement = select(model_type).where(and_(*criteria)).order_by(desc(model_type.priority), model_type.created_at)
    if limit is not None:
        statement = statement.limit(limit)
    if skip_locked:
        statement = statement.with_for_update(skip_locked=True)
    return statement


def _build_claim_lock_statement(model_type: "type[Any]", task_id: "UUID") -> "Any":
    return (
        select(model_type)
        .where(model_type.id == task_id, model_type.status.in_(_DUE_STATUSES))
        .with_for_update(skip_locked=True)
    )


def _build_keyed_enqueue_upsert(
    table: "Any", values: "dict[str, Any]", *, dialect_name: "str", key_column: "str | None" = None
) -> "tuple[Any, dict[str, Any]]":
    key_column = key_column or ("task_key" if "task_key" in table.c else "key")
    if dialect_name == "oracle":
        return OnConflictUpsert.create_merge_upsert(
            table=table,
            values=values,
            conflict_columns=[key_column],
            update_columns=[],
            dialect_name=dialect_name,
            validate_identifiers=True,
        )
    update_columns = [key_column]
    return (
        OnConflictUpsert.create_upsert(
            table=table,
            values=values,
            conflict_columns=[key_column],
            update_columns=update_columns,
            dialect_name=dialect_name,
            validate_identifiers=True,
        ),
        {},
    )


def _mapped_column(model_type: "type[Any]", attribute_name: "str") -> "Any":
    return sqlalchemy_inspect(model_type).column_attrs[attribute_name].columns[0]


def _update_values(
    model_type: "type[Any]", values: "dict[str, Any]", *, now: "datetime | None" = None
) -> "dict[str, Any]":
    if hasattr(model_type, "updated_at") and "updated_at" not in values:
        values = {**values, "updated_at": now or _utc_now()}
    return values


def _model_insert_values(model: "Any", model_type: "type[Any]") -> "dict[str, Any]":
    values: "dict[str, Any]" = {}
    mapper = sqlalchemy_inspect(model_type)
    table = model_type.__table__
    for column in table.columns:
        try:
            attribute_name = mapper.get_property_by_column(column).key
        except UnmappedColumnError:
            attribute_name = column.key or column.name
        if not hasattr(model, attribute_name):
            continue
        value = getattr(model, attribute_name)
        if value is None and attribute_name == "updated_at":
            value = _utc_now()
        if value is None and (column.default is not None or column.server_default is not None):
            continue
        values[column.name] = value
    return values


def _utc_now() -> "datetime":
    return datetime.now(timezone.utc)


def _serialize_json(value: "Any") -> "Any":
    return _decode_json(str(_encode_json(value)))


def _deserialize_json(value: "Any") -> "Any":
    if value is None:
        return None
    if isinstance(value, bytes | bytearray | memoryview):
        return _decode_json(bytes(value))
    return value


def _coerce_datetime(value: "Any") -> "datetime | None":
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


def _coerce_status(value: "Any") -> "TaskStatus":
    status = str(value)
    if status not in {"cancelled", "completed", "failed", "pending", "running", "scheduled"}:
        msg = f"Unknown queued task status from Advanced Alchemy queue backend: {status!r}"
        raise ValueError(msg)
    return cast("TaskStatus", status)

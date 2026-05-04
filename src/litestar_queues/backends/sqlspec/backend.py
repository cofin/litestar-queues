"""SQLSpec queue backend."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from litestar_queues.backends.base import BaseQueueBackend
from litestar_queues.backends.sqlspec._typing import SQLSpecConfigT, SQLSpecT, missing_sqlspec_error
from litestar_queues.backends.sqlspec.config import SQLSpecBackendConfig
from litestar_queues.backends.sqlspec.extension import configure_queue_migration_extension
from litestar_queues.backends.sqlspec.schema import DEFAULT_TABLE_NAME, validate_table_name
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.models import QueueBackendCapabilities, QueuedTaskRecord, QueueStatistics, TaskStatus

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig

__all__ = ("SQLSpecBackendConfig", "SQLSpecQueueBackend")

_DUE_STATUSES = ("pending", "scheduled")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _deserialize_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_status(value: Any) -> TaskStatus:
    status = str(value)
    if status not in {"cancelled", "completed", "failed", "pending", "running", "scheduled"}:
        msg = f"Unknown queued task status from SQLSpec queue backend: {status!r}"
        raise ValueError(msg)
    return cast("TaskStatus", status)


class SQLSpecQueueBackend(BaseQueueBackend):  # noqa: PLR0904
    """SQLSpec-backed queue backend."""

    __slots__ = (
        "_create_schema",
        "_opened",
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
        sqlspec: SQLSpecT | None = None,
        sqlspec_config: SQLSpecConfigT | None = None,
        table_name: str = DEFAULT_TABLE_NAME,
        create_schema: bool = True,
        run_migrations: bool = False,
    ) -> None:
        super().__init__(config=config)
        self._sqlspec = sqlspec
        self._sqlspec_config = sqlspec_config
        self._owns_sqlspec = sqlspec is None
        self._table_name = validate_table_name(table_name)
        self._create_schema = create_schema
        self._run_migrations = run_migrations
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
        self._opened = True
        if self._run_migrations:
            await self.run_migrations()
        if self._create_schema:
            await self.create_schema()
        return True

    async def close(self) -> None:
        """Close SQLSpec resources."""
        if self._owns_sqlspec and self._sqlspec is not None:
            await self._sqlspec.close_all_pools()
            self._sqlspec = None
        self._opened = False

    @property
    def capabilities(self) -> QueueBackendCapabilities:
        """Return backend behavior capabilities."""
        return QueueBackendCapabilities(
            supports_notifications=False,
            notification_backend=None,
            notifications_durable=False,
        )

    async def create_schema(self) -> None:
        """Create the SQLSpec queue table and indexes."""
        async with self._session() as driver:
            for statement in self._get_store().create_statements():
                await driver.execute_script(statement)
            await driver.commit()

    async def run_migrations(self) -> None:
        """Apply packaged SQLSpec migrations."""
        sqlspec_config = self._get_sqlspec_config()
        configure_queue_migration_extension(sqlspec_config, table_name=self._table_name)
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
                if result.rows_affected != 1:
                    await driver.rollback()
                    return None

                updated_row = await self._select_task(driver, task_id)
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
        from sqlspec.utils.serializers import to_json

        now = _utc_now()
        async with self._session() as driver:
            await driver.begin()
            try:
                updated = await driver.execute(
                    self._get_store().complete_task(
                        task_id=str(task_id),
                        completed_at=_serialize_datetime(now),
                        heartbeat_at=_serialize_datetime(now),
                        result_json=to_json(result),
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
        async with self._session() as driver:
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
        async with self._session() as driver:
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
                result = await driver.execute(
                    self._get_store().cleanup_terminal(before=_serialize_datetime(before) or "")
                )
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        return int(result.rows_affected)

    @staticmethod
    def _default_sqlspec_config() -> SQLSpecConfigT:
        try:
            from sqlspec.adapters.aiosqlite import AiosqliteConfig
        except ModuleNotFoundError as exc:
            raise missing_sqlspec_error(exc) from exc
        return AiosqliteConfig()

    def _get_or_create_sqlspec(self) -> SQLSpecT:
        if self._sqlspec is None:
            try:
                from sqlspec import SQLSpec
            except ModuleNotFoundError as exc:
                raise missing_sqlspec_error(exc) from exc
            self._sqlspec = SQLSpec()
        return self._sqlspec

    def _get_sqlspec_config(self) -> SQLSpecConfigT:
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
            from litestar_queues.backends.sqlspec.store import create_queue_store

            self._store = create_queue_store(self._get_sqlspec_config(), table_name=self._table_name)
        return self._store

    @asynccontextmanager
    async def _session(self) -> AsyncIterator[Any]:
        if not self._opened or self._sqlspec is None:
            msg = "SQLSpecQueueBackend.open() must be called before using the backend."
            raise RuntimeError(msg)
        sqlspec_config = self._get_sqlspec_config()
        async with self._get_or_create_sqlspec().provide_session(sqlspec_config) as driver:
            yield driver

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
        from sqlspec.utils.serializers import to_json

        return {
            "args_json": to_json(list(record.args)),
            "completed_at": _serialize_datetime(record.completed_at),
            "created_at": _serialize_datetime(record.created_at),
            "error": record.error,
            "execution_backend": record.execution_backend,
            "execution_profile": record.execution_profile,
            "execution_ref": record.execution_ref,
            "heartbeat_at": _serialize_datetime(record.heartbeat_at),
            "id": str(record.id),
            "kwargs_json": to_json(record.kwargs),
            "max_retries": record.max_retries,
            "metadata_json": to_json(record.metadata),
            "priority": record.priority,
            "queue": record.queue,
            "result_json": to_json(record.result),
            "retry_count": record.retry_count,
            "scheduled_at": _serialize_datetime(record.scheduled_at),
            "started_at": _serialize_datetime(record.started_at),
            "status": record.status,
            "task_key": record.key,
            "task_name": record.task_name,
        }

    def _record_from_row(self, row: dict[str, Any]) -> QueuedTaskRecord:
        from sqlspec.utils.serializers import from_json

        args = from_json(row["args_json"])
        kwargs = from_json(row["kwargs_json"])
        metadata = from_json(row["metadata_json"])
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
            result=from_json(row["result_json"]),
            error=cast("str | None", row["error"]),
            key=cast("str | None", row["task_key"]),
            metadata=metadata,
        )

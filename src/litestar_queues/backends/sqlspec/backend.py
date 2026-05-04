"""SQLSpec storage backend."""

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from litestar_queues.backends.base import BaseStorageBackend
from litestar_queues.backends.sqlspec._typing import missing_sqlspec_error
from litestar_queues.backends.sqlspec.config import SQLSpecBackendConfig
from litestar_queues.backends.sqlspec.litestar import build_sqlspec_plugin
from litestar_queues.backends.sqlspec.schema import (
    DEFAULT_TABLE_NAME,
    load_packaged_sql,
    load_queue_queries,
    migration_paths,
    schema_sql,
    validate_table_name,
)
from litestar_queues.models import QueuedTaskRecord, TaskStatus

if TYPE_CHECKING:
    from litestar.config.app import AppConfig

    from litestar_queues.config import QueueConfig

__all__ = ("SQLSpecBackendConfig", "SQLSpecStorageBackend")

_DUE_STATUSES = ("pending", "scheduled")
_JSON_SEPARATORS = (",", ":")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _dumps_json(value: Any) -> str:
    return json.dumps(value, separators=_JSON_SEPARATORS)


def _loads_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    return json.loads(str(value))


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
        msg = f"Unknown queued task status from SQLSpec storage: {status!r}"
        raise ValueError(msg)
    return cast("TaskStatus", status)


class SQLSpecStorageBackend(BaseStorageBackend):
    """SQLSpec-backed queue storage backend."""

    __slots__ = (
        "_backend_config",
        "_config_registered",
        "_loader",
        "_opened",
        "_queries",
        "_sqlspec",
        "_sqlspec_config",
        "_table_name",
    )

    def __init__(
        self,
        config: "QueueConfig | None" = None,
        *,
        sqlspec: Any | None = None,
        sqlspec_config: Any | None = None,
        sqlspec_plugin: Any | None = None,
        table_name: str = DEFAULT_TABLE_NAME,
        create_schema: bool = True,
        run_migrations: bool = False,
        register_plugin: bool = False,
        loader: Any | None = None,
    ) -> None:
        super().__init__(config=config)
        table_name = validate_table_name(table_name)
        self._backend_config = SQLSpecBackendConfig(
            sqlspec=sqlspec,
            sqlspec_config=sqlspec_config,
            sqlspec_plugin=sqlspec_plugin,
            table_name=table_name,
            create_schema=create_schema,
            run_migrations=run_migrations,
            register_plugin=register_plugin,
            loader=loader,
        )
        self._sqlspec = sqlspec
        self._sqlspec_config = sqlspec_config
        self._loader = loader
        self._table_name = table_name
        self._queries: dict[str, str] | None = None
        self._opened = False
        self._config_registered = False

    def on_app_init(self, app_config: "AppConfig") -> "AppConfig":
        """Let SQLSpec contribute its first-party Litestar plugin when requested."""
        if not self._backend_config.register_plugin:
            return app_config

        plugin = self._backend_config.sqlspec_plugin
        if plugin is None:
            plugin = build_sqlspec_plugin(self._get_or_create_sqlspec(), self._get_loader())
        app_config.plugins.append(plugin)
        return app_config

    async def open(self) -> bool:
        """Open SQLSpec resources.

        Returns:
            True when SQLSpec resources are ready.
        """
        if self._opened:
            return True

        self._get_or_create_sqlspec()
        self._opened = True
        if self._backend_config.run_migrations:
            await self.run_migrations()
        if self._backend_config.create_schema:
            await self.create_schema()
        return True

    async def close(self) -> None:
        """Close SQLSpec resources."""
        if self._sqlspec is not None:
            await self._sqlspec.close_all_pools()
            self._sqlspec = None
        self._opened = False
        self._config_registered = False

    async def create_schema(self) -> None:
        """Create the SQLSpec queue table and indexes."""
        async with self._session() as driver:
            await driver.execute_script(schema_sql(self._get_loader(), self._table_name))
            await driver.commit()

    async def run_migrations(self) -> None:
        """Apply packaged SQLSpec migrations."""
        sqlspec_config = self._get_sqlspec_config()
        paths = migration_paths()
        migration_config = dict(getattr(sqlspec_config, "migration_config", None) or {})
        migration_config["script_location"] = str(Path(paths[0]).parent)
        sqlspec_config.set_migration_config(migration_config)
        for path in paths:
            sqlspec_config.load_migration_sql_files(path)
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
                    status="scheduled" if scheduled_at is not None and scheduled_at > now else "pending",
                    priority=priority,
                    max_retries=max_retries,
                    scheduled_at=scheduled_at,
                    key=key,
                    metadata=dict(metadata or {}),
                )
                await driver.execute(self._get_query("insert_task"), **self._params_from_record(record))
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
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
    ) -> list[QueuedTaskRecord]:
        rows = await self._select_pending_rows(limit=limit, queue=queue)
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
                    self._get_query("claim_task"),
                    id=str(task_id),
                    due_at=_serialize_datetime(now),
                    heartbeat_at=_serialize_datetime(now),
                    started_at=_serialize_datetime(now),
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

    async def claim_next(self, *, queue: str | None = None) -> QueuedTaskRecord | None:
        rows = await self._select_pending_rows(limit=10, queue=queue)
        for row in rows:
            task_id = UUID(str(row["id"]))
            claimed = await self.claim_task(task_id)
            if claimed is not None:
                return claimed
        return None

    async def complete_task(self, task_id: UUID, *, result: Any = None) -> QueuedTaskRecord | None:
        now = _utc_now()
        async with self._session() as driver:
            await driver.begin()
            try:
                updated = await driver.execute(
                    self._get_query("complete_task"),
                    id=str(task_id),
                    completed_at=_serialize_datetime(now),
                    heartbeat_at=_serialize_datetime(now),
                    result_json=_dumps_json(result),
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
                        self._get_query("retry_task"),
                        id=str(task_id),
                        error=error,
                        retry_count=record.retry_count + 1,
                    )
                else:
                    now = _utc_now()
                    await driver.execute(
                        self._get_query("fail_task"),
                        id=str(task_id),
                        completed_at=_serialize_datetime(now),
                        heartbeat_at=_serialize_datetime(now),
                        error=error,
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
                    self._get_query("cancel_task"),
                    id=str(task_id),
                    completed_at=_serialize_datetime(_utc_now()),
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
                    self._get_query("touch_heartbeat"),
                    id=str(task_id),
                    heartbeat_at=_serialize_datetime(_utc_now()),
                )
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
                result = await driver.execute(self._get_query("requeue_stale"), cutoff=_serialize_datetime(cutoff))
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        return int(result.rows_affected)

    @staticmethod
    def _default_sqlspec_config() -> Any:
        try:
            from sqlspec.adapters.aiosqlite import AiosqliteConfig
        except ModuleNotFoundError as exc:
            raise missing_sqlspec_error(exc) from exc
        return AiosqliteConfig()

    def _get_or_create_sqlspec(self) -> Any:
        if self._sqlspec is None:
            try:
                from sqlspec import SQLSpec
            except ModuleNotFoundError as exc:
                raise missing_sqlspec_error(exc) from exc
            self._sqlspec = SQLSpec()
        sqlspec_config = self._get_sqlspec_config()
        if not self._config_registered:
            self._sqlspec.add_config(sqlspec_config)
            self._config_registered = True
        return self._sqlspec

    def _get_sqlspec_config(self) -> Any:
        if self._sqlspec_config is None:
            self._sqlspec_config = self._backend_config.sqlspec_config or self._default_sqlspec_config()
            self._backend_config.sqlspec_config = self._sqlspec_config
        return self._sqlspec_config

    def _get_loader(self) -> Any:
        if self._loader is None:
            try:
                from sqlspec import SQLFileLoader
            except ModuleNotFoundError as exc:
                raise missing_sqlspec_error(exc) from exc
            self._loader = load_packaged_sql(SQLFileLoader())
            self._backend_config.loader = self._loader
        return self._loader

    def _get_query(self, name: str) -> str:
        if self._queries is None:
            self._queries = load_queue_queries(self._get_loader(), self._table_name)
        return self._queries[name]

    @asynccontextmanager
    async def _session(self) -> AsyncIterator[Any]:
        if not self._opened or self._sqlspec is None:
            msg = "SQLSpecStorageBackend.open() must be called before using the backend."
            raise RuntimeError(msg)
        sqlspec_config = self._get_sqlspec_config()
        async with self._sqlspec.provide_session(sqlspec_config) as driver:
            yield driver

    async def _select_pending_rows(self, *, limit: int, queue: str | None) -> list[dict[str, Any]]:
        async with self._session() as driver:
            rows = await driver.select(
                self._get_query("list_pending"),
                now=_serialize_datetime(_utc_now()),
                queue_filter=queue,
                queue_value=queue,
                limit=limit,
            )
        return cast("list[dict[str, Any]]", rows)

    async def _select_task(self, driver: Any, task_id: UUID) -> dict[str, Any] | None:
        row = await driver.select_one_or_none(self._get_query("get_task"), id=str(task_id))
        return cast("dict[str, Any] | None", row)

    async def _select_task_by_key(self, driver: Any, key: str) -> dict[str, Any] | None:
        row = await driver.select_one_or_none(self._get_query("get_task_by_key"), task_key=key)
        return cast("dict[str, Any] | None", row)

    async def _clear_key(self, driver: Any, task_id: UUID) -> None:
        await driver.execute(self._get_query("clear_key"), id=str(task_id))

    def _params_from_record(self, record: QueuedTaskRecord) -> dict[str, Any]:
        return {
            "args_json": _dumps_json(list(record.args)),
            "completed_at": _serialize_datetime(record.completed_at),
            "created_at": _serialize_datetime(record.created_at),
            "error": record.error,
            "heartbeat_at": _serialize_datetime(record.heartbeat_at),
            "id": str(record.id),
            "kwargs_json": _dumps_json(record.kwargs),
            "max_retries": record.max_retries,
            "metadata_json": _dumps_json(record.metadata),
            "priority": record.priority,
            "queue": record.queue,
            "result_json": _dumps_json(record.result),
            "retry_count": record.retry_count,
            "scheduled_at": _serialize_datetime(record.scheduled_at),
            "started_at": _serialize_datetime(record.started_at),
            "status": record.status,
            "task_key": record.key,
            "task_name": record.task_name,
        }

    def _record_from_row(self, row: dict[str, Any]) -> QueuedTaskRecord:
        args = _loads_json(row["args_json"], [])
        kwargs = _loads_json(row["kwargs_json"], {})
        metadata = _loads_json(row["metadata_json"], {})
        return QueuedTaskRecord(
            id=UUID(str(row["id"])),
            task_name=str(row["task_name"]),
            args=tuple(cast("list[Any]", args)),
            kwargs=dict(cast("dict[str, Any]", kwargs)),
            queue=str(row["queue"]),
            status=_coerce_status(row["status"]),
            priority=int(row["priority"]),
            max_retries=int(row["max_retries"]),
            retry_count=int(row["retry_count"]),
            scheduled_at=_deserialize_datetime(row["scheduled_at"]),
            created_at=cast("datetime", _deserialize_datetime(row["created_at"])),
            started_at=_deserialize_datetime(row["started_at"]),
            completed_at=_deserialize_datetime(row["completed_at"]),
            heartbeat_at=_deserialize_datetime(row["heartbeat_at"]),
            result=_loads_json(row["result_json"], None),
            error=cast("str | None", row["error"]),
            key=cast("str | None", row["task_key"]),
            metadata=dict(cast("dict[str, Any]", metadata)),
        )

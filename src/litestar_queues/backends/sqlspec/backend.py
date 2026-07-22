"""SQLSpec queue backend."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager, contextmanager, suppress
from datetime import datetime, timedelta, timezone
from inspect import isawaitable, iscoroutinefunction
from logging import getLogger
from typing import TYPE_CHECKING, Any, cast, overload
from uuid import UUID

from sqlspec import SQLSpec
from sqlspec.exceptions import SerializationConflictError
from sqlspec.extensions.events import normalize_event_channel_name, resolve_adapter_name
from sqlspec.utils.sync_tools import async_

from litestar_queues.backends._notification_wait import PendingNativeRead
from litestar_queues.backends.base import (
    STALE_HEARTBEAT_ERROR,
    BaseQueueBackend,
    record_matches_filters,
    stale_requeue_error,
    stale_requeue_priority,
)
from litestar_queues.backends.sqlspec.config import DEFAULT_NOTIFICATION_CHANNEL, SQLSpecBackendConfig
from litestar_queues.backends.sqlspec.event_log import (
    SQLSpecQueueEventLog,
    create_event_log_store,
    resolve_event_log_table_name,
)
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.schema import (
    DEFAULT_TABLE_NAME,
    resolve_column_map,
    validate_native_json_columns,
    validate_table_name,
)
from litestar_queues.backends.sqlspec.stores.factory import create_queue_store
from litestar_queues.backends.sqlspec.uniqueness import create_tombstone_store
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.models import (
    EnqueueSpec,
    HeartbeatTouchResult,
    QueueBackendCapabilities,
    QueuedTaskRecord,
    QueueStatistics,
    StaleTaskRecoveryResult,
    TaskStatus,
    UniquenessTombstone,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator, Mapping, Sequence

    from litestar_queues.backends.sqlspec._typing import (
        SQLSpecConfig,
        SQLSpecDriver,
        SQLSpecManager,
        SQLSpecSessionConfig,
        SQLSpecStoreConfig,
    )
    from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore
    from litestar_queues.backends.sqlspec.uniqueness import SQLSpecQueueTombstoneStore
    from litestar_queues.config import QueueConfig
    from litestar_queues.events import EventLogConfig, QueueEventLog
    from litestar_queues.models import HeartbeatTouch

__all__ = ("SQLSpecQueueBackend",)

_DUE_STATUSES = ("pending", "scheduled")
_RESERVE_MAX_ATTEMPTS = 12
_RESERVE_BACKOFF_SECONDS = 0.02
_DURABLE_NOTIFICATION_BACKENDS = frozenset({"aq", "notify_queue", "poll_queue", "txeventq"})
_EVENT_EXTENSION_NAME = "events"
_QUEUE_SETTING_EVENT_SETTINGS = ("event_settings", "events")

_NOTIFY_TRANSPORT_POLLING = "polling"
_CANONICAL_EVENT_BACKENDS = frozenset({"aq", "notify", "notify_queue", "poll_queue", "txeventq"})
_CANONICAL_NOTIFY_TRANSPORTS = _CANONICAL_EVENT_BACKENDS | {_NOTIFY_TRANSPORT_POLLING}
# Adapter families that can push worker wakeups. Every real Postgres driver
# (asyncpg, psycopg, psqlpy) ships SQLSpec's durable LISTEN/NOTIFY hybrid, so all
# three advertise ``notify_queue``. DuckDB is embedded with no LISTEN/NOTIFY, so
# it uses the durable table queue polled in-process. Everything else polls.
_NOTIFY_DURABLE_ADAPTERS = frozenset({"asyncpg", "psycopg", "psqlpy"})
_NOTIFY_TABLE_QUEUE_ADAPTERS = frozenset({"duckdb"})
# Durable event transports that ride the SQLSpec events queue table and must have
# it provisioned before a worker can publish or consume wakeups.
_EVENTS_TABLE_BACKENDS = frozenset({"notify_queue", "poll_queue"})
# psqlpy's ``QueryResult`` exposes no command-tag/status metadata and
# arrow-odbc's cursor exposes no portable rowcount API, so SQLSpec's
# rows-affected extraction can only ever report ``0`` for non-SELECT
# statements on these adapters, regardless of whether the statement actually
# matched a row. Every write-then-verify check in this module already treats
# a negative rows-affected as "unknown, verify via SELECT"; normalizing these
# adapters' unconditional ``0`` to that same sentinel reuses that existing
# fallback instead of misreading it as a genuine zero-row result.
_UNRELIABLE_ROWCOUNT_ADAPTERS = frozenset({"psqlpy", "arrow_odbc"})


def _package_queue_observability_enabled(config: "QueueConfig | None") -> "bool":
    """Return whether package-level queue observability should own queue-domain metrics."""
    if config is None:
        return False
    observability_config = config.observability
    if observability_config is None:
        return False
    if not observability_config.disable_sqlspec_queue_observability:
        return False
    return observability_config.enable_prometheus or observability_config.enable_otel is not False


def _adapter_notify_transport(adapter_name: "str | None") -> "str":
    """Return the default wakeup transport for a SQLSpec adapter.

    The wakeup transport is gated purely by adapter knowledge so backends only
    advertise push wakeups where the driver can deliver them.

    Returns:
        ``"notify_queue"`` for the Postgres drivers (asyncpg, psycopg, psqlpy),
        ``"poll_queue"`` for DuckDB, otherwise ``"polling"``.
    """
    if adapter_name in _NOTIFY_DURABLE_ADAPTERS:
        return "notify_queue"
    if adapter_name in _NOTIFY_TABLE_QUEUE_ADAPTERS:
        return "poll_queue"
    return _NOTIFY_TRANSPORT_POLLING


def _canonical_notify_transport(backend_name: "str | None") -> "str | None":
    """Return a canonical SQLSpec event backend name.

    Returns:
        The canonical queue transport name, if the backend is supported.
    """
    return backend_name if backend_name in _CANONICAL_EVENT_BACKENDS else None


def _validate_queue_notify_transport(transport: "str") -> "str":
    """Validate explicit queue transport configuration from extension settings.

    Returns:
        The validated canonical queue transport name.
    """
    if transport in _CANONICAL_NOTIFY_TRANSPORTS:
        return transport
    valid = ", ".join(sorted(_CANONICAL_NOTIFY_TRANSPORTS))
    msg = f"Invalid notify_transport {transport!r}; expected one of: {valid}."
    raise QueueConfigurationError(msg)


class SQLSpecQueueBackend(BaseQueueBackend):
    """SQLSpec-backed queue backend."""

    __slots__ = (
        "_column_map",
        "_event_backend",
        "_event_channel",
        "_event_log",
        "_event_log_store",
        "_event_log_table_name",
        "_event_poll_interval",
        "_event_queue_table",
        "_event_settings",
        "_event_stream",
        "_heartbeat_pool_config",
        "_heartbeat_pool_enabled",
        "_heartbeat_pool_registered",
        "_manage_schema",
        "_native_json_columns",
        "_notification_backend",
        "_notification_channel",
        "_notifications_enabled",
        "_notifications_requested",
        "_notify_transport",
        "_opened",
        "_owns_event_channel",
        "_owns_sqlspec",
        "_pending_read",
        "_queue_observability",
        "_queue_table_name",
        "_sqlspec",
        "_sqlspec_config",
        "_store",
        "_uniqueness_store",
        "_uniqueness_table_name",
    )

    def __init__(
        self, config: "QueueConfig | None" = None, *, backend_config: "SQLSpecBackendConfig | None" = None
    ) -> "None":
        super().__init__(config=config)
        backend_config = backend_config or SQLSpecBackendConfig()
        self._column_map = resolve_column_map(backend_config.column_map)
        self._native_json_columns = validate_native_json_columns(frozenset(backend_config.native_json_columns))
        self._manage_schema = backend_config.manage_schema
        self._sqlspec = backend_config.sqlspec
        self._sqlspec_config: "SQLSpecConfig | SQLSpecStoreConfig | None" = backend_config.config
        self._heartbeat_pool_config: "SQLSpecConfig | SQLSpecStoreConfig | None" = backend_config.heartbeat_pool_config
        self._heartbeat_pool_enabled = self._heartbeat_pool_config is not None
        self._heartbeat_pool_registered = False
        self._owns_sqlspec = self._sqlspec is None
        self._queue_table_name = (
            validate_table_name(backend_config.queue_table_name)
            if backend_config.queue_table_name is not None
            else None
        )
        self._event_channel = backend_config.event_channel
        self._owns_event_channel = self._event_channel is None
        self._notifications_requested = backend_config.notifications
        self._notification_channel = backend_config.notification_channel
        self._notify_transport = backend_config.notify_transport
        self._event_log_table_name = (
            validate_table_name(backend_config.event_log_table_name)
            if backend_config.event_log_table_name is not None
            else None
        )
        self._event_backend = backend_config.event_backend
        self._event_queue_table = backend_config.event_queue_table
        self._event_poll_interval = backend_config.event_poll_interval
        self._event_settings = dict(backend_config.event_settings)
        self._queue_observability = backend_config.queue_observability and not _package_queue_observability_enabled(
            config
        )
        self._notification_backend = _canonical_notify_transport(getattr(self._event_channel, "_backend_name", None))
        self._notifications_enabled = self._event_channel is not None
        self._event_log_store: "Any | None" = None
        self._event_log: "SQLSpecQueueEventLog | None" = None
        self._store: "SQLSpecQueueStore | None" = None
        self._uniqueness_store: "SQLSpecQueueTombstoneStore | None" = None
        self._uniqueness_table_name = (
            validate_table_name(backend_config.uniqueness_table_name)
            if backend_config.uniqueness_table_name is not None
            else None
        )
        self._event_stream: "Any | None" = None
        self._pending_read = PendingNativeRead()
        self._opened = False

    async def open(self) -> "bool":
        """Open SQLSpec resources.

        Returns:
            True when SQLSpec resources are ready.
        """
        if self._opened:
            return True

        self._get_or_create_sqlspec()
        self._resolve_queue_table_name()
        self._configure_notifications()
        self._register_heartbeat_pool()
        self._opened = True
        return True

    async def close(self) -> "None":
        """Close SQLSpec resources."""
        if self._event_log is not None:
            await self._event_log.flush_events()
        await self._close_notification_stream()
        await self._close_heartbeat_pool()
        if self._owns_event_channel and self._event_channel is not None:
            await _invoke_event_channel_method(self._event_channel, "shutdown")
            self._event_channel = None
        if self._owns_sqlspec and self._sqlspec is not None:
            await self._sqlspec.close_all_pools()
            self._sqlspec = None
        self._opened = False

    def get_event_log(self, config: "EventLogConfig") -> "QueueEventLog | None":
        """Return SQLSpec-managed durable queue event history when enabled."""
        if not config.enabled:
            return None
        if self._event_log is None:
            self._event_log = SQLSpecQueueEventLog(
                session_factory=self._session,
                datetime_serializer=self._serialize_datetime,
                config=config,
                store=self._get_event_log_store(),
            )
        return self._event_log

    @property
    def capabilities(self) -> "QueueBackendCapabilities":
        """Backend behavior capabilities."""
        notification_backend = self._notification_backend
        return QueueBackendCapabilities(
            supports_notifications=self._notifications_enabled,
            notification_backend=notification_backend,
            notifications_durable=notification_backend in _DURABLE_NOTIFICATION_BACKENDS,
        )

    async def create_schema(self) -> "None":
        """Create the SQLSpec queue table and indexes.

        Returns:
            None.
        """
        if self._manage_schema:
            store = self._get_store()
            create_for_config = getattr(store, "create_schema_for_config", None)
            if callable(create_for_config):
                result = create_for_config(self._get_sqlspec_config())
                if isawaitable(result):
                    await result
            await self._create_uniqueness_schema()
            if callable(create_for_config) and not self._event_log_enabled():
                return
            async with self._session() as driver:
                statements: "list[str]" = []
                if not callable(create_for_config):
                    statements.extend(await _create_schema_statements(self._get_store(), driver))
                event_log_store = self._get_event_log_store_if_enabled()
                if event_log_store is not None:
                    statements.extend(event_log_store.create_statements())
                if self._should_provision_events_queue():
                    statements.extend(_events_queue_create_statements(self._get_sqlspec_config()))
                for statement in statements:
                    await driver.execute_script(statement)
                await driver.commit()

    async def _create_uniqueness_schema(self) -> "None":
        store = self._get_uniqueness_store()
        create_for_config = getattr(store, "create_schema_for_config", None)
        if callable(create_for_config):
            result = create_for_config(self._get_sqlspec_config())
            if isawaitable(result):
                await result
            return
        async with self._session() as driver:
            for statement in store.create_statements():
                await driver.execute_script(statement)
            await driver.commit()

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
        id: "UUID | None" = None,  # noqa: A002
    ) -> "QueuedTaskRecord":
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
        if id is not None:
            record.id = id
        if key is not None:
            return await self._enqueue_keyed(record, key)
        store = self._get_store()
        if not type(store).supports_dml_returning:
            return await self._enqueue_legacy(record)
        with self._observe_queue_operation("enqueue", queue=queue, task_name=task_name):
            async with self._session() as driver:
                await driver.execute(store.insert_returning_sql(), self._insert_params(record))
        self._increment_queue_metric("enqueue")
        await self.notify_new_task(record)
        return record

    def _insert_params(self, record: "QueuedTaskRecord") -> "dict[str, Any]":
        return self._get_store().bulk_values([self._params_from_record(record)])[0]

    async def _enqueue_keyed(self, record: "QueuedTaskRecord", key: "str") -> "QueuedTaskRecord":
        with self._observe_queue_operation("enqueue", queue=record.queue, task_name=record.task_name):
            async with self._session() as driver:
                await driver.begin()
                try:
                    existing_row = await self._select_task_by_key(driver, key)
                    if existing_row is not None:
                        existing = self._record_from_row(existing_row)
                        if not existing.is_terminal:
                            await driver.rollback()
                            return existing
                        await self._clear_key(driver, existing.id)
                    await driver.execute(self._get_store().insert_task(self._params_from_record(record)))
                    await driver.commit()
                except Exception as exc:
                    with suppress(Exception):
                        await driver.rollback()
                    if _is_unique_violation(exc):
                        winner = await self.get_task_by_key(key)
                        if winner is not None and not winner.is_terminal:
                            return winner
                    raise
        self._increment_queue_metric("enqueue")
        await self.notify_new_task(record)
        return record

    async def _enqueue_legacy(self, record: "QueuedTaskRecord") -> "QueuedTaskRecord":
        with self._observe_queue_operation("enqueue", queue=record.queue, task_name=record.task_name):
            async with self._session() as driver:
                await driver.begin()
                try:
                    await driver.execute(self._get_store().insert_task(self._params_from_record(record)))
                    await driver.commit()
                except Exception:
                    with suppress(Exception):
                        await driver.rollback()
                    raise
        self._increment_queue_metric("enqueue")
        await self.notify_new_task(record)
        return record

    async def enqueue_many(self, specs: "Sequence[EnqueueSpec]") -> "list[QueuedTaskRecord]":
        """Persist many tasks via the adapter's fastest bulk path.

        Resolves existing deduplication keys in one round trip, then inserts the
        remaining rows through the native Arrow ingest path
        (:meth:`load_from_records`) when the adapter supports it, otherwise via a
        batched ``execute_many``. Returns records in input order, with existing
        non-terminal keyed tasks returned as-is (no duplicate insert) to match
        the semantics of :meth:`enqueue`.

        Returns:
            Queue task records in the same order as ``specs``.
        """
        if not specs:
            return []

        store = self._get_store()
        now = _utc_now()
        keyed = [spec.key for spec in specs if spec.key is not None]

        with self._observe_queue_operation("enqueue", task_count=len(specs)):
            async with self._session() as driver:
                await driver.begin()
                try:
                    existing_by_key = await self._existing_records_by_key(driver, store, keyed)
                    results, to_insert, terminal_keys = self._plan_bulk_enqueue(specs, existing_by_key, now)
                    for task_id in terminal_keys:
                        await driver.execute(store.clear_key(task_id=str(task_id)))
                    if to_insert:
                        await self._bulk_insert(driver, store, to_insert)
                    await driver.commit()
                except Exception:
                    with suppress(Exception):
                        await driver.rollback()
                    raise

        self._increment_queue_metric("enqueue", float(len(to_insert)))
        await self.notify_new_tasks(to_insert)
        return results

    async def get_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        async with self._session() as driver:
            row = await self._select_task(driver, task_id)
        return self._record_from_row(row) if row is not None else None

    async def get_task_by_key(self, key: "str") -> "QueuedTaskRecord | None":
        async with self._session() as driver:
            row = await self._select_task_by_key(driver, key)
        return self._record_from_row(row) if row is not None else None

    async def reserve_identity(
        self, key: "str", *, task_id: "UUID", task_name: "str"
    ) -> "UniquenessTombstone | None":
        """Reserve a forever identity via an optimistic insert with a unique-violation fallback.

        The tombstone table's identity-key PRIMARY KEY is the atomicity arbiter:
        exactly one concurrent insert wins; a loser catches the unique violation
        and reads the winning owner. Serializable engines (CockroachDB) may abort
        a losing transaction with a serialization/retry error before the unique
        violation surfaces, so those are retried with bounded backoff. The
        tombstone table is separate from the queue table and terminal cleanup
        never touches it.

        Returns:
            ``None`` when this caller won the reservation; otherwise the existing
            owner tombstone.
        """
        store = self._get_uniqueness_store()
        values = {
            "identity_key": key,
            "task_id": str(task_id),
            "task_name": task_name,
            "created_at": self._serialize_datetime(_utc_now()),
        }
        last_exc: "Exception | None" = None
        for attempt in range(_RESERVE_MAX_ATTEMPTS):
            try:
                return await self._reserve_identity_once(store, key, values)
            except Exception as exc:
                if _is_unique_violation(exc):
                    owner = await self.has_identity(key)
                    if owner is not None:
                        return owner
                elif not _is_serialization_conflict(exc):
                    raise
                last_exc = exc
            await asyncio.sleep(_RESERVE_BACKOFF_SECONDS * (attempt + 1))
        if last_exc is not None:
            raise last_exc
        return None

    async def _reserve_identity_once(
        self, store: "SQLSpecQueueTombstoneStore", key: "str", values: "dict[str, Any]"
    ) -> "UniquenessTombstone | None":
        async with self._session() as driver:
            await driver.begin()
            try:
                existing = await self._select_one_row(driver, store.select_owner(key))
                if existing is not None:
                    # Commit the read-only transaction rather than rolling back so
                    # single-connection engines (Spanner) do not double-finalize.
                    await driver.commit()
                    return _tombstone_from_row(existing)
                await driver.execute(store.insert_reservation(values))
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        return None

    async def has_identity(self, key: "str") -> "UniquenessTombstone | None":
        """Return the tombstone owning a reserved forever identity, if any."""
        store = self._get_uniqueness_store()
        async with self._session() as driver:
            row = await self._select_one_row(driver, store.select_owner(key))
        return _tombstone_from_row(row) if row is not None else None

    async def reset_identity(self, key: "str") -> "bool":
        """Delete a forever identity tombstone via count-then-delete.

        Returns:
            ``True`` when a tombstone was removed.
        """
        store = self._get_uniqueness_store()
        async with self._session() as driver:
            await driver.begin()
            try:
                count_row = await self._select_one_row(driver, store.count_by_key(key))
                removed = int(count_row["tombstone_count"]) if count_row is not None else 0
                if removed > 0:
                    await driver.execute(store.delete_by_key(key))
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        return removed > 0

    async def list_pending(
        self, *, limit: "int" = 1, queue: "str | None" = None, execution_backend: "str | None" = None
    ) -> "list[QueuedTaskRecord]":
        rows = await self._select_pending_rows(limit=limit, queue=queue, execution_backend=execution_backend)
        return [self._record_from_row(row) for row in rows]

    async def claim_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        with self._observe_queue_operation("claim", task_id=str(task_id)):
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
                            due_at=self._serialize_datetime(now),
                            heartbeat_at=self._serialize_datetime(now),
                            started_at=self._serialize_datetime(now),
                        )
                    )
                    if self._resolve_rows_affected(result) == 0:
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
        claimed = self._record_from_row(updated_row) if updated_row is not None else None
        if claimed is not None:
            self._increment_queue_metric("claim")
        return claimed

    async def claim_many(
        self, *, limit: "int", queues: "tuple[str, ...]" = (), execution_backend: "str | None" = None
    ) -> "list[QueuedTaskRecord]":
        """Claim up to ``limit`` due tasks.

        Postgres-family stores use one autocommit
        ``UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED LIMIT $n) ... RETURNING``
        statement. Other adapters fall back to the per-record claim loop.

        Returns:
            Claimed task records.
        """
        if limit <= 0:
            return []
        store = self._get_store()
        if not type(store).supports_returning_claim:
            return await super().claim_many(limit=limit, queues=queues, execution_backend=execution_backend)
        now = _utc_now()
        parameters: "dict[str, Any]" = {
            "now": self._serialize_datetime(now),
            "started_at": self._serialize_datetime(now),
            "heartbeat_at": self._serialize_datetime(now),
            "limit": limit,
        }
        for index, queue in enumerate(queues):
            parameters[f"queue_{index}"] = queue
        if execution_backend is not None:
            parameters["execution_backend"] = execution_backend
        sql_text = store.claim_batch_returning_sql(
            queue_count=len(queues), filter_execution_backend=execution_backend is not None
        )
        with self._observe_queue_operation("claim", execution_backend=execution_backend):
            async with self._session() as driver:
                rows = await self._select_rows(driver, sql_text, parameters)
        records = [self._record_from_row(row) for row in rows]
        if records:
            self._increment_queue_metric("claim", float(len(records)))
        return records

    async def claim_next(
        self, *, queues: "tuple[str, ...]" = (), execution_backend: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        store = self._get_store()
        for queue in queues or (None,):
            if store.supports_skip_locked:
                claimed = await self._claim_next_skip_locked(store, queue=queue, execution_backend=execution_backend)
            else:
                claimed = await self._claim_next_optimistic(store, queue=queue, execution_backend=execution_backend)
            if claimed is not None:
                return claimed
        return None

    async def _claim_next_optimistic(
        self, store: "SQLSpecQueueStore", *, queue: "str | None", execution_backend: "str | None"
    ) -> "QueuedTaskRecord | None":
        rows = await self._select_pending_rows(limit=10, queue=queue, execution_backend=execution_backend)
        for row in rows:
            task_id = UUID(str(row["id"]))
            try:
                claimed = await self.claim_task(task_id)
            except Exception as exc:
                if _is_serialization_conflict(exc):
                    continue
                raise
            if claimed is not None:
                return claimed
        return None

    async def _claim_next_skip_locked(
        self, store: "SQLSpecQueueStore", *, queue: "str | None", execution_backend: "str | None"
    ) -> "QueuedTaskRecord | None":
        """Claim the next due task under ``SELECT ... FOR UPDATE SKIP LOCKED``.

        Locks a single due row and claims it inside one transaction so
        competing workers skip the locked row instead of colliding on the
        optimistic CAS claim. The v1 fenced-claim contract is preserved: a row
        that cannot be transitioned to ``running`` yields ``None``.

        Returns:
            The claimed task record, if a claim was available.
        """
        with self._observe_queue_operation("claim", queue=queue, execution_backend=execution_backend):
            async with self._session() as driver:
                await driver.begin()
                try:
                    now = _utc_now()
                    statement = store.select_claimable(
                        now=self._serialize_datetime(now), limit=1, queue=queue, execution_backend=execution_backend
                    )
                    stream_chunk_size = cast("int | None", getattr(store, "claim_select_stream_chunk_size", None))
                    row = await self._select_one_row(driver, statement, chunk_size=stream_chunk_size)
                    if row is None:
                        await driver.rollback()
                        return None
                    record = self._record_from_row(row)
                    result = await driver.execute(
                        store.claim_task(
                            task_id=str(record.id),
                            due_at=self._serialize_datetime(now),
                            heartbeat_at=self._serialize_datetime(now),
                            started_at=self._serialize_datetime(now),
                        )
                    )
                    if self._resolve_rows_affected(result) == 0:
                        await driver.rollback()
                        return None
                    updated_row = await self._select_task(driver, record.id)
                    if updated_row is None or self._record_from_row(updated_row).status != "running":
                        await driver.rollback()
                        return None
                    await driver.commit()
                except Exception:
                    with suppress(Exception):
                        await driver.rollback()
                    raise
        claimed = self._record_from_row(updated_row)
        self._increment_queue_metric("claim")
        return claimed

    async def complete_task(
        self, task_id: "UUID", *, result: "Any" = None, expected_retry_count: "int | None" = None
    ) -> "QueuedTaskRecord | None":
        now = _utc_now()
        store = self._get_store()
        if not type(store).supports_dml_returning:
            return await self._complete_task_legacy(task_id, result=result, expected_retry_count=expected_retry_count)
        parameters: "dict[str, Any]" = {
            "id": str(task_id),
            "completed_at": self._serialize_datetime(now),
            "result_json": store.serialize_json("result_json", result),
        }
        if expected_retry_count is not None:
            parameters["expected_retry_count"] = expected_retry_count
        sql_text = store.complete_returning_sql(fence_retry_count=expected_retry_count is not None)
        with self._observe_queue_operation("complete", task_id=str(task_id)):
            async with self._session() as driver:
                row = await self._select_one_row(driver, sql_text, parameters)
        completed = self._record_from_row(row) if row is not None else None
        if completed is not None:
            self._increment_queue_metric("complete")
        elif expected_retry_count is not None:
            self._increment_queue_metric("claim_lost")
        return completed

    async def _complete_task_legacy(
        self, task_id: "UUID", *, result: "Any" = None, expected_retry_count: "int | None" = None
    ) -> "QueuedTaskRecord | None":
        now = _utc_now()
        store = self._get_store()
        with self._observe_queue_operation("complete", task_id=str(task_id)):
            async with self._session() as driver:
                await driver.begin()
                try:
                    updated = await driver.execute(
                        store.complete_task(
                            task_id=str(task_id),
                            completed_at=self._serialize_datetime(now),
                            heartbeat_at=self._serialize_datetime(now),
                            result_json=store.serialize_json("result_json", result),
                            expected_retry_count=expected_retry_count,
                        )
                    )
                    rows_affected = self._resolve_rows_affected(updated)
                    row = await self._select_task(driver, task_id) if rows_affected == 1 or rows_affected < 0 else None
                    if row is not None:
                        completed_record = self._record_from_row(row)
                        if completed_record.status != "completed" or (
                            expected_retry_count is not None and completed_record.retry_count != expected_retry_count
                        ):
                            row = None
                    await driver.commit()
                except Exception:
                    with suppress(Exception):
                        await driver.rollback()
                    raise
        completed = self._record_from_row(row) if row is not None else None
        if completed is not None:
            self._increment_queue_metric("complete")
        elif expected_retry_count is not None:
            self._increment_queue_metric("claim_lost")
        return completed

    async def fail_task(
        self, task_id: "UUID", error: "str", *, retry: "bool" = True, expected_retry_count: "int | None" = None
    ) -> "QueuedTaskRecord | None":
        store = self._get_store()
        if not type(store).supports_dml_returning:
            return await self._fail_task_legacy(task_id, error, retry=retry, expected_retry_count=expected_retry_count)
        now = _utc_now()
        stored_error = store.serialize_error(error)
        parameters: "dict[str, Any]" = {
            "id": str(task_id),
            "error": stored_error,
            "retry": retry,
            "completed_at": self._serialize_datetime(now),
        }
        if expected_retry_count is not None:
            parameters["expected_retry_count"] = expected_retry_count
        sql_text = store.fail_returning_sql(fence_retry_count=expected_retry_count is not None)
        with self._observe_queue_operation("fail", task_id=str(task_id), retry=retry):
            async with self._session() as driver:
                row = await self._select_one_row(driver, sql_text, parameters)
        updated = self._record_from_row(row) if row is not None else None
        if updated is None:
            self._increment_queue_metric("claim_lost")
            return None
        self._increment_queue_metric("retry" if updated.status == "pending" else "fail")
        if updated.status == "pending":
            await self.notify_new_task(updated)
        return updated

    async def _fail_task_legacy(
        self, task_id: "UUID", error: "str", *, retry: "bool" = True, expected_retry_count: "int | None" = None
    ) -> "QueuedTaskRecord | None":
        with self._observe_queue_operation("fail", task_id=str(task_id), retry=retry):
            async with self._session() as driver:
                await driver.begin()
                try:
                    store = self._get_store()
                    stored_error = store.serialize_error(error)
                    row = await self._select_task(driver, task_id)
                    if row is None:
                        await driver.rollback()
                        return None

                    record = self._record_from_row(row)
                    if record.status != "running" or (
                        expected_retry_count is not None and record.retry_count != expected_retry_count
                    ):
                        await driver.commit()
                        self._increment_queue_metric("claim_lost")
                        return None
                    metric = "fail"
                    retry_fence = expected_retry_count if expected_retry_count is not None else record.retry_count
                    if retry and record.retry_count < record.max_retries:
                        updated = await driver.execute(
                            store.retry_task(
                                task_id=str(task_id),
                                error=stored_error,
                                retry_count=record.retry_count + 1,
                                expected_retry_count=retry_fence,
                            )
                        )
                        metric = "retry"
                        expected_status = "pending"
                        expected_retry_after_update = record.retry_count + 1
                    else:
                        now = _utc_now()
                        updated = await driver.execute(
                            store.fail_task(
                                task_id=str(task_id),
                                completed_at=self._serialize_datetime(now),
                                heartbeat_at=self._serialize_datetime(now),
                                error=stored_error,
                                expected_retry_count=retry_fence,
                            )
                        )
                        expected_status = "failed"
                        expected_retry_after_update = record.retry_count

                    rows_affected = self._resolve_rows_affected(updated)
                    if rows_affected == 1 or rows_affected < 0:
                        updated_row = await self._select_task(driver, task_id)
                        if updated_row is not None:
                            candidate = self._record_from_row(updated_row)
                            if (
                                candidate.status != expected_status
                                or candidate.retry_count != expected_retry_after_update
                                or candidate.error != stored_error
                            ):
                                updated_row = None
                    else:
                        updated_row = None
                    await driver.commit()
                except Exception:
                    with suppress(Exception):
                        await driver.rollback()
                    raise
        updated_record = self._record_from_row(updated_row) if updated_row is not None else None
        if updated_record is not None:
            self._increment_queue_metric(metric)
        else:
            self._increment_queue_metric("claim_lost")
        return updated_record

    async def cancel_task(self, task_id: "UUID", *, include_running: "bool" = False) -> "bool":
        async with self._session() as driver:
            await driver.begin()
            try:
                result = await driver.execute(
                    self._get_store().cancel_task(
                        task_id=str(task_id),
                        completed_at=self._serialize_datetime(_utc_now()),
                        include_running=include_running,
                    )
                )
                rows_affected = self._resolve_rows_affected(result)
                if rows_affected < 0:
                    updated_row = await self._select_task(driver, task_id)
                    cancelled = updated_row is not None and self._record_from_row(updated_row).status == "cancelled"
                else:
                    cancelled = rows_affected == 1
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        return cancelled

    async def cancel_tasks(
        self,
        *,
        task_name: "str | None" = None,
        queue: "str | None" = None,
        kwargs: "Mapping[str, Any] | None" = None,
        metadata: "Mapping[str, Any] | None" = None,
        include_running: "bool" = False,
    ) -> "int":
        store = self._get_store()
        async with self._session() as driver:
            rows = await self._select_rows(
                driver, store.list_cancellable(include_running=include_running, task_name=task_name, queue=queue)
            )
        cancelled = 0
        for row in rows:
            record = self._record_from_row(row)
            if not record_matches_filters(record, task_name=task_name, queue=queue, kwargs=kwargs, metadata=metadata):
                continue
            if await self.cancel_task(record.id, include_running=include_running):
                cancelled += 1
        return cancelled

    async def touch_heartbeats(self, touches: "Sequence[HeartbeatTouch]") -> "HeartbeatTouchResult":
        result = HeartbeatTouchResult()
        if not touches:
            return result

        store = self._get_store()
        async with self._heartbeat_session() as driver:
            await driver.begin()
            try:
                bulk_result = await self._touch_heartbeats_bulk(driver, store, touches)
                result = (
                    bulk_result
                    if bulk_result is not None
                    else await self._touch_heartbeats_loop(driver, store, touches)
                )
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        return result

    async def _touch_heartbeats_bulk(
        self, driver: "SQLSpecDriver", store: "SQLSpecQueueStore", touches: "Sequence[HeartbeatTouch]"
    ) -> "HeartbeatTouchResult | None":
        if not getattr(type(store), "supports_bulk_touch_heartbeats", False):
            return None
        task_ids = [touch.task_id for touch in touches]
        if len(set(task_ids)) != len(task_ids):
            return None

        bulk_touches: "list[dict[str, Any]]" = []
        for touch in touches:
            metadata_json = None
            if touch.metadata_patch:
                metadata_json = store.serialize_json("metadata_json", touch.metadata_patch)
            bulk_touches.append({
                "task_id": str(touch.task_id),
                "expected_retry_count": touch.expected_retry_count,
                "metadata_json": metadata_json,
            })

        statement = store.bulk_touch_heartbeats(touches=bulk_touches, heartbeat_at=self._serialize_datetime(_utc_now()))
        if statement is None:
            return None

        touched_rows = await driver.select(statement.sql, statement.parameters)
        touched_task_ids = {UUID(str(row["id"])) for row in cast("list[dict[str, Any]]", touched_rows)}
        return HeartbeatTouchResult(touched_task_ids=touched_task_ids, missed_task_ids=set(task_ids) - touched_task_ids)

    async def _touch_heartbeats_loop(
        self, driver: "SQLSpecDriver", store: "SQLSpecQueueStore", touches: "Sequence[HeartbeatTouch]"
    ) -> "HeartbeatTouchResult":
        result = HeartbeatTouchResult()
        for touch in touches:
            row = await self._select_task(driver, touch.task_id)
            if row is None:
                result.missed_task_ids.add(touch.task_id)
                continue
            record = self._record_from_row(row)
            if record.status != "running" or (
                touch.expected_retry_count is not None and record.retry_count != touch.expected_retry_count
            ):
                result.missed_task_ids.add(touch.task_id)
                continue
            metadata_json = None
            if touch.metadata_patch:
                metadata = dict(record.metadata)
                metadata.update(touch.metadata_patch)
                metadata_json = store.serialize_json("metadata_json", metadata)
            execution_result = await driver.execute(
                store.touch_heartbeats(
                    task_id=str(touch.task_id),
                    heartbeat_at=self._serialize_datetime(_utc_now()),
                    expected_retry_count=touch.expected_retry_count,
                    metadata_json=metadata_json,
                )
            )
            rows_affected = self._resolve_rows_affected(execution_result)
            if rows_affected == 1:
                result.touched_task_ids.add(touch.task_id)
            elif rows_affected == 0:
                result.missed_task_ids.add(touch.task_id)
            else:
                touched_row = await self._select_task(driver, touch.task_id)
                touched_record = self._record_from_row(touched_row) if touched_row is not None else None
                if (
                    touched_record is not None
                    and touched_record.status == "running"
                    and (touch.expected_retry_count is None or touched_record.retry_count == touch.expected_retry_count)
                ):
                    result.touched_task_ids.add(touch.task_id)
                else:
                    result.missed_task_ids.add(touch.task_id)
        return result

    async def null_heartbeats(self, task_ids: "list[UUID]", *, expected_retry_count: "int | None" = None) -> "None":
        if not task_ids:
            return
        async with self._heartbeat_session() as driver:
            await driver.begin()
            try:
                filtered_task_ids = task_ids
                if expected_retry_count is not None:
                    filtered_task_ids = []
                    for task_id in task_ids:
                        row = await self._select_task(driver, task_id)
                        if row is None:
                            continue
                        record = self._record_from_row(row)
                        if record.retry_count == expected_retry_count:
                            filtered_task_ids.append(task_id)
                if filtered_task_ids:
                    await driver.execute(
                        self._get_store().null_heartbeats(task_ids=[str(task_id) for task_id in filtered_task_ids])
                    )
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise

    async def requeue_stale_running(self, *, stale_after: "timedelta") -> "StaleTaskRecoveryResult":
        cutoff = _utc_now() - stale_after
        store = self._get_store()
        result = StaleTaskRecoveryResult()
        serialized_cutoff = self._serialize_datetime(cutoff)
        with self._observe_queue_operation("stale_recovered"):
            async with self._session() as driver:
                rows = await self._select_rows(driver, store.list_stale_running(cutoff=serialized_cutoff))
                if not rows:
                    return result
                # Reset any implicit read transaction the SELECT may have opened so
                # stale-recovery writes run in a fresh transaction.
                with suppress(Exception):
                    await driver.rollback()
                await driver.begin()
                try:
                    failed_handler_needed: "list[UUID]" = []
                    for row in rows:
                        record = self._record_from_row(row)
                        requeue_on_stale = record.metadata.get("requeue_on_stale", True) is not False
                        if requeue_on_stale and record.retry_count < record.max_retries:
                            retry_error = stale_requeue_error(record.error)
                            retry_priority = stale_requeue_priority(record.priority)
                            updated = await driver.execute(
                                store.retry_task(
                                    task_id=str(record.id),
                                    error=retry_error,
                                    retry_count=record.retry_count + 1,
                                    expected_retry_count=record.retry_count,
                                    heartbeat_cutoff=serialized_cutoff,
                                    priority=retry_priority,
                                )
                            )
                            rows_affected = self._resolve_rows_affected(updated)
                            if rows_affected == 1 or (
                                rows_affected < 0
                                and await self._stale_retry_updated(
                                    driver,
                                    record.id,
                                    record.retry_count,
                                    expected_error=retry_error,
                                    expected_priority=retry_priority,
                                )
                            ):
                                result.requeued += 1
                            else:
                                result.skipped += 1
                        else:
                            now = _utc_now()
                            updated = await driver.execute(
                                store.fail_task(
                                    task_id=str(record.id),
                                    completed_at=self._serialize_datetime(now),
                                    heartbeat_at=self._serialize_datetime(now),
                                    error=STALE_HEARTBEAT_ERROR,
                                    expected_retry_count=record.retry_count,
                                    heartbeat_cutoff=serialized_cutoff,
                                )
                            )
                            rows_affected = self._resolve_rows_affected(updated)
                            if rows_affected == 1 or (
                                rows_affected < 0 and await self._stale_fail_updated(driver, record.id)
                            ):
                                result.failed += 1
                                result.failed_task_ids.append(record.id)
                                if not requeue_on_stale:
                                    failed_handler_needed.append(record.id)
                            else:
                                result.skipped += 1
                    for task_id in failed_handler_needed:
                        result.handler_needed += 1
                        result.handler_needed_task_ids.append(task_id)
                    await driver.commit()
                except Exception:
                    with suppress(Exception):
                        await driver.rollback()
                    raise
        recovered = result.requeued + result.failed
        if recovered:
            self._increment_queue_metric("stale_recovered", float(recovered))
        if result.requeued:
            self._increment_queue_metric("retry", float(result.requeued))
        if result.failed:
            self._increment_queue_metric("stale_failed", float(result.failed))
        return result

    async def set_execution_ref(
        self, task_id: "UUID", execution_backend: "str", execution_ref: "str", *, execution_profile: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        async with self._session() as driver:
            await driver.begin()
            try:
                result = await driver.execute(
                    self._get_store().set_execution_ref(
                        task_id=str(task_id),
                        execution_backend=execution_backend,
                        execution_profile=execution_profile,
                        execution_ref=execution_ref,
                    )
                )
                row = await self._select_task(driver, task_id) if self._resolve_rows_affected(result) else None
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        return self._record_from_row(row) if row is not None else None

    async def set_execution_backend(
        self, task_id: "UUID", execution_backend: "str", *, execution_profile: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        async with self._session() as driver:
            await driver.begin()
            try:
                result = await driver.execute(
                    self._get_store().set_execution_backend(
                        task_id=str(task_id), execution_backend=execution_backend, execution_profile=execution_profile
                    )
                )
                row = await self._select_task(driver, task_id) if self._resolve_rows_affected(result) else None
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        record = self._record_from_row(row) if row is not None else None
        if record is not None:
            await self.notify_new_task(record)
        return record

    async def list_running_external(self, *, limit: "int | None" = None) -> "list[QueuedTaskRecord]":
        async with self._session() as driver:
            rows = await self._select_rows(driver, self._get_store().list_running_external(limit=limit))
        return [self._record_from_row(row) for row in rows]

    async def get_statistics(self) -> "QueueStatistics":
        statistics = QueueStatistics()
        async with self._session() as driver:
            async for row in _select_stream(driver, self._get_store().list_all()):
                status = _coerce_status(cast("dict[str, Any]", row)["status"])
                setattr(statistics, status, getattr(statistics, status) + 1)
        return statistics

    async def iter_all(self, *, chunk_size: "int" = 1000) -> "AsyncIterator[QueuedTaskRecord]":
        """Stream every queue record without materializing the full table.

        Uses SQLSpec ``select_stream`` so large administrative scans and exports
        consume rows in chunks of ``chunk_size`` rather than loading the entire
        result set into memory. The backend session stays open for the duration
        of iteration, so callers should consume the iterator promptly.

        Yields:
            Queue task records from the backing SQLSpec table.
        """
        session = self._session()
        driver = await session.__aenter__()
        try:
            async for row in _select_stream(driver, self._get_store().list_all(), chunk_size=chunk_size):
                yield self._record_from_row(cast("dict[str, Any]", row))
        except BaseException as exc:
            if not await session.__aexit__(type(exc), exc, exc.__traceback__):
                raise
        else:
            await session.__aexit__(None, None, None)

    async def list_completed_by_task(
        self, task_name: "str", *, since: "datetime | None" = None, limit: "int" = 10
    ) -> "list[QueuedTaskRecord]":
        store = self._get_store()
        statement = store.list_completed_by_task(
            task_name=task_name, since=self._serialize_datetime(since), limit=limit
        )
        built = statement.build(dialect=store.dialect_name)
        async with self._session() as driver:
            rows = await self._select_rows(driver, built.sql, built.parameters)
        return [self._record_from_row(row) for row in rows]

    async def cleanup_terminal(self, before: "datetime") -> "int":
        store = self._get_store()
        before_str = self._serialize_datetime(before)
        async with self._session() as driver:
            await driver.begin()
            try:
                # Some drivers (e.g. psqlpy, see _UNRELIABLE_ROWCOUNT_ADAPTERS)
                # cannot reliably report ``rows_affected`` for DELETE. Count
                # first inside the same transaction so the cleanup count is
                # always exact.
                count_row = await self._select_one_row(driver, store.count_terminal(before=before_str))
                deleted = int(count_row["terminal_count"]) if count_row is not None else 0
                if deleted > 0:
                    await driver.execute(store.cleanup_terminal(before=before_str))
                await driver.commit()
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise
        return deleted

    async def notify_new_task(self, record: "QueuedTaskRecord") -> "None":
        """Publish a SQLSpec event when configured queue work becomes available."""
        if (
            self._notifications_enabled
            and self._event_channel is not None
            and record.status in _DUE_STATUSES
            and record.is_due
        ):
            with self._observe_queue_operation("notify", queue=record.queue):
                await _invoke_event_channel_method(
                    self._event_channel,
                    "publish",
                    self._resolve_notification_channel(),
                    {"event": "task_available"},
                    {"event_type": "litestar_queues.task_available"},
                )
            self._increment_queue_metric("notify")

    async def wait_for_notifications(self, timeout: "float | None" = None) -> "bool":
        """Wait for a SQLSpec event when queue notifications are configured.

        One ``iter_events`` stream and its pending ``anext`` read are retained
        across worker poll timeouts; only an event, a driver failure, or
        backend close ends them.

        Returns:
            True when a notification was received.
        """
        if not self._notifications_enabled or self._event_channel is None:
            return await super().wait_for_notifications(timeout=timeout)

        stream = self._event_stream
        if stream is None:
            stream = self._event_channel.iter_events(
                self._resolve_notification_channel(), poll_interval=self._event_poll_interval
            )
            self._event_stream = stream
        task = await self._pending_read.race(lambda: _next_event(stream), timeout)
        if task is None:
            return False
        exc = task.exception()
        if exc is not None:
            await self._close_notification_stream()
            raise exc
        event = task.result()
        await _invoke_event_channel_method(self._event_channel, "ack", event.event_id)
        return True

    async def time_until_next_due(self, *, queues: "tuple[str, ...]" = ()) -> "float | None":
        """Return seconds until the earliest not-yet-due pending/scheduled record.

        Returns:
            Seconds until the next due record, or ``None`` when there is no
            upcoming scheduled work.
        """
        now = _utc_now()
        async with self._session() as driver:
            row = await self._select_one_row(
                driver, self._get_store().next_scheduled_at(now=self._serialize_datetime(now), queues=queues)
            )
        if row is None:
            return None
        next_at = _deserialize_datetime(row.get("next_scheduled_at"))
        if next_at is None:
            return None
        return max((next_at - _utc_now()).total_seconds(), 0.0)

    async def _close_notification_stream(self) -> "None":
        """Cancel the retained event read and close the iterator."""
        await self._pending_read.aclose()
        stream = self._event_stream
        self._event_stream = None
        if stream is not None:
            with suppress(Exception):
                close = getattr(stream, "aclose", None) or getattr(stream, "close", None)
                if close is not None:
                    result = close()
                    if isawaitable(result):
                        await result

    @staticmethod
    def _default_sqlspec_config() -> "SQLSpecConfig":
        from sqlspec.adapters.aiosqlite import AiosqliteConfig

        return cast("SQLSpecConfig", AiosqliteConfig())

    def _resolve_queue_table_name(self) -> "str":
        if self._queue_table_name is None:
            queue_settings = _queue_extension_settings(self._sqlspec_config)
            configured_table_name = _setting(queue_settings, "table_name") or DEFAULT_TABLE_NAME
            self._queue_table_name = validate_table_name(str(configured_table_name))
        return self._queue_table_name

    def _resolve_notification_channel(self) -> "str":
        if self._notification_channel is not None:
            self._notification_channel = _normalize_notification_channel(str(self._notification_channel))
        else:
            queue_settings = _queue_extension_settings(self._sqlspec_config)
            configured_channel = _setting(queue_settings, "notification_channel") or DEFAULT_NOTIFICATION_CHANNEL
            self._notification_channel = _normalize_notification_channel(str(configured_channel))
        return self._notification_channel

    def _configure_notifications(self) -> "None":
        sqlspec_config = self._get_sqlspec_config()
        queue_settings = _queue_extension_settings(sqlspec_config)
        events_settings = _events_extension_settings(sqlspec_config)

        notifications_requested = self._resolve_notifications_requested(queue_settings)
        transport = self._select_notify_transport(sqlspec_config, queue_settings, events_settings)

        if not self._notifications_should_enable(notifications_requested, transport):
            self._notifications_enabled = False
            self._notification_backend = None
            return

        self._notifications_enabled = True
        self._resolve_notification_channel()
        if self._event_channel is None:
            self._apply_event_settings(sqlspec_config, queue_settings, events_settings, transport)
            self._event_channel = cast("Any", self._get_or_create_sqlspec()).event_channel(sqlspec_config)
            self._owns_event_channel = True
        else:
            # An injected channel already owns its backend; still resolve the
            # configured poll interval so wait_for_notifications honors it.
            self._resolve_event_poll_interval(queue_settings, events_settings)
        self._notification_backend = _canonical_notify_transport(
            cast("str | None", getattr(self._event_channel, "_backend_name", None))
        )

    def _should_provision_events_queue(self) -> "bool":
        """Return whether ``create_schema`` should provision the events queue table.

        Only the durable table-backed transports (``notify_queue`` / ``poll_queue``)
        that this backend owns ride the SQLSpec events queue table. An injected
        event channel owns its own storage, and the transient/native-only or
        Oracle AQ transports never use this table.
        """
        return (
            self._notifications_enabled
            and self._owns_event_channel
            and self._notification_backend in _EVENTS_TABLE_BACKENDS
        )

    def _resolve_notifications_requested(self, queue_settings: "dict[str, Any]") -> "bool | None":
        notifications_requested = self._notifications_requested
        if notifications_requested is None and "notifications" in queue_settings:
            notifications_requested = bool(queue_settings["notifications"])
        return notifications_requested

    def _select_notify_transport(
        self, sqlspec_config: "SQLSpecConfig", queue_settings: "dict[str, Any]", events_settings: "dict[str, Any]"
    ) -> "str":
        """Resolve the effective wakeup transport.

        Explicit ``queue_backend_config`` selections win over
        ``extension_config`` defaults, which in turn win over the per-adapter
        capability gate.

        Returns:
            A canonical wakeup transport name (``notify``, ``notify_queue``,
            ``poll_queue``, ``polling``, ``aq``, or ``txeventq``).
        """
        return _resolve_notify_transport(
            explicit_transport=self._notify_transport,
            event_backend=self._event_backend,
            sqlspec_config=sqlspec_config,
            queue_settings=queue_settings,
            events_settings=events_settings,
        )

    def _notifications_should_enable(self, notifications_requested: "bool | None", transport: "str") -> "bool":
        """Decide whether push wakeups are active for the resolved transport.

        Native wakeups are default-on: whenever the resolved transport is
        capability-native (anything other than ``polling``) an events channel
        backs worker wakeups with no configuration. ``notifications=False`` is
        the explicit opt-out, and a capability-gated ``polling`` transport (an
        adapter that cannot push, with no explicit override) stays on interval
        polling. Notifications only add wakeups; the frozen claim/lease contract
        is unaffected.

        Returns:
            True when an events channel should back worker wakeups.
        """
        if notifications_requested is False:
            return False
        if self._event_channel is not None:
            return True
        return transport != _NOTIFY_TRANSPORT_POLLING

    def _resolve_event_poll_interval(
        self, queue_settings: "dict[str, Any]", events_settings: "dict[str, Any]"
    ) -> "None":
        configured_poll_interval = self._event_poll_interval
        if configured_poll_interval is None:
            configured_poll_interval = _setting(queue_settings, "event_poll_interval")
        if configured_poll_interval is None and "poll_interval" in events_settings:
            configured_poll_interval = events_settings["poll_interval"]
        if configured_poll_interval is not None:
            self._event_poll_interval = float(configured_poll_interval)

    def _apply_event_settings(
        self,
        sqlspec_config: "SQLSpecConfig",
        queue_settings: "dict[str, Any]",
        events_settings: "dict[str, Any]",
        transport: "str",
    ) -> "None":
        merged_event_settings = dict(events_settings)
        for name in _QUEUE_SETTING_EVENT_SETTINGS:
            configured_events = queue_settings.get(name)
            if isinstance(configured_events, dict):
                merged_event_settings.update(configured_events)
        merged_event_settings.update(self._event_settings)
        merged_event_settings["backend"] = transport

        configured_queue_table = self._event_queue_table or _setting(queue_settings, "event_queue_table")
        if configured_queue_table is not None:
            merged_event_settings["queue_table"] = str(configured_queue_table)

        self._resolve_event_poll_interval(queue_settings, merged_event_settings)
        if self._event_poll_interval is not None:
            merged_event_settings["poll_interval"] = self._event_poll_interval

        extension_config = dict(sqlspec_config.extension_config or {})
        extension_config[_EVENT_EXTENSION_NAME] = merged_event_settings
        sqlspec_config.extension_config = extension_config
        migration_config = dict(sqlspec_config.migration_config or {})
        sqlspec_config.set_migration_config(migration_config)

    def _get_or_create_sqlspec(self) -> "SQLSpec":
        if self._sqlspec is None:
            self._sqlspec = SQLSpec()
        return self._sqlspec

    def _get_sqlspec_config(self) -> "SQLSpecConfig":
        if self._sqlspec_config is None:
            registered_configs = tuple(cast("dict[int, SQLSpecConfig]", self._get_or_create_sqlspec().configs).values())
            if len(registered_configs) == 1:
                self._sqlspec_config = registered_configs[0]
            elif len(registered_configs) > 1:
                msg = (
                    "SQLSpecQueueBackend received a SQLSpec manager with multiple configs; "
                    "pass config to select the queue database."
                )
                raise QueueConfigurationError(msg)
            else:
                self._sqlspec_config = self._default_sqlspec_config()
        return cast("SQLSpecConfig", self._sqlspec_config)

    def _resolve_rows_affected(self, result: "Any") -> "int":
        """Return :func:`_rows_affected` normalized for this backend's configured adapter."""
        return _rows_affected(result, resolve_adapter_name(self._get_sqlspec_config()))

    def _get_store(self) -> "SQLSpecQueueStore":
        if self._store is None:
            self._store = create_queue_store(
                self._get_sqlspec_config(),
                table_name=self._resolve_queue_table_name(),
                column_map=self._column_map,
                native_json_columns=self._native_json_columns,
                manage_schema=self._manage_schema,
            )
        return self._store

    def _get_event_log_store(self) -> "Any":
        if self._event_log_store is None:
            store = create_event_log_store(
                self._get_sqlspec_config(),
                queue_table_name=self._resolve_queue_table_name(),
                event_log_table_name=self._event_log_table_name,
                manage_schema=self._manage_schema,
            )
            self._event_log_table_name = store.table_name
            self._event_log_store = store
        return self._event_log_store

    def _get_event_log_store_if_enabled(self) -> "Any | None":
        return self._get_event_log_store() if self._event_log_enabled() else None

    def _get_uniqueness_store(self) -> "SQLSpecQueueTombstoneStore":
        if self._uniqueness_store is None:
            store = create_tombstone_store(
                self._get_sqlspec_config(),
                queue_table_name=self._resolve_queue_table_name(),
                uniqueness_table_name=self._uniqueness_table_name,
                manage_schema=self._manage_schema,
            )
            self._uniqueness_table_name = store.table_name
            self._uniqueness_store = store
        return self._uniqueness_store

    def _event_log_enabled(self) -> "bool":
        return bool(self.config is not None and self.config.event_log is not None and self.config.event_log.enabled)

    def _resolve_event_log_table_name(self) -> "str":
        if self._event_log_table_name is None:
            self._event_log_table_name = resolve_event_log_table_name(self._resolve_queue_table_name())
        return self._event_log_table_name

    @asynccontextmanager
    async def _session(self) -> "AsyncIterator[SQLSpecDriver]":
        if not self._opened or self._sqlspec is None:
            msg = "SQLSpecQueueBackend.open() must be called before using the backend."
            raise RuntimeError(msg)
        sqlspec_config = self._get_sqlspec_config()
        store = self._get_store()
        async with _bridge_session(
            cast("SQLSpecManager", self._get_or_create_sqlspec()),
            sqlspec_config,
            skip_explicit_begin=store.skip_explicit_begin,
            skip_cleanup_rollback=store.skip_cleanup_rollback,
        ) as driver:
            yield driver

    @asynccontextmanager
    async def _heartbeat_session(self) -> "AsyncIterator[SQLSpecDriver]":
        """Yield a driver bound to the dedicated heartbeat pool when configured.

        Falls back to the main pool when ``heartbeat_pool_config`` is not set,
        or when the dedicated pool failed to register at ``open()`` time.

        Yields:
            A SQLSpec driver bound to the heartbeat or main pool.

        Raises:
            RuntimeError: When ``open()`` has not been called on the backend.
        """
        if not self._opened or self._sqlspec is None:
            msg = "SQLSpecQueueBackend.open() must be called before using the backend."
            raise RuntimeError(msg)
        if self._heartbeat_pool_enabled and self._heartbeat_pool_registered and self._heartbeat_pool_config is not None:
            async with _bridge_session(
                cast("SQLSpecManager", self._sqlspec),
                cast("SQLSpecSessionConfig", self._heartbeat_pool_config),
                skip_cleanup_rollback=self._get_store().skip_cleanup_rollback,
            ) as driver:
                yield driver
        else:
            async with self._session() as driver:
                yield driver

    def _register_heartbeat_pool(self) -> "None":
        """Register the dedicated heartbeat pool with the SQLSpec manager.

        Best effort. On failure the backend logs a warning and continues with
        the main pool for heartbeats.
        """
        if (
            self._heartbeat_pool_enabled
            and self._heartbeat_pool_config is not None
            and not self._heartbeat_pool_registered
        ):
            try:
                cast("Any", self._get_or_create_sqlspec()).add_config(self._heartbeat_pool_config)
            except Exception:
                getLogger("litestar_queues").warning(
                    "SQLSpecQueueBackend heartbeat pool registration failed; "
                    "falling back to main pool for heartbeat writes.",
                    exc_info=True,
                )
                self._heartbeat_pool_enabled = False
                self._heartbeat_pool_registered = False
            else:
                self._heartbeat_pool_registered = True

    async def _close_heartbeat_pool(self) -> "None":
        """Close the dedicated heartbeat pool if the backend opened one."""
        if self._heartbeat_pool_registered and self._heartbeat_pool_config is not None:
            try:
                close_result = cast("SQLSpecConfig", self._heartbeat_pool_config).close_pool()
                if isawaitable(close_result):
                    await close_result
            except Exception:
                getLogger("litestar_queues").debug("SQLSpecQueueBackend heartbeat pool close failed.", exc_info=True)
            self._heartbeat_pool_registered = False

    async def _select_pending_rows(
        self, *, limit: "int", queue: "str | None", execution_backend: "str | None"
    ) -> "list[dict[str, Any]]":
        async with self._session() as driver:
            return await self._select_rows(
                driver,
                self._get_store().list_pending(
                    now=self._serialize_datetime(_utc_now()),
                    limit=limit,
                    queue=queue,
                    execution_backend=execution_backend,
                ),
            )

    async def _select_task(self, driver: "SQLSpecDriver", task_id: "UUID") -> "dict[str, Any] | None":
        return await self._select_one_row(driver, self._get_store().select_task(str(task_id)))

    async def _select_task_by_key(self, driver: "SQLSpecDriver", key: "str") -> "dict[str, Any] | None":
        return await self._select_one_row(driver, self._get_store().select_task_by_key(key))

    async def _select_rows(
        self, driver: "SQLSpecDriver", statement: "Any", *parameters: "Any", chunk_size: "int | None" = None
    ) -> "list[dict[str, Any]]":
        stream_chunk_size = chunk_size
        if stream_chunk_size is None:
            stream_chunk_size = cast("int | None", getattr(self._get_store(), "select_stream_chunk_size", None))
        if stream_chunk_size is not None and not isinstance(driver, _ManagedAsyncDriver):
            rows: "list[dict[str, Any]]" = []
            async for row in _select_stream(driver, statement, *parameters, chunk_size=stream_chunk_size):
                rows.append(cast("dict[str, Any]", row))
            return rows
        return cast("list[dict[str, Any]]", await driver.select(statement, *parameters))

    async def _select_one_row(
        self, driver: "SQLSpecDriver", statement: "Any", *parameters: "Any", chunk_size: "int | None" = None
    ) -> "dict[str, Any] | None":
        rows = await self._select_rows(driver, statement, *parameters, chunk_size=chunk_size)
        return rows[0] if rows else None

    async def _clear_key(self, driver: "SQLSpecDriver", task_id: "UUID") -> "None":
        await driver.execute(self._get_store().clear_key(task_id=str(task_id)))

    async def _stale_retry_updated(
        self,
        driver: "SQLSpecDriver",
        task_id: "UUID",
        previous_retry_count: "int",
        *,
        expected_error: "str",
        expected_priority: "int",
    ) -> "bool":
        row = await self._select_task(driver, task_id)
        if row is None:
            return False
        record = self._record_from_row(row)
        return (
            record.status == "pending"
            and record.retry_count == previous_retry_count + 1
            and record.error == expected_error
            and record.priority == expected_priority
        )

    async def _stale_fail_updated(self, driver: "SQLSpecDriver", task_id: "UUID") -> "bool":
        row = await self._select_task(driver, task_id)
        if row is None:
            return False
        record = self._record_from_row(row)
        return record.status == "failed" and record.error == STALE_HEARTBEAT_ERROR

    def _get_observability_runtime(self) -> "Any | None":
        if not self._queue_observability:
            return None
        return self._get_sqlspec_config().get_observability_runtime()

    @contextmanager
    def _observe_queue_operation(self, operation: "str", **attributes: "Any") -> "Iterator[None]":
        runtime = self._get_observability_runtime()
        if runtime is None:
            yield
            return
        span_attributes = {
            "sqlspec.queue.operation": operation,
            **{f"litestar_queues.{key}": value for key, value in attributes.items() if value is not None},
        }
        span = runtime.start_span(f"sqlspec.queue.{operation}", attributes=span_attributes)
        error: "Exception | None" = None
        try:
            yield
        except Exception as exc:
            error = exc
            raise
        finally:
            if span is not None:
                runtime.end_span(span, error=error)

    def _increment_queue_metric(self, name: "str", amount: "float" = 1.0) -> "None":
        runtime = self._get_observability_runtime()
        if runtime is not None and amount:
            runtime.increment_metric(f"queue.{name}", amount)

    async def _existing_records_by_key(
        self, driver: "SQLSpecDriver", store: "SQLSpecQueueStore", keys: "list[str]"
    ) -> "dict[str, QueuedTaskRecord]":
        """Return a map of deduplication key to existing record for the given keys."""
        existing: "dict[str, QueuedTaskRecord]" = {}
        if not keys:
            return existing
        rows = await self._select_rows(driver, store.select_tasks_by_keys(keys))
        for row in rows:
            record = self._record_from_row(row)
            if record.key is not None:
                existing[record.key] = record
        return existing

    def _plan_bulk_enqueue(
        self, specs: "Sequence[EnqueueSpec]", existing_by_key: "dict[str, QueuedTaskRecord]", now: "datetime"
    ) -> "tuple[list[QueuedTaskRecord], list[QueuedTaskRecord], list[UUID]]":
        """Resolve deduplication keys and build records, preserving input order.

        Returns the ordered result records, the subset that must be inserted, and
        the ids of terminal-key rows whose key must be cleared before insert.
        Active (non-terminal) keys, whether already persisted or earlier in the
        batch, reuse the existing record instead of inserting a duplicate.

        Returns:
            Ordered result records, records to insert, and terminal-key ids to clear.
        """
        results: "list[QueuedTaskRecord]" = []
        to_insert: "list[QueuedTaskRecord]" = []
        terminal_keys_to_clear: "list[UUID]" = []
        batch_new_by_key: "dict[str, QueuedTaskRecord]" = {}
        for spec in specs:
            key = spec.key
            if key is not None:
                reused = self._reuse_for_key(key, existing_by_key, batch_new_by_key, terminal_keys_to_clear)
                if reused is not None:
                    results.append(reused)
                    continue
            record = self._record_from_spec(spec, now)
            results.append(record)
            to_insert.append(record)
            if key is not None:
                batch_new_by_key[key] = record
        return results, to_insert, terminal_keys_to_clear

    @staticmethod
    def _reuse_for_key(
        key: "str",
        existing_by_key: "dict[str, QueuedTaskRecord]",
        batch_new_by_key: "dict[str, QueuedTaskRecord]",
        terminal_keys_to_clear: "list[UUID]",
    ) -> "QueuedTaskRecord | None":
        """Return the record to reuse for ``key``, or ``None`` if a new row is needed.

        Records a terminal key for clearing so its row can be replaced.
        """
        active = existing_by_key.get(key)
        if active is not None and not active.is_terminal:
            return active
        earlier = batch_new_by_key.get(key)
        if earlier is not None:
            return earlier
        if active is not None:
            terminal_keys_to_clear.append(active.id)
            del existing_by_key[key]
        return None

    @staticmethod
    def _record_from_spec(spec: "EnqueueSpec", now: "datetime") -> "QueuedTaskRecord":
        return QueuedTaskRecord(
            task_name=spec.task_name,
            args=spec.args,
            kwargs=dict(spec.kwargs or {}),
            queue=spec.queue,
            execution_backend=spec.execution_backend,
            execution_profile=spec.execution_profile,
            status="scheduled" if spec.scheduled_at is not None and spec.scheduled_at > now else "pending",
            priority=spec.priority,
            max_retries=spec.max_retries,
            scheduled_at=spec.scheduled_at,
            key=spec.key,
            metadata=dict(spec.metadata or {}),
        )

    async def _bulk_insert(
        self, driver: "SQLSpecDriver", store: "SQLSpecQueueStore", records: "list[QueuedTaskRecord]"
    ) -> "None":
        """Insert records using the adapter's fastest available bulk tier."""
        values = store.bulk_values([self._params_from_record(record) for record in records])
        if store.supports_native_bulk_ingest:
            await driver.load_from_records(store.table_name, values)
        else:
            await driver.execute_many(store.insert_tasks_template(), values)

    @overload
    def _serialize_datetime(self, value: "datetime") -> "datetime | str": ...

    @overload
    def _serialize_datetime(self, value: "None") -> "None": ...

    def _serialize_datetime(self, value: "datetime | None") -> "datetime | str | None":
        serialized = _serialize_datetime(value)
        store = self._get_store()
        if serialized is not None and store.bind_datetime_as_text:
            formatter = getattr(store, "serialize_datetime_text", None)
            if callable(formatter):
                return cast("str", formatter(serialized))
            return serialized.isoformat()
        if serialized is not None and store.bind_datetime_as_naive_utc:
            return serialized.replace(tzinfo=None)
        return serialized

    def _params_from_record(self, record: "QueuedTaskRecord") -> "dict[str, Any]":
        store = self._get_store()
        return {
            "args_json": store.serialize_json("args_json", list(record.args)),
            "completed_at": self._serialize_datetime(record.completed_at),
            "created_at": self._serialize_datetime(record.created_at),
            "error": record.error,
            "execution_backend": record.execution_backend,
            "execution_profile": record.execution_profile,
            "execution_ref": record.execution_ref,
            "heartbeat_at": self._serialize_datetime(record.heartbeat_at),
            "id": str(record.id),
            "kwargs_json": store.serialize_json("kwargs_json", record.kwargs),
            "max_retries": record.max_retries,
            "metadata_json": store.serialize_json("metadata_json", record.metadata),
            "priority": record.priority,
            "queue": record.queue,
            "result_json": store.serialize_json("result_json", record.result),
            "retry_count": record.retry_count,
            "scheduled_at": self._serialize_datetime(record.scheduled_at),
            "started_at": self._serialize_datetime(record.started_at),
            "status": record.status,
            "task_key": record.key,
            "task_name": record.task_name,
        }

    def _record_from_row(self, row: "dict[str, Any]") -> "QueuedTaskRecord":
        store = self._get_store()
        args = _coerce_record_args(store.deserialize_json("args_json", row["args_json"]))
        kwargs = _coerce_record_mapping("kwargs_json", store.deserialize_json("kwargs_json", row["kwargs_json"]))
        metadata = _coerce_record_mapping(
            "metadata_json", store.deserialize_json("metadata_json", row["metadata_json"])
        )
        return QueuedTaskRecord(
            id=UUID(str(row["id"])),
            task_name=str(row["task_name"]),
            args=args,
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
            result=store.deserialize_json("result_json", row["result_json"]),
            error=cast("str | None", row["error"]),
            key=cast("str | None", row["task_key"]),
            metadata=metadata,
        )


class _ManagedAsyncDriver:
    """Expose sync SQLSpec driver methods through one session-bound executor."""

    __slots__ = ("_driver", "_executor", "_skip_explicit_begin", "_transaction_finalized")

    def __init__(
        self, driver: "object", executor: "ThreadPoolExecutor", *, skip_explicit_begin: "bool" = False
    ) -> "None":
        self._driver = cast("Any", driver)
        self._executor = executor
        self._skip_explicit_begin = skip_explicit_begin
        self._transaction_finalized = False

    @property
    def transaction_finalized(self) -> "bool":
        """Whether the session explicitly committed or rolled back."""
        return self._transaction_finalized

    async def begin(self) -> "Any":
        self._transaction_finalized = False
        if self._skip_explicit_begin:
            return None
        return await async_(self._driver.begin, executor=self._executor)()

    async def commit(self) -> "Any":
        result = await async_(self._driver.commit, executor=self._executor)()
        self._transaction_finalized = True
        return result

    async def rollback(self) -> "Any":
        result = await async_(self._driver.rollback, executor=self._executor)()
        self._transaction_finalized = True
        return result

    async def execute(self, statement: "Any", *parameters: "Any", **kwargs: "Any") -> "Any":
        return await async_(self._driver.execute, executor=self._executor)(statement, *parameters, **kwargs)

    async def execute_many(self, statement: "Any", parameters: "Sequence[dict[str, Any]]") -> "Any":
        return await async_(self._driver.execute_many, executor=self._executor)(statement, parameters)

    async def execute_script(self, statement: "str") -> "Any":
        return await async_(self._driver.execute_script, executor=self._executor)(statement)

    async def load_from_records(self, table_name: "str", records: "Sequence[dict[str, Any]]") -> "Any":
        return await async_(self._driver.load_from_records, executor=self._executor)(table_name, records)

    async def select(self, statement: "Any", *parameters: "Any", **kwargs: "Any") -> "list[Any]":
        return cast(
            "list[Any]", await async_(self._driver.select, executor=self._executor)(statement, *parameters, **kwargs)
        )

    async def select_one_or_none(self, statement: "Any", *parameters: "Any", **kwargs: "Any") -> "Any | None":
        return await async_(self._driver.select_one_or_none, executor=self._executor)(statement, *parameters, **kwargs)

    async def select_stream(self, statement: "Any", *, chunk_size: "int | None" = None) -> "AsyncIterator[Any]":
        del chunk_size
        for row in await self.select(statement):
            yield row

    def __getattr__(self, name: "str") -> "Any":
        attr = getattr(self._driver, name)
        if callable(attr):
            return async_(attr, executor=self._executor)
        return attr


@asynccontextmanager
async def _bridge_session(
    sqlspec_manager: "SQLSpecManager",
    sqlspec_config: "SQLSpecSessionConfig",
    *,
    skip_explicit_begin: "bool" = False,
    skip_cleanup_rollback: "bool" = False,
) -> "AsyncIterator[SQLSpecDriver]":
    """Yield a SQLSpec driver regardless of sync/async config.

    Sync SQLSpec configs (``SqliteConfig``, ``DuckDBConfig``, ``MysqlConnectorSyncConfig``, etc.)
    return sync context managers and sync drivers. They are bridged with
    ``sqlspec.utils.sync_tools.async_`` so blocking operations use SQLSpec's
    managed executor and honor ``SQLSPEC_ASYNC_THREAD_LIMIT``.

    Yields:
        A SQLSpec driver whose methods can be awaited regardless of whether the
        underlying config is sync or async.
    """
    session_cm = sqlspec_manager.provide_session(sqlspec_config)
    if sqlspec_config.is_async:
        async with session_cm as driver:
            yield cast("SQLSpecDriver", driver)
    else:
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="litestar-queues-sqlspec-sync")
        driver = await async_(session_cm.__enter__, executor=executor)()
        managed_driver = _ManagedAsyncDriver(driver, executor, skip_explicit_begin=skip_explicit_begin)
        try:
            yield managed_driver
        except BaseException as exc:
            if not managed_driver.transaction_finalized and not skip_cleanup_rollback:
                await _rollback_sync_session(driver, executor=executor)
            if not await async_(session_cm.__exit__, executor=executor)(type(exc), exc, exc.__traceback__):
                raise
        else:
            if not managed_driver.transaction_finalized and not skip_cleanup_rollback:
                await _rollback_sync_session(driver, executor=executor)
            await async_(session_cm.__exit__, executor=executor)(None, None, None)
        finally:
            executor.shutdown(wait=False)


async def _rollback_sync_session(driver: "object", *, executor: "ThreadPoolExecutor | None" = None) -> "None":
    """Best-effort cleanup for sync SQLSpec sessions before pool return."""
    rollback = getattr(driver, "rollback", None)
    if callable(rollback):
        with suppress(Exception):
            await async_(rollback, executor=executor)()


async def _select_stream(
    driver: "SQLSpecDriver", statement: "Any", *parameters: "Any", chunk_size: "int | None" = None
) -> "AsyncIterator[Any]":
    """Yield rows from SQLSpec async and sync stream implementations.

    Yields:
        Rows returned by the SQLSpec statement.
    """
    if isinstance(driver, _ManagedAsyncDriver):
        rows = await driver.select(statement, *parameters)
        for row in rows:
            yield row
    else:
        if chunk_size is None:
            stream = driver.select_stream(statement, *parameters)
        else:
            stream = driver.select_stream(statement, *parameters, chunk_size=chunk_size)
        if isawaitable(stream):
            stream = await stream
        async for row in stream:
            yield row


def _utc_now() -> "datetime":
    return datetime.now(timezone.utc)


def _rows_affected(result: "Any", adapter_name: "str | None" = None) -> "int":
    """Return the reported affected-row count.

    Normalized to ``-1`` (the existing "unknown, verify" sentinel) when
    ``adapter_name`` is one of :data:`_UNRELIABLE_ROWCOUNT_ADAPTERS`, whose
    driver can report a genuine ``0`` and an unparsable result identically.
    """
    rows_affected = int(getattr(result, "rows_affected", 0) or 0)
    if rows_affected == 0 and adapter_name in _UNRELIABLE_ROWCOUNT_ADAPTERS:
        return -1
    return rows_affected


def _is_unique_violation(exc: "BaseException") -> "bool":
    current: "BaseException | None" = exc
    while current is not None:
        sqlstate = getattr(current, "sqlstate", None) or getattr(current, "pgcode", None)
        if sqlstate in {"23000", "23505"}:
            return True
        message = str(current).lower()
        if any(
            token in message
            for token in ("duplicate entry", "duplicate key", "unique constraint", "unique violation", "ora-00001")
        ):
            return True
        current = current.__cause__ or current.__context__
    return False


def _is_serialization_conflict(exc: "BaseException") -> "bool":
    current: "BaseException | None" = exc
    while current is not None:
        if isinstance(current, SerializationConflictError):
            return True
        sqlstate = getattr(current, "sqlstate", None) or getattr(current, "pgcode", None)
        if sqlstate == "40001":
            return True
        message = str(current).lower()
        if "restart transaction" in message or "writetooold" in message or "serialization" in message:
            return True
        current = current.__cause__ or current.__context__
    return False


def _coerce_record_args(value: "Any") -> "tuple[Any, ...]":
    if isinstance(value, (list, tuple)):
        return tuple(value)
    msg = f"SQLSpec queue backend expected args_json to decode to a JSON array, got {type(value).__name__}"
    raise ValueError(msg)


def _coerce_record_mapping(canonical: "str", value: "Any") -> "dict[str, Any]":
    if isinstance(value, dict):
        return value
    msg = f"SQLSpec queue backend expected {canonical} to decode to a JSON object, got {type(value).__name__}"
    raise ValueError(msg)


@overload
def _serialize_datetime(value: "datetime") -> "datetime": ...


@overload
def _serialize_datetime(value: "None") -> "None": ...


def _serialize_datetime(value: "datetime | None") -> "datetime | None":
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _deserialize_datetime(value: "Any") -> "datetime | None":
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


def _coerce_status(value: "Any") -> "TaskStatus":
    status = str(value)
    if status not in {"cancelled", "completed", "failed", "pending", "running", "scheduled"}:
        msg = f"Unknown queued task status from SQLSpec queue backend: {status!r}"
        raise ValueError(msg)
    return cast("TaskStatus", status)


def _tombstone_from_row(row: "dict[str, Any]") -> "UniquenessTombstone":
    return UniquenessTombstone(
        key=str(row["identity_key"]),
        task_id=UUID(str(row["task_id"])),
        task_name=str(row["task_name"]),
        created_at=_deserialize_datetime(row["created_at"]) or _utc_now(),
    )


def _queue_extension_settings(sqlspec_config: "SQLSpecStoreConfig | None") -> "dict[str, Any]":
    if sqlspec_config is None:
        return {}
    extension_config = sqlspec_config.extension_config or {}
    return dict(extension_config.get(QUEUE_EXTENSION_NAME, {}) or {})


async def _create_schema_statements(store: "SQLSpecQueueStore", driver: "SQLSpecDriver") -> "list[str]":
    create_for_driver = getattr(store, "create_statements_for_driver", None)
    if callable(create_for_driver):
        result = create_for_driver(driver)
        if isawaitable(result):
            return cast("list[str]", await result)
        return cast("list[str]", result)
    return store.create_statements()


def _resolve_notify_transport(
    *,
    explicit_transport: "str | None",
    event_backend: "str | None",
    sqlspec_config: "SQLSpecConfig",
    queue_settings: "dict[str, Any]",
    events_settings: "dict[str, Any]",
) -> "str":
    """Resolve the effective wakeup transport from config precedence.

    Explicit ``queue_backend_config`` selections win over ``extension_config``
    defaults, which in turn win over the per-adapter capability gate.

    Returns:
        A canonical wakeup transport name.
    """
    if explicit_transport is not None:
        return explicit_transport
    configured_transport = _setting(queue_settings, "notify_transport")
    if configured_transport is not None:
        return _validate_queue_notify_transport(str(configured_transport))
    configured_backend = event_backend or _setting(queue_settings, "event_backend") or events_settings.get("backend")
    if configured_backend is not None:
        return _validate_queue_notify_transport(str(configured_backend))
    return _adapter_notify_transport(resolve_adapter_name(sqlspec_config))


def resolve_events_migration_backend(
    backend_config: "SQLSpecBackendConfig", sqlspec_config: "SQLSpecConfig"
) -> "str | None":
    """Return the durable events-table transport to register for migrations.

    Mirrors the runtime notification decision so a capability-native adapter
    provisions its events queue table through SQLSpec migrations with zero
    configuration. Returns the transport name (``notify_queue`` / ``poll_queue``)
    when the events queue table must exist, or ``None`` when notifications are
    opted out, transient (``notify``), Oracle AQ (provisioned separately), or the
    adapter polls.

    Returns:
        The durable events-table transport name, or ``None``.
    """
    if backend_config.notifications is False or backend_config.event_channel is not None:
        return None
    queue_settings = _queue_extension_settings(sqlspec_config)
    if (
        backend_config.notifications is None
        and "notifications" in queue_settings
        and not queue_settings["notifications"]
    ):
        return None
    transport = _resolve_notify_transport(
        explicit_transport=backend_config.notify_transport,
        event_backend=backend_config.event_backend,
        sqlspec_config=sqlspec_config,
        queue_settings=queue_settings,
        events_settings=_events_extension_settings(sqlspec_config),
    )
    return transport if transport in _EVENTS_TABLE_BACKENDS else None


def _events_queue_create_statements(sqlspec_config: "SQLSpecConfig") -> "list[str]":
    """Return the DDL provisioning the durable events queue table for this adapter.

    Resolves the adapter's :class:`~sqlspec.extensions.events.BaseEventQueueStore`
    the same way SQLSpec's events extension migration does and returns its
    dialect-correct ``CREATE TABLE``/``CREATE INDEX`` statements. Emitting these
    alongside the queue table makes the durable ``notify_queue`` / ``poll_queue``
    wakeup transports work on a fresh database with no separate migration step.

    Returns:
        The ``CREATE`` statements for the events queue table and its index.
    """
    from sqlspec.utils.module_loader import import_string

    config_class = type(sqlspec_config)
    adapter_name = config_class.__module__.split(".")[2]
    store_class_name = config_class.__name__.replace("Config", "EventQueueStore")
    store_class = import_string(f"sqlspec.adapters.{adapter_name}.events.store.{store_class_name}")
    return cast("list[str]", store_class(sqlspec_config).create_statements())


def _events_extension_settings(sqlspec_config: "SQLSpecStoreConfig | None") -> "dict[str, Any]":
    if sqlspec_config is None:
        return {}
    extension_config = sqlspec_config.extension_config or {}
    return dict(extension_config.get(_EVENT_EXTENSION_NAME, {}) or {})


async def _invoke_event_channel_method(event_channel: "Any", method_name: "str", *args: "Any") -> "Any":
    """Invoke a SQLSpec sync or async event-channel method without blocking the loop.

    Returns:
        The event-channel method result.
    """
    method = getattr(event_channel, method_name)
    if iscoroutinefunction(method):
        return await method(*args)
    result = await async_(method)(*args)
    if isawaitable(result):
        return await result
    return result


async def _next_event(stream: "Any") -> "Any":
    """Read from a SQLSpec sync or async event iterator.

    Returns:
        The next event message.
    """
    if hasattr(stream, "__anext__"):
        return await anext(stream)
    has_event, event = await async_(_next_sync_event)(stream)
    if not has_event:
        raise StopAsyncIteration
    return event


def _next_sync_event(stream: "Iterator[Any]") -> "tuple[bool, Any]":
    """Read one sync event without leaking ``StopIteration`` through a future.

    Returns:
        A pair indicating whether an event was read and the event value.
    """
    try:
        return True, next(stream)
    except StopIteration:
        return False, None


def _setting(queue_settings: "dict[str, Any]", *names: "str") -> "Any":
    for name in names:
        if name in queue_settings:
            return queue_settings[name]
    return None


def _normalize_notification_channel(channel: "str") -> "str":
    try:
        return str(normalize_event_channel_name(channel))
    except Exception as exc:
        msg = f"Invalid SQLSpec queue notification channel: {channel!r}"
        raise QueueConfigurationError(msg) from exc

"""Server-local ephemeral SQLite queue backend.

Every public async method runs one bounded synchronous helper through
``asyncio.to_thread``. Each helper opens its own connection, performs one
operation inside ``BEGIN IMMEDIATE`` when it mutates state, and closes before
returning. Nothing is cached across event loops, processes, or threads, and no
cross-process signaling primitive exists: a committed row is the only
communication.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import UUID

from litestar_queues.backends._notification_wait import PendingNativeRead
from litestar_queues.backends.base import (
    STALE_HEARTBEAT_ERROR,
    BaseQueueBackend,
    attempts_consumed,
    interruption_count,
    is_external_dispatch_reservation,
    record_matches_filters,
    retry_schedule,
    stale_requeue_error,
    stale_requeue_priority,
)
from litestar_queues.backends.ephemeral.codec import record_from_payload, record_to_payload
from litestar_queues.backends.ephemeral.event_log import EphemeralQueueEventLog
from litestar_queues.backends.ephemeral.schema import (
    SCHEMA_VERSION,
    connect,
    is_private_directory,
    read_environment,
    read_runtime,
    sqlite_errors,
)
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.models import (
    HeartbeatTouchResult,
    QueueBackendCapabilities,
    QueuedTaskRecord,
    QueueStatistics,
    StaleTaskRecoveryResult,
    TaskReservation,
)

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Mapping, Sequence

    from litestar_queues.config import QueueConfig
    from litestar_queues.events import EventHistoryConfig, QueueEventLog
    from litestar_queues.models import HeartbeatTouch, TaskRequest

__all__ = ("EphemeralQueueBackend",)

T = TypeVar("T")

_ACTIVE_STATUSES = ("pending", "scheduled")
_TERMINAL_STATUSES = ("completed", "failed", "cancelled", "expired")
_MIN_POLL = 0.01
_MAX_POLL = 0.1
_NOT_OPEN_ERROR = (
    "The ephemeral queue backend is only available inside a Litestar CLI server lifespan. "
    'Start the application with "litestar run" or select another queue backend.'
)
_MISMATCH_ERROR = (
    "The ephemeral queue database does not belong to this server invocation. "
    'Start the application with "litestar run" or select another queue backend.'
)


def _utc_now() -> "datetime":
    return datetime.now(timezone.utc)


def _iso(value: "datetime | None") -> "str | None":
    return None if value is None else value.astimezone(timezone.utc).isoformat()


def _columns(record: "QueuedTaskRecord") -> "tuple[Any, ...]":
    return (
        str(record.id),
        record.task_name,
        record.queue,
        record.execution_backend,
        record.worker_id,
        record.status,
        record.priority,
        record.retry_count,
        _iso(record.scheduled_at),
        _iso(record.expires_at),
        _iso(record.created_at),
        _iso(record.queued_at),
        _iso(record.completed_at),
        _iso(record.heartbeat_at),
        record.key,
        record_to_payload(record),
    )


_INSERT = """
INSERT INTO queue_task (
    id, task_name, queue, execution_backend, worker_id, status, priority, retry_count,
    scheduled_at, expires_at, created_at, queued_at, completed_at, heartbeat_at, task_key, payload
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_UPDATE = """
UPDATE queue_task SET
    task_name = ?, queue = ?, execution_backend = ?, worker_id = ?, status = ?, priority = ?, retry_count = ?,
    scheduled_at = ?, expires_at = ?, created_at = ?, queued_at = ?, completed_at = ?, heartbeat_at = ?, task_key = ?, payload = ?
WHERE id = ?
"""


def _write(connection: "sqlite3.Connection", record: "QueuedTaskRecord") -> "None":
    values = _columns(record)
    connection.execute(_UPDATE, (*values[1:], values[0]))


def _insert(connection: "sqlite3.Connection", record: "QueuedTaskRecord") -> "None":
    connection.execute(_INSERT, _columns(record))


def _decode(row: "sqlite3.Row") -> "QueuedTaskRecord":
    return record_from_payload(row["payload"])


def _decode_all(rows: "Sequence[sqlite3.Row]") -> "list[QueuedTaskRecord]":
    return [_decode(row) for row in rows]


def _belongs_to_invocation(path: "str", nonce: "str") -> "bool":
    database = Path(path)
    if not is_private_directory(database.parent) or database.is_symlink():
        return False
    runtime = read_runtime(path)
    return runtime is not None and runtime[0] == SCHEMA_VERSION and runtime[1] == nonce


class EphemeralQueueBackend(BaseQueueBackend):
    """Private-file SQLite queue backend owned by one Litestar server invocation.

    This backend is ephemeral, not durable. The database lives in a private
    temporary directory created and removed by the server lifespan, so queued
    work does not survive the server that produced it.

    Worker wakeups are discovered by polling. There is no cross-process
    signalling primitive: a producer commits a row and a waiter notices it
    through short bounded reads, then the worker performs the authoritative
    transactional claim.
    """

    __slots__ = ("_event_log", "_notification_event", "_path", "_pending_read")

    def __init__(self, config: "QueueConfig | None" = None) -> "None":
        """Initialize without resolving the database path.

        The path is resolved in :meth:`open`, because the top-level application
        constructs its backend before the server lifespan creates the database.
        """
        super().__init__(config=config)
        self._path: "str | None" = None
        self._notification_event = asyncio.Event()
        self._pending_read = PendingNativeRead()
        self._event_log: "QueueEventLog | None" = None

    @property
    def capabilities(self) -> "QueueBackendCapabilities":
        """Backend behavior capabilities."""
        return QueueBackendCapabilities(
            supports_worker_wakeups=True, wakeup_backend="sqlite-poll", wakeups_durable=False, supports_maintenance=True
        )

    @property
    def path(self) -> "str":
        """Absolute path of the active ephemeral database.

        Returns:
            The resolved database path.

        Raises:
            QueueConfigurationError: If the backend is not open.
        """
        if self._path is None:
            raise QueueConfigurationError(_NOT_OPEN_ERROR)
        return self._path

    async def open(self) -> "bool":
        """Resolve and validate the server-owned database.

        Returns:
            True when the database is ready.

        Raises:
            QueueConfigurationError: If no server lifespan created the database,
                or the location, schema, or nonce does not match this invocation.
        """
        resolved = read_environment(self.config.names if self.config is not None else None)
        if resolved is None:
            raise QueueConfigurationError(_NOT_OPEN_ERROR)
        path, nonce = resolved
        if not await asyncio.to_thread(_belongs_to_invocation, path, nonce):
            raise QueueConfigurationError(_MISMATCH_ERROR)
        self._path = path
        return True

    async def close(self) -> "None":
        """Cancel any retained notification wait."""
        await self._pending_read.aclose()

    def get_event_log(self, config: "EventHistoryConfig") -> "QueueEventLog | None":
        """Return the file-backed queue event history."""
        if self._event_log is None:
            self._event_log = EphemeralQueueEventLog(config, backend=self)  # type: ignore[assignment]
        return self._event_log

    async def _run(self, operation: "Callable[[sqlite3.Connection], T]") -> "T":
        """Run one read operation on its own connection in a worker thread.

        Returns:
            Whatever ``operation`` returns.
        """
        return await asyncio.to_thread(self._call, operation)

    def _call(self, operation: "Callable[[sqlite3.Connection], T]") -> "T":
        with sqlite_errors():
            connection = connect(self.path)
            try:
                return operation(connection)
            finally:
                connection.close()

    def _atomic(self, operation: "Callable[[sqlite3.Connection], T]") -> "T":
        with sqlite_errors():
            connection = connect(self.path)
            try:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    result = operation(connection)
                except BaseException:
                    connection.execute("ROLLBACK")
                    raise
                connection.execute("COMMIT")
                return result
            finally:
                connection.close()

    async def _transaction(self, operation: "Callable[[sqlite3.Connection], T]") -> "T":
        """Run one read-modify-write operation inside ``BEGIN IMMEDIATE``.

        Returns:
            Whatever ``operation`` returns.
        """
        return await asyncio.to_thread(self._atomic, operation)

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
        expires_at: "datetime | None" = None,
        key: "str | None" = None,
        execution_backend: "str" = "local",
        execution_profile: "str | None" = None,
        metadata: "dict[str, Any] | None" = None,
        id: "UUID | None" = None,  # noqa: A002
    ) -> "QueuedTaskRecord":
        """Persist one task, returning any active record already holding ``key``.

        Returns:
            The stored or pre-existing queue record.
        """
        now = _utc_now()
        record = QueuedTaskRecord(
            task_name=task_name,
            args=args,
            kwargs=dict(kwargs or {}),
            queue=queue,
            execution_backend=execution_backend,
            execution_profile=execution_profile,
            status="scheduled" if scheduled_at is not None and scheduled_at > _utc_now() else "pending",
            priority=priority,
            max_retries=max_retries,
            scheduled_at=scheduled_at,
            expires_at=expires_at,
            key=key,
            metadata=dict(metadata or {}),
            created_at=now,
            queued_at=now,
        )
        if id is not None:
            record.id = id

        def operation(connection: "sqlite3.Connection") -> "QueuedTaskRecord":
            if key is not None:
                existing = _active_by_key(connection, key)
                if existing is not None:
                    return existing
            _insert(connection, record)
            return record

        stored = await self._transaction(operation)
        await self.notify_new_task(stored)
        return stored

    async def enqueue_many(self, requests: "Sequence[TaskRequest]") -> "list[QueuedTaskRecord]":
        """Persist several tasks in one transaction.

        Returns:
            Queue task records in the same order as ``requests``.
        """
        if not requests:
            return []

        def operation(connection: "sqlite3.Connection") -> "list[QueuedTaskRecord]":
            now = _utc_now()
            records: "list[QueuedTaskRecord]" = []
            for request in requests:
                if request.key is not None:
                    existing = _active_by_key(connection, request.key)
                    if existing is not None:
                        records.append(existing)
                        continue
                record = QueuedTaskRecord(
                    task_name=request.task_name,
                    args=request.args,
                    kwargs=dict(request.kwargs or {}),
                    queue=request.queue,
                    execution_backend=request.execution_backend,
                    execution_profile=request.execution_profile,
                    status=(
                        "scheduled" if request.scheduled_at is not None and request.scheduled_at > now else "pending"
                    ),
                    priority=request.priority,
                    max_retries=request.max_retries,
                    scheduled_at=request.scheduled_at,
                    expires_at=request.expires_at,
                    key=request.key,
                    metadata=dict(request.metadata or {}),
                    created_at=now,
                    queued_at=now,
                )
                _insert(connection, record)
                records.append(record)
            return records

        records = await self._transaction(operation)
        await self.notify_new_tasks(records)
        self._record_enqueue_batch(len(requests))
        return records

    async def get_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        """Return one record by id.

        Returns:
            The record, or ``None`` when it does not exist.
        """

        def operation(connection: "sqlite3.Connection") -> "QueuedTaskRecord | None":
            row = connection.execute("SELECT payload FROM queue_task WHERE id = ?", (str(task_id),)).fetchone()
            return None if row is None else _decode(row)

        return await self._run(operation)

    async def get_tasks(self, task_ids: "Sequence[UUID]") -> "list[QueuedTaskRecord]":
        records = await asyncio.gather(*(self.get_task(task_id) for task_id in task_ids))
        return [record for record in records if record is not None]

    async def notify_worker_control(self, worker_id: "str | None") -> "None":
        """Rely on durable polling for cross-process cancellation.

        A process-local signal would only ever reach the emitting process, and
        the owning worker usually runs in another one.
        """

    async def wait_for_worker_control(self, *, worker_id: "str", timeout: "float | None" = None) -> "bool":
        """Rely on durable polling for cross-process cancellation.

        Returns:
            Always False: this backend has no cross-process control transport.
        """
        return await super().wait_for_worker_control(worker_id=worker_id, timeout=timeout)

    async def assign_worker(
        self, task_id: "UUID", *, worker_id: "str", expected_retry_count: "int"
    ) -> "QueuedTaskRecord | None":
        def operation(connection: "sqlite3.Connection") -> "QueuedTaskRecord | None":
            row = connection.execute("SELECT payload FROM queue_task WHERE id = ?", (str(task_id),)).fetchone()
            if row is None:
                return None
            record = _decode(row)
            if record.status != "running" or record.retry_count != expected_retry_count:
                return None
            record.worker_id = worker_id
            _write(connection, record)
            return record

        return await self._transaction(operation)

    async def get_task_by_key(self, key: "str") -> "QueuedTaskRecord | None":
        """Return the newest record stored under ``key``.

        Returns:
            The record, or ``None`` when no record holds the key.
        """

        def operation(connection: "sqlite3.Connection") -> "QueuedTaskRecord | None":
            row = connection.execute(
                "SELECT payload FROM queue_task WHERE task_key = ? ORDER BY created_at DESC, id DESC LIMIT 1", (key,)
            ).fetchone()
            return None if row is None else _decode(row)

        return await self._run(operation)

    async def list_pending(
        self, *, limit: "int" = 1, queue: "str | None" = None, execution_backend: "str | None" = None
    ) -> "list[QueuedTaskRecord]":
        """Return due pending/scheduled records in claim order.

        Returns:
            Up to ``limit`` due records.
        """

        def operation(connection: "sqlite3.Connection") -> "list[QueuedTaskRecord]":
            now = _iso(_utc_now())
            sql = (
                "SELECT payload FROM queue_task WHERE status IN (?, ?) "
                "AND (scheduled_at IS NULL OR scheduled_at <= ?) AND (expires_at IS NULL OR expires_at > ?)"
            )
            values: "list[Any]" = [*_ACTIVE_STATUSES, now, now]
            if queue is not None:
                sql += " AND queue = ?"
                values.append(queue)
            if execution_backend is not None:
                sql += " AND execution_backend = ?"
                values.append(execution_backend)
            sql += " ORDER BY priority DESC, queued_at ASC, created_at ASC, id ASC LIMIT ?"
            values.append(limit)
            return [
                record
                for record in _decode_all(connection.execute(sql, values).fetchall())
                if not is_external_dispatch_reservation(record.execution_ref)
            ]

        return await self._run(operation)

    async def claim_task(
        self, task_id: "UUID", *, expected_retry_count: "int | None" = None, expected_execution_ref: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        claimed, _ = await self.claim_task_with_expired(
            task_id, expected_retry_count=expected_retry_count, expected_execution_ref=expected_execution_ref
        )
        return claimed

    async def claim_task_with_expired(
        self, task_id: "UUID", *, expected_retry_count: "int | None" = None, expected_execution_ref: "str | None" = None
    ) -> "tuple[QueuedTaskRecord | None, QueuedTaskRecord | None]":
        """Claim one task and return an expiry transitioned in the transaction."""

        def operation(connection: "sqlite3.Connection") -> "tuple[QueuedTaskRecord | None, QueuedTaskRecord | None]":
            row = connection.execute("SELECT payload FROM queue_task WHERE id = ?", (str(task_id),)).fetchone()
            if row is None:
                return None, None
            record = _decode(row)
            if (
                record.status not in _ACTIVE_STATUSES
                or not record.is_due
                or is_external_dispatch_reservation(record.execution_ref)
                or (expected_retry_count is not None and record.retry_count != expected_retry_count)
                or (expected_execution_ref is not None and record.execution_ref != expected_execution_ref)
            ):
                return None, None
            now = _utc_now()
            if record.execution_ref is None and record.expires_at is not None and record.expires_at <= now:
                _expire_record(record, now)
                _write(connection, record)
                return None, record
            record.status = "running"
            record.started_at = now
            record.heartbeat_at = now
            _write(connection, record)
            return record, None

        return await self._transaction(operation)

    async def claim_many(
        self,
        *,
        limit: "int",
        queues: "tuple[str, ...]" = (),
        execution_backend: "str | None" = None,
        queue_limits: "Mapping[str, int] | None" = None,
    ) -> "list[QueuedTaskRecord]":
        """Claim up to ``limit`` due records in one transaction.

        Returns:
            Claimed task records in claim order.
        """
        claimed, _ = await self.claim_many_with_expired(
            limit=limit, queues=queues, execution_backend=execution_backend, queue_limits=queue_limits
        )
        return claimed

    async def claim_many_with_expired(
        self,
        *,
        limit: "int",
        queues: "tuple[str, ...]" = (),
        execution_backend: "str | None" = None,
        queue_limits: "Mapping[str, int] | None" = None,
    ) -> "tuple[list[QueuedTaskRecord], list[QueuedTaskRecord]]":
        """Claim records and return overdue records expired in the transaction."""
        if queue_limits is not None:
            return await super().claim_many_with_expired(
                limit=limit, queues=queues, execution_backend=execution_backend, queue_limits=queue_limits
            )
        if limit <= 0:
            return [], []

        def operation(connection: "sqlite3.Connection") -> "tuple[list[QueuedTaskRecord], list[QueuedTaskRecord]]":
            now = _utc_now()
            expired_rows = connection.execute(
                "SELECT payload FROM queue_task WHERE status IN (?, ?) AND expires_at IS NOT NULL AND expires_at <= ?",
                (*_ACTIVE_STATUSES, _iso(now)),
            ).fetchall()
            expired = [
                record
                for record in _decode_all(expired_rows)
                if not is_external_dispatch_reservation(record.execution_ref)
                and (not queues or record.queue in queues)
                and (execution_backend is None or record.execution_backend == execution_backend)
            ]
            for record in expired:
                _expire_record(record, now)
                _write(connection, record)
            sql = (
                "SELECT payload FROM queue_task WHERE status IN (?, ?) "
                "AND (scheduled_at IS NULL OR scheduled_at <= ?) AND (expires_at IS NULL OR expires_at > ?)"
            )
            values: "list[Any]" = [*_ACTIVE_STATUSES, _iso(now), _iso(now)]
            if queues:
                sql += f" AND queue IN ({','.join('?' * len(queues))})"
                values.extend(queues)
            if execution_backend is not None:
                sql += " AND execution_backend = ?"
                values.append(execution_backend)
            sql += " ORDER BY priority DESC, queued_at ASC, created_at ASC, id ASC LIMIT ?"
            values.append(limit)
            claimed: "list[QueuedTaskRecord]" = []
            for row in connection.execute(sql, values).fetchall():
                record = _decode(row)
                if is_external_dispatch_reservation(record.execution_ref):
                    continue
                record.status = "running"
                record.started_at = now
                record.heartbeat_at = now
                _write(connection, record)
                claimed.append(record)
            return claimed, expired

        return await self._transaction(operation)

    async def expire_overdue(self, *, limit: "int | None" = None) -> "list[QueuedTaskRecord]":
        """Transition overdue pending or scheduled records to ``expired``.

        Returns:
            Records transitioned during this call.
        """

        def operation(connection: "sqlite3.Connection") -> "list[QueuedTaskRecord]":
            now = _utc_now()
            sql = (
                "SELECT payload FROM queue_task WHERE status IN (?, ?) "
                "AND expires_at IS NOT NULL AND expires_at <= ? ORDER BY expires_at ASC, created_at ASC, id ASC"
            )
            values: "list[Any]" = [*_ACTIVE_STATUSES, _iso(now)]
            if limit is not None:
                sql += " LIMIT ?"
                values.append(limit)
            expired = [
                record
                for record in _decode_all(connection.execute(sql, values).fetchall())
                if record.execution_ref is None
            ]
            for record in expired:
                _expire_record(record, now)
                _write(connection, record)
            return expired

        return await self._transaction(operation)

    async def reserve_external_dispatch(
        self,
        task_id: "UUID",
        execution_backend: "str",
        reservation_ref: "str",
        *,
        execution_profile: "str | None" = None,
        expected_retry_count: "int | None" = None,
    ) -> "QueuedTaskRecord | None":
        def operation(connection: "sqlite3.Connection") -> "QueuedTaskRecord | None":
            row = connection.execute("SELECT payload FROM queue_task WHERE id = ?", (str(task_id),)).fetchone()
            if row is None:
                return None
            record = _decode(row)
            now = _utc_now()
            if (
                record.status not in _ACTIVE_STATUSES
                or not record.is_due
                or record.execution_ref is not None
                or (expected_retry_count is not None and record.retry_count != expected_retry_count)
            ):
                return None
            if record.expires_at is not None and record.expires_at <= now:
                _expire_record(record, now)
                _write(connection, record)
                return None
            record.execution_backend = execution_backend
            record.execution_profile = execution_profile
            record.execution_ref = reservation_ref
            _write(connection, record)
            return record

        return await self._transaction(operation)

    async def clear_execution_ref(
        self, task_id: "UUID", expected_retry_count: "int", expected_execution_ref: "str"
    ) -> "QueuedTaskRecord | None":
        def operation(connection: "sqlite3.Connection") -> "QueuedTaskRecord | None":
            row = connection.execute("SELECT payload FROM queue_task WHERE id = ?", (str(task_id),)).fetchone()
            if row is None:
                return None
            record = _decode(row)
            if (
                record.status not in _ACTIVE_STATUSES
                or record.retry_count != expected_retry_count
                or record.execution_ref != expected_execution_ref
            ):
                return None
            record.execution_ref = None
            _write(connection, record)
            return record

        record = await self._transaction(operation)
        if record is not None:
            await self.notify_new_task(record)
        return record

    async def replace_execution_ref(
        self, task_id: "UUID", expected_retry_count: "int", expected_execution_ref: "str", execution_ref: "str"
    ) -> "QueuedTaskRecord | None":
        def operation(connection: "sqlite3.Connection") -> "QueuedTaskRecord | None":
            row = connection.execute("SELECT payload FROM queue_task WHERE id = ?", (str(task_id),)).fetchone()
            if row is None:
                return None
            record = _decode(row)
            if (
                record.status not in _ACTIVE_STATUSES
                or record.retry_count != expected_retry_count
                or record.execution_ref != expected_execution_ref
            ):
                return None
            record.execution_ref = execution_ref
            _write(connection, record)
            return record

        return await self._transaction(operation)

    async def release_external_dispatch(
        self,
        task_id: "UUID",
        reservation_ref: "str",
        execution_backend: "str",
        *,
        execution_profile: "str | None" = None,
    ) -> "QueuedTaskRecord | None":
        def operation(connection: "sqlite3.Connection") -> "QueuedTaskRecord | None":
            row = connection.execute("SELECT payload FROM queue_task WHERE id = ?", (str(task_id),)).fetchone()
            if row is None:
                return None
            record = _decode(row)
            if record.execution_ref != reservation_ref:
                return None
            record.execution_backend = execution_backend
            record.execution_profile = execution_profile
            record.execution_ref = None
            _write(connection, record)
            return record

        record = await self._transaction(operation)
        if record is not None:
            await self.notify_new_task(record)
        return record

    async def finalize_external_dispatch(
        self,
        task_id: "UUID",
        reservation_ref: "str",
        execution_backend: "str",
        execution_ref: "str",
        *,
        execution_profile: "str | None" = None,
    ) -> "QueuedTaskRecord | None":
        def operation(connection: "sqlite3.Connection") -> "QueuedTaskRecord | None":
            row = connection.execute("SELECT payload FROM queue_task WHERE id = ?", (str(task_id),)).fetchone()
            if row is None:
                return None
            record = _decode(row)
            if record.execution_ref != reservation_ref or record.status not in _ACTIVE_STATUSES:
                return None
            record.execution_backend = execution_backend
            record.execution_profile = execution_profile
            record.execution_ref = execution_ref
            _write(connection, record)
            return record

        return await self._transaction(operation)

    async def complete_task(
        self, task_id: "UUID", *, result: "Any" = None, expected_retry_count: "int | None" = None
    ) -> "QueuedTaskRecord | None":
        """Mark a running record completed under optional retry fencing.

        Returns:
            The completed record, or ``None`` when fencing rejected the update.
        """

        def operation(connection: "sqlite3.Connection") -> "QueuedTaskRecord | None":
            record = _locked_record(connection, task_id, expected_retry_count)
            if record is None:
                return None
            record.status = "completed"
            record.completed_at = _utc_now()
            record.heartbeat_at = None
            record.result = result
            record.error = None
            _write(connection, record)
            return record

        return await self._transaction(operation)

    async def fail_task(
        self,
        task_id: "UUID",
        error: "str",
        *,
        retry: "bool" = True,
        expected_retry_count: "int | None" = None,
        retry_at: "datetime | None" = None,
        queued_at: "datetime | None" = None,
    ) -> "QueuedTaskRecord | None":
        """Retry or fail a running record under optional retry fencing.

        Returns:
            The updated record, or ``None`` when fencing rejected the update.
        """

        def operation(connection: "sqlite3.Connection") -> "QueuedTaskRecord | None":
            record = _locked_record(connection, task_id, expected_retry_count)
            if record is None:
                return None
            record.error = error
            if retry and attempts_consumed(record) < record.max_retries:
                now = queued_at or _utc_now()
                record.retry_count += 1
                record.queued_at = now
                record.scheduled_at = retry_at
                record.status = "scheduled" if retry_at is not None and retry_at > now else "pending"
                record.started_at = None
                record.heartbeat_at = None
            else:
                record.status = "failed"
                record.completed_at = _utc_now()
                record.heartbeat_at = None
            _write(connection, record)
            return record

        return await self._transaction(operation)

    async def cancel_task(
        self, task_id: "UUID", *, include_running: "bool" = False, expected_retry_count: "int | None" = None
    ) -> "bool":
        """Cancel one record.

        Returns:
            True when the record moved to cancelled.
        """
        statuses = (*_ACTIVE_STATUSES, "running") if include_running else _ACTIVE_STATUSES

        def operation(connection: "sqlite3.Connection") -> "bool":
            row = connection.execute("SELECT payload FROM queue_task WHERE id = ?", (str(task_id),)).fetchone()
            if row is None:
                return False
            record = _decode(row)
            if record.status not in statuses or (
                expected_retry_count is not None and record.retry_count != expected_retry_count
            ):
                return False
            record.status = "cancelled"
            record.completed_at = _utc_now()
            record.heartbeat_at = None
            _write(connection, record)
            return True

        return await self._transaction(operation)

    async def interrupt_task(
        self, task_id: "UUID", *, expected_retry_count: "int", worker_id: "str", queued_at: "datetime"
    ) -> "QueuedTaskRecord | None":
        def operation(connection: "sqlite3.Connection") -> "QueuedTaskRecord | None":
            row = connection.execute("SELECT payload FROM queue_task WHERE id = ?", (str(task_id),)).fetchone()
            if row is None:
                return None
            record = _decode(row)
            if (
                record.status != "running"
                or record.retry_count != expected_retry_count
                or record.worker_id != worker_id
            ):
                return None
            record.status = "pending"
            record.queued_at = queued_at
            record.scheduled_at = None
            record.started_at = None
            record.heartbeat_at = None
            record.completed_at = None
            record.execution_ref = None
            record.worker_id = None
            record.metadata["interruptions"] = interruption_count(record) + 1
            record.retry_count += 1
            _write(connection, record)
            return record

        return await self._transaction(operation)

    async def cancel_tasks(
        self,
        *,
        task_name: "str | None" = None,
        queue: "str | None" = None,
        kwargs: "Mapping[str, Any] | None" = None,
        metadata: "Mapping[str, Any] | None" = None,
        include_running: "bool" = False,
    ) -> "int":
        """Cancel every record matching the given filters.

        Returns:
            The number of cancelled records.
        """
        statuses = (*_ACTIVE_STATUSES, "running") if include_running else _ACTIVE_STATUSES

        def operation(connection: "sqlite3.Connection") -> "int":
            placeholders = ",".join("?" * len(statuses))
            rows = connection.execute(
                f"SELECT payload FROM queue_task WHERE status IN ({placeholders})",  # noqa: S608 - placeholders only
                statuses,
            ).fetchall()
            cancelled = 0
            now = _utc_now()
            for row in rows:
                record = _decode(row)
                if not record_matches_filters(
                    record, task_name=task_name, queue=queue, kwargs=kwargs, metadata=metadata
                ):
                    continue
                record.status = "cancelled"
                record.completed_at = now
                record.heartbeat_at = None
                _write(connection, record)
                cancelled += 1
            return cancelled

        return await self._transaction(operation)

    async def touch_heartbeats(self, touches: "Sequence[HeartbeatTouch]") -> "HeartbeatTouchResult":
        """Refresh heartbeats for running records under retry fencing.

        Returns:
            The touched and missed task ids.
        """
        result = HeartbeatTouchResult()
        if not touches:
            return result

        def operation(connection: "sqlite3.Connection") -> "HeartbeatTouchResult":
            now = _utc_now()
            for touch in touches:
                row = connection.execute(
                    "SELECT payload FROM queue_task WHERE id = ?", (str(touch.task_id),)
                ).fetchone()
                if row is None:
                    result.missed_task_ids.add(touch.task_id)
                    continue
                record = _decode(row)
                if record.status != "running":
                    result.missed_task_ids.add(touch.task_id)
                    continue
                if touch.expected_retry_count is not None and record.retry_count != touch.expected_retry_count:
                    result.missed_task_ids.add(touch.task_id)
                    continue
                record.heartbeat_at = now
                if touch.metadata_patch:
                    record.metadata.update(touch.metadata_patch)
                _write(connection, record)
                result.touched_task_ids.add(touch.task_id)
            return result

        return await self._transaction(operation)

    async def null_heartbeats(self, task_ids: "list[UUID]", *, expected_retry_count: "int | None" = None) -> "None":
        """Clear heartbeats for the given records."""
        if not task_ids:
            return

        def operation(connection: "sqlite3.Connection") -> "None":
            for task_id in set(task_ids):
                row = connection.execute("SELECT payload FROM queue_task WHERE id = ?", (str(task_id),)).fetchone()
                if row is None:
                    continue
                record = _decode(row)
                if expected_retry_count is not None and record.retry_count != expected_retry_count:
                    continue
                record.heartbeat_at = None
                _write(connection, record)

        await self._transaction(operation)

    async def requeue_stale_running(
        self, *, stale_after: "timedelta", limit: "int | None" = None
    ) -> "StaleTaskRecoveryResult":
        """Requeue or fail running records whose heartbeat expired.

        Returns:
            The recovery counts and affected task ids.
        """

        policy = self._stale_requeue_priority_policy()

        def operation(connection: "sqlite3.Connection") -> "StaleTaskRecoveryResult":
            now = _utc_now()
            cutoff = now - stale_after
            result = StaleTaskRecoveryResult()
            rows = connection.execute("SELECT payload FROM queue_task WHERE status = 'running'").fetchall()
            candidates = [
                record for record in _decode_all(rows) if record.heartbeat_at is None or record.heartbeat_at < cutoff
            ]
            candidates.sort(key=_stale_sort_key)
            if limit is not None:
                candidates = candidates[:limit]
            for record in candidates:
                requeue_on_stale = record.metadata.get("requeue_on_stale", True) is not False
                if requeue_on_stale and attempts_consumed(record) < record.max_retries:
                    queued_at, retry_at = retry_schedule(record, now=now)
                    record.status = "scheduled" if retry_at is not None else "pending"
                    record.queued_at = queued_at
                    record.scheduled_at = retry_at
                    record.priority = stale_requeue_priority(record.priority, policy)
                    record.started_at = None
                    record.heartbeat_at = None
                    record.error = stale_requeue_error(record.error)
                    record.retry_count += 1
                    result.requeued += 1
                    _write(connection, record)
                    continue
                record.status = "failed"
                record.completed_at = now
                record.heartbeat_at = None
                record.error = STALE_HEARTBEAT_ERROR
                result.failed += 1
                result.failed_task_ids.append(record.id)
                if not requeue_on_stale:
                    result.handler_needed += 1
                    result.handler_needed_task_ids.append(record.id)
                _write(connection, record)
            return result

        return await self._transaction(operation)

    async def set_execution_ref(
        self, task_id: "UUID", execution_backend: "str", execution_ref: "str", *, execution_profile: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        """Record an external execution reference.

        Returns:
            The updated record, or ``None`` when it does not exist.
        """

        def operation(connection: "sqlite3.Connection") -> "QueuedTaskRecord | None":
            row = connection.execute("SELECT payload FROM queue_task WHERE id = ?", (str(task_id),)).fetchone()
            if row is None:
                return None
            record = _decode(row)
            record.execution_backend = execution_backend
            record.execution_profile = execution_profile
            record.execution_ref = execution_ref
            _write(connection, record)
            return record

        return await self._transaction(operation)

    async def set_execution_backend(
        self, task_id: "UUID", execution_backend: "str", *, execution_profile: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        """Reassign a record to another execution backend.

        Returns:
            The updated record, or ``None`` when it does not exist.
        """

        def operation(connection: "sqlite3.Connection") -> "QueuedTaskRecord | None":
            row = connection.execute("SELECT payload FROM queue_task WHERE id = ?", (str(task_id),)).fetchone()
            if row is None:
                return None
            record = _decode(row)
            record.execution_backend = execution_backend
            record.execution_profile = execution_profile
            record.execution_ref = None
            _write(connection, record)
            return record

        record = await self._transaction(operation)
        if record is not None:
            await self.notify_new_task(record)
        return record

    async def list_running_external(self, *, limit: "int | None" = None) -> "list[QueuedTaskRecord]":
        """Return non-terminal records holding an execution reference.

        Returns:
            Matching records ordered by start time.
        """

        def operation(connection: "sqlite3.Connection") -> "list[QueuedTaskRecord]":
            placeholders = ",".join("?" * len(_TERMINAL_STATUSES))
            rows = connection.execute(
                f"SELECT payload FROM queue_task WHERE status NOT IN ({placeholders})",  # noqa: S608 - placeholders only
                _TERMINAL_STATUSES,
            ).fetchall()
            records = [record for record in _decode_all(rows) if record.execution_ref is not None]
            records.sort(key=lambda record: (record.started_at or record.created_at, str(record.id)))
            return records[:limit] if limit is not None else records

        return await self._run(operation)

    async def get_statistics(self, *, queue: "str | None" = None) -> "QueueStatistics":
        """Return per-status record counts.

        Returns:
            The queue statistics snapshot.
        """

        def operation(connection: "sqlite3.Connection") -> "QueueStatistics":
            statistics = QueueStatistics()
            sql = "SELECT status, COUNT(*) AS total FROM queue_task"
            parameters: "tuple[str, ...]" = ()
            if queue is not None:
                sql += " WHERE queue = ?"
                parameters = (queue,)
            sql += " GROUP BY status"
            for row in connection.execute(sql, parameters):
                status = str(row["status"])
                if hasattr(statistics, status):
                    setattr(statistics, status, int(row["total"]))
            return statistics

        return await self._run(operation)

    async def list_completed_by_task(
        self, task_name: "str", *, since: "datetime | None" = None, limit: "int" = 10
    ) -> "list[QueuedTaskRecord]":
        """Return recently completed records for one task name.

        Returns:
            Completed records, newest first.
        """

        def operation(connection: "sqlite3.Connection") -> "list[QueuedTaskRecord]":
            sql = "SELECT payload FROM queue_task WHERE task_name = ? AND status = 'completed' AND completed_at IS NOT NULL"
            values: "list[Any]" = [task_name]
            if since is not None:
                sql += " AND completed_at >= ?"
                values.append(_iso(since))
            sql += " ORDER BY completed_at DESC LIMIT ?"
            values.append(limit)
            return _decode_all(connection.execute(sql, values).fetchall())

        return await self._run(operation)

    async def cleanup_terminal(self, before: "datetime", *, limit: "int | None" = None) -> "int":
        """Delete terminal records completed before ``before``.

        Returns:
            The number of removed records.
        """

        def operation(connection: "sqlite3.Connection") -> "int":
            placeholders = ",".join("?" * len(_TERMINAL_STATUSES))
            sql = (
                f"SELECT id FROM queue_task WHERE status IN ({placeholders}) "  # noqa: S608 - placeholders only
                "AND completed_at IS NOT NULL AND completed_at < ? ORDER BY completed_at ASC, id ASC"
            )
            values: "list[Any]" = [*_TERMINAL_STATUSES, _iso(before)]
            if limit is not None:
                sql += " LIMIT ?"
                values.append(limit)
            ids = [row["id"] for row in connection.execute(sql, values).fetchall()]
            for task_id in ids:
                connection.execute("DELETE FROM queue_task WHERE id = ?", (task_id,))
            return len(ids)

        return await self._transaction(operation)

    async def acquire_maintenance(self, name: "str", token: "str", *, ttl: "timedelta") -> "bool":
        """Acquire expiring, token-fenced maintenance ownership.

        Returns:
            True when ownership was granted to ``token``.
        """

        def operation(connection: "sqlite3.Connection") -> "bool":
            now = _utc_now()
            row = connection.execute(
                "SELECT token, expires_at FROM queue_maintenance WHERE name = ?", (name,)
            ).fetchone()
            if row is not None:
                expires_at = datetime.fromisoformat(str(row["expires_at"]))
                if expires_at > now and str(row["token"]) != token:
                    return False
            connection.execute(
                "INSERT OR REPLACE INTO queue_maintenance (name, token, expires_at) VALUES (?, ?, ?)",
                (name, token, _iso(now + ttl)),
            )
            return True

        return await self._transaction(operation)

    async def release_maintenance(self, name: "str", token: "str") -> "bool":
        """Release maintenance ownership held under ``token``.

        Returns:
            True when ownership was released.
        """

        def operation(connection: "sqlite3.Connection") -> "bool":
            cursor = connection.execute("DELETE FROM queue_maintenance WHERE name = ? AND token = ?", (name, token))
            return cursor.rowcount > 0

        return await self._transaction(operation)

    async def reserve_identity(self, key: "str", *, task_id: "UUID", task_name: "str") -> "TaskReservation | None":
        """Reserve a forever identity.

        Returns:
            ``None`` when this caller won the reservation; otherwise the existing
            owner reservation.
        """

        def operation(connection: "sqlite3.Connection") -> "TaskReservation | None":
            existing = _reservation(connection, key)
            if existing is not None:
                return existing
            connection.execute(
                "INSERT INTO queue_reservation (identity_key, task_id, task_name, created_at) VALUES (?, ?, ?, ?)",
                (key, str(task_id), task_name, _iso(_utc_now())),
            )
            return None

        return await self._transaction(operation)

    async def has_identity(self, key: "str") -> "TaskReservation | None":
        """Return the reservation owning ``key``.

        Returns:
            The reservation, or ``None`` when the identity is free.
        """
        return await self._run(lambda connection: _reservation(connection, key))

    async def reset_identity(self, key: "str", *, expected_task_id: "UUID | None" = None) -> "bool":
        """Delete a forever identity reservation.

        Returns:
            True when a reservation was removed.
        """

        def operation(connection: "sqlite3.Connection") -> "bool":
            if expected_task_id is not None:
                cursor = connection.execute(
                    "DELETE FROM queue_reservation WHERE identity_key = ? AND task_id = ?", (key, str(expected_task_id))
                )
            else:
                cursor = connection.execute("DELETE FROM queue_reservation WHERE identity_key = ?", (key,))
            return cursor.rowcount > 0

        return await self._transaction(operation)

    async def notify_new_task(self, record: "QueuedTaskRecord") -> "None":
        """Signal same-instance waiters that due work exists."""
        if record.status in _ACTIVE_STATUSES and record.is_due:
            self._notification_event.set()
            self._record_wakeup_emitted()

    async def wait_for_wakeups(self, timeout: "float | None" = None) -> "bool":
        """Wait for due work using bounded SQLite existence polling.

        No cross-process signal is used: the poll is a low-latency hint and the
        transactional claim remains authoritative, so false positives are safe.

        Returns:
            True when due work exists, False when ``timeout`` expired.
        """
        if self._notification_event.is_set():
            self._notification_event.clear()
            return True
        interval = _MAX_POLL
        if self.config is not None:
            interval = min(max(self.config.worker.poll_interval, _MIN_POLL), _MAX_POLL)
        deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
        while True:
            if await self._has_due_work():
                self._notification_event.clear()
                return True
            if self._notification_event.is_set():
                self._notification_event.clear()
                return True
            remaining = interval
            if deadline is not None:
                remaining = min(interval, deadline - asyncio.get_running_loop().time())
                if remaining <= 0:
                    return False
            await asyncio.sleep(remaining)

    async def _has_due_work(self) -> "bool":
        def operation(connection: "sqlite3.Connection") -> "bool":
            now = _iso(_utc_now())
            row = connection.execute(
                "SELECT EXISTS(SELECT 1 FROM queue_task WHERE status IN (?, ?) "
                "AND (scheduled_at IS NULL OR scheduled_at <= ?) "
                "AND (expires_at IS NULL OR expires_at > ?)) AS due",
                (*_ACTIVE_STATUSES, now, now),
            ).fetchone()
            return bool(row["due"])

        return await self._run(operation)

    async def time_until_next_due(self, *, queues: "tuple[str, ...]" = ()) -> "float | None":
        """Return seconds until the earliest not-yet-due record.

        Returns:
            Seconds until the next due record, or ``None`` when none is upcoming.
        """

        def operation(connection: "sqlite3.Connection") -> "str | None":
            sql = (
                "SELECT MIN(scheduled_at) AS next_due FROM queue_task WHERE status IN (?, ?) "
                "AND scheduled_at IS NOT NULL AND scheduled_at > ? AND (expires_at IS NULL OR expires_at > ?)"
            )
            now = _iso(_utc_now())
            values: "list[Any]" = [*_ACTIVE_STATUSES, now, now]
            if queues:
                sql += f" AND queue IN ({','.join('?' * len(queues))})"
                values.extend(queues)
            row = connection.execute(sql, values).fetchone()
            return None if row is None or row["next_due"] is None else str(row["next_due"])

        next_due = await self._run(operation)
        if next_due is None:
            return None
        return max((datetime.fromisoformat(next_due) - _utc_now()).total_seconds(), 0.0)

    async def clear(self) -> "None":
        """Remove every stored row."""

        def operation(connection: "sqlite3.Connection") -> "None":
            for table in ("queue_task", "queue_reservation", "queue_maintenance", "queue_event"):
                connection.execute(f"DELETE FROM {table}")  # noqa: S608 - fixed table names

        await self._transaction(operation)
        self._notification_event.clear()
        await self._pending_read.aclose()


def _active_by_key(connection: "sqlite3.Connection", key: "str") -> "QueuedTaskRecord | None":
    placeholders = ",".join("?" * len(_TERMINAL_STATUSES))
    sql = f"SELECT payload FROM queue_task WHERE task_key = ? AND status NOT IN ({placeholders}) LIMIT 1"  # noqa: S608 - bound placeholders only
    row = connection.execute(sql, (key, *_TERMINAL_STATUSES)).fetchone()
    return None if row is None else _decode(row)


def _expire_record(record: "QueuedTaskRecord", completed_at: "datetime") -> "None":
    record.status = "expired"
    record.completed_at = completed_at
    record.heartbeat_at = None


def _locked_record(
    connection: "sqlite3.Connection", task_id: "UUID", expected_retry_count: "int | None"
) -> "QueuedTaskRecord | None":
    row = connection.execute("SELECT payload FROM queue_task WHERE id = ?", (str(task_id),)).fetchone()
    if row is None:
        return None
    record = _decode(row)
    if expected_retry_count is not None and (record.status != "running" or record.retry_count != expected_retry_count):
        return None
    return record


def _reservation(connection: "sqlite3.Connection", key: "str") -> "TaskReservation | None":
    row = connection.execute(
        "SELECT identity_key, task_id, task_name, created_at FROM queue_reservation WHERE identity_key = ?", (key,)
    ).fetchone()
    if row is None:
        return None
    return TaskReservation(
        key=str(row["identity_key"]),
        task_id=UUID(str(row["task_id"])),
        task_name=str(row["task_name"]),
        created_at=datetime.fromisoformat(str(row["created_at"])),
    )


_MIN_DATETIME = datetime(1, 1, 1, tzinfo=timezone.utc)


def _stale_sort_key(record: "QueuedTaskRecord") -> "tuple[datetime, str]":
    return (record.heartbeat_at or _MIN_DATETIME, str(record.id))

"""Shared SQLSpec queue store primitives."""

from typing import Any, cast

from sqlspec import sql

from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.schema import DEFAULT_TABLE_NAME, validate_table_name

__all__ = ("SQLSpecQueueStore", "adapter_name")

_TASK_COLUMNS = (
    "id",
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
    "created_at",
    "started_at",
    "completed_at",
    "heartbeat_at",
    "result_json",
    "error",
    "task_key",
    "metadata_json",
)
_DUE_STATUSES = ("pending", "scheduled")


def _configured_table_name(config: Any, table_name: str | None) -> str:
    if table_name is not None:
        return validate_table_name(table_name)
    extension_config = cast("dict[str, Any]", getattr(config, "extension_config", {}) or {})
    queue_settings = cast("dict[str, Any]", extension_config.get(QUEUE_EXTENSION_NAME, {}) or {})
    return validate_table_name(str(queue_settings.get("table_name", DEFAULT_TABLE_NAME)))


def adapter_name(config: Any) -> str:
    """Return the SQLSpec adapter name for a config object without importing the adapter."""
    module_name = type(config).__module__
    if module_name.startswith("sqlspec.adapters."):
        return module_name.split(".")[2]
    return ""


class SQLSpecQueueStore:  # noqa: PLR0904
    """Base SQLSpec queue statement store."""

    __slots__ = ("_config", "_table_name")

    id_type = "TEXT"
    text_type = "TEXT"
    indexed_text_type = "TEXT"
    integer_type = "INTEGER"
    json_type = "TEXT"
    payload_json_type: str | None = None
    result_json_type: str | None = None
    metadata_json_type: str | None = None
    timestamp_type = "TEXT"
    error_type = "TEXT"

    def __init__(self, config: Any, *, table_name: str | None = None) -> None:
        self._config = config
        self._table_name = _configured_table_name(config, table_name)

    @property
    def table_name(self) -> str:
        """Return the configured queue table name."""
        return self._table_name

    @property
    def dialect_name(self) -> str | None:
        """Return the SQLSpec dialect configured for this store."""
        statement_config = getattr(self._config, "statement_config", None)
        dialect = getattr(statement_config, "dialect", None)
        return str(dialect) if dialect is not None else None

    def create_statements(self) -> list[str]:
        """Return statements that create the queue table and indexes."""
        return [
            self._to_sql(self._create_table_statement()),
            *self._create_index_statements(),
        ]

    def drop_statements(self) -> list[str]:
        """Return statements that drop queue artifacts."""
        return [
            self._to_sql(sql.drop_index(self._index_name("heartbeat")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("pending")).if_exists()),
            self._to_sql(sql.drop_table(self.table_name).if_exists()),
        ]

    def insert_task(self, values: dict[str, Any]) -> Any:
        """Return an INSERT statement for a queued task."""
        return sql.insert(self.table_name).columns(*values.keys()).values(**values)

    def select_task(self, task_id: str) -> Any:
        """Return a SELECT statement for one task id."""
        return self._select_all().where_eq("id", task_id)

    def select_task_by_key(self, key: str) -> Any:
        """Return a SELECT statement for one task key."""
        return self._select_all().where_eq("task_key", key)

    def list_pending(
        self,
        *,
        now: str,
        limit: int,
        queue: str | None = None,
        execution_backend: str | None = None,
    ) -> Any:
        """Return a SELECT statement for due pending tasks."""
        statement = (
            self
            ._select_all()
            .where_in("status", _DUE_STATUSES)
            .where("scheduled_at IS NULL OR scheduled_at <= :now", now=now)
        )
        if queue is not None:
            statement = statement.where_eq("queue", queue)
        if execution_backend is not None:
            statement = statement.where_eq("execution_backend", execution_backend)
        # ``order_by(name, desc=True)`` currently produces invalid SQL on several
        # dialects (Postgres / DuckDB emit ``NULLS FIRST DESC``); pass raw column
        # expressions so SQLSpec doesn't append dialect-specific NULL ordering.
        return statement.order_by(sql.raw("priority DESC"), sql.raw("created_at ASC")).limit(limit)

    def claim_task(self, *, task_id: str, due_at: str, started_at: str, heartbeat_at: str) -> Any:
        """Return an UPDATE statement that claims a due task."""
        return (
            sql
            .update(self.table_name)
            .set(status="running", started_at=started_at, heartbeat_at=heartbeat_at)
            .where_eq("id", task_id)
            .where_in("status", _DUE_STATUSES)
            .where("scheduled_at IS NULL OR scheduled_at <= :due_at", due_at=due_at)
        )

    def complete_task(self, *, task_id: str, completed_at: str, heartbeat_at: str, result_json: Any) -> Any:
        """Return an UPDATE statement that completes a task."""
        return (
            sql
            .update(self.table_name)
            .set(
                status="completed",
                completed_at=completed_at,
                heartbeat_at=heartbeat_at,
                result_json=result_json,
                error=None,
            )
            .where_eq("id", task_id)
        )

    def retry_task(self, *, task_id: str, error: str, retry_count: int) -> Any:
        """Return an UPDATE statement that schedules a retry."""
        return (
            sql
            .update(self.table_name)
            .set(
                status="pending",
                retry_count=retry_count,
                started_at=None,
                heartbeat_at=None,
                error=error,
            )
            .where_eq("id", task_id)
        )

    def fail_task(self, *, task_id: str, completed_at: str, heartbeat_at: str, error: str) -> Any:
        """Return an UPDATE statement that permanently fails a task."""
        return (
            sql
            .update(self.table_name)
            .set(status="failed", completed_at=completed_at, heartbeat_at=heartbeat_at, error=error)
            .where_eq("id", task_id)
        )

    def cancel_task(self, *, task_id: str, completed_at: str) -> Any:
        """Return an UPDATE statement that cancels a due task."""
        return (
            sql
            .update(self.table_name)
            .set(status="cancelled", completed_at=completed_at)
            .where_eq("id", task_id)
            .where_in("status", _DUE_STATUSES)
        )

    def touch_heartbeat(self, *, task_id: str, heartbeat_at: str) -> Any:
        """Return an UPDATE statement that touches a running task heartbeat."""
        return (
            sql
            .update(self.table_name)
            .set(heartbeat_at=heartbeat_at)
            .where_eq("id", task_id)
            .where_eq("status", "running")
        )

    def null_heartbeats(self, *, task_ids: list[str]) -> Any:
        """Return an UPDATE statement that clears task heartbeats."""
        return sql.update(self.table_name).set(heartbeat_at=None).where_in("id", task_ids)

    def requeue_stale(self, *, cutoff: str) -> Any:
        """Return an UPDATE statement that requeues stale running tasks."""
        return (
            sql
            .update(self.table_name)
            .set(
                status="pending",
                started_at=None,
                heartbeat_at=None,
                retry_count=sql.raw("retry_count + 1"),
            )
            .where_eq("status", "running")
            .where("heartbeat_at IS NULL OR heartbeat_at < :cutoff", cutoff=cutoff)
        )

    def clear_key(self, *, task_id: str) -> Any:
        """Return an UPDATE statement that releases a terminal task key."""
        return sql.update(self.table_name).set(task_key=None).where_eq("id", task_id)

    def set_execution_ref(
        self,
        *,
        task_id: str,
        execution_backend: str,
        execution_ref: str,
        execution_profile: str | None,
    ) -> Any:
        """Return an UPDATE statement that stores an external execution reference."""
        return (
            sql
            .update(self.table_name)
            .set(
                execution_backend=execution_backend,
                execution_profile=execution_profile,
                execution_ref=execution_ref,
            )
            .where_eq("id", task_id)
        )

    def set_execution_backend(
        self,
        *,
        task_id: str,
        execution_backend: str,
        execution_profile: str | None,
    ) -> Any:
        """Return an UPDATE statement that changes execution routing."""
        return (
            sql
            .update(self.table_name)
            .set(
                execution_backend=execution_backend,
                execution_profile=execution_profile,
                execution_ref=None,
            )
            .where_eq("id", task_id)
        )

    def list_running_external(self, *, limit: int | None = None) -> Any:
        """Return a SELECT statement for externally dispatched records."""
        statement = (
            self
            ._select_all()
            .where("status IN ('pending', 'scheduled', 'running')")
            .where("execution_ref IS NOT NULL")
            .order_by(sql.raw("started_at ASC"), sql.raw("created_at ASC"))
        )
        return statement.limit(limit) if limit is not None else statement

    def list_all(self) -> Any:
        """Return a SELECT statement for all queue records."""
        return self._select_all()

    def list_completed_by_task(self, *, task_name: str, since: str | None = None, limit: int = 10) -> Any:
        """Return a SELECT statement for completed records by task name."""
        statement = self._select_all().where_eq("task_name", task_name).where_eq("status", "completed")
        if since is not None:
            statement = statement.where("completed_at >= :completed_since", completed_since=since)
        return statement.order_by(sql.raw("completed_at DESC")).limit(limit)

    def cleanup_terminal(self, *, before: str) -> Any:
        """Return a DELETE statement for terminal records before a cutoff."""
        return (
            sql
            .delete(self.table_name)
            .where_in("status", ("completed", "failed", "cancelled"))
            .where("completed_at IS NOT NULL AND completed_at < :terminal_before", terminal_before=before)
        )

    def serialize_payload_json(self, value: Any) -> Any:
        """Serialize task payload JSON values for this store.

        Returns:
            A JSON value suitable for the configured adapter.
        """
        return self._serialize_json(value)

    def serialize_result_json(self, value: Any) -> Any:
        """Serialize task result JSON values for this store.

        Returns:
            A JSON value suitable for the configured adapter.
        """
        return self._serialize_json(value)

    def serialize_metadata_json(self, value: Any) -> Any:
        """Serialize task metadata JSON values for this store.

        Returns:
            A JSON value suitable for the configured adapter.
        """
        return self._serialize_json(value)

    def deserialize_json(self, value: Any) -> Any:
        """Deserialize a task JSON value returned by the database driver.

        Returns:
            The decoded Python JSON value.
        """
        if value is None:
            return None
        read = getattr(value, "read", None)
        if callable(read):
            value = read()
        if isinstance(value, (list, dict)):
            # Some drivers (e.g., psycopg/psqlpy with JSONB columns) auto-decode
            # JSON values; pass them through unchanged.
            return value
        from sqlspec.utils.serializers import from_json

        return from_json(value)

    def _select_all(self) -> Any:
        return sql.select(*_TASK_COLUMNS).from_(self.table_name)

    def _create_table_statement(self) -> Any:
        return (
            sql
            .create_table(self.table_name)
            .if_not_exists()
            .column("id", self._id_type(), primary_key=True)
            .column("task_name", self._indexed_text_type(), not_null=True)
            .column("args_json", self._payload_json_type("args_json"), not_null=True)
            .column("kwargs_json", self._payload_json_type("kwargs_json"), not_null=True)
            .column("queue", self._indexed_text_type(), not_null=True)
            .column("execution_backend", self._indexed_text_type(), not_null=True)
            .column("execution_profile", self._indexed_text_type())
            .column("execution_ref", self._indexed_text_type())
            .column("status", self._indexed_text_type(), not_null=True)
            .column("priority", self._integer_type(), not_null=True)
            .column("max_retries", self._integer_type(), not_null=True)
            .column("retry_count", self._integer_type(), not_null=True)
            .column("scheduled_at", self._timestamp_type())
            .column("created_at", self._timestamp_type(), not_null=True)
            .column("started_at", self._timestamp_type())
            .column("completed_at", self._timestamp_type())
            .column("heartbeat_at", self._timestamp_type())
            .column("result_json", self._result_json_type("result_json"), not_null=True)
            .column("error", self._error_type())
            .column("task_key", self._indexed_text_type(), unique=True)
            .column("metadata_json", self._metadata_json_type("metadata_json"), not_null=True)
        )

    def _create_index_statements(self) -> list[str]:
        return [
            self._to_sql(
                sql
                .create_index(self._index_name("pending"))
                .if_not_exists()
                .on_table(self.table_name)
                .columns("status", "queue", "execution_backend", "scheduled_at", "priority", "created_at")
            ),
            self._to_sql(
                sql
                .create_index(self._index_name("heartbeat"))
                .if_not_exists()
                .on_table(self.table_name)
                .columns("status", "heartbeat_at")
            ),
        ]

    def _index_name(self, suffix: str) -> str:
        return validate_table_name(f"ix_{self.table_name}_{suffix}")

    def _id_type(self) -> str:
        return self.id_type

    def _indexed_text_type(self) -> str:
        return self.indexed_text_type

    def _integer_type(self) -> str:
        return self.integer_type

    def _json_type(self) -> str:
        return self.json_type

    def _payload_json_type(self, column_name: str) -> str:
        return self.payload_json_type or self._json_type()

    def _result_json_type(self, column_name: str) -> str:
        return self.result_json_type or self._json_type()

    def _metadata_json_type(self, column_name: str) -> str:
        return self.metadata_json_type or self._json_type()

    def _timestamp_type(self) -> str:
        return self.timestamp_type

    def _error_type(self) -> str:
        return self.error_type

    def _serialize_json(self, value: Any) -> str:
        from sqlspec.utils.serializers import to_json

        return to_json(value)

    def _to_sql(self, statement: Any) -> str:
        built = statement.build(dialect=self.dialect_name)
        return cast("str", built.sql)

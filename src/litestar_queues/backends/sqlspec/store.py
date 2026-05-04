"""SQLSpec queue store statements."""

from typing import Any, cast

from sqlspec import sql

from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.schema import DEFAULT_TABLE_NAME, validate_table_name

__all__ = (
    "MySQLQueueStore",
    "OracleQueueStore",
    "PostgresQueueStore",
    "SQLSpecQueueStore",
    "SQLiteQueueStore",
    "create_queue_store",
)

_TASK_COLUMNS = (
    "id",
    "task_name",
    "args_json",
    "kwargs_json",
    "queue",
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
_ADAPTER_STORE_TYPES: dict[str, type["SQLSpecQueueStore"]] = {}


def _configured_table_name(config: Any, table_name: str | None) -> str:
    if table_name is not None:
        return validate_table_name(table_name)
    extension_config = cast("dict[str, Any]", getattr(config, "extension_config", {}) or {})
    queue_settings = cast("dict[str, Any]", extension_config.get(QUEUE_EXTENSION_NAME, {}) or {})
    return validate_table_name(str(queue_settings.get("table_name", DEFAULT_TABLE_NAME)))


def _adapter_name(config: Any) -> str:
    module_name = type(config).__module__
    if module_name.startswith("sqlspec.adapters."):
        return module_name.split(".")[2]
    return ""


class SQLSpecQueueStore:
    """Base SQLSpec queue statement store."""

    __slots__ = ("_config", "_table_name")

    id_type = "TEXT"
    text_type = "TEXT"
    indexed_text_type = "TEXT"
    integer_type = "INTEGER"
    json_type = "TEXT"
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

    def list_pending(self, *, now: str, limit: int, queue: str | None = None) -> Any:
        """Return a SELECT statement for due pending tasks."""
        statement = (
            self
            ._select_all()
            .where_in("status", _DUE_STATUSES)
            .where("scheduled_at IS NULL OR scheduled_at <= :now", now=now)
        )
        if queue is not None:
            statement = statement.where_eq("queue", queue)
        return statement.order_by("priority", desc=True).order_by("created_at").limit(limit)

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

    def complete_task(self, *, task_id: str, completed_at: str, heartbeat_at: str, result_json: str) -> Any:
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

    def _select_all(self) -> Any:
        return sql.select(*_TASK_COLUMNS).from_(self.table_name)

    def _create_table_statement(self) -> Any:
        return (
            sql
            .create_table(self.table_name)
            .if_not_exists()
            .column("id", self.id_type, primary_key=True)
            .column("task_name", self.indexed_text_type, not_null=True)
            .column("args_json", self.json_type, not_null=True)
            .column("kwargs_json", self.json_type, not_null=True)
            .column("queue", self.indexed_text_type, not_null=True)
            .column("status", self.indexed_text_type, not_null=True)
            .column("priority", self.integer_type, not_null=True)
            .column("max_retries", self.integer_type, not_null=True)
            .column("retry_count", self.integer_type, not_null=True)
            .column("scheduled_at", self.timestamp_type)
            .column("created_at", self.timestamp_type, not_null=True)
            .column("started_at", self.timestamp_type)
            .column("completed_at", self.timestamp_type)
            .column("heartbeat_at", self.timestamp_type)
            .column("result_json", self.json_type, not_null=True)
            .column("error", self.error_type)
            .column("task_key", self.indexed_text_type, unique=True)
            .column("metadata_json", self.json_type, not_null=True)
        )

    def _create_index_statements(self) -> list[str]:
        return [
            self._to_sql(
                sql
                .create_index(self._index_name("pending"))
                .if_not_exists()
                .on_table(self.table_name)
                .columns("status", "queue", "scheduled_at", "priority", "created_at")
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

    def _to_sql(self, statement: Any) -> str:
        built = statement.build(dialect=self.dialect_name)
        return cast("str", built.sql)


class SQLiteQueueStore(SQLSpecQueueStore):
    """SQLite-specific SQLSpec queue statement store."""

    __slots__ = ()


class PostgresQueueStore(SQLSpecQueueStore):
    """PostgreSQL-specific SQLSpec queue statement store."""

    __slots__ = ()

    def drop_statements(self) -> list[str]:
        """Return statements that drop PostgreSQL queue artifacts."""
        return [
            self._to_sql(sql.drop_index(self._index_name("heartbeat")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("scheduled")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("pending")).if_exists()),
            self._to_sql(sql.drop_table(self.table_name).if_exists()),
        ]

    def _create_index_statements(self) -> list[str]:
        table_name = self.table_name
        return [
            (
                f"CREATE INDEX IF NOT EXISTS {self._index_name('pending')} "
                f"ON {table_name} (queue, priority DESC, created_at) "
                "WHERE status IN ('pending', 'scheduled')"
            ),
            (
                f"CREATE INDEX IF NOT EXISTS {self._index_name('scheduled')} "
                f"ON {table_name} (scheduled_at) WHERE status = 'scheduled'"
            ),
            (
                f"CREATE INDEX IF NOT EXISTS {self._index_name('heartbeat')} "
                f"ON {table_name} (heartbeat_at) WHERE status = 'running'"
            ),
        ]


class MySQLQueueStore(SQLSpecQueueStore):
    """MySQL-specific SQLSpec queue statement store."""

    __slots__ = ()

    id_type = "VARCHAR(64)"
    indexed_text_type = "VARCHAR(255)"
    json_type = "LONGTEXT"
    timestamp_type = "VARCHAR(64)"
    error_type = "LONGTEXT"


class OracleQueueStore(SQLSpecQueueStore):
    """Oracle-specific SQLSpec queue statement store."""

    __slots__ = ()

    id_type = "VARCHAR2(64)"
    text_type = "CLOB"
    indexed_text_type = "VARCHAR2(255)"
    integer_type = "NUMBER(10)"
    json_type = "CLOB"
    timestamp_type = "VARCHAR2(64)"
    error_type = "CLOB"

    def create_statements(self) -> list[str]:
        """Return statements that create Oracle queue artifacts."""
        return [
            self._create_table_block(),
            self._create_index_block("pending", "status, queue, scheduled_at, priority, created_at"),
            self._create_index_block("heartbeat", "status, heartbeat_at"),
        ]

    def drop_statements(self) -> list[str]:
        """Return statements that drop Oracle queue artifacts."""
        return [
            self._drop_index_block("heartbeat"),
            self._drop_index_block("pending"),
            self._drop_table_block(),
        ]

    def _create_table_block(self) -> str:
        table_name = self.table_name
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {table_name} (
                id VARCHAR2(64) PRIMARY KEY,
                task_name VARCHAR2(255) NOT NULL,
                args_json CLOB NOT NULL,
                kwargs_json CLOB NOT NULL,
                queue VARCHAR2(255) NOT NULL,
                status VARCHAR2(255) NOT NULL,
                priority NUMBER(10) NOT NULL,
                max_retries NUMBER(10) NOT NULL,
                retry_count NUMBER(10) NOT NULL,
                scheduled_at VARCHAR2(64),
                created_at VARCHAR2(64) NOT NULL,
                started_at VARCHAR2(64),
                completed_at VARCHAR2(64),
                heartbeat_at VARCHAR2(64),
                result_json CLOB NOT NULL,
                error CLOB,
                task_key VARCHAR2(255) UNIQUE,
                metadata_json CLOB NOT NULL
            )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    def _create_index_block(self, suffix: str, columns: str) -> str:
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX {self._index_name(suffix)}
                ON {self.table_name}({columns})';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    def _drop_index_block(self, suffix: str) -> str:
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'DROP INDEX {self._index_name(suffix)}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -1418 THEN
                    RAISE;
                END IF;
        END;
        """

    def _drop_table_block(self) -> str:
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE {self.table_name}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN
                    RAISE;
                END IF;
        END;
        """


_ADAPTER_STORE_TYPES.update({
    "aiomysql": MySQLQueueStore,
    "aiosqlite": SQLiteQueueStore,
    "asyncmy": MySQLQueueStore,
    "asyncpg": PostgresQueueStore,
    "mysql": MySQLQueueStore,
    "oracledb": OracleQueueStore,
    "psycopg": PostgresQueueStore,
    "sqlite": SQLiteQueueStore,
})


def create_queue_store(config: Any, *, table_name: str | None = None) -> SQLSpecQueueStore:
    """Create a queue store for a SQLSpec adapter configuration.

    Returns:
        The queue store implementation for the SQLSpec adapter.
    """
    return _ADAPTER_STORE_TYPES.get(_adapter_name(config), SQLSpecQueueStore)(config, table_name=table_name)

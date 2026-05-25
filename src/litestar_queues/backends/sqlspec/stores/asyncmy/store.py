"""asyncmy SQLSpec queue store."""

from sqlspec import sql

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("AsyncmyQueueStore",)


class AsyncmyQueueStore(SQLSpecQueueStore):
    """asyncmy-specific SQLSpec queue statement store."""

    __slots__ = ()

    id_type = "VARCHAR(64)"
    indexed_text_type = "VARCHAR(255)"
    json_type = "JSON"
    timestamp_type = "VARCHAR(64)"
    error_type = "LONGTEXT"

    def create_statements(self) -> list[str]:
        """Return statements that create asyncmy queue artifacts."""
        if not self._manage_schema:
            return []
        table_name = self.table_name
        return [
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {self._col("id")} VARCHAR(64) PRIMARY KEY,
                {self._col("task_name")} VARCHAR(255) NOT NULL,
                {self._col("args_json")} {self._payload_json_type("args_json")} NOT NULL,
                {self._col("kwargs_json")} {self._payload_json_type("kwargs_json")} NOT NULL,
                {self._col("queue")} VARCHAR(255) NOT NULL,
                {self._col("execution_backend")} VARCHAR(255) NOT NULL,
                {self._col("execution_profile")} VARCHAR(255),
                {self._col("execution_ref")} VARCHAR(255),
                {self._col("status")} VARCHAR(255) NOT NULL,
                {self._col("priority")} INTEGER NOT NULL,
                {self._col("max_retries")} INTEGER NOT NULL,
                {self._col("retry_count")} INTEGER NOT NULL,
                {self._col("scheduled_at")} VARCHAR(64),
                {self._col("created_at")} VARCHAR(64) NOT NULL,
                {self._col("started_at")} VARCHAR(64),
                {self._col("completed_at")} VARCHAR(64),
                {self._col("heartbeat_at")} VARCHAR(64),
                {self._col("result_json")} {self._result_json_type("result_json")} NOT NULL,
                {self._col("error")} LONGTEXT,
                {self._col("task_key")} VARCHAR(255) UNIQUE,
                {self._col("metadata_json")} {self._metadata_json_type("metadata_json")} NOT NULL,
                INDEX {self._index_name("pending")} (
                    {self._col("status")}(32), {self._col("queue")}(191), {self._col("execution_backend")}(191),
                    {self._col("scheduled_at")}, {self._col("priority")}, {self._col("created_at")}
                ),
                INDEX {self._index_name("heartbeat")} ({self._col("status")}(32), {self._col("heartbeat_at")})
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        ]

    def drop_statements(self) -> list[str]:
        """Return statements that drop asyncmy queue artifacts."""
        if not self._manage_schema:
            return []
        return [self._to_sql(sql.drop_table(self.table_name).if_exists())]

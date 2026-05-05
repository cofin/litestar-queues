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
        table_name = self.table_name
        return [
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id VARCHAR(64) PRIMARY KEY,
                task_name VARCHAR(255) NOT NULL,
                args_json {self._payload_json_type("args_json")} NOT NULL,
                kwargs_json {self._payload_json_type("kwargs_json")} NOT NULL,
                queue VARCHAR(255) NOT NULL,
                execution_backend VARCHAR(255) NOT NULL,
                execution_profile VARCHAR(255),
                execution_ref VARCHAR(255),
                status VARCHAR(255) NOT NULL,
                priority INTEGER NOT NULL,
                max_retries INTEGER NOT NULL,
                retry_count INTEGER NOT NULL,
                scheduled_at VARCHAR(64),
                created_at VARCHAR(64) NOT NULL,
                started_at VARCHAR(64),
                completed_at VARCHAR(64),
                heartbeat_at VARCHAR(64),
                result_json {self._result_json_type("result_json")} NOT NULL,
                error LONGTEXT,
                task_key VARCHAR(255) UNIQUE,
                metadata_json {self._metadata_json_type("metadata_json")} NOT NULL,
                INDEX {self._index_name("pending")} (
                    status, queue, execution_backend, scheduled_at, priority, created_at
                ),
                INDEX {self._index_name("heartbeat")} (status, heartbeat_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        ]

    def drop_statements(self) -> list[str]:
        """Return statements that drop asyncmy queue artifacts."""
        return [self._to_sql(sql.drop_table(self.table_name).if_exists())]

"""spanner SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("SpannerQueueStore",)


class SpannerQueueStore(SQLSpecQueueStore):
    """spanner-specific SQLSpec queue statement store."""

    __slots__ = ()

    id_type = "STRING(64)"
    indexed_text_type = "STRING(255)"
    integer_type = "INT64"
    json_type = "JSON"
    timestamp_type = "TIMESTAMP"
    error_type = "STRING(MAX)"

    def create_statements(self) -> list[str]:
        """Return statements that create Spanner queue artifacts."""
        table_name = self.table_name
        return [
            f"""
            CREATE TABLE {table_name} (
                id STRING(64) NOT NULL,
                task_name STRING(255) NOT NULL,
                args_json {self._payload_json_type("args_json")} NOT NULL,
                kwargs_json {self._payload_json_type("kwargs_json")} NOT NULL,
                queue STRING(255) NOT NULL,
                execution_backend STRING(255) NOT NULL,
                execution_profile STRING(255),
                execution_ref STRING(255),
                status STRING(255) NOT NULL,
                priority INT64 NOT NULL,
                max_retries INT64 NOT NULL,
                retry_count INT64 NOT NULL,
                scheduled_at TIMESTAMP,
                created_at TIMESTAMP NOT NULL,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                heartbeat_at TIMESTAMP,
                result_json {self._result_json_type("result_json")} NOT NULL,
                error STRING(MAX),
                task_key STRING(255),
                metadata_json {self._metadata_json_type("metadata_json")} NOT NULL
            ) PRIMARY KEY (id)
            """,
            (
                f"CREATE INDEX {self._index_name('pending')} "
                f"ON {table_name}(status, queue, execution_backend, scheduled_at, priority, created_at)"
            ),
            f"CREATE INDEX {self._index_name('heartbeat')} ON {table_name}(status, heartbeat_at)",
        ]

    def drop_statements(self) -> list[str]:
        """Return statements that drop Spanner queue artifacts."""
        return [
            f"DROP INDEX {self._index_name('heartbeat')}",
            f"DROP INDEX {self._index_name('pending')}",
            f"DROP TABLE {self.table_name}",
        ]

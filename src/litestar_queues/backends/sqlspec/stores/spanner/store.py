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
        if not self._manage_schema:
            return []
        table_name = self.table_name
        return [
            f"""
            CREATE TABLE {table_name} (
                {self._col("id")} STRING(64) NOT NULL,
                {self._col("task_name")} STRING(255) NOT NULL,
                {self._col("args_json")} {self._payload_json_type("args_json")} NOT NULL,
                {self._col("kwargs_json")} {self._payload_json_type("kwargs_json")} NOT NULL,
                {self._col("queue")} STRING(255) NOT NULL,
                {self._col("execution_backend")} STRING(255) NOT NULL,
                {self._col("execution_profile")} STRING(255),
                {self._col("execution_ref")} STRING(255),
                {self._col("status")} STRING(255) NOT NULL,
                {self._col("priority")} INT64 NOT NULL,
                {self._col("max_retries")} INT64 NOT NULL,
                {self._col("retry_count")} INT64 NOT NULL,
                {self._col("scheduled_at")} TIMESTAMP,
                {self._col("created_at")} TIMESTAMP NOT NULL,
                {self._col("started_at")} TIMESTAMP,
                {self._col("completed_at")} TIMESTAMP,
                {self._col("heartbeat_at")} TIMESTAMP,
                {self._col("result_json")} {self._result_json_type("result_json")} NOT NULL,
                {self._col("error")} STRING(MAX),
                {self._col("task_key")} STRING(255),
                {self._col("metadata_json")} {self._metadata_json_type("metadata_json")} NOT NULL
            ) PRIMARY KEY ({self._col("id")})
            """,
            (
                f"CREATE INDEX {self._index_name('pending')} "
                f"ON {table_name}({self._col('status')}, {self._col('queue')}, "
                f"{self._col('execution_backend')}, {self._col('scheduled_at')}, "
                f"{self._col('priority')}, {self._col('created_at')})"
            ),
            (
                f"CREATE INDEX {self._index_name('heartbeat')} "
                f"ON {table_name}({self._col('status')}, {self._col('heartbeat_at')})"
            ),
        ]

    def drop_statements(self) -> list[str]:
        """Return statements that drop Spanner queue artifacts."""
        if not self._manage_schema:
            return []
        return [
            f"DROP INDEX {self._index_name('heartbeat')}",
            f"DROP INDEX {self._index_name('pending')}",
            f"DROP TABLE {self.table_name}",
        ]

"""bigquery SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("BigQueryQueueStore",)


class BigQueryQueueStore(SQLSpecQueueStore):
    """bigquery-specific SQLSpec queue statement store."""

    __slots__ = ()

    id_type = "STRING"
    indexed_text_type = "STRING"
    integer_type = "INT64"
    json_type = "JSON"
    timestamp_type = "TIMESTAMP"
    error_type = "STRING"

    def create_statements(self) -> list[str]:
        """Return statements that create BigQuery queue artifacts."""
        if not self._manage_schema:
            return []
        table_name = self.table_name
        return [
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {self._col("id")} STRING NOT NULL,
                {self._col("task_name")} STRING NOT NULL,
                {self._col("args_json")} {self._payload_json_type("args_json")} NOT NULL,
                {self._col("kwargs_json")} {self._payload_json_type("kwargs_json")} NOT NULL,
                {self._col("queue")} STRING NOT NULL,
                {self._col("execution_backend")} STRING NOT NULL,
                {self._col("execution_profile")} STRING,
                {self._col("execution_ref")} STRING,
                {self._col("status")} STRING NOT NULL,
                {self._col("priority")} INT64 NOT NULL,
                {self._col("max_retries")} INT64 NOT NULL,
                {self._col("retry_count")} INT64 NOT NULL,
                {self._col("scheduled_at")} TIMESTAMP,
                {self._col("created_at")} TIMESTAMP NOT NULL,
                {self._col("started_at")} TIMESTAMP,
                {self._col("completed_at")} TIMESTAMP,
                {self._col("heartbeat_at")} TIMESTAMP,
                {self._col("result_json")} {self._result_json_type("result_json")} NOT NULL,
                {self._col("error")} STRING,
                {self._col("task_key")} STRING,
                {self._col("metadata_json")} {self._metadata_json_type("metadata_json")} NOT NULL
            )
            """
        ]

    def drop_statements(self) -> list[str]:
        """Return statements that drop BigQuery queue artifacts."""
        if not self._manage_schema:
            return []
        return [f"DROP TABLE IF EXISTS {self.table_name}"]

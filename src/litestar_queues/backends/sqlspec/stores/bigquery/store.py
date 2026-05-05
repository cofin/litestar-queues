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
        table_name = self.table_name
        return [
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id STRING NOT NULL,
                task_name STRING NOT NULL,
                args_json {self._payload_json_type("args_json")} NOT NULL,
                kwargs_json {self._payload_json_type("kwargs_json")} NOT NULL,
                queue STRING NOT NULL,
                execution_backend STRING NOT NULL,
                execution_profile STRING,
                execution_ref STRING,
                status STRING NOT NULL,
                priority INT64 NOT NULL,
                max_retries INT64 NOT NULL,
                retry_count INT64 NOT NULL,
                scheduled_at TIMESTAMP,
                created_at TIMESTAMP NOT NULL,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                heartbeat_at TIMESTAMP,
                result_json {self._result_json_type("result_json")} NOT NULL,
                error STRING,
                task_key STRING,
                metadata_json {self._metadata_json_type("metadata_json")} NOT NULL
            )
            CLUSTER BY status, queue, execution_backend, scheduled_at
            """
        ]

    def drop_statements(self) -> list[str]:
        """Return statements that drop BigQuery queue artifacts."""
        return [f"DROP TABLE IF EXISTS {self.table_name}"]

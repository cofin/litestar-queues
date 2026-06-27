"""bigquery SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("BigQueryQueueStore",)


class BigQueryQueueStore(SQLSpecQueueStore):
    """bigquery-specific SQLSpec queue statement store."""

    __slots__ = ()

    data_dictionary_dialect = "bigquery"
    identifier_quote_style = "backtick"

    def create_statements(self) -> list[str]:
        """Return statements that create BigQuery queue artifacts."""
        if not self._manage_schema:
            return []
        return [
            f"""
            CREATE TABLE IF NOT EXISTS {self._quoted_table_name()} (
                {self._quoted_col("id")} {self._id_type()} NOT NULL,
                {self._quoted_col("task_name")} {self._indexed_text_type()} NOT NULL,
                {self._quoted_col("args_json")} {self._payload_json_type("args_json")} NOT NULL,
                {self._quoted_col("kwargs_json")} {self._payload_json_type("kwargs_json")} NOT NULL,
                {self._quoted_col("queue")} {self._indexed_text_type()} NOT NULL,
                {self._quoted_col("execution_backend")} {self._indexed_text_type()} NOT NULL,
                {self._quoted_col("execution_profile")} {self._indexed_text_type()},
                {self._quoted_col("execution_ref")} {self._indexed_text_type()},
                {self._quoted_col("status")} {self._indexed_text_type()} NOT NULL,
                {self._quoted_col("priority")} {self._integer_type()} NOT NULL,
                {self._quoted_col("max_retries")} {self._integer_type()} NOT NULL,
                {self._quoted_col("retry_count")} {self._integer_type()} NOT NULL,
                {self._quoted_col("scheduled_at")} {self._timestamp_type()},
                {self._quoted_col("created_at")} {self._timestamp_type()} NOT NULL,
                {self._quoted_col("started_at")} {self._timestamp_type()},
                {self._quoted_col("completed_at")} {self._timestamp_type()},
                {self._quoted_col("heartbeat_at")} {self._timestamp_type()},
                {self._quoted_col("result_json")} {self._result_json_type("result_json")} NOT NULL,
                {self._quoted_col("error")} {self._error_type()},
                {self._quoted_col("task_key")} {self._indexed_text_type()},
                {self._quoted_col("metadata_json")} {self._metadata_json_type("metadata_json")} NOT NULL
            )
            """
        ]

    def drop_statements(self) -> list[str]:
        """Return statements that drop BigQuery queue artifacts."""
        if not self._manage_schema:
            return []
        return [f"DROP TABLE IF EXISTS {self._quoted_table_name()}"]

    def _string_type(self, length: int | None = None) -> str:
        del length
        return "STRING"

    def _integer_type(self) -> str:
        return "INT64"

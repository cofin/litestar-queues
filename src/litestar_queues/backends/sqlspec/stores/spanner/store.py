"""spanner SQLSpec queue store."""

from typing import ClassVar, Literal

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("SpannerQueueStore",)


class SpannerQueueStore(SQLSpecQueueStore):
    """spanner-specific SQLSpec queue statement store."""

    __slots__ = ()

    data_dictionary_dialect: ClassVar[str | None] = "spanner"
    identifier_quote_style: ClassVar[Literal["double", "backtick", "none"]] = "backtick"

    def create_statements(self) -> list[str]:
        """Return statements that create Spanner queue artifacts."""
        if not self._manage_schema:
            return []
        return [
            f"""
            CREATE TABLE {self._quoted_table_name()} (
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
            ) PRIMARY KEY ({self._quoted_col("id")})
            """,
            (
                f"CREATE INDEX {self._quoted_index_name('pending')} "
                f"ON {self._quoted_table_name()}({self._quoted_col('status')}, {self._quoted_col('queue')}, "
                f"{self._quoted_col('execution_backend')}, {self._quoted_col('scheduled_at')}, "
                f"{self._quoted_col('priority')}, {self._quoted_col('created_at')})"
            ),
            (
                f"CREATE INDEX {self._quoted_index_name('heartbeat')} "
                f"ON {self._quoted_table_name()}({self._quoted_col('status')}, {self._quoted_col('heartbeat_at')})"
            ),
        ]

    def drop_statements(self) -> list[str]:
        """Return statements that drop Spanner queue artifacts."""
        if not self._manage_schema:
            return []
        return [
            f"DROP INDEX {self._quoted_index_name('heartbeat')}",
            f"DROP INDEX {self._quoted_index_name('pending')}",
            f"DROP TABLE {self._quoted_table_name()}",
        ]

    def _string_type(self, length: int | None = None) -> str:
        return f"STRING({length})" if length is not None else "STRING(MAX)"

    def _integer_type(self) -> str:
        return "INT64"

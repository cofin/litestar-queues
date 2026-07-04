"""spanner SQLSpec queue store."""

from typing import Any, ClassVar, Literal

from sqlspec import sql

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("SpannerQueueStore",)


class SpannerQueueStore(SQLSpecQueueStore):
    """spanner-specific SQLSpec queue statement store."""

    __slots__ = ()

    data_dictionary_dialect: "ClassVar[str | None]" = "spanner"
    identifier_quote_style: 'ClassVar[Literal["double", "backtick", "none"]]' = "backtick"
    auto_native_json_columns: "ClassVar[frozenset[str]]" = frozenset({
        "args_json",
        "kwargs_json",
        "metadata_json",
        "result_json",
    })

    def drop_statements(self) -> "list[str]":
        """Return statements that drop Spanner queue artifacts."""
        if not self._manage_schema:
            return []
        return [self._to_sql(sql.drop_index(self._index_name("task_key")).if_exists()), *super().drop_statements()]

    def _create_table_statement(self) -> "Any":
        return (
            sql
            .create_table(self.table_name)
            .if_not_exists()
            .column(self._col("id"), self._id_type(), primary_key=True)
            .column(self._col("task_name"), self._indexed_text_type(), not_null=True)
            .column(self._col("args_json"), self._payload_json_type("args_json"), not_null=True)
            .column(self._col("kwargs_json"), self._payload_json_type("kwargs_json"), not_null=True)
            .column(self._col("queue"), self._indexed_text_type(), not_null=True)
            .column(self._col("execution_backend"), self._indexed_text_type(), not_null=True)
            .column(self._col("execution_profile"), self._indexed_text_type())
            .column(self._col("execution_ref"), self._indexed_text_type())
            .column(self._col("status"), self._indexed_text_type(), not_null=True)
            .column(self._col("priority"), self._integer_type(), not_null=True)
            .column(self._col("max_retries"), self._integer_type(), not_null=True)
            .column(self._col("retry_count"), self._integer_type(), not_null=True)
            .column(self._col("scheduled_at"), self._timestamp_type())
            .column(self._col("created_at"), self._timestamp_type(), not_null=True)
            .column(self._col("started_at"), self._timestamp_type())
            .column(self._col("completed_at"), self._timestamp_type())
            .column(self._col("heartbeat_at"), self._timestamp_type())
            .column(self._col("result_json"), self._result_json_type("result_json"), not_null=True)
            .column(self._col("error"), self._error_type())
            .column(self._col("task_key"), self._indexed_text_type())
            .column(self._col("metadata_json"), self._metadata_json_type("metadata_json"), not_null=True)
        )

    def _create_index_statements(self) -> "list[str]":
        statements = super()._create_index_statements()
        statements.append(
            f"CREATE UNIQUE NULL_FILTERED INDEX IF NOT EXISTS {self._quoted_index_name('task_key')} "
            f"ON {self._quoted_table_name()} ({self._quoted_col('task_key')})"
        )
        return statements

    def _string_type(self, length: "int | None" = None) -> "str":
        return "STRING(MAX)" if length is None else f"STRING({length})"

    def _integer_type(self) -> "str":
        return "INT64"

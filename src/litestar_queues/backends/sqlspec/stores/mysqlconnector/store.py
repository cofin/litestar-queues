"""mysqlconnector SQLSpec queue stores."""

from sqlspec import sql

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("MysqlConnectorAsyncQueueStore", "MysqlConnectorSyncQueueStore")


class MysqlConnectorSyncQueueStore(SQLSpecQueueStore):
    """mysqlconnector sync SQLSpec queue statement store."""

    __slots__ = ()

    id_type = "VARCHAR(64)"
    indexed_text_type = "VARCHAR(255)"
    json_type = "JSON"
    timestamp_type = "VARCHAR(64)"
    error_type = "LONGTEXT"
    auto_native_json_columns = frozenset({"args_json", "kwargs_json", "metadata_json", "result_json"})

    def create_statements(self) -> list[str]:
        """Return statements that create mysqlconnector sync queue artifacts."""
        if not self._manage_schema:
            return []
        return [_create_table_statement(self)]

    def drop_statements(self) -> list[str]:
        """Return statements that drop mysqlconnector sync queue artifacts."""
        if not self._manage_schema:
            return []
        return [self._to_sql(sql.drop_table(self.table_name).if_exists())]


class MysqlConnectorAsyncQueueStore(SQLSpecQueueStore):
    """mysqlconnector async SQLSpec queue statement store."""

    __slots__ = ()

    id_type = "VARCHAR(64)"
    indexed_text_type = "VARCHAR(255)"
    json_type = "JSON"
    timestamp_type = "VARCHAR(64)"
    error_type = "LONGTEXT"
    auto_native_json_columns = frozenset({"args_json", "kwargs_json", "metadata_json", "result_json"})

    def create_statements(self) -> list[str]:
        """Return statements that create mysqlconnector async queue artifacts."""
        if not self._manage_schema:
            return []
        return [_create_table_statement(self)]

    def drop_statements(self) -> list[str]:
        """Return statements that drop mysqlconnector async queue artifacts."""
        if not self._manage_schema:
            return []
        return [self._to_sql(sql.drop_table(self.table_name).if_exists())]


def _create_table_statement(store: SQLSpecQueueStore) -> str:
    table_name = store.table_name
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {store._col("id")} VARCHAR(64) PRIMARY KEY,
        {store._col("task_name")} VARCHAR(255) NOT NULL,
        {store._col("args_json")} {store._payload_json_type("args_json")} NOT NULL,
        {store._col("kwargs_json")} {store._payload_json_type("kwargs_json")} NOT NULL,
        {store._col("queue")} VARCHAR(255) NOT NULL,
        {store._col("execution_backend")} VARCHAR(255) NOT NULL,
        {store._col("execution_profile")} VARCHAR(255),
        {store._col("execution_ref")} VARCHAR(255),
        {store._col("status")} VARCHAR(255) NOT NULL,
        {store._col("priority")} INTEGER NOT NULL,
        {store._col("max_retries")} INTEGER NOT NULL,
        {store._col("retry_count")} INTEGER NOT NULL,
        {store._col("scheduled_at")} VARCHAR(64),
        {store._col("created_at")} VARCHAR(64) NOT NULL,
        {store._col("started_at")} VARCHAR(64),
        {store._col("completed_at")} VARCHAR(64),
        {store._col("heartbeat_at")} VARCHAR(64),
        {store._col("result_json")} {store._result_json_type("result_json")} NOT NULL,
        {store._col("error")} LONGTEXT,
        {store._col("task_key")} VARCHAR(255) UNIQUE,
        {store._col("metadata_json")} {store._metadata_json_type("metadata_json")} NOT NULL,
        INDEX {store._index_name("pending")} (
            {store._col("status")}(32), {store._col("queue")}(191), {store._col("execution_backend")}(191),
            {store._col("scheduled_at")}, {store._col("priority")}, {store._col("created_at")}
        ),
        INDEX {store._index_name("heartbeat")} ({store._col("status")}(32), {store._col("heartbeat_at")})
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """

"""oracledb SQLSpec queue stores."""

from enum import Enum
from typing import Any, cast

from sqlspec.utils.serializers import from_json, to_json

from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("OracledbAsyncQueueStore", "OracledbSyncQueueStore")


class OracleJSONStorageType(str, Enum):
    """Oracle JSON storage modes."""

    JSON_NATIVE = "json"
    BLOB_JSON = "blob_json"
    BLOB_PLAIN = "blob"


def _queue_settings(config: Any) -> dict[str, Any]:
    extension_config = cast("dict[str, Any]", getattr(config, "extension_config", {}) or {})
    return cast("dict[str, Any]", extension_config.get(QUEUE_EXTENSION_NAME, {}) or {})


def _json_storage_from_settings(settings: dict[str, Any]) -> OracleJSONStorageType:
    configured = settings.get("json_storage")
    if configured == OracleJSONStorageType.JSON_NATIVE.value:
        return OracleJSONStorageType.JSON_NATIVE
    if configured in {OracleJSONStorageType.BLOB_PLAIN.value, "blob_plain"}:
        return OracleJSONStorageType.BLOB_PLAIN
    return OracleJSONStorageType.BLOB_JSON


def _json_column_type(column_name: str, storage_type: OracleJSONStorageType) -> str:
    if storage_type == OracleJSONStorageType.JSON_NATIVE:
        return "JSON"
    if storage_type == OracleJSONStorageType.BLOB_JSON:
        return f"BLOB CHECK ({column_name} IS JSON)"
    return "BLOB"


def _serialize_oracle_json(value: Any, storage_type: OracleJSONStorageType) -> str | bytes:
    if storage_type == OracleJSONStorageType.JSON_NATIVE:
        return to_json(value)
    return to_json(value, as_bytes=True)


def _deserialize_oracle_json(value: Any) -> Any:
    if value is None:
        return None
    read = getattr(value, "read", None)
    if callable(read):
        value = read()
    if isinstance(value, (dict, list)):
        return value
    return from_json(value)


def _index_name(store: SQLSpecQueueStore, suffix: str) -> str:
    return SQLSpecQueueStore._index_name(store, suffix)[:30]


def _create_table_block(store: SQLSpecQueueStore, storage_type: OracleJSONStorageType, in_memory: bool) -> str:
    table_name = store.table_name
    in_memory_clause = " INMEMORY PRIORITY HIGH" if in_memory else ""
    return f"""
    BEGIN
        EXECUTE IMMEDIATE 'CREATE TABLE {table_name} (
            id VARCHAR2(64) PRIMARY KEY,
            task_name VARCHAR2(255) NOT NULL,
            args_json {_json_column_type("args_json", storage_type)} NOT NULL,
            kwargs_json {_json_column_type("kwargs_json", storage_type)} NOT NULL,
            queue VARCHAR2(255) NOT NULL,
            execution_backend VARCHAR2(255) NOT NULL,
            execution_profile VARCHAR2(255),
            execution_ref VARCHAR2(255),
            status VARCHAR2(255) NOT NULL,
            priority NUMBER(10) NOT NULL,
            max_retries NUMBER(10) NOT NULL,
            retry_count NUMBER(10) NOT NULL,
            scheduled_at VARCHAR2(64),
            created_at VARCHAR2(64) NOT NULL,
            started_at VARCHAR2(64),
            completed_at VARCHAR2(64),
            heartbeat_at VARCHAR2(64),
            result_json {_json_column_type("result_json", storage_type)} NOT NULL,
            error VARCHAR2(4000),
            task_key VARCHAR2(255) UNIQUE,
            metadata_json {_json_column_type("metadata_json", storage_type)} NOT NULL
        ){in_memory_clause}';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -955 THEN
                RAISE;
            END IF;
    END;
    """


def _create_index_block(store: SQLSpecQueueStore, suffix: str, columns: str) -> str:
    return f"""
    BEGIN
        EXECUTE IMMEDIATE 'CREATE INDEX {_index_name(store, suffix)}
            ON {store.table_name}({columns})';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -955 THEN
                RAISE;
            END IF;
    END;
    """


def _drop_index_block(store: SQLSpecQueueStore, suffix: str) -> str:
    return f"""
    BEGIN
        EXECUTE IMMEDIATE 'DROP INDEX {_index_name(store, suffix)}';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -1418 THEN
                RAISE;
            END IF;
    END;
    """


def _drop_table_block(store: SQLSpecQueueStore) -> str:
    return f"""
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE {store.table_name}';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN
                RAISE;
            END IF;
    END;
    """


class OracledbSyncQueueStore(SQLSpecQueueStore):
    """oracledb sync SQLSpec queue statement store."""

    __slots__ = ("_in_memory", "_json_storage")

    id_type = "VARCHAR2(64)"
    indexed_text_type = "VARCHAR2(255)"
    integer_type = "NUMBER(10)"
    timestamp_type = "VARCHAR2(64)"
    error_type = "VARCHAR2(4000)"

    def __init__(self, config: Any, *, table_name: str | None = None) -> None:
        super().__init__(config, table_name=table_name)
        queue_settings = _queue_settings(config)
        self._in_memory = bool(queue_settings.get("in_memory", False))
        self._json_storage = _json_storage_from_settings(queue_settings)

    def create_statements(self) -> list[str]:
        """Return statements that create oracledb sync queue artifacts."""
        return [
            _create_table_block(self, self._json_storage, self._in_memory),
            _create_index_block(self, "pending", "status, queue, execution_backend, scheduled_at, priority, created_at"),
            _create_index_block(self, "heartbeat", "status, heartbeat_at"),
        ]

    def drop_statements(self) -> list[str]:
        """Return statements that drop oracledb sync queue artifacts."""
        return [_drop_index_block(self, "heartbeat"), _drop_index_block(self, "pending"), _drop_table_block(self)]

    def serialize_payload_json(self, value: Any) -> str | bytes:
        """Serialize Oracle payload JSON according to configured storage.

        Returns:
            A string for native JSON or bytes for BLOB-backed JSON.
        """
        return _serialize_oracle_json(value, self._json_storage)

    def serialize_result_json(self, value: Any) -> str | bytes:
        """Serialize Oracle result JSON according to configured storage.

        Returns:
            A string for native JSON or bytes for BLOB-backed JSON.
        """
        return _serialize_oracle_json(value, self._json_storage)

    def serialize_metadata_json(self, value: Any) -> str | bytes:
        """Serialize Oracle metadata JSON according to configured storage.

        Returns:
            A string for native JSON or bytes for BLOB-backed JSON.
        """
        return _serialize_oracle_json(value, self._json_storage)

    def deserialize_json(self, value: Any) -> Any:
        """Deserialize Oracle JSON, BLOB, or LOB values.

        Returns:
            The decoded Python JSON value.
        """
        return _deserialize_oracle_json(value)

    def _index_name(self, suffix: str) -> str:
        return _index_name(self, suffix)


class OracledbAsyncQueueStore(SQLSpecQueueStore):
    """oracledb async SQLSpec queue statement store."""

    __slots__ = ("_in_memory", "_json_storage")

    id_type = "VARCHAR2(64)"
    indexed_text_type = "VARCHAR2(255)"
    integer_type = "NUMBER(10)"
    timestamp_type = "VARCHAR2(64)"
    error_type = "VARCHAR2(4000)"

    def __init__(self, config: Any, *, table_name: str | None = None) -> None:
        super().__init__(config, table_name=table_name)
        queue_settings = _queue_settings(config)
        self._in_memory = bool(queue_settings.get("in_memory", False))
        self._json_storage = _json_storage_from_settings(queue_settings)

    def create_statements(self) -> list[str]:
        """Return statements that create oracledb async queue artifacts."""
        return [
            _create_table_block(self, self._json_storage, self._in_memory),
            _create_index_block(self, "pending", "status, queue, execution_backend, scheduled_at, priority, created_at"),
            _create_index_block(self, "heartbeat", "status, heartbeat_at"),
        ]

    def drop_statements(self) -> list[str]:
        """Return statements that drop oracledb async queue artifacts."""
        return [_drop_index_block(self, "heartbeat"), _drop_index_block(self, "pending"), _drop_table_block(self)]

    def serialize_payload_json(self, value: Any) -> str | bytes:
        """Serialize Oracle payload JSON according to configured storage.

        Returns:
            A string for native JSON or bytes for BLOB-backed JSON.
        """
        return _serialize_oracle_json(value, self._json_storage)

    def serialize_result_json(self, value: Any) -> str | bytes:
        """Serialize Oracle result JSON according to configured storage.

        Returns:
            A string for native JSON or bytes for BLOB-backed JSON.
        """
        return _serialize_oracle_json(value, self._json_storage)

    def serialize_metadata_json(self, value: Any) -> str | bytes:
        """Serialize Oracle metadata JSON according to configured storage.

        Returns:
            A string for native JSON or bytes for BLOB-backed JSON.
        """
        return _serialize_oracle_json(value, self._json_storage)

    def deserialize_json(self, value: Any) -> Any:
        """Deserialize Oracle JSON, BLOB, or LOB values.

        Returns:
            The decoded Python JSON value.
        """
        return _deserialize_oracle_json(value)

    def _index_name(self, suffix: str) -> str:
        return _index_name(self, suffix)

"""oracledb SQLSpec queue stores."""

from enum import Enum
from inspect import isawaitable
from typing import Any, ClassVar, Literal, cast

from sqlspec import sql
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import async_

from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore
from litestar_queues.exceptions import QueueConfigurationError

__all__ = ("OracledbAsyncQueueStore", "OracledbSyncQueueStore")

_ORACLE_IDENTIFIER_MAX_LENGTH = 30
_ORACLE_ERROR_MAX_LENGTH = 4000


class _OracledbQueueStore(SQLSpecQueueStore):
    """Shared Oracle DDL type hooks."""

    __slots__ = ("_json_storage",)

    _json_storage: "_OracleJSONStorageType | None"

    data_dictionary_dialect: "ClassVar[str | None]" = "oracle"
    identifier_quote_style: 'ClassVar[Literal["double", "backtick", "none"]]' = "none"

    def create_statements(self) -> "list[str]":
        """Return statements that create Oracle queue artifacts."""
        return self._create_statements_with_storage(self._configured_json_storage())

    async def create_statements_for_driver(self, driver: "Any") -> "list[str]":
        """Return schema statements using cached Oracle version-aware JSON storage."""
        storage_type = await self._detect_json_storage_type(driver)
        return self._create_statements_with_storage(storage_type)

    def drop_statements(self) -> "list[str]":
        """Return statements that drop Oracle queue artifacts."""
        if not self._manage_schema:
            return []
        return [_drop_index_block(self, "heartbeat"), _drop_index_block(self, "pending"), _drop_table_block(self)]

    def serialize_json(self, canonical: "str", value: "Any") -> "str | bytes":
        """Serialize Oracle JSON according to configured storage.

        Returns:
            A string for native JSON or bytes for BLOB-backed JSON.
        """
        return _serialize_oracle_json(value, self._configured_json_storage())

    def deserialize_json(self, canonical: "str", value: "Any") -> "Any":
        """Deserialize Oracle JSON, BLOB, or LOB values.

        Returns:
            The decoded Python JSON value.
        """
        if canonical in self._native_json_columns:
            return _deserialize_native_oracle_json(canonical, value)
        return _deserialize_oracle_json(value)

    def _string_type(self, length: "int | None" = None) -> "str":
        return "CLOB" if length is None else f"VARCHAR2({length})"

    def _integer_type(self) -> "str":
        return "NUMBER(10)"

    def _error_type(self) -> "str":
        return "VARCHAR2(4000)"

    def serialize_error(self, error: "str") -> "str":
        """Return an Oracle-safe error value for the VARCHAR2 error column."""
        return error[:_ORACLE_ERROR_MAX_LENGTH]

    def _index_name(self, suffix: "str") -> "str":
        return _index_name(self, suffix)

    def _configured_json_storage(self) -> "_OracleJSONStorageType":
        configured = self._json_storage
        return configured if configured is not None else _OracleJSONStorageType.BLOB_JSON

    def _create_statements_with_storage(self, storage_type: "_OracleJSONStorageType") -> "list[str]":
        if not self._manage_schema:
            return []
        self._apply_json_storage(storage_type)
        return [
            _create_table_block(self, storage_type, bool(getattr(self, "_in_memory", False))),
            _create_index_block(
                self,
                "pending",
                (
                    f"{self._col('status')}, {self._col('queue')}, {self._col('execution_backend')}, "
                    f"{self._col('scheduled_at')}, {self._col('priority')}, {self._col('created_at')}"
                ),
            ),
            _create_index_block(self, "heartbeat", f"{self._col('status')}, {self._col('heartbeat_at')}"),
        ]

    async def _detect_json_storage_type(self, driver: "Any") -> "_OracleJSONStorageType":
        configured = self._json_storage
        if configured is not None:
            return configured
        version_info = await _oracle_version_info(driver)
        return self._apply_json_storage(_json_storage_from_version_info(version_info))

    def _apply_json_storage(self, storage_type: "_OracleJSONStorageType") -> "_OracleJSONStorageType":
        self._json_storage = storage_type
        if storage_type in {_OracleJSONStorageType.JSON_NATIVE, _OracleJSONStorageType.BLOB_JSON}:
            self._native_json_columns |= frozenset({"args_json", "kwargs_json", "metadata_json", "result_json"})
        return storage_type

    def _select_claimable_unlimited(
        self, *, now: "Any", queue: "str | None" = None, execution_backend: "str | None" = None
    ) -> "Any":
        statement = (
            self
            ._select_all()
            .where_in(self._col("status"), ("pending", "scheduled"))
            .where(f"{self._col('scheduled_at')} IS NULL OR {self._col('scheduled_at')} <= :now", now=now)
        )
        if queue is not None:
            statement = statement.where_eq(self._col("queue"), queue)
        if execution_backend is not None:
            statement = statement.where_eq(self._col("execution_backend"), execution_backend)
        return statement.order_by(
            sql.raw(f"{self._col('priority')} DESC"), sql.raw(f"{self._col('created_at')} ASC")
        ).for_update(skip_locked=True)


class OracledbSyncQueueStore(_OracledbQueueStore):
    """oracledb sync SQLSpec queue statement store."""

    __slots__ = ("_in_memory",)

    _in_memory: "bool"

    def __init__(self, config: "Any", *, table_name: "str | None" = None, **kwargs: "Any") -> "None":
        super().__init__(config, table_name=table_name, **kwargs)
        queue_settings = _queue_settings(config)
        self._in_memory = bool(queue_settings.get("in_memory", False))
        self._json_storage = _json_storage_from_settings(queue_settings)
        if self._json_storage is not None:
            self._apply_json_storage(self._json_storage)

    @property
    def supports_skip_locked(self) -> "bool":
        """Bridge-managed sync Oracle claims stay on the CAS path."""
        return False


class OracledbAsyncQueueStore(_OracledbQueueStore):
    """oracledb async SQLSpec queue statement store."""

    __slots__ = ("_in_memory",)

    claim_select_stream_chunk_size: "ClassVar[int | None]" = 1
    _in_memory: "bool"

    def __init__(self, config: "Any", *, table_name: "str | None" = None, **kwargs: "Any") -> "None":
        super().__init__(config, table_name=table_name, **kwargs)
        _disable_async_lob_fetching(config)
        queue_settings = _queue_settings(config)
        self._in_memory = bool(queue_settings.get("in_memory", False))
        self._json_storage = _json_storage_from_settings(queue_settings)
        if self._json_storage is not None:
            self._apply_json_storage(self._json_storage)

    def select_claimable(
        self, *, now: "Any", limit: "int", queue: "str | None" = None, execution_backend: "str | None" = None
    ) -> "Any":
        """Return an ordered Oracle row-locking query consumed through a one-row stream."""
        return self._select_claimable_unlimited(now=now, queue=queue, execution_backend=execution_backend)


class _OracleJSONStorageType(str, Enum):
    JSON_NATIVE = "json"
    BLOB_JSON = "blob_json"
    BLOB_PLAIN = "blob"


_ORACLE_CONTAINER_JSON_COLUMNS = frozenset({"args_json", "kwargs_json", "metadata_json"})


def _queue_settings(config: "Any") -> "dict[str, Any]":
    extension_config = cast("dict[str, Any]", getattr(config, "extension_config", {}) or {})
    return cast("dict[str, Any]", extension_config.get(QUEUE_EXTENSION_NAME, {}) or {})


def _disable_async_lob_fetching(config: "Any") -> "None":
    driver_features = getattr(config, "driver_features", None)
    if isinstance(driver_features, dict):
        driver_features["fetch_lobs"] = False


def _json_storage_from_settings(settings: "dict[str, Any]") -> "_OracleJSONStorageType | None":
    configured = settings.get("json_storage")
    if configured is None:
        return None
    if configured == _OracleJSONStorageType.JSON_NATIVE.value:
        return _OracleJSONStorageType.JSON_NATIVE
    if configured == _OracleJSONStorageType.BLOB_JSON.value:
        return _OracleJSONStorageType.BLOB_JSON
    if configured == _OracleJSONStorageType.BLOB_PLAIN.value:
        return _OracleJSONStorageType.BLOB_PLAIN
    valid = ", ".join(storage_type.value for storage_type in _OracleJSONStorageType)
    msg = f"Invalid Oracle json_storage {configured!r}; expected one of: {valid}."
    raise QueueConfigurationError(msg)


def _json_storage_from_version_info(version_info: "Any") -> "_OracleJSONStorageType":
    if version_info is None:
        return _OracleJSONStorageType.BLOB_JSON
    supports_native_json = getattr(version_info, "supports_native_json", None)
    if callable(supports_native_json) and supports_native_json():
        return _OracleJSONStorageType.JSON_NATIVE
    supports_json_blob = getattr(version_info, "supports_json_blob", None)
    if callable(supports_json_blob) and supports_json_blob():
        return _OracleJSONStorageType.BLOB_JSON
    return _OracleJSONStorageType.BLOB_PLAIN


async def _oracle_version_info(driver: "Any") -> "Any":
    sync_driver = getattr(driver, "_driver", None)
    if sync_driver is not None:
        return await async_(_sync_oracle_version_info)(sync_driver)
    return await _async_oracle_version_info(driver)


def _sync_oracle_version_info(driver: "Any") -> "Any":
    detect_version = getattr(driver, "_detect_oracle_version", None)
    if callable(detect_version):
        return detect_version()
    data_dictionary = getattr(driver, "data_dictionary", None)
    get_version = getattr(data_dictionary, "get_version", None)
    if callable(get_version):
        return get_version(driver)
    return None


async def _async_oracle_version_info(driver: "Any") -> "Any":
    detect_version = getattr(driver, "_detect_oracle_version", None)
    if callable(detect_version):
        version_info = detect_version()
        if isawaitable(version_info):
            return await version_info
        return version_info
    data_dictionary = getattr(driver, "data_dictionary", None)
    get_version = getattr(data_dictionary, "get_version", None)
    if callable(get_version):
        version_info = get_version(driver)
        if isawaitable(version_info):
            return await version_info
        return version_info
    return None


def _json_column_type(column_name: "str", storage_type: "_OracleJSONStorageType") -> "str":
    if storage_type == _OracleJSONStorageType.JSON_NATIVE:
        return "JSON"
    if storage_type == _OracleJSONStorageType.BLOB_JSON:
        return f"BLOB CHECK ({column_name} IS JSON)"
    return "BLOB"


def _serialize_oracle_json(value: "Any", storage_type: "_OracleJSONStorageType") -> "str | bytes":
    if storage_type == _OracleJSONStorageType.JSON_NATIVE:
        return to_json(value)
    return to_json(value, as_bytes=True)


def _deserialize_oracle_json(value: "Any") -> "Any":
    if value is None:
        return None
    read = getattr(value, "read", None)
    if callable(read):
        value = read()
    if isinstance(value, (dict, list)):
        return value
    return from_json(value)


def _deserialize_native_oracle_json(canonical: "str", value: "Any") -> "Any":
    if value is None:
        return None
    read = getattr(value, "read", None)
    if callable(read):
        value = read()
    if isinstance(value, bytes):
        return from_json(value)
    if isinstance(value, str):
        stripped = value.lstrip()
        if canonical in _ORACLE_CONTAINER_JSON_COLUMNS or stripped.startswith(("{", "[", '"')):
            return from_json(value)
        return value
    return value


def _index_name(store: "SQLSpecQueueStore", suffix: "str") -> "str":
    suffix_text = f"_{suffix}"
    prefix = "ix_"
    table_name = store.table_name.replace(".", "_")
    table_budget = _ORACLE_IDENTIFIER_MAX_LENGTH - len(prefix) - len(suffix_text)
    if table_budget < 1:
        return f"{prefix}{suffix_text.lstrip('_')}"[:_ORACLE_IDENTIFIER_MAX_LENGTH]
    return f"{prefix}{table_name[:table_budget]}{suffix_text}"


def _create_table_block(store: "SQLSpecQueueStore", storage_type: "_OracleJSONStorageType", in_memory: "bool") -> "str":
    table_name = store.table_name
    in_memory_clause = " INMEMORY PRIORITY HIGH" if in_memory else ""
    return f"""
    BEGIN
        EXECUTE IMMEDIATE 'CREATE TABLE {table_name} (
            {store._col("id")} {store._id_type()} PRIMARY KEY,
            {store._col("task_name")} {store._indexed_text_type()} NOT NULL,
            {store._col("args_json")} {_json_column_type(store._col("args_json"), storage_type)} NOT NULL,
            {store._col("kwargs_json")} {_json_column_type(store._col("kwargs_json"), storage_type)} NOT NULL,
            {store._col("queue")} {store._indexed_text_type()} NOT NULL,
            {store._col("execution_backend")} {store._indexed_text_type()} NOT NULL,
            {store._col("execution_profile")} {store._indexed_text_type()},
            {store._col("execution_ref")} {store._indexed_text_type()},
            {store._col("status")} {store._indexed_text_type()} NOT NULL,
            {store._col("priority")} {store._integer_type()} NOT NULL,
            {store._col("max_retries")} {store._integer_type()} NOT NULL,
            {store._col("retry_count")} {store._integer_type()} NOT NULL,
            {store._col("scheduled_at")} {store._timestamp_type()},
            {store._col("created_at")} {store._timestamp_type()} NOT NULL,
            {store._col("started_at")} {store._timestamp_type()},
            {store._col("completed_at")} {store._timestamp_type()},
            {store._col("heartbeat_at")} {store._timestamp_type()},
            {store._col("result_json")} {_json_column_type(store._col("result_json"), storage_type)} NOT NULL,
            {store._col("error")} {store._error_type()},
            {store._col("task_key")} {store._indexed_text_type()} UNIQUE,
            {store._col("metadata_json")} {_json_column_type(store._col("metadata_json"), storage_type)} NOT NULL
        ){in_memory_clause}';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -955 THEN
                RAISE;
            END IF;
    END;
    """


def _create_index_block(store: "SQLSpecQueueStore", suffix: "str", columns: "str") -> "str":
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


def _drop_index_block(store: "SQLSpecQueueStore", suffix: "str") -> "str":
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


def _drop_table_block(store: "SQLSpecQueueStore") -> "str":
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

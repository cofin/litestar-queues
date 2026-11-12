"""Oracle DDL choice tests for the SQLSpec queue backend.

Verifies BLOB-with-IS-JSON, JSON column, BLOB plain, and INMEMORY hint behaviors
across the sync + async oracledb SQLSpec adapters. Operates entirely on
constructed fake configs (no Oracle service required).
"""

from types import SimpleNamespace
from typing import Any, ClassVar, cast

import pytest

pytest.importorskip("sqlspec")

from sqlspec.migrations import SchemaTarget

from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.stores import create_queue_store
from litestar_queues.backends.sqlspec.stores.oracledb import OracledbAsyncQueueStore, OracledbSyncQueueStore
from litestar_queues.exceptions import QueueConfigurationError


class FakeOracleConfig(SimpleNamespace):
    """Structural config used by Oracle store dispatch tests."""

    is_async: ClassVar[bool] = False
    extension_config: "dict[str, object]"
    statement_config: "SimpleNamespace"
    connection_config: "dict[str, object]"
    driver_features: "dict[str, object]"


class FakeOracleVersionInfo:
    """Oracle version feature probe used by storage-detection tests."""

    def __init__(self, *, native_json: "bool" = False, json_blob: "bool" = True) -> "None":
        self.native_json = native_json
        self.json_blob = json_blob

    def supports_native_json(self) -> "bool":
        return self.native_json

    def supports_json_blob(self) -> "bool":
        return self.json_blob


class FakeOracleDriver:
    """Fake Oracle driver exposing SQLSpec's cached version hook shape."""

    def __init__(self, version_info: "FakeOracleVersionInfo | None") -> "None":
        self.calls = 0
        self.version_info = version_info

    async def _detect_oracle_version(self) -> "FakeOracleVersionInfo | None":
        self.calls += 1
        return self.version_info


class FakeSyncOracleDriver:
    """Fake sync Oracle driver wrapped by the backend sync bridge."""

    def __init__(self, version_info: "FakeOracleVersionInfo | None") -> "None":
        self.calls = 0
        self.version_info = version_info

    def _detect_oracle_version(self) -> "FakeOracleVersionInfo | None":
        self.calls += 1
        return self.version_info


class FakeManagedDriver:
    """Minimal stand-in for the backend's sync-driver bridge."""

    def __init__(self, driver: "FakeSyncOracleDriver") -> "None":
        self._driver = driver


class FakeDataDictionaryDriver:
    """Fake sync Oracle driver exposing ``data_dictionary.get_version``."""

    def __init__(self, version_info: "FakeOracleVersionInfo | None") -> "None":
        self.calls = 0
        self.version_info = version_info
        self.data_dictionary = SimpleNamespace(get_version=self._get_version)

    def _get_version(self, driver: "object") -> "FakeOracleVersionInfo | None":
        assert driver is self
        self.calls += 1
        return self.version_info


class FakeAsyncDataDictionaryDriver:
    """Fake async Oracle driver exposing async ``data_dictionary.get_version``."""

    def __init__(self, version_info: "FakeOracleVersionInfo | None") -> "None":
        self.calls = 0
        self.version_info = version_info
        self.data_dictionary = SimpleNamespace(get_version=self._get_version)

    async def _get_version(self, driver: "object") -> "FakeOracleVersionInfo | None":
        assert driver is self
        self.calls += 1
        return self.version_info


def _fail_provide_session(*_args: "Any", **_kwargs: "Any") -> "Any":
    msg = "provide_session must not be called during prepare_schema"
    raise AssertionError(msg)


def _fake_oracle_config(
    *, config_type_name: "str", extension_config: "dict[str, object] | None" = None
) -> "FakeOracleConfig":
    config_type = cast(
        "type[FakeOracleConfig]",
        type(
            config_type_name,
            (FakeOracleConfig,),
            {"__module__": "sqlspec.adapters.oracledb.config", "is_async": "Async" in config_type_name},
        ),
    )
    config = config_type()
    config.extension_config = extension_config or {}
    config.statement_config = SimpleNamespace(dialect="oracle")
    config.connection_config = {}
    config.driver_features = {}
    return config


@pytest.mark.parametrize(
    ("config_type_name", "expected_store_type"),
    (("OracleSyncConfig", OracledbSyncQueueStore), ("OracleAsyncConfig", OracledbAsyncQueueStore)),
)
@pytest.mark.parametrize(
    ("queue_settings", "expected_json_fragment", "expected_serialized_type"),
    (
        ({}, "BLOB CHECK ({column} IS JSON)", bytes),
        ({"json_storage": "json", "in_memory": True}, "JSON", str),
        ({"json_storage": "blob"}, "BLOB", bytes),
    ),
)
def test_sqlspec_backend_oracledb_json_storage_avoids_clob_and_honors_settings(
    config_type_name: "str",
    expected_store_type: "type[object]",
    queue_settings: "dict[str, object]",
    expected_json_fragment: "str",
    expected_serialized_type: "type[object]",
) -> "None":
    store = create_queue_store(
        _fake_oracle_config(config_type_name=config_type_name, extension_config={QUEUE_EXTENSION_NAME: queue_settings}),
        table_name="queue_tasks",
    )

    ddl = "\n".join(store.create_statements())

    assert isinstance(store, expected_store_type)
    assert "CLOB" not in ddl
    for column_name in ("task_args", "task_kwargs", "result", "metadata"):
        expected_column_type = expected_json_fragment.format(column=column_name)
        assert f"{column_name} {expected_column_type} NOT NULL" in ddl
    assert ("INMEMORY PRIORITY HIGH" in ddl) is bool(queue_settings.get("in_memory"))
    serialized = store.serialize_json("kwargs_json", {"ok": True})
    assert isinstance(serialized, expected_serialized_type)
    assert store.deserialize_json("kwargs_json", serialized) == {"ok": True}
    if queue_settings.get("json_storage") != "blob":
        assert store.deserialize_json("result_json", "ok") == "ok"


def test_sqlspec_backend_oracledb_async_store_disables_lob_fetching() -> "None":
    config = _fake_oracle_config(config_type_name="OracleAsyncConfig")
    config.driver_features["fetch_lobs"] = True

    store = create_queue_store(config, table_name="queue_tasks")

    assert isinstance(store, OracledbAsyncQueueStore)
    assert config.driver_features["fetch_lobs"] is False


def test_sqlspec_backend_oracledb_rejects_invalid_json_storage() -> "None":
    with pytest.raises(QueueConfigurationError, match="Invalid Oracle json_storage"):
        create_queue_store(
            _fake_oracle_config(
                config_type_name="OracleAsyncConfig", extension_config={QUEUE_EXTENSION_NAME: {"json_storage": "clob"}}
            ),
            table_name="queue_tasks",
        )


def test_sqlspec_backend_oracledb_native_json_preserves_scalar_string_results() -> "None":
    store = create_queue_store(
        _fake_oracle_config(
            config_type_name="OracleAsyncConfig", extension_config={QUEUE_EXTENSION_NAME: {"json_storage": "json"}}
        ),
        table_name="queue_tasks",
    )

    assert store.deserialize_json("result_json", "123") == "123"
    assert store.deserialize_json("result_json", '"123"') == "123"
    assert store.deserialize_json("kwargs_json", '{"ok":true}') == {"ok": True}


def test_sqlspec_backend_oracledb_index_names_stay_unique_after_truncation() -> "None":
    store = create_queue_store(
        _fake_oracle_config(config_type_name="OracleAsyncConfig"), table_name="queue_tasks_with_a_very_long_name"
    )

    pending_index = store._index_name("pending")
    heartbeat_index = store._index_name("heartbeat")

    assert len(pending_index) <= 30
    assert len(heartbeat_index) <= 30
    assert pending_index.endswith("_pending")
    assert heartbeat_index.endswith("_heartbeat")
    assert pending_index != heartbeat_index


def test_sqlspec_backend_oracledb_truncates_error_text_for_failure_statements() -> "None":
    store = create_queue_store(_fake_oracle_config(config_type_name="OracleAsyncConfig"), table_name="queue_tasks")
    long_error = "x" * 4005

    failed = store.fail_task(task_id="task-1", completed_at="now", heartbeat_at="now", error=long_error).build(
        dialect="oracle"
    )
    retry = store.retry_task(task_id="task-1", error=long_error, retry_count=1).build(dialect="oracle")

    assert len(failed.parameters["error"]) == 4000
    assert failed.parameters["error"] == long_error[:4000]
    assert len(retry.parameters["error"]) == 4000
    assert retry.parameters["error"] == long_error[:4000]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("driver", "expected_json_fragment"),
    (
        (FakeOracleDriver(FakeOracleVersionInfo(native_json=True)), "JSON"),
        (FakeOracleDriver(FakeOracleVersionInfo(json_blob=True)), "BLOB CHECK (task_args IS JSON)"),
        (FakeOracleDriver(FakeOracleVersionInfo(native_json=False, json_blob=False)), "BLOB"),
        (FakeManagedDriver(FakeSyncOracleDriver(FakeOracleVersionInfo(native_json=True))), "JSON"),
    ),
)
async def test_sqlspec_backend_oracledb_detects_json_storage_from_driver_version(
    driver: "Any", expected_json_fragment: "str"
) -> "None":
    store = create_queue_store(_fake_oracle_config(config_type_name="OracleAsyncConfig"), table_name="queue_tasks")

    assert isinstance(store, OracledbAsyncQueueStore)
    first_ddl = "\n".join(await store.create_statements_for_driver(driver))
    second_ddl = "\n".join(await store.create_statements_for_driver(driver))

    assert expected_json_fragment in first_ddl
    assert second_ddl == first_ddl
    raw_driver = getattr(driver, "_driver", driver)
    assert raw_driver.calls == 1


@pytest.mark.parametrize(
    ("driver_factory", "expected_column_type", "expected_serialized_type", "expects_native_scalar"),
    (
        (lambda: FakeSyncOracleDriver(FakeOracleVersionInfo(native_json=True)), "JSON", str, True),
        (
            lambda: FakeSyncOracleDriver(FakeOracleVersionInfo(json_blob=True)),
            "BLOB CHECK (task_args IS JSON)",
            bytes,
            True,
        ),
        (lambda: FakeSyncOracleDriver(FakeOracleVersionInfo(native_json=False, json_blob=False)), "BLOB", bytes, False),
        (lambda: FakeSyncOracleDriver(None), "BLOB CHECK (task_args IS JSON)", bytes, True),
        (lambda: FakeManagedDriver(FakeSyncOracleDriver(FakeOracleVersionInfo(native_json=True))), "JSON", str, True),
        (lambda: FakeDataDictionaryDriver(FakeOracleVersionInfo(native_json=True)), "JSON", str, True),
    ),
)
def test_prepare_schema_sync_detects_and_caches_without_session(
    driver_factory: "Any",
    expected_column_type: "str",
    expected_serialized_type: "type[object]",
    expects_native_scalar: "bool",
) -> "None":
    config = _fake_oracle_config(config_type_name="OracleSyncConfig")
    config.provide_session = _fail_provide_session
    store = create_queue_store(config, table_name="queue_tasks")
    assert isinstance(store, OracledbSyncQueueStore)

    driver = driver_factory()
    raw_driver = getattr(driver, "_driver", driver)

    store.prepare_schema_sync(driver)
    assert raw_driver.calls == 1
    store.prepare_schema_sync(driver)
    assert raw_driver.calls == 1

    ddl = store.create_table_statement()
    assert f"task_args {expected_column_type} NOT NULL" in ddl
    assert isinstance(store.serialize_json("kwargs_json", {"ok": True}), expected_serialized_type)
    if expects_native_scalar:
        assert store.deserialize_json("result_json", "ok") == "ok"


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("config_type_name", "expected_store_type", "is_async"),
    (("OracleSyncConfig", OracledbSyncQueueStore, False), ("OracleAsyncConfig", OracledbAsyncQueueStore, True)),
)
async def test_create_table_statement_before_prepare_schema_preserves_23c_detection(
    config_type_name: "str", expected_store_type: "type[object]", is_async: "bool"
) -> "None":
    store = create_queue_store(_fake_oracle_config(config_type_name=config_type_name), table_name="queue_tasks")
    assert isinstance(store, expected_store_type)
    assert isinstance(store, (OracledbSyncQueueStore, OracledbAsyncQueueStore))

    initial_ddl = store.create_table_statement()
    assert "task_args BLOB CHECK (task_args IS JSON) NOT NULL" in initial_ddl
    assert store._json_storage is None

    version_info = FakeOracleVersionInfo(native_json=True)
    driver: "FakeOracleDriver | FakeSyncOracleDriver"
    if is_async:
        driver = FakeOracleDriver(version_info)
        await store.prepare_schema_async(driver)
    else:
        driver = FakeSyncOracleDriver(version_info)
        store.prepare_schema_sync(driver)

    assert driver.calls == 1
    upgraded_ddl = store.create_table_statement()
    assert "task_args JSON NOT NULL" in upgraded_ddl
    assert "BLOB CHECK" not in upgraded_ddl


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("driver_factory", "expected_column_type"),
    (
        (lambda: FakeOracleDriver(FakeOracleVersionInfo(native_json=True)), "JSON"),
        (lambda: FakeOracleDriver(FakeOracleVersionInfo(json_blob=True)), "BLOB CHECK (task_args IS JSON)"),
        (lambda: FakeOracleDriver(FakeOracleVersionInfo(native_json=False, json_blob=False)), "BLOB"),
        (lambda: FakeOracleDriver(None), "BLOB CHECK (task_args IS JSON)"),
        (lambda: FakeManagedDriver(FakeSyncOracleDriver(FakeOracleVersionInfo(native_json=True))), "JSON"),
        (lambda: FakeAsyncDataDictionaryDriver(FakeOracleVersionInfo(native_json=True)), "JSON"),
    ),
)
async def test_prepare_schema_async_detects_and_caches(driver_factory: "Any", expected_column_type: "str") -> "None":
    config = _fake_oracle_config(config_type_name="OracleAsyncConfig")
    config.provide_session = _fail_provide_session
    store = create_queue_store(config, table_name="queue_tasks")
    assert isinstance(store, OracledbAsyncQueueStore)

    driver = driver_factory()
    raw_driver = getattr(driver, "_driver", driver)

    await store.prepare_schema_async(driver)
    assert raw_driver.calls == 1
    await store.prepare_schema_async(driver)
    assert raw_driver.calls == 1
    assert f"task_args {expected_column_type} NOT NULL" in store.create_table_statement()


@pytest.mark.anyio
@pytest.mark.parametrize(("config_type_name", "is_async"), (("OracleSyncConfig", False), ("OracleAsyncConfig", True)))
@pytest.mark.parametrize(
    ("json_storage", "expected_column_type"),
    (("json", "JSON"), ("blob_json", "BLOB CHECK (task_args IS JSON)"), ("blob", "BLOB")),
)
async def test_explicit_json_storage_override_skips_driver_probe(
    config_type_name: "str", is_async: "bool", json_storage: "str", expected_column_type: "str"
) -> "None":
    config = _fake_oracle_config(
        config_type_name=config_type_name, extension_config={QUEUE_EXTENSION_NAME: {"json_storage": json_storage}}
    )
    config.provide_session = _fail_provide_session
    store = create_queue_store(config, table_name="queue_tasks")
    assert isinstance(store, (OracledbSyncQueueStore, OracledbAsyncQueueStore))

    driver: "FakeOracleDriver | FakeSyncOracleDriver"
    if is_async:
        driver = FakeOracleDriver(FakeOracleVersionInfo(native_json=True))
        await store.prepare_schema_async(driver)
    else:
        driver = FakeSyncOracleDriver(FakeOracleVersionInfo(native_json=True))
        store.prepare_schema_sync(driver)

    assert driver.calls == 0
    assert f"task_args {expected_column_type} NOT NULL" in store.create_table_statement()


def test_oracle_create_table_and_companion_indexes_split() -> "None":
    store = create_queue_store(
        _fake_oracle_config(config_type_name="OracleAsyncConfig"), table_name="queue_tasks_with_a_very_long_name"
    )
    assert isinstance(store, OracledbAsyncQueueStore)

    table_ddl = store.create_table_statement()
    indexes = store.companion_indexes()

    assert store.sqlglot_dialect() == "oracle"
    assert "CREATE TABLE queue_tasks_with_a_very_long_name" in table_ddl
    assert "CREATE INDEX" not in table_ddl
    assert len(indexes) == 5
    (
        (pending_name, pending_sql),
        (heartbeat_name, heartbeat_sql),
        (repair_name, repair_sql),
        (created_name, created_sql),
        (list_name, list_sql),
    ) = indexes
    assert pending_name.endswith("_pending")
    assert heartbeat_name.endswith("_heartbeat")
    assert repair_name.endswith("_dispatch_repair")
    assert created_name.endswith("_created")
    assert list_name.endswith("_list")
    assert len(pending_name) <= 30
    assert len(heartbeat_name) <= 30
    assert len(repair_name) <= 30
    assert len(created_name) <= 30
    assert len(list_name) <= 30
    assert len({pending_name, heartbeat_name, repair_name, created_name, list_name}) == 5
    assert f"CREATE INDEX {pending_name}" in pending_sql
    assert f"CREATE INDEX {heartbeat_name}" in heartbeat_sql
    assert f"CREATE INDEX {repair_name}" in repair_sql
    assert f"CREATE INDEX {created_name}" in created_sql
    assert f"CREATE INDEX {list_name}" in list_sql
    assert store.create_statements() == [table_ddl, pending_sql, heartbeat_sql, repair_sql, created_sql, list_sql]


@pytest.mark.parametrize(
    ("json_storage", "in_memory"), (("json", False), ("blob_json", False), ("blob", False), ("json", True))
)
def test_oracle_schema_target_from_ddl_parses_25_columns(json_storage: "str", in_memory: "bool") -> "None":
    store = create_queue_store(
        _fake_oracle_config(
            config_type_name="OracleAsyncConfig",
            extension_config={QUEUE_EXTENSION_NAME: {"json_storage": json_storage, "in_memory": in_memory}},
        ),
        table_name="queue_tasks",
    )
    ddl = store.create_table_statement()
    target = SchemaTarget.from_ddl(store.table_name, ddl, dialect=store.sqlglot_dialect())

    assert target.create_table is not None
    assert len(target.create_table.columns) == 25
    assert ("INMEMORY PRIORITY HIGH" in ddl) is in_memory


@pytest.mark.parametrize("config_type_name", ("OracleSyncConfig", "OracleAsyncConfig"))
def test_oracle_manage_schema_false(config_type_name: "str") -> "None":
    store = create_queue_store(
        _fake_oracle_config(config_type_name=config_type_name), table_name="queue_tasks", manage_schema=False
    )

    assert store.create_table_statement() == ""
    assert store.companion_indexes() == ()
    assert store.create_statements() == []
    assert store.drop_statements() == []


@pytest.mark.parametrize("config_type_name", ("OracleSyncConfig", "OracleAsyncConfig"))
def test_oracle_listing_index_names_stay_unique_within_30_chars(config_type_name: "str") -> "None":
    store = create_queue_store(
        _fake_oracle_config(config_type_name=config_type_name), table_name="queue_tasks_with_a_very_long_name"
    )

    created_index = store._index_name("created")
    list_index = store._index_name("list")
    pending_index = store._index_name("pending")
    heartbeat_index = store._index_name("heartbeat")
    repair_index = store._index_name("dispatch_repair")

    assert created_index == "ix_queue_tasks_with_a__created"
    assert list_index == "ix_queue_tasks_with_a_ver_list"
    assert len(created_index) <= 30
    assert len(list_index) <= 30
    assert len({created_index, list_index, pending_index, heartbeat_index, repair_index}) == 5


@pytest.mark.parametrize("config_type_name", ("OracleSyncConfig", "OracleAsyncConfig"))
@pytest.mark.parametrize(
    ("column_map", "expected_created_cols", "expected_list_cols"),
    (
        (None, "created_at, id", "status, queue, created_at, id"),
        (
            {"status": "state", "queue": "lane", "created_at": "created", "id": "task_id"},
            "created, task_id",
            "state, lane, created, task_id",
        ),
    ),
)
def test_oracle_emits_and_drops_listing_indexes_without_desc(
    config_type_name: "str",
    column_map: "dict[str, str] | None",
    expected_created_cols: "str",
    expected_list_cols: "str",
) -> "None":
    store = create_queue_store(
        _fake_oracle_config(config_type_name=config_type_name), table_name="queue_tasks", column_map=column_map
    )

    companion = dict(store.companion_indexes())
    created_sql = companion["ix_queue_tasks_created"]
    list_sql = companion["ix_queue_tasks_list"]

    assert f"ON queue_tasks({expected_created_cols})" in created_sql
    assert f"ON queue_tasks({expected_list_cols})" in list_sql
    assert "DESC" not in created_sql
    assert "DESC" not in list_sql

    statements = store.create_statements()
    assert created_sql in statements
    assert list_sql in statements

    drops = store.drop_statements()
    drop_table_pos = next(index for index, stmt in enumerate(drops) if "DROP TABLE queue_tasks" in stmt)
    drop_list_pos = next(index for index, stmt in enumerate(drops) if "DROP INDEX ix_queue_tasks_list" in stmt)
    drop_created_pos = next(index for index, stmt in enumerate(drops) if "DROP INDEX ix_queue_tasks_created" in stmt)
    assert drop_list_pos < drop_table_pos
    assert drop_created_pos < drop_table_pos


@pytest.mark.anyio
async def test_oracle_create_statements_for_driver_includes_listing_indexes() -> "None":
    store = create_queue_store(_fake_oracle_config(config_type_name="OracleAsyncConfig"), table_name="queue_tasks")
    assert isinstance(store, OracledbAsyncQueueStore)

    statements = await store.create_statements_for_driver(FakeOracleDriver(FakeOracleVersionInfo(native_json=True)))
    joined = "\n".join(statements)

    assert "CREATE INDEX ix_queue_tasks_created" in joined
    assert "ON queue_tasks(created_at, id)" in joined
    assert "CREATE INDEX ix_queue_tasks_list" in joined
    assert "ON queue_tasks(status, queue, created_at, id)" in joined

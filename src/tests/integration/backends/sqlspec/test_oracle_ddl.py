"""Oracle DDL choice tests for the SQLSpec queue backend.

Verifies BLOB-with-IS-JSON, JSON column, BLOB plain, and INMEMORY hint behaviors
across the sync + async oracledb SQLSpec adapters. Operates entirely on
constructed fake configs (no Oracle service required).
"""

from types import SimpleNamespace
from typing import Any, cast

import pytest

pytest.importorskip("sqlspec")

from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.stores import OracledbAsyncQueueStore, OracledbSyncQueueStore, create_queue_store


class FakeOracleConfig(SimpleNamespace):
    """Structural config used by Oracle store dispatch tests."""

    extension_config: dict[str, object]
    statement_config: SimpleNamespace
    connection_config: dict[str, object]


class FakeOracleVersionInfo:
    """Oracle version feature probe used by storage-detection tests."""

    def __init__(self, *, native_json: bool = False, json_blob: bool = True) -> None:
        self.native_json = native_json
        self.json_blob = json_blob

    def supports_native_json(self) -> bool:
        return self.native_json

    def supports_json_blob(self) -> bool:
        return self.json_blob


class FakeOracleDriver:
    """Fake Oracle driver exposing SQLSpec's cached version hook shape."""

    def __init__(self, version_info: FakeOracleVersionInfo | None) -> None:
        self.calls = 0
        self.version_info = version_info

    async def _detect_oracle_version(self) -> FakeOracleVersionInfo | None:
        self.calls += 1
        return self.version_info


class FakeSyncOracleDriver:
    """Fake sync Oracle driver wrapped by the backend sync bridge."""

    def __init__(self, version_info: FakeOracleVersionInfo | None) -> None:
        self.calls = 0
        self.version_info = version_info

    def _detect_oracle_version(self) -> FakeOracleVersionInfo | None:
        self.calls += 1
        return self.version_info


class FakeManagedDriver:
    """Minimal stand-in for the backend's sync-driver bridge."""

    def __init__(self, driver: FakeSyncOracleDriver) -> None:
        self._driver = driver


def _fake_oracle_config(
    *, config_type_name: str, extension_config: dict[str, object] | None = None
) -> FakeOracleConfig:
    config_type = cast(
        "type[FakeOracleConfig]",
        type(config_type_name, (FakeOracleConfig,), {"__module__": "sqlspec.adapters.oracledb.config"}),
    )
    config = config_type()
    config.extension_config = extension_config or {}
    config.statement_config = SimpleNamespace(dialect="oracle")
    config.connection_config = {}
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
    config_type_name: str,
    expected_store_type: type[object],
    queue_settings: dict[str, object],
    expected_json_fragment: str,
    expected_serialized_type: type[object],
) -> None:
    store = create_queue_store(
        _fake_oracle_config(config_type_name=config_type_name, extension_config={QUEUE_EXTENSION_NAME: queue_settings}),
        table_name="queue_tasks",
    )

    ddl = "\n".join(store.create_statements())

    assert isinstance(store, expected_store_type)
    assert "CLOB" not in ddl
    for column_name in ("args_json", "kwargs_json", "result_json", "metadata_json"):
        expected_column_type = expected_json_fragment.format(column=column_name)
        assert f"{column_name} {expected_column_type} NOT NULL" in ddl
    assert ("INMEMORY PRIORITY HIGH" in ddl) is bool(queue_settings.get("in_memory"))
    serialized = store.serialize_json("kwargs_json", {"ok": True})
    assert isinstance(serialized, expected_serialized_type)
    assert store.deserialize_json("kwargs_json", serialized) == {"ok": True}
    if queue_settings.get("json_storage") != "blob":
        assert store.deserialize_json("result_json", "ok") == "ok"


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("driver", "expected_json_fragment"),
    (
        (FakeOracleDriver(FakeOracleVersionInfo(native_json=True)), "JSON"),
        (FakeOracleDriver(FakeOracleVersionInfo(json_blob=True)), "BLOB CHECK (args_json IS JSON)"),
        (FakeOracleDriver(FakeOracleVersionInfo(native_json=False, json_blob=False)), "BLOB"),
        (FakeManagedDriver(FakeSyncOracleDriver(FakeOracleVersionInfo(native_json=True))), "JSON"),
    ),
)
async def test_sqlspec_backend_oracledb_detects_json_storage_from_driver_version(
    driver: Any, expected_json_fragment: str
) -> None:
    store = create_queue_store(_fake_oracle_config(config_type_name="OracleAsyncConfig"), table_name="queue_tasks")

    first_ddl = "\n".join(await store.create_statements_for_driver(driver))
    second_ddl = "\n".join(await store.create_statements_for_driver(driver))

    assert expected_json_fragment in first_ddl
    assert second_ddl == first_ddl
    raw_driver = getattr(driver, "_driver", driver)
    assert raw_driver.calls == 1

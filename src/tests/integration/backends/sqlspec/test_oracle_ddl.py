"""Oracle DDL choice tests for the SQLSpec queue backend.

Verifies BLOB-with-IS-JSON, JSON column, BLOB plain, and INMEMORY hint behaviors
across the sync + async oracledb SQLSpec adapters. Operates entirely on
constructed fake configs (no Oracle service required).
"""

from types import SimpleNamespace
from typing import cast

import pytest

pytest.importorskip("sqlspec")

from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.store import (
    OracledbAsyncQueueStore,
    OracledbSyncQueueStore,
    create_queue_store,
)


class FakeOracleConfig(SimpleNamespace):
    """Structural config used by Oracle store dispatch tests."""

    extension_config: dict[str, object]
    statement_config: SimpleNamespace
    connection_config: dict[str, object]


def _fake_oracle_config(
    *,
    config_type_name: str,
    extension_config: dict[str, object] | None = None,
) -> FakeOracleConfig:
    config_type = cast(
        "type[FakeOracleConfig]",
        type(
            config_type_name,
            (FakeOracleConfig,),
            {"__module__": "sqlspec.adapters.oracledb.config"},
        ),
    )
    config = config_type()
    config.extension_config = extension_config or {}
    config.statement_config = SimpleNamespace(dialect="oracle")
    config.connection_config = {}
    return config


@pytest.mark.parametrize(
    ("config_type_name", "expected_store_type"),
    (
        ("OracleSyncConfig", OracledbSyncQueueStore),
        ("OracleAsyncConfig", OracledbAsyncQueueStore),
    ),
)
@pytest.mark.parametrize(
    ("queue_settings", "expected_json_fragment", "expected_serialized_type"),
    (
        ({}, "BLOB CHECK ({column} IS JSON)", bytes),
        ({"json_storage": "json", "in_memory": True}, "JSON", str),
        ({"json_storage": "blob"}, "BLOB", bytes),
        ({"json_storage": "blob_plain"}, "BLOB", bytes),
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
        _fake_oracle_config(
            config_type_name=config_type_name,
            extension_config={QUEUE_EXTENSION_NAME: queue_settings},
        ),
        table_name="queue_tasks",
    )

    ddl = "\n".join(store.create_statements())

    assert isinstance(store, expected_store_type)
    assert "CLOB" not in ddl
    for column_name in ("args_json", "kwargs_json", "result_json", "metadata_json"):
        expected_column_type = expected_json_fragment.format(column=column_name)
        assert f"{column_name} {expected_column_type} NOT NULL" in ddl
    assert ("INMEMORY PRIORITY HIGH" in ddl) is bool(queue_settings.get("in_memory"))
    serialized = store.serialize_json_column("kwargs_json", {"ok": True})
    assert isinstance(serialized, expected_serialized_type)
    assert store.deserialize_json(serialized) == {"ok": True}

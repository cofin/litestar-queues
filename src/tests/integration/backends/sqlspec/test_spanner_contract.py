"""Optional live contract coverage for the SQLSpec Spanner backend."""

import os
from contextlib import suppress
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

pytest.importorskip("sqlspec")
pytest.importorskip("google.cloud.spanner_v1")

from google.api_core.exceptions import GoogleAPICallError
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import spanner
from sqlspec.adapters.spanner import SpannerSyncConfig, spanner_json

from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend
from litestar_queues.backends.sqlspec.stores.spanner import SpannerQueueStore
from tests.integration._names import table_name_for_test

if TYPE_CHECKING:
    from pytest_databases.docker.spanner import SpannerService

pytestmark = pytest.mark.anyio

_SPANNER_ENV_VARS = ("SPANNER_PROJECT", "SPANNER_INSTANCE", "SPANNER_DATABASE")


def test_sqlspec_spanner_store_deserializes_native_json_wrappers() -> "None":
    store = SpannerQueueStore(config=cast("Any", SimpleNamespace(extension_config={})))

    args = store.deserialize_json("args_json", spanner_json([1, {"ok": True}]))
    kwargs = store.deserialize_json("kwargs_json", spanner_json({"ok": True}))

    assert isinstance(args, list)
    assert args == [1, {"ok": True}]
    assert type(kwargs) is dict
    assert kwargs == {"ok": True}
    assert store.deserialize_json("result_json", spanner_json("done")) == "done"
    assert store.deserialize_json("result_json", spanner_json(None)) is None


async def test_sqlspec_spanner_backend_live_contract_round_trip() -> "None":
    try:
        await _run_spanner_contract(
            _spanner_env_connection_config(), table_name=table_name_for_test("litestar_queue_spanner", "live", __name__)
        )
    except (DefaultCredentialsError, GoogleAPICallError, OSError) as exc:
        pytest.skip(f"Spanner live test unavailable: {exc}")


async def test_sqlspec_spanner_backend_emulator_contract_round_trip(spanner_service: "SpannerService") -> "None":
    _ensure_spanner_emulator_database(spanner_service)
    await _run_spanner_contract(
        _spanner_emulator_connection_config(spanner_service),
        table_name=table_name_for_test("litestar_queue_spanner", "emulator", __name__),
    )


async def _run_spanner_contract(connection_config: "dict[str, object]", *, table_name: "str") -> "None":
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=SpannerSyncConfig(connection_config=connection_config, driver_features={"timeout": 30.0}),
            queue_table_name=table_name,
        )
    )
    try:
        await backend.open()
        await backend.create_schema()
        record = await backend.enqueue("tasks.spanner.live", kwargs={"ok": True}, metadata={"source": "live"})
        claimed = await backend.claim_task(record.id)
        assert claimed is not None
        assert claimed.status == "running"
        await backend.complete_task(claimed.id, result={"done": True})

        stored = await backend.get_task(record.id)
        assert stored is not None
        assert stored.status == "completed"
        assert stored.kwargs == {"ok": True}
        assert stored.metadata == {"source": "live"}
        assert stored.result == {"done": True}
    finally:
        await backend.close()


def _spanner_env_connection_config() -> "dict[str, object]":
    missing = [name for name in _SPANNER_ENV_VARS if not os.getenv(name)]
    if missing:
        joined = ", ".join(missing)
        pytest.skip(f"Spanner live test requires {joined} to be set")
    return {
        "project": os.environ["SPANNER_PROJECT"],
        "instance_id": os.environ["SPANNER_INSTANCE"],
        "database_id": os.environ["SPANNER_DATABASE"],
        "disable_builtin_metrics": True,
    }


def _spanner_emulator_connection_config(spanner_service: "SpannerService") -> "dict[str, object]":
    return {
        "project": spanner_service.project,
        "instance_id": spanner_service.instance_name,
        "database_id": spanner_service.database_name,
        "credentials": spanner_service.credentials,
        "client_options": spanner_service.client_options,
        "disable_builtin_metrics": True,
    }


def _ensure_spanner_emulator_database(spanner_service: "SpannerService") -> "None":
    from google.api_core.exceptions import AlreadyExists

    client = cast(
        "Any",
        spanner.Client(
            project=spanner_service.project,
            credentials=spanner_service.credentials,
            client_options=spanner_service.client_options,
            disable_builtin_metrics=True,
        ),
    )
    try:
        instance = client.instance(
            spanner_service.instance_name,
            configuration_name="emulator-config",
            display_name=spanner_service.instance_name,
            node_count=1,
        )
        with suppress(AlreadyExists):
            instance.create().result()

        database = instance.database(spanner_service.database_name)
        with suppress(AlreadyExists):
            database.create().result()
    finally:
        client.close()

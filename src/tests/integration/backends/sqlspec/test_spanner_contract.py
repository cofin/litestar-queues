"""Optional live contract coverage for the SQLSpec Spanner backend."""

import os

import pytest

pytest.importorskip("sqlspec")
pytest.importorskip("google.cloud.spanner_v1")

from google.api_core.exceptions import GoogleAPICallError
from google.auth.exceptions import DefaultCredentialsError
from sqlspec.adapters.spanner import SpannerSyncConfig

from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend

pytestmark = pytest.mark.anyio

_SPANNER_ENV_VARS = ("SPANNER_PROJECT", "SPANNER_INSTANCE", "SPANNER_DATABASE")


def _spanner_connection_config() -> "dict[str, object]":
    missing = [name for name in _SPANNER_ENV_VARS if not os.getenv(name)]
    if missing:
        joined = ", ".join(missing)
        pytest.skip(f"Spanner live test requires {joined} to be set")
    return {
        "project": os.environ["SPANNER_PROJECT"],
        "instance_id": os.environ["SPANNER_INSTANCE"],
        "database_id": os.environ["SPANNER_DATABASE"],
    }


async def test_sqlspec_spanner_backend_live_contract_round_trip() -> "None":
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(config=SpannerSyncConfig(connection_config=_spanner_connection_config()))
    )
    try:
        try:
            await backend.open()
        except (DefaultCredentialsError, GoogleAPICallError, OSError) as exc:
            pytest.skip(f"Spanner live test unavailable: {exc}")

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

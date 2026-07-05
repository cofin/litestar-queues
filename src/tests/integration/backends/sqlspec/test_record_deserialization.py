"""SQLSpec queue record deserialization validation tests."""

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("sqlspec")

from litestar_queues.backends.sqlspec import SQLSpecQueueBackend


def test_sqlspec_backend_rejects_scalar_args_json_from_row() -> "None":
    backend = SQLSpecQueueBackend()
    backend._store = _PassthroughJSONStore()

    with pytest.raises(ValueError, match="args_json"):
        backend._record_from_row(_sqlspec_row(args_json="abc"))


class _PassthroughJSONStore:
    def deserialize_json(self, canonical: "str", value: "Any") -> "Any":
        return value


def _sqlspec_row(**overrides: "Any") -> "dict[str, Any]":
    now = datetime.now(timezone.utc).isoformat()
    row: "dict[str, Any]" = {
        "id": str(uuid4()),
        "task_name": "tasks.example",
        "args_json": [],
        "kwargs_json": {},
        "metadata_json": {},
        "queue": "default",
        "execution_backend": "local",
        "execution_profile": None,
        "execution_ref": None,
        "status": "pending",
        "priority": 0,
        "max_retries": 0,
        "retry_count": 0,
        "scheduled_at": now,
        "created_at": now,
        "started_at": None,
        "completed_at": None,
        "heartbeat_at": None,
        "result_json": None,
        "error": None,
        "task_key": None,
    }
    row.update(overrides)
    return row

"""Extension-migration tests for the SQLSpec queue backend.

Covers the ``ext_litestar_queues_0001`` packaged migration script: that it
dispatches through the per-adapter queue store, that the packaged asset is
discoverable, and that running it twice is idempotent (the
``run_migrations``-twice flow is exercised by the contract suite).
"""

import importlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("sqlspec")

from litestar_queues.backends.sqlspec.schema import migration_paths

pytestmark = pytest.mark.anyio


def _fake_adapter_config(
    adapter_name: str,
    *,
    dialect: str | None = None,
    config_type_name: str | None = None,
    connection_config: dict[str, Any] | None = None,
    extension_config: dict[str, Any] | None = None,
) -> Any:
    config_type = type(
        config_type_name or f"Fake{adapter_name.title().replace('_', '')}Config",
        (),
        {"__module__": f"sqlspec.adapters.{adapter_name}.config"},
    )
    config = config_type()
    config.extension_config = extension_config or {}
    config.statement_config = SimpleNamespace(dialect=dialect)
    config.connection_config = connection_config or {}
    return config


async def test_sqlspec_backend_migration_uses_adapter_specific_queue_store() -> None:
    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    context = SimpleNamespace(config=_fake_adapter_config("bigquery", dialect="bigquery"))

    statements = await migration.up(context)

    assert "CLUSTER BY" in statements[0]
    assert not any("CREATE INDEX" in statement for statement in statements)


async def test_sqlspec_backend_exposes_packaged_migration_assets() -> None:
    paths = tuple(Path(path) for path in migration_paths())

    assert [path.name for path in paths] == ["0001_create_queue_tasks.py"]
    content = paths[0].read_text()
    assert "create_queue_store" in content
    assert "return SQLSpecQueueStore(" not in content
    assert "CREATE TABLE IF NOT EXISTS litestar_queue_tasks" not in content

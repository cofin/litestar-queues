"""Advanced Alchemy custom queue model integration tests."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Protocol, cast

import pytest

pytest.importorskip("advanced_alchemy")
pytest.importorskip("aiosqlite")
pytest.importorskip("sqlalchemy")

from advanced_alchemy.base import UUIDAuditBase
from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig

from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyBackendConfig, AdvancedAlchemyQueueBackend
from litestar_queues.backends.advanced_alchemy.mixins import QueueTaskModelMixin
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from pathlib import Path

    from sqlalchemy import Table

pytestmark = pytest.mark.anyio


def _sqlite_config(path: "Path") -> "SQLAlchemyAsyncConfig":
    return SQLAlchemyAsyncConfig(connection_string=f"sqlite+aiosqlite:///{path}")


class MappedQueueModel(Protocol):
    """Structural type for app-owned SQLAlchemy queue models."""

    __table__: "Table"


def _table(model: "type[object]") -> "Table":
    return cast("MappedQueueModel", model).__table__


def _column_names(model: "type[object]") -> "set[str]":
    return {column.name for column in _table(model).columns}


def _index_names(model: "type[object]") -> "set[str]":
    return {str(index.name) for index in _table(model).indexes if index.name is not None}


class BareQueueTaskModel(UUIDAuditBase, QueueTaskModelMixin):
    __tablename__ = "bare_queue_tasks"


class AppQueueTask(UUIDAuditBase, QueueTaskModelMixin):
    __tablename__ = "app_queue_tasks"


class CustomQueueTaskModel(UUIDAuditBase, QueueTaskModelMixin):
    __tablename__ = "custom_queue_tasks"


class AbstractQueueTaskModel(QueueTaskModelMixin):
    __abstract__ = True


def test_queue_task_model_mixin_adds_queue_schema_to_app_owned_model() -> "None":
    assert {"id", "created_at", "updated_at"} <= _column_names(BareQueueTaskModel)
    assert {"task_name", "kwargs_json", "execution_ref", "metadata_json"} <= _column_names(BareQueueTaskModel)
    assert {
        "ix_bare_queue_tasks_pending",
        "ix_bare_queue_tasks_heartbeat",
        "ix_bare_queue_tasks_execution",
    } <= _index_names(BareQueueTaskModel)


def test_queue_task_model_mixin_composes_with_custom_advanced_alchemy_base() -> "None":
    assert {"id", "created_at", "updated_at"} <= _column_names(AppQueueTask)
    assert {"task_name", "kwargs_json", "execution_ref", "metadata_json"} <= _column_names(AppQueueTask)
    assert {
        "ix_app_queue_tasks_pending",
        "ix_app_queue_tasks_heartbeat",
        "ix_app_queue_tasks_execution",
    } <= _index_names(AppQueueTask)


def test_advanced_alchemy_backend_uses_default_model_class(tmp_path: "Path") -> "None":
    from litestar_queues.backends.advanced_alchemy import QueueTaskModel

    backend = AdvancedAlchemyQueueBackend(
        backend_config=AdvancedAlchemyBackendConfig(sqlalchemy_config=_sqlite_config(tmp_path / "default-model.db"))
    )

    assert backend._model_class is QueueTaskModel
    assert _table(QueueTaskModel).name == "litestar_queue_task"


async def test_advanced_alchemy_backend_uses_supplied_model_class(tmp_path: "Path") -> "None":
    backend = AdvancedAlchemyQueueBackend(
        backend_config=AdvancedAlchemyBackendConfig(
            sqlalchemy_config=_sqlite_config(tmp_path / "custom-model.db"),
            model_class=CustomQueueTaskModel,
            create_schema=True,
        )
    )
    await backend.open()
    try:
        enqueued = await backend.enqueue("tasks.custom", kwargs={"tenant": "acme"}, key="tenant:acme")
        claimed = await backend.claim_task(enqueued.id)
        completed = await backend.complete_task(enqueued.id, result={"ok": True})
    finally:
        await backend.close()

    assert claimed is not None
    assert claimed.status == "running"
    assert completed is not None
    assert completed.result == {"ok": True}
    assert _table(CustomQueueTaskModel).name == "custom_queue_tasks"


async def test_advanced_alchemy_requeues_heartbeat_at_exact_stale_cutoff(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    from litestar_queues.backends.advanced_alchemy import service as service_module

    fixed_now = datetime(2026, 5, 25, tzinfo=timezone.utc)
    monkeypatch.setattr(service_module, "_utc_now", lambda: fixed_now)
    backend = AdvancedAlchemyQueueBackend(
        backend_config=AdvancedAlchemyBackendConfig(
            sqlalchemy_config=_sqlite_config(tmp_path / "stale-cutoff.db"),
            model_class=CustomQueueTaskModel,
            create_schema=True,
        )
    )
    await backend.open()
    try:
        enqueued = await backend.enqueue("tasks.stale", max_retries=1)
        claimed = await backend.claim_task(enqueued.id)
        requeued = await backend.requeue_stale_running(stale_after=timedelta(seconds=0))
        stored = await backend.get_task(enqueued.id)
    finally:
        await backend.close()

    assert claimed is not None
    assert claimed.heartbeat_at == fixed_now
    assert requeued.requeued == 1
    assert stored is not None
    assert stored.status == "pending"


def test_advanced_alchemy_backend_rejects_invalid_model_class(tmp_path: "Path") -> "None":
    config = _sqlite_config(tmp_path / "invalid-model.db")

    with pytest.raises(QueueConfigurationError, match="QueueTaskModelMixin"):
        AdvancedAlchemyQueueBackend(
            backend_config=AdvancedAlchemyBackendConfig(sqlalchemy_config=config, model_class=object)
        )

    with pytest.raises(QueueConfigurationError, match="__tablename__"):
        AdvancedAlchemyQueueBackend(
            backend_config=AdvancedAlchemyBackendConfig(sqlalchemy_config=config, model_class=AbstractQueueTaskModel)
        )

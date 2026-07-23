"""Advanced Alchemy custom queue model integration tests."""

import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Protocol, cast
from uuid import uuid4

import pytest

pytest.importorskip("advanced_alchemy")
pytest.importorskip("aiosqlite")
pytest.importorskip("sqlalchemy")

from advanced_alchemy.base import UUIDAuditBase
from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig
from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from litestar_queues import EventHistoryConfig, QueueConfig
from litestar_queues.backends.advanced_alchemy import SQLAlchemyBackend, SQLAlchemyBackendConfig
from litestar_queues.backends.advanced_alchemy.mixins import QueueEventHistoryModelMixin, QueueTaskModelMixin
from litestar_queues.events import QueueEventsConfig
from litestar_queues.exceptions import QueueConfigurationError
from tests.integration.backends.advanced_alchemy._aa_schema import create_tables

if TYPE_CHECKING:
    from pathlib import Path

    from sqlalchemy import Table

pytestmark = pytest.mark.anyio


class MappedQueueModel(Protocol):
    """Structural type for app-owned SQLAlchemy queue models."""

    __table__: "Table"


class BareQueueTaskModel(UUIDAuditBase, QueueTaskModelMixin):
    __tablename__ = "bare_queue_task"


class AppQueueTask(UUIDAuditBase, QueueTaskModelMixin):
    __tablename__ = "app_queue_task"


class CustomQueueTaskModel(UUIDAuditBase, QueueTaskModelMixin):
    __tablename__ = "custom_queue_task"


class CustomQueueEventHistoryModel(UUIDAuditBase, QueueEventHistoryModelMixin):
    __tablename__ = "custom_queue_task_event_log"


class AbstractQueueTaskModel(QueueTaskModelMixin):
    __abstract__ = True


class AbstractQueueEventHistoryModel(QueueEventHistoryModelMixin):
    __abstract__ = True


class RenamedColumnQueueTaskModel(UUIDAuditBase, QueueTaskModelMixin):
    __tablename__ = "renamed_column_queue_task"

    task_name: "Mapped[str]" = mapped_column("queue_task_name", String(length=500), nullable=False)


def test_queue_task_model_mixin_adds_queue_schema_to_app_owned_model() -> "None":
    assert {"id", "created_at", "updated_at"} <= _column_names(BareQueueTaskModel)
    assert {"task_name", "task_args", "task_kwargs", "execution_ref", "metadata"} <= _column_names(BareQueueTaskModel)
    assert {
        "ix_bare_queue_task_pending",
        "ix_bare_queue_task_heartbeat",
        "ix_bare_queue_task_execution",
    } <= _index_names(BareQueueTaskModel)


def test_queue_task_model_mixin_composes_with_custom_advanced_alchemy_base() -> "None":
    assert {"id", "created_at", "updated_at"} <= _column_names(AppQueueTask)
    assert {"task_name", "task_args", "task_kwargs", "execution_ref", "metadata"} <= _column_names(AppQueueTask)
    assert {"ix_app_queue_task_pending", "ix_app_queue_task_heartbeat", "ix_app_queue_task_execution"} <= _index_names(
        AppQueueTask
    )


def test_queue_event_history_model_mixin_adds_generic_event_history_schema() -> "None":
    assert {"id", "created_at", "updated_at"} <= _column_names(CustomQueueEventHistoryModel)
    assert {
        "event_id",
        "event_type",
        "task_id",
        "task_name",
        "queue",
        "worker_id",
        "execution_backend",
        "execution_profile",
        "level",
        "message",
        "detail_json",
        "progress_current",
        "progress_total",
        "progress_percent",
        "sequence",
        "occurred_at",
    } <= _column_names(CustomQueueEventHistoryModel)
    assert {"stage", "duration_ms"}.isdisjoint(_column_names(CustomQueueEventHistoryModel))
    assert {
        "ix_custom_queue_task_event_log_task_id",
        "ix_custom_queue_task_event_log_task_name",
        "ix_custom_queue_task_event_log_event_type",
        "ix_custom_queue_task_event_log_occurred_at",
    } <= _index_names(CustomQueueEventHistoryModel)


def test_queue_task_model_mixin_preserves_canonical_python_attributes() -> "None":
    assert _mapped_column_name(BareQueueTaskModel, "task_key") == "task_key"
    assert _mapped_column_name(BareQueueTaskModel, "task_name") == "task_name"
    assert _mapped_column_name(BareQueueTaskModel, "args_json") == "task_args"
    assert _mapped_column_name(BareQueueTaskModel, "kwargs_json") == "task_kwargs"
    assert _mapped_column_name(BareQueueTaskModel, "execution_backend") == "execution_backend"
    assert _mapped_column_name(BareQueueTaskModel, "result_json") == "result"
    assert _mapped_column_name(BareQueueTaskModel, "metadata_json") == "metadata"


def test_advanced_alchemy_backend_uses_default_model_class(tmp_path: "Path") -> "None":
    from litestar_queues.backends.advanced_alchemy import QueueEventHistoryModel, QueueTaskModel

    backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(sqlalchemy_config=_sqlite_config(tmp_path / "default-model.db"))
    )

    assert backend._model_class is QueueTaskModel
    assert backend._event_history_model_class is QueueEventHistoryModel
    assert _table(QueueTaskModel).name == "queue_task"
    assert _table(QueueEventHistoryModel).name == "queue_task_event_history"


async def test_advanced_alchemy_schema_is_created_by_native_sqlalchemy_lifecycle(tmp_path: "Path") -> "None":
    disabled_db = tmp_path / "event-log-disabled.db"
    disabled_config = _sqlite_config(disabled_db)
    await create_tables(disabled_config, CustomQueueTaskModel)
    disabled_backend = SQLAlchemyBackend(
        QueueConfig(),
        backend_config=SQLAlchemyBackendConfig(
            sqlalchemy_config=disabled_config,
            model_class=CustomQueueTaskModel,
            event_history_model_class=CustomQueueEventHistoryModel,
        ),
    )

    await disabled_backend.open()
    await disabled_backend.close()

    assert "custom_queue_task" in _sqlite_table_names(disabled_db)
    assert "custom_queue_task_event_log" not in _sqlite_table_names(disabled_db)

    enabled_db = tmp_path / "event-log-enabled.db"
    enabled_config = _sqlite_config(enabled_db)
    await create_tables(enabled_config, CustomQueueTaskModel, CustomQueueEventHistoryModel)
    enabled_backend = SQLAlchemyBackend(
        QueueConfig(events=QueueEventsConfig(history=EventHistoryConfig())),
        backend_config=SQLAlchemyBackendConfig(
            sqlalchemy_config=enabled_config,
            model_class=CustomQueueTaskModel,
            event_history_model_class=CustomQueueEventHistoryModel,
        ),
    )

    await enabled_backend.open()
    await enabled_backend.close()

    assert "custom_queue_task" in _sqlite_table_names(enabled_db)
    assert "custom_queue_task_event_log" in _sqlite_table_names(enabled_db)


async def test_advanced_alchemy_backend_uses_supplied_model_class(tmp_path: "Path") -> "None":
    config = _sqlite_config(tmp_path / "custom-model.db")
    await create_tables(config, CustomQueueTaskModel)
    backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(sqlalchemy_config=config, model_class=CustomQueueTaskModel)
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
    assert _table(CustomQueueTaskModel).name == "custom_queue_task"


async def test_advanced_alchemy_keyed_enqueue_recovers_from_racing_insert(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    from litestar_queues.backends.advanced_alchemy.service import QueueTaskService

    config = _sqlite_config(tmp_path / "key-race.db")
    await create_tables(config, CustomQueueTaskModel)
    backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(sqlalchemy_config=config, model_class=CustomQueueTaskModel)
    )
    select_count = 0
    select_gate = asyncio.Event()
    original_select_task_by_key = QueueTaskService._select_task_by_key

    async def select_task_by_key_with_race(self: "QueueTaskService", key: "str") -> "Any | None":
        nonlocal select_count
        model = await original_select_task_by_key(self, key)
        if key == "race:key" and model is None:
            select_count += 1
            if select_count == 2:
                select_gate.set()
            await asyncio.wait_for(select_gate.wait(), timeout=5)
        return model

    monkeypatch.setattr(QueueTaskService, "_select_task_by_key", select_task_by_key_with_race)

    await backend.open()
    try:
        first, second = await asyncio.gather(
            backend.enqueue("tasks.race", key="race:key"), backend.enqueue("tasks.race", key="race:key")
        )
        stored = await backend.get_task_by_key("race:key")
    finally:
        await backend.close()

    assert first.id == second.id
    assert stored is not None
    assert stored.id == first.id


def test_advanced_alchemy_insert_values_use_mapped_attributes_for_renamed_columns() -> "None":
    from litestar_queues.backends.advanced_alchemy.service import _model_insert_values, _serialize_json

    model = RenamedColumnQueueTaskModel(
        id=uuid4(),
        task_name="tasks.renamed",
        args_json=_serialize_json([]),
        kwargs_json=_serialize_json({}),
        queue="default",
        execution_backend="local",
        status="pending",
        priority=0,
        max_retries=0,
        retry_count=0,
        result_json=_serialize_json(None),
        task_key="renamed:key",
        metadata_json=_serialize_json({}),
    )

    values = _model_insert_values(model, RenamedColumnQueueTaskModel)

    assert values["queue_task_name"] == "tasks.renamed"
    assert "task_name" not in values


async def test_advanced_alchemy_core_updates_touch_updated_at(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    from litestar_queues.backends.advanced_alchemy import service as service_module

    claim_time = datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)
    config = _sqlite_config(tmp_path / "updated-at.db")
    await create_tables(config, CustomQueueTaskModel)
    backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(sqlalchemy_config=config, model_class=CustomQueueTaskModel)
    )

    await backend.open()
    try:
        record = await backend.enqueue("tasks.audit")
        monkeypatch.setattr(service_module, "_utc_now", lambda: claim_time)
        claimed = await backend.claim_task(record.id)
        async with backend._service() as service:
            model = await service._select_task(record.id)
    finally:
        await backend.close()

    assert claimed is not None
    assert model is not None
    assert _as_utc(model.updated_at) == claim_time


async def test_advanced_alchemy_requeues_heartbeat_at_exact_stale_cutoff(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    from litestar_queues.backends.advanced_alchemy import service as service_module

    fixed_now = datetime(2026, 5, 25, tzinfo=timezone.utc)
    monkeypatch.setattr(service_module, "_utc_now", lambda: fixed_now)
    config = _sqlite_config(tmp_path / "stale-cutoff.db")
    await create_tables(config, CustomQueueTaskModel)
    backend = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(sqlalchemy_config=config, model_class=CustomQueueTaskModel)
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
        SQLAlchemyBackend(backend_config=SQLAlchemyBackendConfig(sqlalchemy_config=config, model_class=object))

    with pytest.raises(QueueConfigurationError, match="__tablename__"):
        SQLAlchemyBackend(
            backend_config=SQLAlchemyBackendConfig(sqlalchemy_config=config, model_class=AbstractQueueTaskModel)
        )

    with pytest.raises(QueueConfigurationError, match="QueueEventHistoryModelMixin"):
        SQLAlchemyBackend(
            backend_config=SQLAlchemyBackendConfig(sqlalchemy_config=config, event_history_model_class=object)
        )

    with pytest.raises(QueueConfigurationError, match="__tablename__"):
        SQLAlchemyBackend(
            backend_config=SQLAlchemyBackendConfig(
                sqlalchemy_config=config, event_history_model_class=AbstractQueueEventHistoryModel
            )
        )


def _sqlite_config(path: "Path") -> "SQLAlchemyAsyncConfig":
    return SQLAlchemyAsyncConfig(connection_string=f"sqlite+aiosqlite:///{path}")


def _table(model: "type[object]") -> "Table":
    return cast("MappedQueueModel", model).__table__


def _column_names(model: "type[object]") -> "set[str]":
    return {column.name for column in _table(model).columns}


def _index_names(model: "type[object]") -> "set[str]":
    return {str(index.name) for index in _table(model).indexes if index.name is not None}


def _sqlite_table_names(path: "Path") -> "set[str]":
    with sqlite3.connect(path) as connection:
        rows = connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        return {cast("str", row[0]) for row in rows}


def _mapped_column_name(model: "type[object]", attribute_name: "str") -> "str":
    return str(getattr(model, attribute_name).property.columns[0].name)


def _as_utc(value: "datetime") -> "datetime":
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)

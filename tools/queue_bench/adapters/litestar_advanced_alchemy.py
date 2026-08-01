"""Advanced Alchemy benchmark adapter with adopter-owned queue models."""

import re
from dataclasses import replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig
    from sqlalchemy import MetaData
    from sqlalchemy.ext.asyncio import AsyncEngine

    from tools.queue_bench.adapters.base import AdapterRequest, AdapterResult

_POSTGRES_IDENTIFIER_MAX_LENGTH = 63


async def run(request: "AdapterRequest") -> "AdapterResult":
    """Run one core sample through the Advanced Alchemy queue backend.

    Returns:
        Timed result with Advanced Alchemy-specific comparison metadata.
    """
    _validate_request(request)
    sqlalchemy_config, metadata, models = _build_backend(request)
    engine = sqlalchemy_config.get_engine()
    try:
        await _create_schema(engine, request.namespace, metadata)

        from litestar_queues.backends.advanced_alchemy import SQLAlchemyBackendConfig
        from tools.queue_bench.adapters.litestar_queues import _run as run_core_scenario

        backend_config = SQLAlchemyBackendConfig(
            sqlalchemy_config=sqlalchemy_config,
            model_class=models[0],
            event_history_model_class=models[1],
            maintenance_model_class=models[2],
            task_reservation_model_class=models[3],
            worker_wakeups=False,
        )
        result = await run_core_scenario(request, backend_config)
        metadata_result = {
            **result.metadata,
            "comparison_class": "feature-cost",
            "driver": f"advanced-alchemy-{request.backend_variant}",
            "persistence_stack": "advanced-alchemy",
            "schema": request.namespace,
        }
        return replace(result, metadata=metadata_result)
    finally:
        try:
            await _drop_schema(engine, request.namespace)
        finally:
            await engine.dispose()


def _validate_request(request: "AdapterRequest") -> None:
    if request.system != "litestar-queues" or request.backend != "postgres":
        msg = "advanced-alchemy profile requires Litestar Queues with PostgreSQL"
        raise ValueError(msg)
    if request.backend_variant not in {"psycopg", "asyncpg"}:
        msg = "advanced-alchemy profile requires an explicit psycopg or asyncpg backend variant"
        raise ValueError(msg)
    if (
        len(request.namespace) > _POSTGRES_IDENTIFIER_MAX_LENGTH
        or re.fullmatch(r"lqb_[a-z0-9_]+", request.namespace) is None
    ):
        msg = "advanced-alchemy schema must be an lqb_ prefixed lowercase PostgreSQL identifier"
        raise ValueError(msg)


def _build_backend(
    request: "AdapterRequest",
) -> "tuple[SQLAlchemyAsyncConfig, MetaData, tuple[type[object], type[object], type[object], type[object]]]":
    from advanced_alchemy.base import UUIDAuditBase, create_registry
    from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig
    from sqlalchemy.engine import make_url

    from litestar_queues.backends.advanced_alchemy import (
        QueueEventHistoryModelMixin,
        QueueMaintenanceModelMixin,
        QueueTaskModelMixin,
        QueueTaskReservationModelMixin,
    )

    model_registry = create_registry()
    model_registry.metadata.schema = request.namespace

    class BenchmarkBase(UUIDAuditBase):
        __abstract__ = True
        metadata = model_registry.metadata
        registry = model_registry

    class BenchmarkQueueTask(BenchmarkBase, QueueTaskModelMixin):
        __tablename__ = "queue_task"

    class BenchmarkQueueEventHistory(BenchmarkBase, QueueEventHistoryModelMixin):
        __tablename__ = "queue_task_event_history"

    class BenchmarkQueueMaintenance(BenchmarkBase, QueueMaintenanceModelMixin):
        __tablename__ = "queue_maintenance"

    class BenchmarkQueueTaskReservation(BenchmarkBase, QueueTaskReservationModelMixin):
        __tablename__ = "queue_task_reservation"

    driver_name = "postgresql+asyncpg" if request.backend_variant == "asyncpg" else "postgresql+psycopg"
    connection_string = make_url(request.dsn).set(drivername=driver_name).render_as_string(hide_password=False)
    sqlalchemy_config = SQLAlchemyAsyncConfig(connection_string=connection_string, metadata=model_registry.metadata)
    return (
        sqlalchemy_config,
        model_registry.metadata,
        (BenchmarkQueueTask, BenchmarkQueueEventHistory, BenchmarkQueueMaintenance, BenchmarkQueueTaskReservation),
    )


async def _create_schema(engine: "AsyncEngine", schema: str, metadata: "MetaData") -> None:
    from sqlalchemy.schema import CreateSchema

    async with engine.begin() as connection:
        await connection.execute(CreateSchema(schema, if_not_exists=True))
        await connection.run_sync(metadata.create_all)


async def _drop_schema(engine: "AsyncEngine", schema: str) -> None:
    from sqlalchemy.schema import DropSchema

    async with engine.begin() as connection:
        await connection.execute(DropSchema(schema, cascade=True, if_exists=True))


__all__ = ("run",)

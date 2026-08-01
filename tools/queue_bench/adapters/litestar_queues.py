"""Litestar Queues adapter using only public queue and worker APIs while timed."""

import asyncio
import contextlib
import time
from typing import Any

from tools.queue_bench.adapters.base import AdapterRequest, AdapterResult, gather_bounded


async def run(request: AdapterRequest) -> AdapterResult:
    """Run one isolated Litestar Queues sample.

    Returns:
        Timed result and correctness counters.
    """
    backend_config = _backend_config(request)
    try:
        result = await _run(request, backend_config)
    except BaseException:
        with contextlib.suppress(Exception):
            await _cleanup(request, backend_config)
        raise
    await _cleanup(request, backend_config)
    return result


async def _run(request: AdapterRequest, backend_config: Any) -> AdapterResult:
    from litestar_queues import QueueConfig, QueueService, Worker, WorkerConfig, task

    @task(f"queue_bench_noop_{request.namespace}", queue=request.namespace)
    async def noop(payload: str) -> int:
        return len(payload)

    config = QueueConfig(
        queue_backend=backend_config,
        execution_backend="local",
        initialize_schedules=False,
        log_success=False,
        worker=WorkerConfig(
            batch_size=max(10, request.concurrency),
            max_concurrency=request.concurrency,
            poll_interval=0.01,
            queues=(request.namespace,),
        ),
    )
    async with QueueService(config) as service:
        if request.backend == "postgres":
            from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

            sqlspec_backend = service.get_queue_backend()
            if not isinstance(sqlspec_backend, SQLSpecQueueBackend):
                msg = "PostgreSQL benchmark expected SQLSpecQueueBackend"
                raise TypeError(msg)
            await sqlspec_backend.create_schema()
        worker = Worker(
            service,
            WorkerConfig(
                batch_size=max(10, request.concurrency),
                max_concurrency=request.concurrency,
                poll_interval=0.01,
                queues=(request.namespace,),
            ),
        )
        worker_task: asyncio.Task[None] | None = None
        if request.scenario == "roundtrip":
            worker_task = asyncio.create_task(worker.start())
            await asyncio.sleep(0.05)
        started_at = time.perf_counter()
        results = [await service.enqueue(noop, request.payload) for _ in range(request.operations)]
        if request.scenario == "roundtrip":
            await gather_bounded(
                (result.wait(timeout=request.timeout_seconds, poll_interval=0.01) for result in results),
                limit=request.concurrency,
            )
        duration = time.perf_counter() - started_at
        statistics = await service.get_queue_backend().get_statistics()
        completed = sum(result.status == "completed" for result in results)
        counters = {
            "enqueued": len(results),
            "started": completed if request.scenario == "roundtrip" else 0,
            "completed": completed,
            "remaining": statistics.pending + statistics.scheduled + statistics.running,
        }
        if worker_task is not None:
            await worker.stop()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
    return AdapterResult(
        duration_seconds=duration,
        counters=counters,
        metadata={
            "task_body": "return payload byte length",
            "backend_config": type(backend_config).__name__,
            "driver": _driver_name(request),
            "namespace": request.namespace,
        },
    )


def _backend_config(request: AdapterRequest) -> Any:
    if request.backend == "redis":
        from litestar_queues.backends.redis import RedisBackendConfig

        return RedisBackendConfig(
            url=request.dsn, key_prefix=request.namespace, wakeup_channel=f"{request.namespace}:wakeups"
        )
    if request.backend == "valkey":
        from litestar_queues.backends.valkey import ValkeyBackendConfig

        return ValkeyBackendConfig(
            url=request.dsn, key_prefix=request.namespace, wakeup_channel=f"{request.namespace}:wakeups"
        )
    if request.backend == "postgres":
        from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecWorkerWakeupConfig
        from litestar_queues.backends.sqlspec.schema import (
            event_history_table_name_for,
            maintenance_table_name_for,
            task_reservation_table_name_for,
        )

        sqlspec_config: Any
        if request.backend_variant == "asyncpg":
            from sqlspec.adapters.asyncpg import AsyncpgConfig

            sqlspec_config = AsyncpgConfig(connection_config={"dsn": request.dsn})
        else:
            from sqlspec.adapters.psycopg import PsycopgAsyncConfig

            sqlspec_config = PsycopgAsyncConfig(connection_config={"conninfo": request.dsn, "autocommit": True})
        queue_table_name = request.namespace

        return SQLSpecBackendConfig(
            sqlspec_config=sqlspec_config,
            queue_table_name=queue_table_name,
            event_history_table_name=event_history_table_name_for(queue_table_name),
            maintenance_table_name=maintenance_table_name_for(queue_table_name),
            task_reservation_table_name=task_reservation_table_name_for(queue_table_name),
            worker_wakeups=SQLSpecWorkerWakeupConfig(channel_name=f"{request.namespace}_wakeups", transport="notify"),
        )
    msg = f"unsupported Litestar Queues backend {request.backend!r}"
    raise ValueError(msg)


def _driver_name(request: AdapterRequest) -> str:
    if request.backend == "postgres":
        return "sqlspec-asyncpg" if request.backend_variant == "asyncpg" else "sqlspec-psycopg"
    return "valkey-asyncio" if request.backend == "valkey" else "redis-asyncio"


async def _cleanup(request: AdapterRequest, backend_config: Any) -> None:
    if request.backend == "redis":
        from redis.asyncio import Redis

        client = Redis.from_url(request.dsn)
        keys = [key async for key in client.scan_iter(match=f"{request.namespace}*")]
        if keys:
            await client.delete(*keys)
        await client.aclose()
        return
    if request.backend == "valkey":
        from valkey.asyncio import Valkey

        client = Valkey.from_url(request.dsn)
        keys = [key async for key in client.scan_iter(match=f"{request.namespace}*")]
        if keys:
            await client.delete(*keys)
        await client.aclose()
        return
    if request.backend == "postgres":
        from sqlspec.utils.text import quote_identifier

        table_names = (
            backend_config.event_history_table_name,
            backend_config.task_reservation_table_name,
            backend_config.maintenance_table_name,
            backend_config.queue_table_name,
        )
        if request.backend_variant == "asyncpg":
            import asyncpg  # type: ignore[import-untyped]

            connection = await asyncpg.connect(request.dsn)
            try:
                for table_name in table_names:
                    await connection.execute(f"DROP TABLE IF EXISTS {quote_identifier(table_name)} CASCADE")
            finally:
                await connection.close()
            return
        import psycopg
        from psycopg import sql

        async with (
            await psycopg.AsyncConnection.connect(request.dsn, autocommit=True) as connection,
            connection.cursor() as cursor,
        ):
            for table_name in table_names:
                await cursor.execute(
                    sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(*table_name.split(".")))
                )


__all__ = ("run",)

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
    try:
        return await _run(request)
    except BaseException:
        with contextlib.suppress(Exception):
            await _cleanup(request)
        raise


async def _run(request: AdapterRequest) -> AdapterResult:
    from litestar_queues import QueueConfig, QueueService, Worker, task

    @task(f"queue_bench_noop_{request.namespace}", queue=request.namespace)
    async def noop(payload: str) -> int:
        return len(payload)

    backend_config = _backend_config(request)
    config = QueueConfig(
        queue_backend=backend_config,
        execution_backend="local",
        initialize_schedules=False,
        quiet_success=False,
        worker_batch_size=max(10, request.concurrency),
        worker_max_concurrency=request.concurrency,
        worker_poll_interval=0.01,
        worker_queues=(request.namespace,),
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
            batch_size=max(10, request.concurrency),
            max_concurrency=request.concurrency,
            poll_interval=0.01,
            queues=(request.namespace,),
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
    await _cleanup(request)
    return AdapterResult(
        duration_seconds=duration,
        counters=counters,
        metadata={"task_body": "return payload byte length", "driver": _driver_name(request)},
    )


def _backend_config(request: AdapterRequest) -> Any:
    if request.backend in {"redis", "valkey"}:
        from litestar_queues.backends.redis import RedisBackendConfig

        return RedisBackendConfig(
            url=request.dsn,
            key_prefix=request.namespace,
            notification_channel=f"{request.namespace}:notifications",
        )
    if request.backend == "postgres":
        from sqlspec.adapters.psycopg import PsycopgAsyncConfig

        from litestar_queues.backends.sqlspec import SQLSpecBackendConfig

        return SQLSpecBackendConfig(
            config=PsycopgAsyncConfig(
                connection_config={"conninfo": request.dsn, "autocommit": True},
                extension_config={"events": {"backend": "notify"}},
            ),
            queue_table_name=request.namespace,
            notification_channel=f"{request.namespace}_notify",
            notify_transport="notify",
        )
    msg = f"unsupported Litestar Queues backend {request.backend!r}"
    raise ValueError(msg)


def _driver_name(request: AdapterRequest) -> str:
    return "psycopg-async" if request.backend == "postgres" else "redis-asyncio"


async def _cleanup(request: AdapterRequest) -> None:
    if request.backend in {"redis", "valkey"}:
        from redis.asyncio import Redis

        client = Redis.from_url(request.dsn)
        keys = [key async for key in client.scan_iter(match=f"{request.namespace}*")]
        if keys:
            await client.delete(*keys)
        await client.aclose()
        return
    if request.backend == "postgres":
        import psycopg

        async with (
            await psycopg.AsyncConnection.connect(request.dsn, autocommit=True) as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(f'DROP TABLE IF EXISTS "{request.namespace}"')


__all__ = ("run",)

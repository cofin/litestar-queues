"""SAQ and litestar-saq adapters sharing the same broker service."""

import asyncio
import contextlib
import time
from typing import Any

from tools.queue_bench.adapters.base import AdapterRequest, AdapterResult, gather_bounded


async def noop(ctx: dict[str, Any], payload: str) -> int:
    """Shared async no-op task body.

    Returns:
        Payload character count.
    """
    del ctx
    return len(payload)


async def run(request: AdapterRequest) -> AdapterResult:
    """Run one raw-SAQ or litestar-saq sample.

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
    from saq import Queue, Worker  # type: ignore[import-not-found]

    plugin_startup_seconds = 0.0
    if request.system == "litestar-saq":
        from litestar_saq import QueueConfig, SAQConfig  # type: ignore[import-not-found]

        plugin_started_at = time.perf_counter()
        config = SAQConfig(
            queue_configs=[
                QueueConfig(
                    dsn=request.dsn,
                    name=request.namespace,
                    concurrency=request.concurrency,
                    tasks=[noop],
                    separate_process=False,
                    broker_options=_saq_options(request),
                )
            ],
            web_enabled=False,
            use_server_lifespan=False,
        )
        queue = config.get_queues().queues[request.namespace]
        plugin_startup_seconds = time.perf_counter() - plugin_started_at
    else:
        queue = Queue.from_url(request.dsn, name=request.namespace, **_saq_options(request))
    await queue.connect()
    worker = Worker(queue, functions=[noop], concurrency=request.concurrency, dequeue_timeout=0.01, poll_interval=0.01)
    worker_task: asyncio.Task[None] | None = None
    if request.scenario == "roundtrip":
        worker_task = asyncio.create_task(worker.start())
        await asyncio.sleep(0.05)
    started_at = time.perf_counter()
    jobs = [await queue.enqueue("noop", payload=request.payload) for _ in range(request.operations)]
    accepted = [job for job in jobs if job is not None]
    if request.scenario == "roundtrip":
        await gather_bounded(
            (job.refresh(until_complete=request.timeout_seconds) for job in accepted), limit=request.concurrency
        )
    duration = time.perf_counter() - started_at
    completed = sum(job.status.value == "complete" for job in accepted)
    remaining = await queue.count("queued") + await queue.count("active")
    counters = {
        "enqueued": len(accepted),
        "started": completed if request.scenario == "roundtrip" else 0,
        "completed": completed,
        "remaining": remaining,
    }
    if worker_task is not None:
        await worker.stop()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
    await queue.disconnect()
    await _cleanup(request)
    return AdapterResult(
        duration_seconds=duration,
        counters=counters,
        metadata={
            "task_body": "return payload byte length",
            "driver": "psycopg-async" if request.backend == "postgres" else "redis-asyncio",
            "plugin_startup_seconds": plugin_startup_seconds,
        },
    )


def _saq_options(request: AdapterRequest) -> dict[str, Any]:
    if request.backend != "postgres":
        return {}
    return {
        "versions_table": f"{request.namespace}_versions",
        "jobs_table": f"{request.namespace}_jobs",
        "stats_table": f"{request.namespace}_stats",
        "min_size": 1,
        "max_size": max(2, request.concurrency + 1),
    }


async def _cleanup(request: AdapterRequest) -> None:
    if request.backend == "redis":
        from redis.asyncio import Redis

        client = Redis.from_url(request.dsn)
        keys = [key async for key in client.scan_iter(match=f"*{request.namespace}*")]
        if keys:
            await client.delete(*keys)
        await client.aclose()
        return
    import psycopg

    table_names = [f"{request.namespace}_versions", f"{request.namespace}_jobs", f"{request.namespace}_stats"]
    async with (
        await psycopg.AsyncConnection.connect(request.dsn, autocommit=True) as connection,
        connection.cursor() as cursor,
    ):
        for table_name in table_names:
            await cursor.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')


__all__ = ("run",)

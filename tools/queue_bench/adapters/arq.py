"""ARQ adapter using public pool, job, and worker APIs."""

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
    """Run one isolated ARQ sample.

    Returns:
        Timed result and correctness counters.
    """
    try:
        return await _run(request)
    except BaseException:
        with contextlib.suppress(Exception):
            await _cleanup_failed(request)
        raise


async def _run(request: AdapterRequest) -> AdapterResult:
    from arq import create_pool  # type: ignore[import-not-found]
    from arq.connections import RedisSettings  # type: ignore[import-not-found]
    from arq.worker import Worker  # type: ignore[import-not-found]

    pool = await create_pool(RedisSettings.from_dsn(request.dsn), default_queue_name=request.namespace)
    await _delete_namespace(pool, request.namespace)
    worker = Worker(
        functions=[noop],
        redis_pool=pool,
        queue_name=request.namespace,
        handle_signals=False,
        max_jobs=request.concurrency,
        poll_delay=0.01,
        keep_result=60,
    )
    worker_task: asyncio.Task[None] | None = None
    if request.scenario == "roundtrip":
        worker_task = asyncio.create_task(worker.async_run())
        await asyncio.sleep(0.05)
    started_at = time.perf_counter()
    jobs = [
        await pool.enqueue_job("noop", request.payload, _queue_name=request.namespace)
        for _ in range(request.operations)
    ]
    accepted = [job for job in jobs if job is not None]
    if request.scenario == "roundtrip":
        await gather_bounded(
            (job.result(timeout=request.timeout_seconds, poll_delay=0.01) for job in accepted),
            limit=request.concurrency,
        )
    duration = time.perf_counter() - started_at
    completed = request.operations if request.scenario == "roundtrip" and len(accepted) == request.operations else 0
    remaining = await pool.zcard(request.namespace)
    counters = {"enqueued": len(accepted), "started": completed, "completed": completed, "remaining": int(remaining)}
    if worker_task is not None:
        await worker.close()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
    else:
        await _delete_namespace(pool, request.namespace)
        await pool.aclose()
    return AdapterResult(
        duration_seconds=duration,
        counters=counters,
        metadata={"task_body": "return payload byte length", "driver": "redis-asyncio"},
    )


async def _delete_namespace(pool: Any, namespace: str) -> None:
    keys = [key async for key in pool.scan_iter(match=f"{namespace}*")]
    keys.extend([key async for key in pool.scan_iter(match=f"arq:result:*{namespace}*")])
    if keys:
        await pool.delete(*set(keys))


async def _cleanup_failed(request: AdapterRequest) -> None:
    from redis.asyncio import Redis

    client = Redis.from_url(request.dsn)
    keys = [key async for key in client.scan_iter(match=f"*{request.namespace}*")]
    if keys:
        await client.delete(*keys)
    await client.aclose()


__all__ = ("run",)

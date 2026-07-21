"""Taskiq adapter using the Redis broker, result backend, and receiver APIs."""

import asyncio
import contextlib
import time

from tools.queue_bench.adapters.base import AdapterRequest, AdapterResult, gather_bounded


async def run(request: AdapterRequest) -> AdapterResult:
    """Run one isolated Taskiq sample.

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
    from taskiq.receiver import Receiver  # type: ignore[import-not-found]
    from taskiq_redis import ListQueueBroker, RedisAsyncResultBackend  # type: ignore[import-not-found]

    result_backend = RedisAsyncResultBackend(
        redis_url=request.dsn, result_ex_time=60, prefix_str=f"{request.namespace}:result"
    )
    broker = ListQueueBroker(
        request.dsn, queue_name=request.namespace, max_connection_pool_size=max(2, request.concurrency + 1)
    ).with_result_backend(result_backend)

    @broker.task(task_name=f"{request.namespace}.noop")  # type: ignore[untyped-decorator]
    async def noop(payload: str) -> int:
        return len(payload)

    await _cleanup(request)
    await broker.startup()
    finish_event = asyncio.Event()
    receiver = Receiver(
        broker,
        max_async_tasks=request.concurrency,
        run_startup=False,
        max_tasks_to_execute=request.operations if request.scenario == "roundtrip" else None,
    )
    receiver_task: asyncio.Task[None] | None = None
    if request.scenario == "roundtrip":
        receiver_task = asyncio.create_task(receiver.listen(finish_event))
        await asyncio.sleep(0.05)
    started_at = time.perf_counter()
    tasks = [await noop.kiq(request.payload) for _ in range(request.operations)]
    if request.scenario == "roundtrip":
        await gather_bounded(
            (task.wait_result(check_interval=0.01, timeout=request.timeout_seconds) for task in tasks),
            limit=request.concurrency,
        )
    duration = time.perf_counter() - started_at
    completed = request.operations if request.scenario == "roundtrip" else 0
    from redis.asyncio import Redis

    client = Redis.from_url(request.dsn)
    remaining = await client.llen(request.namespace)
    await client.aclose()
    counters = {"enqueued": len(tasks), "started": completed, "completed": completed, "remaining": int(remaining)}
    if receiver_task is not None:
        finish_event.set()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(receiver_task, timeout=2)
    await broker.shutdown()
    await _cleanup(request)
    return AdapterResult(
        duration_seconds=duration,
        counters=counters,
        metadata={"task_body": "return payload byte length", "driver": "redis-asyncio"},
    )


async def _cleanup(request: AdapterRequest) -> None:
    from redis.asyncio import Redis

    client = Redis.from_url(request.dsn)
    keys = [key async for key in client.scan_iter(match=f"*{request.namespace}*")]
    if keys:
        await client.delete(*keys)
    await client.aclose()


__all__ = ("run",)

"""Opt-in process/sync-oriented Redis competitor adapters."""

import asyncio
import contextlib
import threading
import time
from typing import Any

from tools.queue_bench.adapters.base import AdapterRequest, AdapterResult, gather_bounded


def sync_noop(payload: str) -> int:
    """Return the payload length for sync worker systems.

    Returns:
        Payload character count.
    """
    return len(payload)


async def run(request: AdapterRequest) -> AdapterResult:
    """Dispatch one sample to the selected sync-oriented adapter.

    Returns:
        Timed result and correctness counters.
    """
    try:
        return await _dispatch(request)
    except BaseException:
        with contextlib.suppress(Exception):
            await _cleanup_redis(request)
        raise


async def _dispatch(request: AdapterRequest) -> AdapterResult:
    if request.system == "dramatiq":
        return await _run_dramatiq(request)
    if request.system == "rq":
        return await _run_rq(request)
    if request.system == "celery":
        return await _run_celery(request)
    msg = f"unsupported sync competitor {request.system!r}"
    raise ValueError(msg)


async def _run_dramatiq(request: AdapterRequest) -> AdapterResult:
    import dramatiq  # type: ignore[import-not-found]
    from dramatiq import Worker
    from dramatiq.brokers.redis import RedisBroker  # type: ignore[import-not-found]
    from dramatiq.results import Results  # type: ignore[import-not-found]
    from dramatiq.results.backends import RedisBackend  # type: ignore[import-not-found]

    result_backend = RedisBackend(url=request.dsn, namespace=f"{request.namespace}:results")
    broker = RedisBroker(
        url=request.dsn,
        namespace=request.namespace,
        middleware=[Results(backend=result_backend, store_results=True, result_ttl=60_000)],
    )
    dramatiq.set_broker(broker)
    actor = dramatiq.actor(
        sync_noop,
        actor_name=f"{request.namespace}.noop",
        queue_name=request.namespace,
        broker=broker,
        store_results=True,
    )
    worker: Worker | None = None
    if request.scenario == "roundtrip":
        worker = Worker(broker, queues={request.namespace}, worker_threads=1, worker_timeout=50)
        worker.start()
    started_at = time.perf_counter()
    messages = [actor.send(request.payload) for _ in range(request.operations)]
    if request.scenario == "roundtrip":
        await gather_bounded(
            (
                asyncio.to_thread(
                    message.get_result, backend=result_backend, block=True, timeout=int(request.timeout_seconds * 1_000)
                )
                for message in messages
            ),
            limit=1,
        )
    duration = time.perf_counter() - started_at
    completed = request.operations if request.scenario == "roundtrip" else 0
    counters = {
        "enqueued": len(messages),
        "started": completed,
        "completed": completed,
        "remaining": 0 if completed else len(messages),
    }
    if worker is not None:
        worker.stop(timeout=int(request.timeout_seconds * 1_000))
    broker.close()
    await _cleanup_redis(request)
    return AdapterResult(
        duration_seconds=duration,
        counters=counters,
        metadata={"task_body": "return payload byte length", "driver": "redis-sync", "worker_threads": 1},
    )


async def _run_rq(request: AdapterRequest) -> AdapterResult:
    from redis import Redis
    from rq import Queue, SimpleWorker  # type: ignore[import-not-found]
    from rq.timeouts import TimerDeathPenalty  # type: ignore[import-not-found]

    connection = Redis.from_url(request.dsn)
    await _cleanup_redis(request)
    queue = Queue(name=request.namespace, connection=connection, death_penalty_class=TimerDeathPenalty)
    started_at = time.perf_counter()
    jobs = [
        queue.enqueue_call(sync_noop, args=(request.payload,), result_ttl=60, job_id=f"{request.namespace}-{index}")
        for index in range(request.operations)
    ]
    if request.scenario == "roundtrip":
        worker = SimpleWorker([queue], connection=connection)
        worker.death_penalty_class = TimerDeathPenalty
        worker.work(burst=True, max_jobs=request.operations, with_scheduler=False, logging_level="WARNING")
        for job in jobs:
            job.refresh()
    duration = time.perf_counter() - started_at
    completed = sum(job.is_finished for job in jobs)
    counters = {
        "enqueued": len(jobs),
        "started": completed if request.scenario == "roundtrip" else 0,
        "completed": completed,
        "remaining": len(queue),
    }
    connection.close()
    await _cleanup_redis(request)
    return AdapterResult(
        duration_seconds=duration,
        counters=counters,
        metadata={"task_body": "return payload byte length", "driver": "redis-sync", "worker_processes": 1},
    )


async def _run_celery(request: AdapterRequest) -> AdapterResult:
    from celery import Celery  # type: ignore[import-not-found]

    app = Celery(request.namespace, broker=request.dsn, backend=request.dsn)
    app.conf.update(
        task_default_queue=request.namespace,
        task_track_started=True,
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        result_expires=60,
        worker_concurrency=1,
        worker_prefetch_multiplier=1,
    )
    task = app.task(sync_noop, name=f"{request.namespace}.noop")
    worker: Any | None = None
    worker_thread: threading.Thread | None = None
    if request.scenario == "roundtrip":
        worker = app.Worker(
            pool="solo",
            concurrency=1,
            loglevel="ERROR",
            without_heartbeat=True,
            without_gossip=True,
            without_mingle=True,
            queues=[request.namespace],
        )
        worker_thread = threading.Thread(target=worker.start, name=f"{request.namespace}-worker", daemon=True)
        worker_thread.start()
        await asyncio.sleep(0.25)
    started_at = time.perf_counter()
    results = [task.apply_async(args=(request.payload,), queue=request.namespace) for _ in range(request.operations)]
    if request.scenario == "roundtrip":
        await gather_bounded(
            (
                asyncio.to_thread(
                    result.get, timeout=request.timeout_seconds, interval=0.01, disable_sync_subtasks=False
                )
                for result in results
            ),
            limit=1,
        )
    duration = time.perf_counter() - started_at
    completed = sum(result.successful() for result in results) if request.scenario == "roundtrip" else 0
    from redis import Redis

    connection = Redis.from_url(request.dsn)
    remaining = connection.llen(request.namespace)
    connection.close()
    counters = {"enqueued": len(results), "started": completed, "completed": completed, "remaining": int(remaining)}
    if worker is not None and worker_thread is not None:
        worker.stop()
        worker_thread.join(timeout=5)
    app.close()
    await _cleanup_redis(request)
    return AdapterResult(
        duration_seconds=duration,
        counters=counters,
        metadata={"task_body": "return payload byte length", "driver": "redis-sync", "worker_pool": "solo"},
    )


async def _cleanup_redis(request: AdapterRequest) -> None:
    from redis.asyncio import Redis

    client = Redis.from_url(request.dsn)
    patterns = (f"*{request.namespace}*", f"rq:job:{request.namespace}-*")
    keys: set[bytes] = set()
    for pattern in patterns:
        keys.update([key async for key in client.scan_iter(match=pattern)])
    if keys:
        await client.delete(*keys)
    with contextlib.suppress(AttributeError):
        await client.aclose()


__all__ = ("run", "sync_noop")

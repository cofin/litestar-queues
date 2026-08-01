"""Litestar Queues adapter using only public queue and worker APIs while timed."""

import asyncio
import contextlib
import time
from typing import Any

from tools.queue_bench.adapters.base import AdapterRequest, AdapterResult, gather_bounded
from tools.queue_bench.measurements import SampleMeasurementCollector


async def run(request: AdapterRequest) -> AdapterResult:
    """Run one isolated Litestar Queues sample.

    Returns:
        Timed result and correctness counters.
    """
    if request.profile in {"cloud-tasks", "cloud-run-jobs"}:
        from tools.queue_bench.adapters.litestar_managed_google import run as run_managed_google

        return await run_managed_google(request)
    if request.profile == "advanced-alchemy":
        from tools.queue_bench.adapters.litestar_advanced_alchemy import run as run_advanced_alchemy

        return await run_advanced_alchemy(request)

    backend_config = _backend_config(request)
    try:
        if request.profile == "maintenance":
            from tools.queue_bench.adapters.litestar_maintenance import run as run_maintenance

            result = await run_maintenance(request, backend_config)
        elif request.profile in {"heartbeat", "events", "uniqueness"}:
            from tools.queue_bench.adapters.litestar_features import run as run_feature

            result = await run_feature(request, backend_config)
        else:
            result = await _run(request, backend_config)
    except BaseException:
        with contextlib.suppress(Exception):
            await _cleanup(request, backend_config)
        raise
    await _cleanup(request, backend_config)
    return result


async def _run(request: AdapterRequest, backend_config: Any) -> AdapterResult:
    from litestar_queues import QueueConfig, QueueService, TaskRequest, Worker, WorkerConfig, task
    from litestar_queues.observability import ObservabilityConfig

    attempts: dict[int, int] = {}
    started_count = 0

    @task(f"queue_bench_noop_{request.namespace}", queue=request.namespace)
    async def noop(payload: str) -> int:
        nonlocal started_count
        started_count += 1
        return len(payload)

    @task(f"queue_bench_delayed_{request.namespace}", queue=request.namespace)
    async def delayed(payload: str) -> int:
        nonlocal started_count
        started_count += 1
        return len(payload)

    @task(f"queue_bench_retry_{request.namespace}", queue=request.namespace, retries=1)
    async def retry_once(index: int, payload: str) -> int:
        nonlocal started_count
        started_count += 1
        attempts[index] = attempts.get(index, 0) + 1
        if attempts[index] == 1:
            msg = "intentional benchmark retry"
            raise RuntimeError(msg)
        return len(payload)

    measurement_collector = SampleMeasurementCollector.create()
    config = QueueConfig(
        queue_backend=backend_config,
        execution_backend="local",
        initialize_schedules=False,
        log_success=False,
        observability=ObservabilityConfig(
            enable_otel=False,
            enable_prometheus=True,
            enable_sqlcommenter=False,
            prometheus_registry=measurement_collector.registry,
        ),
        worker=WorkerConfig(
            batch_size=max(10, request.concurrency),
            max_concurrency=request.concurrency,
            poll_interval=0.01,
            queues=(request.namespace,),
        ),
    )
    async with QueueService(config) as service:
        if request.backend == "postgres" and request.profile != "advanced-alchemy":
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
        worker_task = (
            await _start_worker(worker)
            if request.scenario in {"roundtrip", "delayed-lateness", "retry-once", "idle"}
            else None
        )
        try:
            cpu_started, started_at = measurement_collector.snapshot_cpu(), time.perf_counter()
            results, record_count, request_count = await _execute_scenario(
                request,
                service=service,
                task_request_type=TaskRequest,
                noop=noop,
                delayed=delayed,
                retry_once=retry_once,
            )
            duration = time.perf_counter() - started_at
            measurements = measurement_collector.finish(cpu_started)
            statistics = await service.get_queue_backend().get_statistics()
            completed = sum(result.status == "completed" for result in results)
            failed = sum(result.status == "failed" for result in results)
            retried = sum(result.record.retry_count for result in results if result.record is not None)
            counters = {
                "requests": request_count,
                "records": record_count,
                "started": started_count,
                "completed": completed,
                "failed": failed,
                "retried": retried,
                "remaining": statistics.pending + statistics.scheduled + statistics.running,
            }
            if request.scenario == "delayed-lateness":
                not_early = sum(
                    result.record is not None
                    and result.record.scheduled_at is not None
                    and result.record.started_at is not None
                    and result.record.started_at >= result.record.scheduled_at
                    for result in results
                )
                counters.update({"scheduled": record_count, "not_early": not_early})
            elif request.scenario == "idle":
                counters["idle_observations"] = 1
        finally:
            if worker_task is not None:
                await worker.stop()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task
    return AdapterResult(
        duration_seconds=duration,
        counters=counters,
        measurements=measurements,
        metadata={
            "task_body": "return payload byte length",
            "backend_config": type(backend_config).__name__,
            "driver": _driver_name(request),
            "namespace": request.namespace,
            "comparison_class": _comparison_class(request.scenario),
        },
    )


async def _start_worker(worker: Any) -> asyncio.Task[None]:
    worker_task = asyncio.create_task(worker.start())
    try:
        await worker.wait_started()
    except BaseException:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        raise
    return worker_task


async def _execute_scenario(
    request: AdapterRequest, *, service: Any, task_request_type: Any, noop: Any, delayed: Any, retry_once: Any
) -> tuple[list[Any], int, int]:
    if request.scenario == "enqueue":
        results = [await service.enqueue(noop, request.payload) for _ in range(request.operations)]
        return results, len(results), len(results)
    if request.scenario == "enqueue-concurrent":
        producer_concurrency = int(request.parameters.get("producer_concurrency", 32))
        results = await gather_bounded(
            (service.enqueue(noop, request.payload) for _ in range(request.operations)), limit=producer_concurrency
        )
        return results, len(results), len(results)
    if request.scenario == "enqueue-many":
        batch_size = int(request.parameters.get("batch_size", 100))
        pending_requests = [
            task_request_type(task_name=noop.name, args=(request.payload,), queue=request.namespace)
            for _ in range(request.operations)
        ]
        batches = [
            pending_requests[offset : offset + batch_size] for offset in range(0, len(pending_requests), batch_size)
        ]
        records = [record for batch in batches for record in await service.get_queue_backend().enqueue_many(batch)]
        return [], len(records), len(batches)
    if request.scenario == "roundtrip":
        results = [await service.enqueue(noop, request.payload) for _ in range(request.operations)]
        await _wait_for_results(results, request)
        return results, len(results), len(results)
    if request.scenario == "delayed-lateness":
        delay_seconds = float(request.parameters.get("delay_seconds", 1.0))
        results = [
            await service.enqueue(delayed, request.payload, run_after=delay_seconds) for _ in range(request.operations)
        ]
        await _wait_for_results(results, request)
        return results, len(results), len(results)
    if request.scenario == "retry-once":
        results = [await service.enqueue(retry_once, index, request.payload) for index in range(request.operations)]
        await _wait_for_results(results, request)
        return results, len(results), len(results)
    if request.scenario == "idle":
        idle_duration = float(request.parameters.get("idle_duration_seconds", 60.0))
        await asyncio.wait_for(asyncio.sleep(idle_duration), timeout=request.timeout_seconds)
        return [], 0, 0
    msg = f"unsupported Litestar Queues scenario {request.scenario!r}"
    raise ValueError(msg)


def _comparison_class(scenario: str) -> str:
    if scenario == "enqueue-many":
        return "feature-advantaged"
    if scenario in {"roundtrip", "delayed-lateness", "retry-once"}:
        return "feature-cost"
    if scenario == "idle":
        return "no-counterpart"
    return "equivalent"


async def _wait_for_results(results: list[Any], request: AdapterRequest) -> None:
    await gather_bounded(
        (result.wait(timeout=request.timeout_seconds, poll_interval=0.01) for result in results),
        limit=request.concurrency,
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
        if request.profile == "advanced-alchemy":
            return f"advanced-alchemy-{request.backend_variant}"
        return "sqlspec-asyncpg" if request.backend_variant == "asyncpg" else "sqlspec-psycopg"
    return "valkey-asyncio" if request.backend == "valkey" else "redis-asyncio"


async def _cleanup(request: AdapterRequest, backend_config: Any) -> None:
    if request.backend == "redis":
        from redis.asyncio import Redis

        client = Redis.from_url(request.dsn)
        keys = await _namespace_keys(client, request.namespace)
        if keys:
            await client.delete(*keys)
        await client.aclose()
        return
    if request.backend == "valkey":
        from valkey.asyncio import Valkey

        client = Valkey.from_url(request.dsn)
        keys = await _namespace_keys(client, request.namespace)
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


async def _namespace_keys(client: Any, namespace: str) -> list[Any]:
    keys = {key async for key in client.scan_iter(match=f"{namespace}:*")}
    if await client.exists(namespace):
        keys.add(namespace)
    return list(keys)


__all__ = ("run",)

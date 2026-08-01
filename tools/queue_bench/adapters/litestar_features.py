"""Heartbeat and event benchmarks built exclusively on public queue APIs."""

import asyncio
import contextlib
import time
from typing import Any

from tools.queue_bench.adapters.base import AdapterRequest, AdapterResult, gather_bounded
from tools.queue_bench.measurements import SampleMeasurementCollector


async def run(request: AdapterRequest, backend_config: Any) -> AdapterResult:
    """Run one heartbeat, event, or uniqueness feature sample.

    Returns:
        Timed result, exact correctness counters, and bounded measurements.
    """
    from litestar_queues import QueueService, Worker, WorkerConfig, task
    from litestar_queues.events import beat

    measurement_collector = SampleMeasurementCollector.create()
    if request.profile == "uniqueness":
        return await _run_uniqueness_sample(request, backend_config, measurement_collector)
    started_count = 0
    beats_called = 0
    all_started = asyncio.Event()
    release_tasks = asyncio.Event()

    @task(f"queue_bench_heartbeat_{request.namespace}", queue=request.namespace)
    async def heartbeat_task(payload: str) -> int:
        nonlocal beats_called, started_count
        started_count += 1
        beats_called += 1
        beat(f"observing:{started_count}")
        if started_count == request.operations:
            all_started.set()
        await release_tasks.wait()
        return len(payload)

    @task(f"queue_bench_events_{request.namespace}", queue=request.namespace)
    async def event_task(payload: str) -> int:
        nonlocal started_count
        started_count += 1
        return len(payload)

    worker_config = _worker_config(request, WorkerConfig)
    events_config, event_sink = _events_config(request)
    config = _queue_config(backend_config, worker_config, events_config, measurement_collector.registry)
    async with QueueService(config) as service:
        await _create_postgres_schema(request, service)
        worker = Worker(service, worker_config)
        worker_task = await _start_worker(worker)
        try:
            cpu_started, started_at = measurement_collector.snapshot_cpu(), time.perf_counter()
            if request.scenario == "heartbeat":
                results, feature_counters, feature_measurements = await _run_heartbeat(
                    request,
                    service=service,
                    heartbeat_task=heartbeat_task,
                    all_started=all_started,
                    release_tasks=release_tasks,
                    beats_called=lambda: beats_called,
                )
            elif request.scenario == "events":
                results, feature_counters, feature_measurements = await _run_events(
                    request, service=service, event_task=event_task, event_sink=event_sink
                )
            else:  # pragma: no cover - profile validation rejects this before child execution.
                msg = f"unsupported Litestar Queues feature scenario {request.scenario!r}"
                raise ValueError(msg)
            duration = time.perf_counter() - started_at
            measurements = measurement_collector.finish(cpu_started)
            measurements.update(feature_measurements)
            statistics = await service.get_queue_backend().get_statistics()
            completed = sum(result.status == "completed" for result in results)
            failed = sum(result.status == "failed" for result in results)
            retried = sum(result.record.retry_count for result in results if result.record is not None)
            counters = {
                "requests": request.operations,
                "records": len(results),
                "started": started_count,
                "completed": completed,
                "failed": failed,
                "retried": retried,
                "remaining": statistics.pending + statistics.scheduled + statistics.running,
                **feature_counters,
            }
        finally:
            release_tasks.set()
            await worker.stop()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
    return AdapterResult(
        duration_seconds=duration,
        counters=counters,
        measurements=measurements,
        metadata={
            "task_body": _task_body(request),
            "backend_config": type(backend_config).__name__,
            "namespace": request.namespace,
            "comparison_class": "feature-cost",
        },
    )


async def _run_uniqueness_sample(
    request: AdapterRequest, backend_config: Any, measurement_collector: SampleMeasurementCollector
) -> AdapterResult:
    from litestar_queues import QueueService, WorkerConfig, task

    @task(f"queue_bench_unique_none_{request.namespace}", queue=request.namespace)
    async def unique_none_task(index: int, payload: str) -> int:
        return index + len(payload)

    @task(f"queue_bench_unique_task_{request.namespace}", queue=request.namespace, unique_by="task")
    async def unique_by_task(index: int, payload: str) -> int:
        return index + len(payload)

    @task(f"queue_bench_unique_arguments_{request.namespace}", queue=request.namespace, unique_by="arguments")
    async def unique_by_arguments(index: int, payload: str) -> int:
        return index + len(payload)

    mode = str(request.parameters["mode"])
    worker_config = WorkerConfig(queues=(request.namespace,))
    config = _queue_config(backend_config, worker_config, None, measurement_collector.registry)
    async with QueueService(config) as service:
        await _create_postgres_schema(request, service)
        cpu_started, started_at = measurement_collector.snapshot_cpu(), time.perf_counter()
        if mode == "none":
            results = [
                await service.enqueue(unique_none_task, index, request.payload) for index in range(request.operations)
            ]
        elif mode == "explicit-key":
            key = f"{request.namespace}:explicit"
            results = [
                await service.enqueue(unique_none_task, index, request.payload, key=key)
                for index in range(request.operations)
            ]
        elif mode == "unique-by-task":
            results = [
                await service.enqueue(unique_by_task, index, request.payload) for index in range(request.operations)
            ]
        else:
            results = [
                await service.enqueue(unique_by_arguments, index, request.payload)
                for index in range(request.operations)
            ]
        duration = time.perf_counter() - started_at
        measurements = measurement_collector.finish(cpu_started)
        measurements["uniqueness.mode"] = mode
        distinct_records = len({result.id for result in results})
        statistics = await service.get_queue_backend().get_statistics()
        counters = {
            "requests": request.operations,
            "records": distinct_records,
            "started": 0,
            "completed": 0,
            "failed": 0,
            "retried": 0,
            "deduplicated": request.operations - distinct_records,
            "remaining": statistics.pending + statistics.scheduled + statistics.running,
        }
    return AdapterResult(
        duration_seconds=duration,
        counters=counters,
        measurements=measurements,
        metadata={
            "task_body": "public enqueue identity resolution",
            "backend_config": type(backend_config).__name__,
            "namespace": request.namespace,
            "comparison_class": "no-counterpart",
            "uniqueness_mode": mode,
            "identity_lifetime": "terminal",
        },
    )


def _task_body(request: AdapterRequest) -> str:
    if request.profile == "heartbeat":
        return "public heartbeat observation"
    if request.profile == "events":
        return "lifecycle events"
    return "lifecycle events"


async def _run_heartbeat(
    request: AdapterRequest,
    *,
    service: Any,
    heartbeat_task: Any,
    all_started: asyncio.Event,
    release_tasks: asyncio.Event,
    beats_called: Any,
) -> tuple[list[Any], dict[str, int], dict[str, int | float | str | bool | None]]:
    results = [await service.enqueue(heartbeat_task, request.payload) for _ in range(request.operations)]
    await asyncio.wait_for(all_started.wait(), timeout=request.timeout_seconds)
    initial_records = await gather_bounded(
        (service.get_queue_backend().get_task(result.id) for result in results), limit=request.concurrency
    )
    initial_heartbeats = {record.id: record.heartbeat_at for record in initial_records if record is not None}
    observation_seconds = float(request.parameters.get("observation_seconds", 60.0))
    await asyncio.wait_for(asyncio.sleep(observation_seconds), timeout=request.timeout_seconds)
    observed_records = await gather_bounded(
        (service.get_queue_backend().get_task(result.id) for result in results), limit=request.concurrency
    )
    touched = sum(
        record is not None
        and record.status == "running"
        and record.heartbeat_at is not None
        and initial_heartbeats.get(record.id) is not None
        and record.heartbeat_at > initial_heartbeats[record.id]
        for record in observed_records
    )
    observed_running = sum(record is not None and record.status == "running" for record in observed_records)
    release_tasks.set()
    await _wait_for_results(results, request)
    return (
        results,
        {"beats_called": int(beats_called()), "heartbeat_touched": touched, "observed_running": observed_running},
        {
            "heartbeat.observation_seconds": observation_seconds,
            "heartbeat.interval_seconds": float(request.parameters.get("heartbeat_interval", 1.0)),
            "heartbeat.controlled_job_minutes": request.operations * observation_seconds / 60.0,
        },
    )


async def _run_events(
    request: AdapterRequest, *, service: Any, event_task: Any, event_sink: Any
) -> tuple[list[Any], dict[str, int], dict[str, int | float | str | bool | None]]:
    results = [await service.enqueue(event_task, request.payload) for _ in range(request.operations)]
    await _wait_for_results(results, request)
    mode = str(request.parameters.get("mode", "disabled"))
    live_events: list[Any] = []
    history_events: list[Any] = []
    if mode in {"live-only", "durable-history"}:
        await service.get_event_publisher().flush_buffer()
        live_events = list(event_sink.events)
    if mode == "durable-history":
        event_log = service.get_event_log()
        if event_log is None:
            msg = "durable-history mode requires a configured public event log"
            raise RuntimeError(msg)
        await event_log.flush_events()
        history_events = await event_log.list_events(limit=request.operations * 2 + 1)
    events = live_events if mode != "disabled" else history_events
    event_types = [event.type for event in events]
    live_identity = [(event.id, event.type) for event in live_events]
    history_identity = [(event.event_id, event.event_type) for event in history_events]
    lifecycle_events = sum(event_type in {"task.started", "task.completed"} for event_type in event_types)
    counters = {
        "lifecycle_events": lifecycle_events,
        "started_events": event_types.count("task.started"),
        "completed_events": event_types.count("task.completed"),
        "live_events": len(live_events),
        "history_events": len(history_events),
        "event_parity": (
            len(history_events)
            if mode == "durable-history" and sorted(live_identity) == sorted(history_identity)
            else 0
        ),
    }
    return results, counters, {"events.mode": mode}


def _events_config(request: AdapterRequest) -> tuple[Any, Any]:
    if request.scenario != "events":
        return None, None
    from litestar_queues.events import (
        EventDeliveryConfig,
        EventHistoryConfig,
        InMemoryQueueEventSink,
        QueueEventsConfig,
    )

    mode = str(request.parameters.get("mode", "disabled"))
    if mode == "disabled":
        return None, None
    if mode == "live-only":
        sink = InMemoryQueueEventSink()
        return QueueEventsConfig(delivery=EventDeliveryConfig(sinks=(sink,), strict=True)), sink
    sink = InMemoryQueueEventSink()
    return (
        QueueEventsConfig(
            delivery=EventDeliveryConfig(sinks=(sink,), strict=True),
            history=EventHistoryConfig(batch_size=20, flush_interval=1.0, strict=True),
        ),
        sink,
    )


def _queue_config(backend_config: Any, worker_config: Any, events_config: Any, registry: Any) -> Any:
    from litestar_queues import QueueConfig
    from litestar_queues.observability import ObservabilityConfig

    return QueueConfig(
        queue_backend=backend_config,
        execution_backend="local",
        events=events_config,
        initialize_schedules=False,
        log_success=False,
        observability=ObservabilityConfig(
            enable_otel=False, enable_prometheus=True, enable_sqlcommenter=False, prometheus_registry=registry
        ),
        worker=worker_config,
    )


def _worker_config(request: AdapterRequest, worker_config_type: Any) -> Any:
    worker_concurrency = request.operations if request.scenario == "heartbeat" else request.concurrency
    return worker_config_type(
        batch_size=max(10, worker_concurrency),
        heartbeat_interval=float(request.parameters.get("heartbeat_interval", 1.0)),
        max_concurrency=worker_concurrency,
        poll_interval=0.01,
        queues=(request.namespace,),
    )


async def _create_postgres_schema(request: AdapterRequest, service: Any) -> None:
    if request.backend != "postgres":
        return
    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    sqlspec_backend = service.get_queue_backend()
    if not isinstance(sqlspec_backend, SQLSpecQueueBackend):
        msg = "PostgreSQL benchmark expected SQLSpecQueueBackend"
        raise TypeError(msg)
    await sqlspec_backend.create_schema()


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


async def _wait_for_results(results: list[Any], request: AdapterRequest) -> None:
    await gather_bounded(
        (result.wait(timeout=request.timeout_seconds, poll_interval=0.01) for result in results),
        limit=request.concurrency,
    )


__all__ = ("run",)

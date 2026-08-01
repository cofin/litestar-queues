"""Bounded maintenance benchmarks using public Litestar Queues APIs."""

import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Literal
from uuid import uuid4

from tools.queue_bench.adapters.base import AdapterRequest, AdapterResult, gather_bounded
from tools.queue_bench.measurements import SampleMeasurementCollector

if TYPE_CHECKING:
    from litestar_queues.maintenance import MaintenancePhase


async def run(request: AdapterRequest, backend_config: Any) -> AdapterResult:
    """Run one isolated maintenance-profile sample.

    Returns:
        Timed drain or lease-denial result with exact maintenance counters.
    """
    from litestar_queues import QueueConfig, QueueMaintenanceConfig, QueueService, WorkerConfig
    from litestar_queues.events import EventHistoryConfig, QueueEventsConfig
    from litestar_queues.observability import ObservabilityConfig

    record_count = int(request.parameters.get("record_count", request.operations))
    limit = int(request.parameters.get("limit", 1000))
    collector = SampleMeasurementCollector.create()
    maintenance_config = QueueMaintenanceConfig(
        time_budget=max(1.0, request.timeout_seconds),
        coordination_timeout=max(2.0, request.timeout_seconds + 60.0),
        terminal_retention=1.0,
        terminal_limit=limit,
        event_retention=1.0,
        event_limit=limit,
    )
    events_config = (
        QueueEventsConfig(
            history=EventHistoryConfig(
                batch_size=max(1, min(record_count, 1000)), flush_interval=3600.0, strict=True
            )
        )
        if request.scenario == "event-retention"
        else None
    )
    config = QueueConfig(
        queue_backend=backend_config,
        execution_backend="local",
        events=events_config,
        initialize_schedules=False,
        log_success=False,
        maintenance=maintenance_config,
        observability=ObservabilityConfig(
            enable_otel=False,
            enable_prometheus=True,
            enable_sqlcommenter=False,
            prometheus_registry=collector.registry,
        ),
        worker=WorkerConfig(queues=(request.namespace,)),
    )
    async with QueueService(config) as service:
        await _create_postgres_schema(request, service)
        if request.scenario == "terminal-retention":
            completed_at = await _seed_terminal_records(request, service, record_count)
            result = await _run_drain(
                request,
                service=service,
                config=maintenance_config,
                collector=collector,
                phase="terminal",
                record_count=record_count,
                utcnow=lambda: completed_at + timedelta(seconds=2),
            )
            remaining = _terminal_count(await service.get_queue_backend().get_statistics())
        elif request.scenario == "event-retention":
            occurred_at = datetime(2020, 1, 1, tzinfo=timezone.utc)
            await _seed_events(request, service, record_count, occurred_at)
            result = await _run_drain(
                request,
                service=service,
                config=maintenance_config,
                collector=collector,
                phase="events",
                record_count=record_count,
                utcnow=lambda: occurred_at + timedelta(seconds=2),
            )
            event_log = service.get_event_log()
            if event_log is None:  # pragma: no cover - configured above.
                msg = "event-retention requires a public event log"
                raise RuntimeError(msg)
            remaining = len(await event_log.list_events(limit=1))
        elif request.scenario == "lease-contention":
            return await _run_lease_contention(request, service, maintenance_config, collector)
        else:  # pragma: no cover - profile validation rejects this before child execution.
            msg = f"unsupported maintenance scenario {request.scenario!r}"
            raise ValueError(msg)
    if remaining != 0:
        msg = f"maintenance drain left {remaining} eligible records"
        raise RuntimeError(msg)
    result.counters["remaining"] = remaining
    return result


async def _seed_terminal_records(request: AdapterRequest, service: Any, record_count: int) -> datetime:
    from litestar_queues import TaskRequest

    backend = service.get_queue_backend()
    completed_at: list[datetime] = []
    remaining = record_count
    seed_batch_size = min(remaining, 1000)
    while remaining:
        batch_count = min(remaining, seed_batch_size)
        records = await backend.enqueue_many([
            TaskRequest(task_name=f"queue_bench_maintenance_{request.namespace}", queue=request.namespace)
            for _ in range(batch_count)
        ])
        claimed = await backend.claim_many(limit=batch_count, queues=(request.namespace,))
        if len(records) != batch_count or len(claimed) != batch_count:
            msg = f"expected {batch_count} seeded and claimed records, got {len(records)} and {len(claimed)}"
            raise RuntimeError(msg)
        completed = await gather_bounded(
            (backend.complete_task(record.id, expected_retry_count=record.retry_count) for record in claimed),
            limit=max(1, request.concurrency),
        )
        for record in completed:
            if record is None or record.completed_at is None:
                msg = "terminal maintenance seed did not complete"
                raise RuntimeError(msg)
            completed_at.append(record.completed_at)
        remaining -= batch_count
    return max(completed_at)


async def _seed_events(
    request: AdapterRequest, service: Any, record_count: int, occurred_at: datetime
) -> None:
    from litestar_queues.events import QueueEvent

    event_log = service.get_event_log()
    if event_log is None:  # pragma: no cover - configured by the caller.
        msg = "event-retention requires a public event log"
        raise RuntimeError(msg)
    for index in range(record_count):
        await event_log.publish_event(
            QueueEvent(
                id=f"{request.namespace}-{index:08d}",
                type="task.event",
                scope="task",
                scope_key=request.namespace,
                task_name=f"queue_bench_maintenance_{request.namespace}",
                queue=request.namespace,
                occurred_at=occurred_at,
            )
        )
    await event_log.flush_events()


async def _run_drain(
    request: AdapterRequest,
    *,
    service: Any,
    config: Any,
    collector: SampleMeasurementCollector,
    phase: Literal["terminal", "events"],
    record_count: int,
    utcnow: Any,
) -> AdapterResult:
    from litestar_queues import QueueMaintenanceService

    maintenance = QueueMaintenanceService(service, config, utcnow=utcnow)
    total_changed = 0
    invocations = 0
    first_invocation_seconds = 0.0
    first_batch_changed = 0
    first_phase_duration_ms = 0.0
    phase_duration_ms = 0.0
    cpu_started = collector.snapshot_cpu()
    started_at = time.perf_counter()
    while total_changed < record_count:
        invocation_started = time.perf_counter()
        selected_phases: tuple[MaintenancePhase, ...] = (phase,)
        summary = await maintenance.run(selected_phases)
        invocation_seconds = time.perf_counter() - invocation_started
        invocations += 1
        if invocations == 1:
            first_invocation_seconds = invocation_seconds
        if not summary.acquired or summary.outcome != "completed" or len(summary.phases) != 1:
            msg = f"maintenance {phase} invocation failed: {summary.to_payload()}"
            raise RuntimeError(msg)
        phase_result = summary.phases[0]
        if phase_result.status != "completed" or phase_result.changed <= 0:
            msg = f"maintenance {phase} drain stopped early: {phase_result.to_payload()}"
            raise RuntimeError(msg)
        total_changed += phase_result.changed
        phase_duration_ms += phase_result.duration_ms
        if invocations == 1:
            first_phase_duration_ms = phase_result.duration_ms
            first_batch_changed = phase_result.changed
        if total_changed > record_count:
            msg = f"maintenance {phase} changed {total_changed} rows after seeding {record_count}"
            raise RuntimeError(msg)
    duration = time.perf_counter() - started_at
    measurements = collector.finish(cpu_started)
    measurements.update({
        "maintenance.first_invocation_seconds": first_invocation_seconds,
        "maintenance.first_phase_duration_ms": first_phase_duration_ms,
        "maintenance.phase_duration_ms": phase_duration_ms,
        "maintenance.rows_per_second": record_count / duration,
        "maintenance.limit": int(request.parameters.get("limit", 1000)),
        "maintenance.outcome": "completed",
        "maintenance.wall_seconds": duration,
    })
    return AdapterResult(
        duration_seconds=duration,
        effective_operations=record_count,
        counters={
            "requests": invocations,
            "records": record_count,
            "started": 0,
            "completed": 0,
            "failed": 0,
            "retried": 0,
            "remaining": 0,
            "changed": total_changed,
            "maintenance_invocations": invocations,
            "continuation_count": max(0, invocations - 1),
            "first_batch_changed": first_batch_changed,
            "lease_denied": 0,
        },
        measurements=measurements,
        metadata={
            "backend_config": type(service.config.queue_backend).__name__,
            "namespace": request.namespace,
            "comparison_class": "no-counterpart",
            "maintenance_phase": phase,
            "seeded_outside_timing": True,
        },
    )


async def _run_lease_contention(
    request: AdapterRequest, service: Any, config: Any, collector: SampleMeasurementCollector
) -> AdapterResult:
    from litestar_queues import QueueMaintenanceService

    backend = service.get_queue_backend()
    token = uuid4().hex
    acquired = await backend.acquire_maintenance(
        service.config.maintenance_name, token, ttl=timedelta(seconds=config.coordination_timeout)
    )
    if not acquired:
        msg = "benchmark lease holder could not acquire maintenance ownership"
        raise RuntimeError(msg)
    try:
        cpu_started = collector.snapshot_cpu()
        started_at = time.perf_counter()
        selected_phases: tuple[MaintenancePhase, ...] = ("terminal",)
        summary = await QueueMaintenanceService(service, config).run(selected_phases)
        duration = time.perf_counter() - started_at
        measurements = collector.finish(cpu_started)
    finally:
        released = await backend.release_maintenance(service.config.maintenance_name, token)
    if not released:
        msg = "benchmark lease holder could not release maintenance ownership"
        raise RuntimeError(msg)
    if summary.outcome != "already_running" or summary.acquired or any(
        phase.status != "skipped" for phase in summary.phases
    ):
        msg = f"maintenance lease denial contract failed: {summary.to_payload()}"
        raise RuntimeError(msg)
    measurements.update({
        "maintenance.lease_denial_seconds": duration,
        "maintenance.outcome": summary.outcome,
        "maintenance.phase_duration_ms": sum(phase.duration_ms for phase in summary.phases),
        "maintenance.rows_per_second": None,
        "maintenance.rows_per_second_available": False,
        "maintenance.wall_seconds": duration,
    })
    return AdapterResult(
        duration_seconds=duration,
        effective_operations=1,
        counters={
            "requests": 1,
            "records": 0,
            "started": 0,
            "completed": 0,
            "failed": 0,
            "retried": 0,
            "remaining": 0,
            "changed": 0,
            "maintenance_invocations": 1,
            "continuation_count": 0,
            "first_batch_changed": 0,
            "lease_denied": 1,
        },
        measurements=measurements,
        metadata={
            "backend_config": type(service.config.queue_backend).__name__,
            "namespace": request.namespace,
            "comparison_class": "no-counterpart",
            "maintenance_phase": "coordination",
            "seeded_outside_timing": True,
        },
    )


async def _create_postgres_schema(request: AdapterRequest, service: Any) -> None:
    if request.backend != "postgres":
        return
    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    backend = service.get_queue_backend()
    if not isinstance(backend, SQLSpecQueueBackend):
        msg = "PostgreSQL maintenance benchmark expected SQLSpecQueueBackend"
        raise TypeError(msg)
    await backend.create_schema()


def _terminal_count(statistics: Any) -> int:
    return int(statistics.completed + statistics.failed + statistics.cancelled + statistics.expired)


__all__ = ("run",)

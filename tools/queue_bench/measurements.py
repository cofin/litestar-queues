"""Bounded, sample-local benchmark measurements."""

import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

import psutil  # type: ignore[import-untyped]
from prometheus_client import CollectorRegistry

from tools.queue_bench.statistics import percentile

MeasurementValue = int | float | str | bool | None


class PickupRecord(Protocol):
    """Persisted timestamps needed to measure backend claim latency."""

    created_at: datetime
    scheduled_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None


# Package-owned transport and heartbeat instruments. Exported histogram buckets
# and ``_created`` series are intentionally excluded to keep every sample bounded.
_PROMETHEUS_INSTRUMENT_SERIES = {
    "litestar_queues.enqueue.batch.size": (
        "litestar_queues_enqueue_batch_size_records_count",
        "litestar_queues_enqueue_batch_size_records_sum",
    ),
    "litestar_queues.wakeup.emitted": ("litestar_queues_wakeup_emitted_total",),
    "litestar_queues.wakeup.coalesced": ("litestar_queues_wakeup_coalesced_total",),
    "litestar_queues.worker.poll.empty": ("litestar_queues_worker_poll_empty_total",),
    "litestar_queues.worker.poll.delay": (
        "litestar_queues_worker_poll_delay_s_count",
        "litestar_queues_worker_poll_delay_s_sum",
    ),
    "litestar_queues.worker.wait.duration": (
        "litestar_queues_worker_wait_duration_seconds_count",
        "litestar_queues_worker_wait_duration_seconds_sum",
    ),
    "litestar_queues.worker.wakeup_to_claim.duration": (
        "litestar_queues_worker_wakeup_to_claim_duration_seconds_count",
        "litestar_queues_worker_wakeup_to_claim_duration_seconds_sum",
    ),
    "litestar_queues.listener.reconnect": ("litestar_queues_listener_reconnect_total",),
    "litestar_queues.listener.error": ("litestar_queues_listener_error_total",),
    "litestar_queues.claim.batch.size": (
        "litestar_queues_claim_batch_size_records_count",
        "litestar_queues_claim_batch_size_records_sum",
    ),
    "litestar_queues.event.flush.size": (
        "litestar_queues_event_flush_size_events_count",
        "litestar_queues_event_flush_size_events_sum",
    ),
    "litestar_queues.event.flush.duration": (
        "litestar_queues_event_flush_duration_seconds_count",
        "litestar_queues_event_flush_duration_seconds_sum",
    ),
    "litestar_queues.event.dropped": ("litestar_queues_event_dropped_total",),
    "litestar_queues.heartbeat.active": ("litestar_queues_heartbeat_active",),
    "litestar_queues.heartbeat.flush": ("litestar_queues_heartbeat_flush_total",),
    "litestar_queues.heartbeat.flush.duration": (
        "litestar_queues_heartbeat_flush_duration_seconds_count",
        "litestar_queues_heartbeat_flush_duration_seconds_sum",
    ),
    "litestar_queues.heartbeat.missed": ("litestar_queues_heartbeat_missed_total",),
    "litestar_queues.heartbeat.failure": ("litestar_queues_heartbeat_failure_total",),
}
_PROMETHEUS_SERIES = tuple(
    series_name for instrument_series in _PROMETHEUS_INSTRUMENT_SERIES.values() for series_name in instrument_series
)


@dataclass(frozen=True, slots=True)
class SampleMeasurementCollector:
    """Own measurement state for exactly one raw benchmark sample."""

    registry: CollectorRegistry

    @classmethod
    def create(cls) -> "SampleMeasurementCollector":
        """Create a collector with a fresh sample-local registry."""
        return cls(registry=CollectorRegistry())

    @staticmethod
    def snapshot_cpu() -> tuple[float, float]:
        """Snapshot child user and system CPU time outside the timed callable."""
        cpu = psutil.Process().cpu_times()
        return float(cpu.user), float(cpu.system)

    def finish(self, cpu_started: tuple[float, float]) -> dict[str, MeasurementValue]:
        """Collect bounded public metrics and elapsed child CPU time."""
        cpu = psutil.Process().cpu_times()
        values: dict[str, MeasurementValue] = {
            "cpu.available": True,
            "cpu.user_seconds": max(0.0, float(cpu.user) - cpu_started[0]),
            "cpu.system_seconds": max(0.0, float(cpu.system) - cpu_started[1]),
            "prometheus.available": True,
            "storage.bytes.available": False,
            "storage.bytes": None,
            "backend.operations.available": False,
            "backend.operations": None,
        }
        values.update(_unavailable_pickup_measurements(0, "scenario_does_not_observe_worker_claims"))
        observed = _collect_allowed_series(self.registry)
        for series_name in _PROMETHEUS_SERIES:
            key = f"prometheus.{series_name}"
            values[f"{key}.available"] = series_name in observed
            values[key] = observed.get(series_name)
        return values

    def prometheus_value(self, series_name: str) -> float:
        """Return the current value of one allowlisted Prometheus series."""
        if series_name not in _PROMETHEUS_SERIES:
            msg = f"Prometheus series is not allowlisted: {series_name}"
            raise ValueError(msg)
        return _collect_allowed_series(self.registry).get(series_name, 0.0)


def summarize_pickup_latency(
    records: list[PickupRecord | None], *, unavailable_reason: str | None = None
) -> dict[str, MeasurementValue]:
    """Summarize persisted enqueue-to-claim and ready-to-claim timestamps.

    ``started_at`` is written by queue backends when a worker successfully
    claims a job. Ready time is the later of creation and an explicit schedule,
    so scheduled delay is not attributed to worker pickup.

    Returns:
        Scalar measurements suitable for one raw benchmark sample.
    """
    if unavailable_reason is not None:
        return _unavailable_pickup_measurements(len(records), unavailable_reason)

    enqueue_latencies: list[float] = []
    ready_latencies: list[float] = []
    for record in records:
        if record is None or record.started_at is None:
            continue
        ready_at = max(record.created_at, record.scheduled_at or record.created_at)
        enqueue_latency = (record.started_at - record.created_at).total_seconds()
        ready_latency = (record.started_at - ready_at).total_seconds()
        if enqueue_latency < 0 or ready_latency < 0:
            msg = "persisted worker claim timestamp precedes task eligibility"
            raise ValueError(msg)
        enqueue_latencies.append(enqueue_latency)
        ready_latencies.append(ready_latency)

    observed_count = len(enqueue_latencies)
    missing_count = len(records) - observed_count
    if not enqueue_latencies:
        return _unavailable_pickup_measurements(len(records), "no_persisted_started_at")
    measurements: dict[str, MeasurementValue] = {
        "queue.pickup.available": True,
        "queue.pickup.observed_count": observed_count,
        "queue.pickup.missing_count": missing_count,
        "queue.pickup.unavailable_reason": None,
        "queue.pickup.timestamp_source": "persisted_backend_claim",
    }
    measurements.update(_latency_summary("queue.pickup.enqueue_to_started", enqueue_latencies))
    measurements.update(_latency_summary("queue.pickup.ready_to_started", ready_latencies))
    return measurements


def summarize_task_phases(records: list[PickupRecord | None], observed_at: datetime) -> dict[str, MeasurementValue]:
    """Summarize persisted execution and observer-return phases."""
    execution: list[float] = []
    observation: list[float] = []
    for record in records:
        if record is None or record.started_at is None or record.completed_at is None:
            continue
        execution_seconds = (record.completed_at - record.started_at).total_seconds()
        observation_seconds = (observed_at - record.completed_at).total_seconds()
        if execution_seconds < 0 or observation_seconds < 0:
            msg = "persisted task phase timestamps are out of order"
            raise ValueError(msg)
        execution.append(execution_seconds)
        observation.append(observation_seconds)
    if not execution:
        return {"queue.execution.available": False, "queue.observer_return.available": False}
    values: dict[str, MeasurementValue] = {
        "queue.execution.available": True,
        "queue.observer_return.available": True,
        "queue.observer_return.timestamp_source": "child_wall_clock_utc",
    }
    values.update(_latency_summary("queue.execution.started_to_completed", execution))
    values.update(_latency_summary("queue.observer_return.completed_to_return", observation))
    return values


def summarize_durations(prefix: str, values: list[float]) -> dict[str, MeasurementValue]:
    """Summarize a non-empty collection of monotonic operation durations."""
    if not values:
        return {f"{prefix}.available": False}
    return {f"{prefix}.available": True, **_latency_summary(prefix, values)}


def summarize_saq_pickup(jobs: list[object]) -> dict[str, MeasurementValue]:
    """Summarize SAQ's persisted millisecond queued-to-started timestamps."""
    values: list[float] = []
    for job in jobs:
        queued = getattr(job, "queued", None)
        started = getattr(job, "started", None)
        if queued is None or started is None:
            continue
        duration = (float(started) - float(queued)) / 1000.0
        if duration < 0:
            msg = "SAQ persisted started timestamp precedes queued timestamp"
            raise ValueError(msg)
        values.append(duration)
    if not values:
        return _unavailable_pickup_measurements(len(jobs), "no_saq_persisted_queued_started")
    measurements: dict[str, MeasurementValue] = {
        "queue.pickup.available": True,
        "queue.pickup.observed_count": len(values),
        "queue.pickup.missing_count": len(jobs) - len(values),
        "queue.pickup.unavailable_reason": None,
        "queue.pickup.timestamp_source": "saq_persisted_queued_started_ms",
    }
    measurements.update(_latency_summary("queue.pickup.ready_to_started", values))
    return measurements


def _latency_summary(prefix: str, values: list[float]) -> dict[str, MeasurementValue]:
    return {
        f"{prefix}.min_seconds": min(values),
        f"{prefix}.mean_seconds": statistics.fmean(values),
        f"{prefix}.p50_seconds": percentile(values, 50),
        f"{prefix}.p95_seconds": percentile(values, 95),
        f"{prefix}.p99_seconds": percentile(values, 99),
        f"{prefix}.max_seconds": max(values),
    }


def _unavailable_pickup_measurements(expected_count: int, reason: str) -> dict[str, MeasurementValue]:
    values: dict[str, MeasurementValue] = {
        "queue.pickup.available": False,
        "queue.pickup.observed_count": 0,
        "queue.pickup.missing_count": expected_count,
        "queue.pickup.unavailable_reason": reason,
        "queue.pickup.timestamp_source": "persisted_backend_claim",
    }
    for prefix in ("queue.pickup.enqueue_to_started", "queue.pickup.ready_to_started"):
        for statistic in ("min", "mean", "p50", "p95", "p99", "max"):
            values[f"{prefix}.{statistic}_seconds"] = None
    return values


def _collect_allowed_series(registry: CollectorRegistry) -> dict[str, float]:
    suffix_to_canonical = {name.removeprefix("litestar_queues_"): name for name in _PROMETHEUS_SERIES}
    totals: dict[str, float] = {}
    for metric in registry.collect():
        for sample in metric.samples:
            canonical = next(
                (name for suffix, name in suffix_to_canonical.items() if sample.name.endswith(f"_{suffix}")), None
            )
            if canonical is not None:
                totals[canonical] = totals.get(canonical, 0.0) + float(sample.value)
    return totals


__all__ = (
    "MeasurementValue",
    "SampleMeasurementCollector",
    "summarize_durations",
    "summarize_pickup_latency",
    "summarize_saq_pickup",
    "summarize_task_phases",
)

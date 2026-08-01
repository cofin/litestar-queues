"""Bounded, sample-local benchmark measurements."""

from dataclasses import dataclass

import psutil  # type: ignore[import-untyped]
from prometheus_client import CollectorRegistry

MeasurementValue = int | float | str | bool | None

# The 13 package-owned transport instruments. Exported histogram buckets and
# ``_created`` series are intentionally excluded to keep every sample bounded.
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
        observed = _collect_allowed_series(self.registry)
        for series_name in _PROMETHEUS_SERIES:
            key = f"prometheus.{series_name}"
            values[f"{key}.available"] = series_name in observed
            values[key] = observed.get(series_name)
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


__all__ = ("MeasurementValue", "SampleMeasurementCollector")

"""Run controlled microbenchmarks for queue transport control paths.

The harness uses deterministic fake operations. It measures relative control
path costs and operation counts without starting workers or external services;
``tools/benchmark_queues.py`` owns end-to-end backend benchmarks.
"""

import argparse
import json
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.queue_bench.environment import capture_environment  # noqa: E402

SCHEMA_VERSION = "1.0"


class _OperationCounter:
    __slots__ = ("counts",)

    def __init__(self) -> "None":
        self.counts: "dict[str, int]" = {}

    def call(self, operation: "str", count: "int" = 1) -> "None":
        self.counts[operation] = self.counts.get(operation, 0) + count


def _percentile(samples: "list[float]", percentile: "float") -> "float | None":
    if not samples:
        return None
    ordered = sorted(samples)
    rank = (len(ordered) - 1) * percentile / 100
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


def _result(
    *,
    scenario: "str",
    variant: "str",
    parameters: "dict[str, Any]",
    count: "int",
    duration: "float",
    operations: "dict[str, int]",
    latencies: "list[float] | None" = None,
) -> "dict[str, Any]":
    samples = latencies or []
    return {
        "scenario": scenario,
        "backend": "controlled-fake",
        "variant": variant,
        "parameters": parameters,
        "count": count,
        "duration_seconds": duration,
        "throughput_per_second": count / duration if duration > 0 else None,
        "p50_seconds": _percentile(samples, 50),
        "p95_seconds": _percentile(samples, 95),
        "p99_seconds": _percentile(samples, 99),
        "operation_counts": operations,
    }


def benchmark_idle_polling(
    *, duration: "float", interval: "float" = 1.0, maximum_interval: "float" = 4.0
) -> "list[dict[str, Any]]":
    """Compare fixed and exponentially backed-off polling over simulated time."""
    results: "list[dict[str, Any]]" = []
    for variant in ("fixed", "adaptive"):
        counter = _OperationCounter()
        elapsed = 0.0
        delay = interval
        while elapsed < duration:
            counter.call("backend_calls")
            elapsed += delay
            if variant == "adaptive":
                delay = min(delay * 2, maximum_interval)
        results.append(
            _result(
                scenario="idle_polling",
                variant=variant,
                parameters={
                    "simulated_duration_seconds": duration,
                    "initial_interval_seconds": interval,
                    "maximum_interval_seconds": maximum_interval,
                },
                count=counter.counts["backend_calls"],
                duration=duration,
                operations=counter.counts,
            )
        )
    return results


def benchmark_native_notifications(*, task_count: "int", reconcile_every: "int") -> "list[dict[str, Any]]":
    """Measure controlled notification-to-claim latency and reconciliation calls."""
    counter = _OperationCounter()
    latencies: "list[float]" = []
    started = time.perf_counter()
    for index in range(task_count):
        notification_started = time.perf_counter()
        counter.call("notification_calls")
        counter.call("claim_calls")
        latencies.append(time.perf_counter() - notification_started)
        if (index + 1) % reconcile_every == 0 or index + 1 == task_count:
            counter.call("reconciliation_calls")
    duration = time.perf_counter() - started
    return [
        _result(
            scenario="native_notification",
            variant="native",
            parameters={"task_count": task_count, "reconcile_every": reconcile_every},
            count=task_count,
            duration=duration,
            operations=counter.counts,
            latencies=latencies,
        )
    ]


def benchmark_claim_batches(*, task_count: "int", batch_size: "int") -> "list[dict[str, Any]]":
    """Compare one-record claims with native batch claims."""
    results: "list[dict[str, Any]]" = []
    for variant, size in (("sequential", 1), ("native_batch", batch_size)):
        counter = _OperationCounter()
        started = time.perf_counter()
        for _offset in range(0, task_count, size):
            counter.call("backend_operations")
        duration = time.perf_counter() - started
        results.append(
            _result(
                scenario="claim_batching",
                variant=variant,
                parameters={"task_count": task_count, "batch_size": size},
                count=task_count,
                duration=duration,
                operations=counter.counts,
            )
        )
    return results


def benchmark_event_batches(*, event_count: "int", batch_size: "int") -> "list[dict[str, Any]]":
    """Compare sequential event publication with ``publish_many`` batches."""
    results: "list[dict[str, Any]]" = []
    for variant, size in (("sequential", 1), ("publish_many", batch_size)):
        counter = _OperationCounter()
        maximum_flush_size = 0
        started = time.perf_counter()
        for offset in range(0, event_count, size):
            flush_size = min(size, event_count - offset)
            maximum_flush_size = max(maximum_flush_size, flush_size)
            counter.call("flushes")
            counter.call("transport_calls")
        duration = time.perf_counter() - started
        results.append(
            _result(
                scenario="event_batching",
                variant=variant,
                parameters={"event_count": event_count, "batch_size": size, "maximum_flush_size": maximum_flush_size},
                count=event_count,
                duration=duration,
                operations=counter.counts,
            )
        )
    return results


def build_report(
    *, task_count: "int" = 1000, batch_size: "int" = 100, idle_duration: "float" = 60.0
) -> "dict[str, Any]":
    """Build one JSON-serializable controlled benchmark report."""
    results = [
        *benchmark_idle_polling(duration=idle_duration),
        *benchmark_native_notifications(task_count=task_count, reconcile_every=batch_size),
        *benchmark_claim_batches(task_count=task_count, batch_size=batch_size),
        *benchmark_event_batches(event_count=task_count, batch_size=batch_size),
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": capture_environment(packages=["litestar-queues"], network_class="none-controlled-fake"),
        "results": results,
    }


def _positive_int(value: "str") -> "int":
    parsed = int(value)
    if parsed <= 0:
        message = "must be greater than zero"
        raise argparse.ArgumentTypeError(message)
    return parsed


def _positive_float(value: "str") -> "float":
    parsed = float(value)
    if parsed <= 0:
        message = "must be greater than zero"
        raise argparse.ArgumentTypeError(message)
    return parsed


def main() -> "None":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", type=_positive_int, default=1000, help="Controlled record/event count.")
    parser.add_argument("--batch-size", type=_positive_int, default=100, help="Native operation batch size.")
    parser.add_argument(
        "--idle-duration", type=_positive_float, default=60.0, help="Simulated idle-poll duration in seconds."
    )
    parser.add_argument("--output", type=Path, help="Write JSON to this path instead of standard output.")
    parser.add_argument("--pretty", action="store_true", help="Indent the JSON output.")
    args = parser.parse_args()

    report = build_report(task_count=args.tasks, batch_size=args.batch_size, idle_duration=args.idle_duration)
    rendered = json.dumps(report, indent=2 if args.pretty else None, sort_keys=True)
    if args.output is None:
        print(rendered)
    else:
        args.output.write_text(f"{rendered}\n", encoding="utf-8")


if __name__ == "__main__":
    main()

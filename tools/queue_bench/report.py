"""Human-readable benchmark reports."""

import statistics

from tools.queue_bench.models import BenchmarkResult


def render_markdown(result: BenchmarkResult) -> str:
    """Render one benchmark result as portable Markdown.

    Returns:
        Markdown document with measurements and comparison context.
    """
    environment = result.environment
    lines = [
        "# Queue benchmark results",
        "",
        f"Generated: `{result.generated_at}`  ",
        f"Schema: `{result.schema_version}`  ",
        f"Git: `{environment.get('git_sha', 'unknown')}`  ",
        f"Git dirty: `{environment.get('git_dirty', 'unknown')}`  ",
        f"Python: `{environment.get('python', 'unknown')}`  ",
        f"Network: `{environment.get('network_class', 'unknown')}`",
        "",
        "## Measurements",
        "",
        "| System | Backend | Scenario | Samples | Median (ms) | p95 (ms) | p99 (ms) | Throughput (ops/s) |",
        "|---|---|---|---:|---:|---:|---:|---:|",
    ]
    lines.extend(
        (
            "| "
            f"{aggregate.system} | {aggregate.backend} | {aggregate.scenario} | {aggregate.sample_count} | "
            f"{aggregate.median_seconds * 1_000:.2f} | {aggregate.p95_seconds * 1_000:.2f} | "
            f"{aggregate.p99_seconds * 1_000:.2f} | {aggregate.median_throughput:.2f} |"
        )
        for aggregate in sorted(result.aggregates, key=lambda item: (item.backend, item.scenario, item.system))
    )
    pickup_rows = _pickup_rows(result)
    if pickup_rows:
        lines.extend([
            "",
            "## Worker pickup latency",
            "",
            (
                "Timestamps come from persisted backend claims. Values are medians of each sample's "
                "job-level percentile; ready-to-claim excludes intentional scheduling delay."
            ),
            "",
            (
                "| System | Backend | Driver | Scenario | Samples | Jobs | Enqueue to claim p50 (ms) | "
                "Ready to claim p50 (ms) | Ready to claim p95 (ms) | Ready to claim p99 (ms) |"
            ),
            "|---|---|---|---|---:|---:|---:|---:|---:|---:|",
        ])
        lines.extend(pickup_rows)
    if result.comparisons:
        lines.extend([
            "",
            "## Paired comparisons",
            "",
            (
                "Ratios are candidate throughput divided by Litestar Queues throughput. Results need the "
                "configured sample count and confidence interval before a difference is marked material."
            ),
            "",
            "| Candidate | Backend | Scenario | Pairs | Ratio | 95% interval | Material | Class |",
            "|---|---|---|---:|---:|---:|---|---|",
        ])
        lines.extend(
            (
                "| "
                f"{comparison['candidate']} | {comparison['backend']} | {comparison['scenario']} | "
                f"{comparison['sample_count']} | {comparison['median_ratio']:.2f}x | "
                f"{comparison['ratio_interval'][0]:.2f}-"
                f"{comparison['ratio_interval'][1]:.2f} | "
                f"{'yes' if comparison['material'] else 'no'} | {comparison['comparison_class']} |"
            )
            for comparison in result.comparisons
        )
    if result.annotations:
        lines.extend([
            "",
            "## Comparison annotations",
            "",
            "| System | Backend | Scenario | Class | Detail |",
            "|---|---|---|---|---|",
        ])
        lines.extend(
            (
                "| "
                f"{annotation.get('system', '')} | {annotation.get('backend', '')} | "
                f"{annotation.get('scenario', '')} | {annotation.get('comparison_class', '')} | "
                f"{annotation.get('detail', '')} |"
            )
            for annotation in result.annotations
        )
    invalid = [sample for sample in result.samples if not sample.valid]
    if invalid:
        lines.extend(["", "## Invalid samples", ""])
        lines.extend(
            (
                f"- `{sample.system}/{sample.backend}/{sample.scenario}#{sample.sample_index}`: "
                f"{sample.error or 'unknown error'}"
            )
            for sample in invalid
        )
    return "\n".join(lines) + "\n"


def _pickup_rows(result: BenchmarkResult) -> list[str]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, int | float | str | bool | None]]] = {}
    for sample in result.samples:
        if sample.valid and sample.measurements.get("queue.pickup.available") is True:
            driver = str(sample.metadata.get("driver") or "-")
            grouped.setdefault((sample.system, sample.backend, driver, sample.scenario), []).append(sample.measurements)
    rows: list[str] = []
    for (system, backend, driver, scenario), measurements in sorted(grouped.items(), key=lambda item: item[0]):
        enqueue_p50 = _median_measurement(measurements, "queue.pickup.enqueue_to_started.p50_seconds")
        ready_p50 = _median_measurement(measurements, "queue.pickup.ready_to_started.p50_seconds")
        ready_p95 = _median_measurement(measurements, "queue.pickup.ready_to_started.p95_seconds")
        ready_p99 = _median_measurement(measurements, "queue.pickup.ready_to_started.p99_seconds")
        observed = sum(int(item.get("queue.pickup.observed_count") or 0) for item in measurements)
        rows.append(
            f"| {system} | {backend} | {driver} | {scenario} | {len(measurements)} | {observed} | "
            f"{_format_milliseconds(enqueue_p50)} | {_format_milliseconds(ready_p50)} | "
            f"{_format_milliseconds(ready_p95)} | {_format_milliseconds(ready_p99)} |"
        )
    return rows


def _median_measurement(measurements: list[dict[str, int | float | str | bool | None]], key: str) -> float | None:
    values = [float(value) for item in measurements if isinstance((value := item.get(key)), (int, float))]
    return float(statistics.median(values)) if values else None


def _format_milliseconds(value: float | None) -> str:
    """Format an optional seconds measurement as milliseconds."""
    return "-" if value is None else f"{value * 1_000:.2f}"


__all__ = ("render_markdown",)

"""Human-readable benchmark reports."""

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
    if result.comparisons:
        lines.extend([
            "",
            "## Paired comparisons",
            "",
            "Ratios are candidate throughput divided by Litestar Queues throughput. Results need the configured "
            "sample count and confidence interval before a difference is marked material.",
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


__all__ = ("render_markdown",)

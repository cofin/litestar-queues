"""Deterministic statistics for benchmark reports."""

import random
import statistics

MAX_PERCENTAGE = 100
MIN_BOOTSTRAP_ITERATIONS = 2


def percentile(values: list[float], percentage: float) -> float:
    """Return a linearly interpolated percentile."""
    if not values:
        msg = "percentile requires at least one value"
        raise ValueError(msg)
    if not 0 <= percentage <= MAX_PERCENTAGE:
        msg = "percentage must be between 0 and 100"
        raise ValueError(msg)
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentage / 100
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def median_absolute_deviation(values: list[float]) -> float:
    """Return the median absolute deviation from the sample median."""
    if not values:
        msg = "median absolute deviation requires at least one value"
        raise ValueError(msg)
    center = statistics.median(values)
    return float(statistics.median(abs(value - center) for value in values))


def bootstrap_ratio_interval(
    baseline: list[float], candidate: list[float], *, seed: int, iterations: int = 2_000
) -> tuple[float, float]:
    """Bootstrap a 95% interval for candidate/baseline median ratio.

    Returns:
        Lower and upper bounds of the ratio interval.
    """
    if not baseline or not candidate:
        msg = "bootstrap samples cannot be empty"
        raise ValueError(msg)
    if iterations < MIN_BOOTSTRAP_ITERATIONS:
        msg = "bootstrap iterations must be at least 2"
        raise ValueError(msg)
    rng = random.Random(seed)  # noqa: S311 - deterministic sampling is required for reproducibility.
    ratios: list[float] = []
    for _ in range(iterations):
        baseline_median = statistics.median(rng.choices(baseline, k=len(baseline)))
        candidate_median = statistics.median(rng.choices(candidate, k=len(candidate)))
        if baseline_median == 0:
            msg = "baseline median cannot be zero"
            raise ValueError(msg)
        ratios.append(float(candidate_median / baseline_median))
    return percentile(ratios, 2.5), percentile(ratios, 97.5)


def bootstrap_paired_ratio_interval(
    baseline: list[float], candidate: list[float], *, seed: int, iterations: int = 2_000
) -> tuple[float, float]:
    """Bootstrap a 95% median ratio interval while preserving sample pairs.

    Returns:
        Lower and upper bounds of the paired ratio interval.
    """
    if not baseline or not candidate:
        msg = "bootstrap samples cannot be empty"
        raise ValueError(msg)
    if len(baseline) != len(candidate):
        msg = "paired bootstrap samples must have equal lengths"
        raise ValueError(msg)
    if iterations < MIN_BOOTSTRAP_ITERATIONS:
        msg = "bootstrap iterations must be at least 2"
        raise ValueError(msg)
    ratios: list[float] = []
    for baseline_value, candidate_value in zip(baseline, candidate, strict=True):
        if baseline_value == 0:
            msg = "baseline values cannot be zero"
            raise ValueError(msg)
        ratios.append(candidate_value / baseline_value)
    rng = random.Random(seed)  # noqa: S311 - deterministic sampling is required for reproducibility.
    medians = [statistics.median(rng.choices(ratios, k=len(ratios))) for _ in range(iterations)]
    return percentile(medians, 2.5), percentile(medians, 97.5)


def is_material_difference(
    *,
    ratio_interval: tuple[float, float],
    median_ratio: float,
    absolute_gap: float,
    is_latency: bool,
    minimum_relative_gap: float = 0.20,
    minimum_local_latency_gap: float = 0.001,
) -> bool:
    """Return whether confidence, relative effect, and latency floors agree."""
    lower, upper = ratio_interval
    excludes_one = lower > 1.0 or upper < 1.0
    relative_gap = abs(median_ratio - 1.0)
    if not excludes_one or relative_gap < minimum_relative_gap:
        return False
    return not is_latency or absolute_gap >= minimum_local_latency_gap


__all__ = (
    "bootstrap_paired_ratio_interval",
    "bootstrap_ratio_interval",
    "is_material_difference",
    "median_absolute_deviation",
    "percentile",
)

"""Versioned JSON-native benchmark result models."""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from tools.queue_bench import SCHEMA_VERSION
from tools.queue_bench.statistics import median_absolute_deviation, percentile


@dataclass(frozen=True, slots=True)
class RawSample:
    system: str
    backend: str
    scenario: str
    sample_index: int
    duration_seconds: float
    operations: int
    valid: bool
    counters: dict[str, int]
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def throughput(self) -> float | None:
        if not self.valid or self.duration_seconds <= 0:
            return None
        return self.operations / self.duration_seconds

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RawSample":
        return cls(
            system=str(value["system"]),
            backend=str(value["backend"]),
            scenario=str(value["scenario"]),
            sample_index=int(value["sample_index"]),
            duration_seconds=float(value["duration_seconds"]),
            operations=int(value["operations"]),
            valid=bool(value["valid"]),
            counters={str(key): int(count) for key, count in dict(value["counters"]).items()},
            error=str(value["error"]) if value.get("error") is not None else None,
            metadata=dict(value.get("metadata", {})),
        )


@dataclass(frozen=True, slots=True)
class ScenarioAggregate:
    system: str
    backend: str
    scenario: str
    sample_count: int
    median_seconds: float
    p50_seconds: float
    p95_seconds: float
    p99_seconds: float
    median_throughput: float
    mad_throughput: float

    @classmethod
    def from_samples(cls, samples: list[RawSample]) -> "ScenarioAggregate":
        valid = [sample for sample in samples if sample.valid and sample.throughput is not None]
        if not valid:
            msg = "aggregate requires at least one valid sample"
            raise ValueError(msg)
        first = valid[0]
        if any(
            (sample.system, sample.backend, sample.scenario) != (first.system, first.backend, first.scenario)
            for sample in valid
        ):
            msg = "aggregate samples must share system, backend, and scenario"
            raise ValueError(msg)
        durations = [sample.duration_seconds for sample in valid]
        throughputs = [sample.throughput for sample in valid if sample.throughput is not None]
        return cls(
            system=first.system,
            backend=first.backend,
            scenario=first.scenario,
            sample_count=len(valid),
            median_seconds=percentile(durations, 50),
            p50_seconds=percentile(durations, 50),
            p95_seconds=percentile(durations, 95),
            p99_seconds=percentile(durations, 99),
            median_throughput=percentile(throughputs, 50),
            mad_throughput=median_absolute_deviation(throughputs),
        )

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ScenarioAggregate":
        return cls(**value)


@dataclass(slots=True)
class BenchmarkResult:
    environment: dict[str, Any]
    samples: list[RawSample]
    aggregates: list[ScenarioAggregate]
    comparisons: list[dict[str, Any]] = field(default_factory=list)
    annotations: list[dict[str, Any]] = field(default_factory=list)
    schema_version: str = SCHEMA_VERSION
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "BenchmarkResult":
        return cls(
            environment=dict(value["environment"]),
            samples=[RawSample.from_dict(item) for item in value.get("samples", [])],
            aggregates=[ScenarioAggregate.from_dict(item) for item in value.get("aggregates", [])],
            comparisons=[dict(item) for item in value.get("comparisons", [])],
            annotations=[dict(item) for item in value.get("annotations", [])],
            schema_version=str(value["schema_version"]),
            generated_at=str(value["generated_at"]),
        )


__all__ = ("BenchmarkResult", "RawSample", "ScenarioAggregate")

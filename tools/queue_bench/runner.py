"""Benchmark orchestration and per-system process isolation."""

import json
import random
import statistics
import subprocess
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from tools.dev_infra import ContainerRuntime, InfraManager
from tools.queue_bench.environment import capture_environment, redact_data
from tools.queue_bench.infra import parse_dsn_overrides, select_local_services
from tools.queue_bench.models import BenchmarkResult, RawSample, ScenarioAggregate
from tools.queue_bench.statistics import bootstrap_paired_ratio_interval, is_material_difference

DEFAULT_SYSTEMS = ("litestar-queues", "litestar-saq", "arq", "taskiq")
MIN_MATERIAL_SAMPLES = 5
SYSTEM_BACKENDS: dict[str, frozenset[str]] = {
    "litestar-queues": frozenset({"redis", "valkey", "postgres"}),
    "litestar-saq": frozenset({"redis", "postgres"}),
    "raw-saq": frozenset({"redis", "postgres"}),
    "arq": frozenset({"redis"}),
    "taskiq": frozenset({"redis"}),
    "dramatiq": frozenset({"redis"}),
    "rq": frozenset({"redis"}),
    "celery": frozenset({"redis"}),
}
COMPETITOR_SCRIPTS = {
    "litestar-saq": "run_saq",
    "raw-saq": "run_saq",
    "arq": "run_arq",
    "taskiq": "run_taskiq",
    "dramatiq": "run_dramatiq",
    "rq": "run_rq",
    "celery": "run_celery",
}
SYSTEM_PACKAGES = {
    "litestar-queues": ["litestar-queues", "redis", "sqlspec", "asyncpg", "psycopg"],
    "litestar-saq": ["litestar-saq", "saq", "redis", "psycopg"],
    "raw-saq": ["saq", "redis", "psycopg"],
    "arq": ["arq", "redis"],
    "taskiq": ["taskiq", "taskiq-redis", "redis"],
    "dramatiq": ["dramatiq", "redis"],
    "rq": ["rq", "redis"],
    "celery": ["celery", "redis"],
}


@dataclass(frozen=True, slots=True)
class RunConfig:
    """Validated benchmark run inputs."""

    systems: tuple[str, ...] = DEFAULT_SYSTEMS
    backends: tuple[str, ...] = ("redis", "postgres")
    scenarios: tuple[str, ...] = ("enqueue", "roundtrip")
    warmups: int = 3
    samples: int = 10
    operations: int = 100
    payload_size: int = 512
    concurrency: int = 1
    seed: int = 20260720
    dsn_overrides: tuple[str, ...] = ()
    pull_images: bool = False
    remote: bool = False
    timeout_seconds: float = 120.0


def validate_run_config(config: RunConfig) -> None:
    """Reject invalid or ambiguous run inputs."""
    unknown_systems = set(config.systems) - SYSTEM_BACKENDS.keys()
    if unknown_systems:
        msg = f"unsupported systems: {', '.join(sorted(unknown_systems))}"
        raise ValueError(msg)
    unknown_backends = set(config.backends) - {"postgres", "redis", "valkey"}
    if unknown_backends:
        msg = f"unsupported backends: {', '.join(sorted(unknown_backends))}"
        raise ValueError(msg)
    unknown_scenarios = set(config.scenarios) - {"enqueue", "roundtrip"}
    if unknown_scenarios:
        msg = f"unsupported scenarios: {', '.join(sorted(unknown_scenarios))}"
        raise ValueError(msg)
    for label, value in (
        ("samples", config.samples),
        ("operations", config.operations),
        ("payload-size", config.payload_size),
        ("concurrency", config.concurrency),
    ):
        if value < 1:
            msg = f"{label} must be at least 1"
            raise ValueError(msg)
    if config.warmups < 0:
        msg = "warmups cannot be negative"
        raise ValueError(msg)
    if config.remote:
        overrides = parse_dsn_overrides(list(config.dsn_overrides))
        missing = set(config.backends) - overrides.keys()
        if missing:
            msg = f"remote runs require --dsn for: {', '.join(sorted(missing))}"
            raise ValueError(msg)


def compatible_pairs(*, systems: Sequence[str], backends: Sequence[str]) -> tuple[tuple[str, str], ...]:
    """Return requested system/backend pairs with a supported broker contract."""
    return tuple((system, backend) for system in systems for backend in backends if backend in SYSTEM_BACKENDS[system])


def build_child_command(system: str, *, root: Path) -> list[str]:
    """Build a reproducible child command for one benchmark system.

    Returns:
        Command arguments for the isolated child process.
    """
    if system == "litestar-queues":
        return ["uv", "run", "--group", "benchmarks", "python", "-m", "tools.queue_bench.child"]
    script_name = COMPETITOR_SCRIPTS[system]
    script_path = root / "tools" / "queue_bench" / "runtimes" / f"{script_name}.py"
    return ["uv", "run", "--script", str(script_path)]


def run_benchmarks(
    config: RunConfig,
    *,
    root: Path,
    runtime_factory: Callable[[], ContainerRuntime] = ContainerRuntime,
    run_child: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> BenchmarkResult:
    """Run selected samples and return a complete, versioned result.

    Returns:
        Environment-stamped raw samples, aggregates, and comparisons.
    """
    validate_run_config(config)
    overrides = parse_dsn_overrides(list(config.dsn_overrides))
    services = select_local_services(list(config.backends), overrides)
    runtime: ContainerRuntime | None = None
    if services:
        runtime = runtime_factory()
        InfraManager(runtime, services).start(pull=config.pull_images, recreate=False)
    service_by_key = {service.key: service for service in services}
    dsns = {backend: overrides.get(backend, service_by_key[backend].url) for backend in config.backends}
    network_class = (
        "remote" if config.remote or len(overrides) == len(config.backends) else "mixed" if overrides else "local"
    )
    environment = capture_environment(
        packages=sorted({package for system in config.systems for package in SYSTEM_PACKAGES[system]}),
        network_class=network_class,
    )
    environment["services"] = _capture_services(runtime, services)
    environment["config"] = redact_data({
        "systems": list(config.systems),
        "backends": list(config.backends),
        "scenarios": list(config.scenarios),
        "warmups": config.warmups,
        "samples": config.samples,
        "operations": config.operations,
        "payload_size": config.payload_size,
        "concurrency": config.concurrency,
        "seed": config.seed,
        "dsns": dsns,
    })

    samples: list[RawSample] = []
    rng = random.Random(config.seed)  # noqa: S311 - deterministic order is required for reproducibility.
    pairs = list(compatible_pairs(systems=config.systems, backends=config.backends))
    for pass_index in range(config.warmups + config.samples):
        rng.shuffle(pairs)
        for system, backend in pairs:
            for scenario in config.scenarios:
                request = {
                    "system": system,
                    "backend": backend,
                    "dsn": dsns[backend],
                    "scenario": scenario,
                    "operations": config.operations,
                    "payload_size": config.payload_size,
                    "concurrency": config.concurrency,
                    "namespace": f"lqb_{uuid.uuid4().hex}",
                    "sample_index": pass_index - config.warmups,
                    "timeout_seconds": config.timeout_seconds,
                }
                sample = _invoke_child(
                    system, request, root=root, timeout_seconds=config.timeout_seconds, run_child=run_child
                )
                if pass_index >= config.warmups:
                    samples.append(sample)

    environment["child_packages"] = _promote_child_packages(samples)
    aggregates = _aggregate(samples)
    return BenchmarkResult(
        environment=environment,
        samples=samples,
        aggregates=aggregates,
        comparisons=_comparisons(samples, seed=config.seed),
        annotations=[*_architecture_annotations(config), *_unsupported_annotations(config)],
    )


def _invoke_child(
    system: str,
    request: dict[str, Any],
    *,
    root: Path,
    timeout_seconds: float,
    run_child: Callable[..., subprocess.CompletedProcess[str]],
) -> RawSample:
    command = [*build_child_command(system, root=root), "--request", json.dumps(request, separators=(",", ":"))]
    process_started_at = time.perf_counter()
    try:
        result = run_child(command, cwd=root, capture_output=True, check=False, text=True, timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        return _invalid_sample(request, f"child exceeded {timeout_seconds}s timeout")
    process_elapsed_seconds = time.perf_counter() - process_started_at
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or f"child exited {result.returncode}"
        return _invalid_sample(request, detail)
    try:
        payload = _decode_child_stdout(result.stdout)
        sample = RawSample.from_dict(payload)
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        return _invalid_sample(request, f"invalid child output: {exc}")
    metadata = dict(sample.metadata)
    metadata["process_elapsed_seconds"] = process_elapsed_seconds
    metadata["stdout"] = _child_log_output(result.stdout)
    metadata["stderr"] = result.stderr
    return RawSample(
        system=sample.system,
        backend=sample.backend,
        scenario=sample.scenario,
        sample_index=sample.sample_index,
        duration_seconds=sample.duration_seconds,
        operations=sample.operations,
        valid=sample.valid,
        counters=sample.counters,
        error=sample.error,
        metadata=redact_data(metadata),
    )


def _decode_child_stdout(stdout: str) -> dict[str, Any]:
    for line in reversed(stdout.splitlines()):
        candidate = line.strip()
        if not candidate.startswith("{"):
            continue
        value = json.loads(candidate)
        if isinstance(value, dict):
            return value
    msg = "child stdout did not contain a JSON object"
    raise ValueError(msg)


def _child_log_output(stdout: str) -> str:
    return "\n".join(line for line in stdout.splitlines() if not line.strip().startswith("{"))


def _invalid_sample(request: dict[str, Any], error: str) -> RawSample:
    return RawSample(
        system=str(request["system"]),
        backend=str(request["backend"]),
        scenario=str(request["scenario"]),
        sample_index=int(request["sample_index"]),
        duration_seconds=0.0,
        operations=int(request["operations"]),
        valid=False,
        counters={"enqueued": 0, "started": 0, "completed": 0, "remaining": 0},
        error=error,
    )


def _aggregate(samples: list[RawSample]) -> list[ScenarioAggregate]:
    grouped: dict[tuple[str, str, str], list[RawSample]] = {}
    for sample in samples:
        grouped.setdefault((sample.system, sample.backend, sample.scenario), []).append(sample)
    return [ScenarioAggregate.from_samples(group) for group in grouped.values() if any(item.valid for item in group)]


def _capture_services(runtime: ContainerRuntime | None, services: Sequence[Any]) -> list[dict[str, Any]]:
    captured: list[dict[str, Any]] = []
    for service in services:
        status = runtime.status(service.container_name) if runtime is not None else None
        captured.append({
            "backend": service.key,
            "image": service.image,
            "image_digest": runtime.image_digest(service.image) if runtime is not None else "",
            "backend_version": _service_version(runtime, service.key, service.container_name),
            "container_id": status.container_id if status is not None else "",
            "ports": status.ports if status is not None else "",
            "url": service.url,
        })
    return cast("list[dict[str, Any]]", redact_data(captured))


def _service_version(runtime: ContainerRuntime | None, backend: str, container_name: str) -> str:
    if runtime is None:
        return ""
    commands = {
        "postgres": ["postgres", "--version"],
        "redis": ["redis-server", "--version"],
        "valkey": ["valkey-server", "--version"],
    }
    result = runtime.run(["exec", container_name, *commands[backend]], check=False)
    return result.stdout.strip() if result.returncode == 0 else ""


def _promote_child_packages(samples: list[RawSample]) -> dict[str, dict[str, str]]:
    packages_by_system: dict[str, dict[str, str]] = {}
    for sample in samples:
        packages = sample.metadata.pop("packages", None)
        if sample.system not in packages_by_system and isinstance(packages, dict):
            packages_by_system[sample.system] = {
                str(name): str(package_version) for name, package_version in packages.items()
            }
    return packages_by_system


def _unsupported_annotations(config: RunConfig) -> list[dict[str, Any]]:
    annotations: list[dict[str, Any]] = []
    for system in config.systems:
        for backend in config.backends:
            if backend in SYSTEM_BACKENDS[system]:
                continue
            annotations.append({
                "system": system,
                "backend": backend,
                "scenario": "core",
                "comparison_class": "no-counterpart",
                "detail": f"{system} does not provide a supported {backend} broker for this comparison.",
            })
    return annotations


def _comparisons(samples: list[RawSample], *, seed: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[int, float]] = {}
    for sample in samples:
        if sample.valid and sample.throughput is not None:
            grouped.setdefault((sample.system, sample.backend, sample.scenario), {})[sample.sample_index] = (
                sample.throughput
            )
    comparisons: list[dict[str, Any]] = []
    baseline_system = "litestar-queues"
    candidates = sorted({sample.system for sample in samples if sample.system != baseline_system})
    for comparison_index, candidate in enumerate(candidates):
        for backend, scenario in sorted({(sample.backend, sample.scenario) for sample in samples}):
            baseline_by_index = grouped.get((baseline_system, backend, scenario))
            candidate_by_index = grouped.get((candidate, backend, scenario))
            if not baseline_by_index or not candidate_by_index:
                continue
            paired_indexes = sorted(baseline_by_index.keys() & candidate_by_index.keys())
            baseline = [baseline_by_index[index] for index in paired_indexes]
            candidate_values = [candidate_by_index[index] for index in paired_indexes]
            baseline_median = statistics.median(baseline)
            candidate_median = statistics.median(candidate_values)
            ratio = candidate_median / baseline_median
            interval = bootstrap_paired_ratio_interval(baseline, candidate_values, seed=seed + comparison_index)
            comparison_class = "feature-cost" if scenario == "roundtrip" else "equivalent"
            comparisons.append({
                "baseline": baseline_system,
                "candidate": candidate,
                "backend": backend,
                "scenario": scenario,
                "metric": "throughput",
                "sample_count": len(paired_indexes),
                "median_ratio": ratio,
                "ratio_interval": [interval[0], interval[1]],
                "material": len(paired_indexes) >= MIN_MATERIAL_SAMPLES
                and is_material_difference(
                    ratio_interval=interval,
                    median_ratio=ratio,
                    absolute_gap=abs(candidate_median - baseline_median),
                    is_latency=False,
                ),
                "comparison_class": comparison_class,
            })
    return comparisons


def _architecture_annotations(config: RunConfig) -> list[dict[str, Any]]:
    details = {
        "litestar-queues": (
            "Indexed task records, fenced state transitions, automatic per-task worker heartbeat registration and "
            "cleanup, and optional events; the core profile disables optional event history."
        ),
        "litestar-saq": (
            "Litestar integration over SAQ's serialized queue records without an equivalent automatic per-task "
            "heartbeat; plugin startup is recorded separately from steady-state queue timing."
        ),
        "raw-saq": (
            "Raw SAQ control using serialized queue records and the same queue and worker APIs without Litestar "
            "plugin construction or an equivalent automatic per-task heartbeat."
        ),
        "arq": (
            "Async Redis sorted-set queue with serialized jobs and a burst-capable async worker; it has no matching "
            "automatic per-task heartbeat or durable event-history contract."
        ),
        "taskiq": (
            "Async Taskiq list broker with a separate Redis result backend and receiver lifecycle; it has no matching "
            "automatic per-task heartbeat or durable event-history contract."
        ),
        "dramatiq": "Process-oriented actor worker using Redis middleware and explicit broker join semantics.",
        "rq": "Synchronous Redis queue with a SimpleWorker baseline constrained to one worker.",
        "celery": "Process-oriented Celery worker with Redis broker/result backend and solo-pool baseline.",
    }
    return [
        {
            "system": system,
            "backend": "all",
            "scenario": "architecture",
            "comparison_class": "equivalent",
            "detail": details[system],
        }
        for system in config.systems
    ]


__all__ = (
    "DEFAULT_SYSTEMS",
    "RunConfig",
    "build_child_command",
    "compatible_pairs",
    "run_benchmarks",
    "validate_run_config",
)

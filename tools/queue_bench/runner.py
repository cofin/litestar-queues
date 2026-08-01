"""Benchmark orchestration and per-system process isolation."""

import hashlib
import json
import os
import random
import re
import statistics
import subprocess
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from tools.dev_infra import ContainerRuntime, InfraManager
from tools.queue_bench.environment import capture_environment, redact_data
from tools.queue_bench.infra import parse_dsn_overrides, select_local_services
from tools.queue_bench.models import BenchmarkResult, RawSample, ScenarioAggregate
from tools.queue_bench.profiles import (
    COMPETITOR_SCENARIOS,
    CORE_SCENARIOS,
    FEATURE_SCENARIOS,
    MAINTENANCE_SCENARIOS,
    MANAGED_GOOGLE_SCENARIOS,
    BackendVariant,
    ProfileName,
    parse_backend_variant,
    parse_profile_name,
    validate_profile_parameters,
)
from tools.queue_bench.statistics import bootstrap_paired_ratio_interval, is_material_difference

DEFAULT_SYSTEMS = ("litestar-queues", "litestar-saq", "arq", "taskiq")
MIN_MATERIAL_SAMPLES = 5
MIN_MANAGED_OBSERVATIONS = 2
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
    "litestar-queues": ["litestar-queues", "advanced-alchemy", "sqlalchemy", "redis", "sqlspec", "asyncpg", "psycopg"],
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
    profile: ProfileName = "core"
    backend_variant: BackendVariant = "default"
    parameters: Mapping[str, Any] = field(default_factory=dict)
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
    acknowledge_cost: bool = False
    managed_namespace: str | None = None
    google_project: str | None = None
    google_credentials_file: Path | None = None
    google_adc: bool = False
    cold_state_evidence: Path | None = None
    cloud_tasks_location: str | None = None
    cloud_tasks_queue: str | None = None
    cloud_tasks_service_url: str | None = None
    cloud_tasks_service_account: str | None = None
    cloud_tasks_audience: str | None = None
    cloud_run_region: str | None = None
    cloud_run_job: str | None = None


def validate_run_config(config: RunConfig) -> None:
    """Reject invalid or ambiguous run inputs."""
    profile = parse_profile_name(config.profile)
    backend_variant = parse_backend_variant(config.backend_variant)
    unknown_systems = set(config.systems) - SYSTEM_BACKENDS.keys()
    if unknown_systems:
        msg = f"unsupported systems: {', '.join(sorted(unknown_systems))}"
        raise ValueError(msg)
    unknown_backends = set(config.backends) - {"postgres", "redis", "valkey"}
    if unknown_backends:
        msg = f"unsupported backends: {', '.join(sorted(unknown_backends))}"
        raise ValueError(msg)
    unknown_scenarios = set(config.scenarios) - {
        *CORE_SCENARIOS,
        *FEATURE_SCENARIOS,
        *MAINTENANCE_SCENARIOS,
        *MANAGED_GOOGLE_SCENARIOS,
    }
    if unknown_scenarios:
        msg = f"unsupported scenarios: {', '.join(sorted(unknown_scenarios))}"
        raise ValueError(msg)
    profile_scenarios = {
        "heartbeat": frozenset({"heartbeat"}),
        "events": frozenset({"events"}),
        "uniqueness": frozenset({"enqueue"}),
        "maintenance": frozenset(MAINTENANCE_SCENARIOS),
        "cloud-tasks": frozenset({"cloud-tasks-delivery"}),
        "cloud-run-jobs": frozenset({"cloud-run-job-dispatch"}),
    }
    expected_scenarios = profile_scenarios.get(config.profile, frozenset(CORE_SCENARIOS))
    mismatched_scenarios = set(config.scenarios) - expected_scenarios
    if mismatched_scenarios:
        msg = f"profile {config.profile!r} does not support scenarios: {', '.join(sorted(mismatched_scenarios))}"
        raise ValueError(msg)
    _validate_managed_google_profile(config)
    expanded_scenarios = set(config.scenarios) - set(COMPETITOR_SCENARIOS)
    _validate_litestar_queues_only(config, expanded_scenarios)
    _validate_advanced_alchemy_profile(config, backend_variant)
    if backend_variant != "default" and any(backend != "postgres" for backend in config.backends):
        msg = "non-default backend variants require PostgreSQL-only runs"
        raise ValueError(msg)
    validate_profile_parameters(profile, config.parameters)
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


def _validate_litestar_queues_only(config: RunConfig, expanded_scenarios: set[str]) -> None:
    if (expanded_scenarios or config.profile in {"uniqueness", "maintenance"}) and config.systems != (
        "litestar-queues",
    ):
        msg = "selected Litestar Queues profile or scenarios require --system litestar-queues"
        raise ValueError(msg)


def _validate_advanced_alchemy_profile(config: RunConfig, backend_variant: BackendVariant) -> None:
    if config.profile != "advanced-alchemy":
        return
    if config.systems != ("litestar-queues",) or config.backends != ("postgres",):
        msg = "advanced-alchemy profile requires exactly --system litestar-queues and --backend postgres"
        raise ValueError(msg)
    if backend_variant not in {"psycopg", "asyncpg"}:
        msg = "advanced-alchemy profile requires explicit --backend-variant psycopg or asyncpg"
        raise ValueError(msg)


def _validate_managed_google_profile(config: RunConfig) -> None:
    managed_profile = config.profile in {"cloud-tasks", "cloud-run-jobs"}
    if not managed_profile:
        if _managed_options_present(config):
            msg = "managed Google options require --profile cloud-tasks or --profile cloud-run-jobs"
            raise ValueError(msg)
        return

    expected_scenario = "cloud-tasks-delivery" if config.profile == "cloud-tasks" else "cloud-run-job-dispatch"
    if config.systems != ("litestar-queues",) or config.scenarios != (expected_scenario,):
        msg = f"profile {config.profile!r} requires exactly --system litestar-queues and --scenario {expected_scenario}"
        raise ValueError(msg)
    if not config.acknowledge_cost:
        msg = "managed Google profiles require --acknowledge-cost"
        raise ValueError(msg)
    if not config.remote:
        msg = "managed Google profiles require --remote"
        raise ValueError(msg)
    if len(config.backends) != 1:
        msg = "managed Google profiles require exactly one persistent --backend"
        raise ValueError(msg)
    overrides = parse_dsn_overrides(list(config.dsn_overrides))
    if len(overrides) != 1 or set(overrides) != set(config.backends):
        msg = "managed Google profiles require exactly one matching --dsn for the selected backend"
        raise ValueError(msg)
    namespace = config.managed_namespace
    if namespace is None or not namespace.startswith("lqb_") or not _is_queue_namespace(namespace):
        msg = "managed Google profiles require an operator-supplied --managed-namespace starting with 'lqb_'"
        raise ValueError(msg)
    resources, incompatible = _managed_resource_values(config)
    missing = [flag for flag, value in resources.items() if not isinstance(value, str) or not value.strip()]
    if missing:
        msg = f"profile {config.profile!r} requires nonblank {', '.join(missing)}"
        raise ValueError(msg)
    if any(value is not None for value in incompatible):
        msg = f"profile {config.profile!r} received options for the other managed Google profile"
        raise ValueError(msg)
    _validate_managed_files(config)
    if config.operations < MIN_MANAGED_OBSERVATIONS:
        msg = "managed Google profiles require --operations of at least 2 for first and subsequent observations"
        raise ValueError(msg)


def _managed_options_present(config: RunConfig) -> bool:
    values = (
        config.acknowledge_cost,
        config.managed_namespace,
        config.google_project,
        config.google_credentials_file,
        config.google_adc,
        config.cold_state_evidence,
        config.cloud_tasks_location,
        config.cloud_tasks_queue,
        config.cloud_tasks_service_url,
        config.cloud_tasks_service_account,
        config.cloud_tasks_audience,
        config.cloud_run_region,
        config.cloud_run_job,
    )
    return any(value is not None and value is not False for value in values)


def _managed_resource_values(config: RunConfig) -> tuple[dict[str, str | None], tuple[str | None, ...]]:
    if config.profile == "cloud-tasks":
        return (
            {
                "--google-project": config.google_project,
                "--cloud-tasks-location": config.cloud_tasks_location,
                "--cloud-tasks-queue": config.cloud_tasks_queue,
                "--cloud-tasks-service-url": config.cloud_tasks_service_url,
                "--cloud-tasks-service-account": config.cloud_tasks_service_account,
            },
            (config.cloud_run_region, config.cloud_run_job),
        )
    return (
        {
            "--google-project": config.google_project,
            "--cloud-run-region": config.cloud_run_region,
            "--cloud-run-job": config.cloud_run_job,
        },
        (
            config.cloud_tasks_location,
            config.cloud_tasks_queue,
            config.cloud_tasks_service_url,
            config.cloud_tasks_service_account,
            config.cloud_tasks_audience,
        ),
    )


def _validate_managed_files(config: RunConfig) -> None:
    if (config.google_credentials_file is None) == (not config.google_adc):
        msg = "managed Google profiles require exactly one of --google-credentials-file or --google-adc"
        raise ValueError(msg)
    if config.google_credentials_file is not None and (
        not config.google_credentials_file.is_file() or not os.access(config.google_credentials_file, os.R_OK)
    ):
        msg = "--google-credentials-file must name a readable file"
        raise ValueError(msg)
    if config.cold_state_evidence is not None and (
        not config.cold_state_evidence.is_file() or not os.access(config.cold_state_evidence, os.R_OK)
    ):
        msg = "--cold-state-evidence must name a readable file"
        raise ValueError(msg)


def _is_queue_namespace(value: str) -> bool:
    return re.fullmatch(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*", value) is not None


def _managed_child_config(config: RunConfig) -> dict[str, Any]:
    if config.profile not in {"cloud-tasks", "cloud-run-jobs"}:
        return {}
    return {
        "cost_acknowledged": config.acknowledge_cost,
        "remote": config.remote,
        "google_project": config.google_project,
        "google_credentials_file": str(config.google_credentials_file) if config.google_credentials_file else None,
        "google_adc": config.google_adc,
        "cold_state_evidence": _cold_state_evidence_metadata(config.cold_state_evidence),
        "cloud_tasks_location": config.cloud_tasks_location,
        "cloud_tasks_queue": config.cloud_tasks_queue,
        "cloud_tasks_service_url": config.cloud_tasks_service_url,
        "cloud_tasks_service_account": config.cloud_tasks_service_account,
        "cloud_tasks_audience": config.cloud_tasks_audience,
        "cloud_run_region": config.cloud_run_region,
        "cloud_run_job": config.cloud_run_job,
    }


def _managed_environment_config(config: RunConfig) -> dict[str, Any] | None:
    if config.profile not in {"cloud-tasks", "cloud-run-jobs"}:
        return None
    return {
        "acknowledge_cost": True,
        "namespace": config.managed_namespace,
        "credential_source": "file" if config.google_credentials_file is not None else "adc",
        "cold_state_evidence": _cold_state_evidence_metadata(config.cold_state_evidence),
    }


def _cold_state_evidence_metadata(path: Path | None) -> dict[str, str] | None:
    if path is None:
        return None
    digest = hashlib.sha256()
    with path.open("rb") as evidence:
        for chunk in iter(lambda: evidence.read(64 * 1024), b""):
            digest.update(chunk)
    return {"filename": path.name, "sha256": digest.hexdigest()}


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
    dsns = {
        backend: overrides[backend] if backend in overrides else service_by_key[backend].url
        for backend in config.backends
    }
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
        "profile": config.profile,
        "backend_variant": config.backend_variant,
        "parameters": validate_profile_parameters(config.profile, config.parameters),
        "warmups": config.warmups,
        "samples": config.samples,
        "operations": config.operations,
        "payload_size": config.payload_size,
        "concurrency": config.concurrency,
        "seed": config.seed,
        "dsns": dsns,
        "managed": _managed_environment_config(config),
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
                    "profile": config.profile,
                    "backend_variant": config.backend_variant,
                    "parameters": validate_profile_parameters(config.profile, config.parameters),
                    "operations": config.operations,
                    "payload_size": config.payload_size,
                    "concurrency": config.concurrency,
                    "namespace": config.managed_namespace or f"lqb_{uuid.uuid4().hex}",
                    "sample_index": pass_index - config.warmups,
                    "timeout_seconds": config.timeout_seconds,
                    **_managed_child_config(config),
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
        annotations=[
            *_architecture_annotations(config),
            *_scenario_annotations(config),
            *_unsupported_annotations(config),
        ],
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
        detail = (
            "managed Google child failed; inspect operator-side logs"
            if request["profile"] in {"cloud-tasks", "cloud-run-jobs"}
            else result.stderr.strip() or result.stdout.strip() or f"child exited {result.returncode}"
        )
        return _invalid_sample(request, detail)
    try:
        payload = _decode_child_stdout(result.stdout)
        sample = RawSample.from_dict(payload)
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        return _invalid_sample(request, f"invalid child output: {exc}")
    metadata = dict(sample.metadata)
    metadata["profile"] = request["profile"]
    metadata["backend_variant"] = request["backend_variant"]
    metadata["parameters"] = dict(request["parameters"])
    metadata["process_elapsed_seconds"] = process_elapsed_seconds
    metadata["stdout"] = _child_log_output(result.stdout)
    metadata["stderr"] = (
        "<managed output suppressed>"
        if request["profile"] in {"cloud-tasks", "cloud-run-jobs"} and result.stderr
        else result.stderr
    )
    return RawSample(
        system=sample.system,
        backend=sample.backend,
        scenario=sample.scenario,
        sample_index=sample.sample_index,
        duration_seconds=sample.duration_seconds,
        operations=sample.operations,
        valid=sample.valid,
        counters=sample.counters,
        measurements=sample.measurements,
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
        counters={"requests": 0, "records": 0, "started": 0, "completed": 0, "failed": 0, "retried": 0, "remaining": 0},
        measurements={},
        error=error,
        metadata={
            "profile": request["profile"],
            "backend_variant": request["backend_variant"],
            "parameters": dict(request["parameters"]),
        },
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


def _scenario_annotations(config: RunConfig) -> list[dict[str, Any]]:
    annotations: list[dict[str, Any]] = []
    if "enqueue-many" in config.scenarios:
        annotations.append({
            "system": "litestar-queues",
            "backend": "all",
            "scenario": "enqueue-many",
            "comparison_class": "feature-advantaged",
            "detail": "Uses the public native enqueue_many(TaskRequest) backend API; it is not equivalent to repeated single enqueue.",
        })
    if config.profile in {"cloud-tasks", "cloud-run-jobs"}:
        annotations.append({
            "system": "litestar-queues",
            "backend": config.backends[0],
            "scenario": config.scenarios[0],
            "comparison_class": "no-counterpart",
            "detail": (
                "Operator-supplied managed-service evidence separates first and subsequent observations; "
                "it makes no cold-state claim without independent evidence."
            ),
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

"""Opt-in managed Google execution benchmarks over public queue APIs."""

import inspect
import os
import re
import statistics
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from tools.queue_bench.adapters.base import AdapterRequest, AdapterResult
from tools.queue_bench.adapters.litestar_queues import _backend_config, _cleanup
from tools.queue_bench.managed_tasks import managed_noop
from tools.queue_bench.measurements import SampleMeasurementCollector, summarize_pickup_latency

CredentialsLoader = Callable[[AdapterRequest], Any]
ClientFactory = Callable[[Any], Any]
Observation = Callable[[], Awaitable[tuple[Any, str | None]]]
MIN_MANAGED_OBSERVATIONS = 2


async def run(
    request: AdapterRequest,
    *,
    credentials_loader: CredentialsLoader | None = None,
    cloud_tasks_client_factory: ClientFactory | None = None,
    cloud_run_jobs_client_factory: ClientFactory | None = None,
) -> AdapterResult:
    """Run a cost-acknowledged managed dispatch sample.

    Credential and client hooks make the public lifecycle injectable without
    letting ordinary validation instantiate a Google client.
    """
    _validate_request_contract(request)
    execution_config = _execution_config(request)
    credentials = (credentials_loader or load_google_credentials)(request)
    if request.profile == "cloud-tasks":
        client = (cloud_tasks_client_factory or create_cloud_tasks_client)(credentials)
        return await _run_cloud_tasks(request, client, execution_config)
    client = (cloud_run_jobs_client_factory or create_cloud_run_jobs_client)(credentials)
    return await _run_cloud_run_jobs(request, client, execution_config)


def load_google_credentials(request: AdapterRequest) -> Any:
    """Resolve the already-validated credential source for a live run."""
    if request.google_credentials_file is not None:
        import google.auth

        credentials, _ = google.auth.load_credentials_from_file(  # type: ignore[no-untyped-call]
            request.google_credentials_file
        )
        return credentials
    if request.google_adc:
        import google.auth

        credentials, _ = google.auth.default()
        return credentials
    msg = "managed Google benchmark credential source was not validated"
    raise ValueError(msg)


def create_cloud_tasks_client(credentials: Any) -> Any:
    """Create the async Cloud Tasks client after every offline gate passed."""
    from google.cloud import tasks_v2

    return tasks_v2.CloudTasksAsyncClient(credentials=credentials)


def create_cloud_run_jobs_client(credentials: Any) -> Any:
    """Create the async Cloud Run Jobs client after every offline gate passed."""
    from google.cloud import run_v2

    return run_v2.JobsAsyncClient(credentials=credentials)


def _validate_request_contract(request: AdapterRequest) -> None:
    expected = "cloud-tasks-delivery" if request.profile == "cloud-tasks" else "cloud-run-job-dispatch"
    if request.system != "litestar-queues" or request.scenario != expected:
        msg = "managed Google adapter received a mismatched profile or scenario"
        raise ValueError(msg)
    if not request.cost_acknowledged or not request.remote:
        msg = "managed Google adapter requires cost acknowledgment and remote execution"
        raise ValueError(msg)
    if not request.dsn.strip() or not re.fullmatch(r"lqb_[a-z0-9]+(?:_[a-z0-9]+)*", request.namespace):
        msg = "managed Google adapter requires one DSN and a valid lqb_ namespace"
        raise ValueError(msg)
    resources = [request.google_project]
    if request.profile == "cloud-tasks":
        resources.extend([
            request.cloud_tasks_location,
            request.cloud_tasks_queue,
            request.cloud_tasks_service_url,
            request.cloud_tasks_service_account,
        ])
    else:
        resources.extend([request.cloud_run_region, request.cloud_run_job])
    if any(value is None or not value.strip() for value in resources):
        msg = "managed Google adapter requires nonblank resource identifiers"
        raise ValueError(msg)
    if (request.google_credentials_file is None) == (not request.google_adc):
        msg = "managed Google adapter requires exactly one credential source"
        raise ValueError(msg)
    if request.google_credentials_file is not None and (
        not Path(request.google_credentials_file).is_file() or not os.access(request.google_credentials_file, os.R_OK)
    ):
        msg = "managed Google credential file must be readable"
        raise ValueError(msg)
    if request.operations < MIN_MANAGED_OBSERVATIONS:
        msg = "managed Google adapter requires at least two observations"
        raise ValueError(msg)


def _execution_config(request: AdapterRequest) -> Any:
    if request.profile == "cloud-tasks":
        from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig

        return CloudTasksExecutionConfig(
            project_id=_require(request.google_project),
            location=_require(request.cloud_tasks_location),
            queue_id=_require(request.cloud_tasks_queue),
            service_url=_require(request.cloud_tasks_service_url),
            service_account_email=_require(request.cloud_tasks_service_account),
            audience=request.cloud_tasks_audience,
            trust_platform_auth=True,
        )
    from litestar_queues.execution.cloudrun import CloudRunExecutionConfig

    config = CloudRunExecutionConfig(
        project_id=_require(request.google_project),
        region=_require(request.cloud_run_region),
        job_name=_require(request.cloud_run_job),
    )
    config.resolve_job_name()
    return config


async def _run_cloud_tasks(request: AdapterRequest, client: Any, execution_config: Any) -> AdapterResult:
    from litestar_queues import QueueService
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend

    execution_backend = CloudTasksExecutionBackend(execution_config=execution_config, client=client)
    backend_config: Any = None

    async def observe(service: QueueService) -> tuple[Any, str | None]:
        result = await service.enqueue(managed_noop, request.payload)
        return result, result.record.execution_ref if result.record is not None else None

    try:
        backend_config = _backend_config(request)
        config = _managed_queue_config(request, execution_config, backend_config)
        async with QueueService(config, execution_backend=execution_backend) as service:
            await _prepare_schema(request, service)
            result = await _observe_dispatches(
                request, lambda: observe(service), service=service, execution_backend="cloudtasks"
            )
    finally:
        try:
            await _close_client(client)
        finally:
            if backend_config is not None:
                await _cleanup(request, backend_config)
    return result


async def _run_cloud_run_jobs(request: AdapterRequest, client: Any, execution_config: Any) -> AdapterResult:
    from litestar_queues import QueueService, Worker, WorkerConfig
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend

    execution_backend = CloudRunExecutionBackend(execution_config=execution_config, jobs_client=client)
    backend_config: Any = None

    async def observe(service: QueueService, worker: Worker) -> tuple[Any, str | None]:
        result = await service.enqueue(managed_noop, request.payload)
        dispatched = await worker.run_once()
        if dispatched != 1:
            msg = "managed Cloud Run worker did not dispatch exactly one record"
            raise RuntimeError(msg)
        await result.refresh()
        return result, result.record.execution_ref if result.record is not None else None

    try:
        backend_config = _backend_config(request)
        config = _managed_queue_config(request, execution_config, backend_config)
        async with QueueService(config, execution_backend=execution_backend) as service:
            await _prepare_schema(request, service)
            worker = Worker(
                service,
                WorkerConfig(
                    placement="external", batch_size=1, max_concurrency=1, queues=("queue_benchmark_managed",)
                ),
            )
            result = await _observe_dispatches(
                request, lambda: observe(service, worker), service=service, execution_backend="cloudrun"
            )
    finally:
        try:
            await _close_client(client)
        finally:
            if backend_config is not None:
                await _cleanup(request, backend_config)
    return result


def _managed_queue_config(request: AdapterRequest, execution_config: Any, backend_config: Any) -> Any:
    from litestar_queues import QueueConfig, WorkerConfig

    return QueueConfig(
        namespace=request.namespace,
        queue_backend=backend_config,
        execution_backend=execution_config,
        task_modules=("tools.queue_bench.managed_tasks",),
        initialize_schedules=False,
        log_success=False,
        worker=WorkerConfig(placement="external", queues=("queue_benchmark_managed",)),
    )


async def _observe_dispatches(
    request: AdapterRequest, observe: Observation, *, service: Any, execution_backend: str
) -> AdapterResult:
    collector = SampleMeasurementCollector.create()
    cpu_started = collector.snapshot_cpu()
    observation_seconds: list[float] = []
    created_to_started_seconds: list[float] = []
    created_to_terminal_seconds: list[float] = []
    results: list[Any] = []
    references: list[str] = []
    started_at = time.perf_counter()
    for _ in range(request.operations):
        operation_started_at = time.perf_counter()
        result, reference = await observe()
        observation_seconds.append(time.perf_counter() - operation_started_at)
        if not reference:
            msg = "managed Google dispatch completed without an execution reference"
            raise RuntimeError(msg)
        references.append(reference)
        await result.wait(timeout=request.timeout_seconds, poll_interval=0.1)
        await result.refresh()
        record = result.record
        if record is None or not record.is_terminal:
            msg = "managed Google dispatch did not reach a terminal queue state"
            raise RuntimeError(msg)
        results.append(result)
        if record.started_at is not None:
            created_to_started_seconds.append((record.started_at - record.created_at).total_seconds())
        if record.completed_at is not None:
            created_to_terminal_seconds.append((record.completed_at - record.created_at).total_seconds())
    duration = time.perf_counter() - started_at
    subsequent = observation_seconds[1:]
    measurements = collector.finish(cpu_started)
    measurements.update(summarize_pickup_latency([result.record for result in results]))
    measurements.update({
        "managed.first_observation_seconds": observation_seconds[0],
        "managed.subsequent_observation_count": len(subsequent),
        "managed.subsequent_observation_median_seconds": statistics.median(subsequent),
        "managed.subsequent_observation_total_seconds": sum(subsequent),
        "managed.created_to_started_count": len(created_to_started_seconds),
        "managed.created_to_started_first_seconds": _first_or_none(created_to_started_seconds),
        "managed.created_to_started_subsequent_count": max(0, len(created_to_started_seconds) - 1),
        "managed.created_to_started_subsequent_median_seconds": _median_or_none(created_to_started_seconds[1:]),
        "managed.created_to_terminal_count": len(created_to_terminal_seconds),
        "managed.created_to_terminal_first_seconds": _first_or_none(created_to_terminal_seconds),
        "managed.created_to_terminal_subsequent_count": max(0, len(created_to_terminal_seconds) - 1),
        "managed.created_to_terminal_subsequent_median_seconds": _median_or_none(created_to_terminal_seconds[1:]),
    })
    statistics_ = await service.get_queue_backend().get_statistics()
    return AdapterResult(
        duration_seconds=duration,
        counters={
            "requests": request.operations,
            "records": request.operations,
            "dispatch_observations": request.operations,
            "execution_refs": len(references),
            "started": sum(result.record is not None and result.record.started_at is not None for result in results),
            "completed": sum(result.status == "completed" for result in results),
            "failed": sum(result.status == "failed" for result in results),
            "retried": sum(result.record.retry_count for result in results if result.record is not None),
            "remaining": statistics_.pending + statistics_.scheduled + statistics_.running,
            "first_observations": 1,
            "subsequent_observations": request.operations - 1,
        },
        measurements=measurements,
        metadata={
            "comparison_class": "no-counterpart",
            "managed_service": execution_backend,
            "credential_source": "file" if request.google_credentials_file is not None else "adc",
            "cost_acknowledged": True,
            "live_evidence": "operator-supplied",
            "observation_classes": ["first", "subsequent"],
            "cold_state_evidence": dict(request.cold_state_evidence) if request.cold_state_evidence else None,
            "cold_state_claim": request.cold_state_evidence is not None,
            "project": request.google_project,
            "location": request.cloud_tasks_location if request.profile == "cloud-tasks" else request.cloud_run_region,
            "queue": request.cloud_tasks_queue,
            "job": request.cloud_run_job,
            "managed_namespace": request.namespace,
            "cleanup": "exact managed namespace after sample",
        },
    )


async def _prepare_schema(request: AdapterRequest, service: Any) -> None:
    if request.backend != "postgres":
        return
    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

    backend = service.get_queue_backend()
    if not isinstance(backend, SQLSpecQueueBackend):
        msg = "managed PostgreSQL benchmark expected SQLSpecQueueBackend"
        raise TypeError(msg)
    await backend.create_schema()


def _median_or_none(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def _first_or_none(values: list[float]) -> float | None:
    return values[0] if values else None


async def _close_client(client: Any) -> None:
    close = getattr(client, "close", None)
    if close is None:
        close = getattr(getattr(client, "transport", None), "close", None)
    if close is None:
        return
    result = close()
    if inspect.isawaitable(result):
        await result


def _require(value: str | None) -> str:
    if value is None or not value.strip():
        msg = "managed Google resource configuration was not validated"
        raise ValueError(msg)
    return value


__all__ = ("create_cloud_run_jobs_client", "create_cloud_tasks_client", "load_google_credentials", "run")

import asyncio
import importlib
import logging
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

import pytest

from litestar_queues import QueueConfig, QueueService, Worker, task
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.task import clear_task_registry, get_task_registry, load_task_modules
from tests.integration.execution.cloudrun.helpers import (
    FakeCloudRunExecution,
    FakeExecutionsClient,
    FakeJobsClient,
    NotFoundError,
    dispatch_envelope,
    env_map,
)

if TYPE_CHECKING:
    from litestar_queues.execution.cloudrun._typing import CloudRunJobsClient

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def clean_task_registry() -> "None":
    clear_task_registry()


def test_load_task_modules_force_reload_imports_never_imported_module(tmp_path: "Any", monkeypatch: "Any") -> "None":
    package_dir = tmp_path / "dynamic_tasks"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("")
    (package_dir / "jobs.py").write_text(
        "from litestar_queues import task\n@task('dynamic.force_reload')\ndef run():\n    return 'ok'\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    sys.modules.pop("dynamic_tasks.jobs", None)

    loaded = load_task_modules(("dynamic_tasks.jobs",), force_reload=True)

    assert loaded == 1
    assert "dynamic.force_reload" in get_task_registry()


def test_load_task_modules_failed_import_does_not_poison_loaded_cache(tmp_path: "Any", monkeypatch: "Any") -> "None":
    package_dir = tmp_path / "recovering_tasks"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("")
    module_path = package_dir / "jobs.py"
    module_path.write_text("raise RuntimeError('temporary import failure')\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setattr(sys, "dont_write_bytecode", True)

    with pytest.raises(RuntimeError, match="temporary import failure"):
        load_task_modules(("recovering_tasks.jobs",))

    module_path.write_text(
        "from litestar_queues import task\n@task('dynamic.recovered')\ndef run():\n    return 'ok'\n"
    )
    sys.modules.pop("recovering_tasks.jobs", None)
    importlib.invalidate_caches()

    loaded = load_task_modules(("recovering_tasks.jobs",))

    assert loaded == 1
    assert "dynamic.recovered" in get_task_registry()


async def test_cloudrun_dispatch_builds_generic_run_job_request_and_stores_execution_ref() -> "None":
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend, CloudRunExecutionConfig

    @task("tasks.remote", timeout=120)
    async def remote_task(value: "int") -> "int":
        return value + 1

    queue_backend = InMemoryQueueBackend()
    jobs_client = FakeJobsClient()
    service = QueueService(
        QueueConfig(execution_backend="cloudrun"),
        queue_backend=queue_backend,
        execution_backend=CloudRunExecutionBackend(
            execution_config=CloudRunExecutionConfig(
                project_id="test-project",
                region="us-central1",
                job_name="default-worker",
                profiles={"heavy": "heavy-worker"},
                extra_env={"EXTRA_SETTING": "enabled"},
            ),
            jobs_client=cast("CloudRunJobsClient", jobs_client),
        ),
    )
    await service.open()
    try:
        result = await service.enqueue(remote_task.using(execution_backend="cloudrun", execution_profile="heavy"), 41)
        record = result.record
        assert record is not None

        execution_ref = await service.get_execution_backend().dispatch(service, record)
        stored = await queue_backend.get_task(record.id)
    finally:
        await service.close()

    request = jobs_client.requests[0]
    envelope = dispatch_envelope(request)

    assert execution_ref == "projects/test/locations/us-central1/jobs/worker/executions/run-1"
    assert request["name"] == "projects/test-project/locations/us-central1/jobs/heavy-worker"
    assert request["overrides"]["timeout"] == "120s"
    assert envelope.task_id == str(record.id)
    assert envelope.task_name == "tasks.remote"
    assert envelope.args == (41,)
    assert envelope.kwargs == {}
    assert envelope.execution_backend == "cloudrun"
    assert envelope.execution_profile == "heavy"
    assert env_map(request)["EXTRA_SETTING"] == "enabled"
    assert stored is not None
    assert stored.execution_backend == "cloudrun"
    assert stored.execution_profile == "heavy"
    assert stored.execution_ref == execution_ref
    assert stored.status == "pending"


async def test_cloudrun_dispatch_env_has_no_legacy_task_fields() -> "None":
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend, CloudRunExecutionConfig

    @task("tasks.remote")
    async def remote_task(value: "int") -> "int":
        return value + 1

    queue_backend = InMemoryQueueBackend()
    jobs_client = FakeJobsClient()
    backend = CloudRunExecutionBackend(
        execution_config=CloudRunExecutionConfig(project_id="test-project", job_name="worker"),
        jobs_client=cast("CloudRunJobsClient", jobs_client),
    )
    async with QueueService(
        QueueConfig(execution_backend="cloudrun"), queue_backend=queue_backend, execution_backend=backend
    ) as service:
        result = await service.enqueue(remote_task.using(execution_backend="cloudrun"), 41)
        record = result.record
        assert record is not None
        await backend.dispatch(service, record)

    env = env_map(jobs_client.requests[0])

    # The whole per-field env-map is gone: the record travels as a single envelope var
    # (no extra_env on this config), so no legacy LITESTAR_QUEUES_<field> vars remain.
    assert set(env) == {"LITESTAR_QUEUES_DISPATCH_ENVELOPE"}
    legacy_names = {f"LITESTAR_QUEUES_{suffix}" for suffix in ("TASK_ID", "TASK_NAME", "TASK_ARGS", "TASK_KWARGS")}
    assert legacy_names.isdisjoint(env)


async def test_cloudrun_dispatch_env_respects_custom_prefix() -> "None":
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend, CloudRunExecutionConfig

    @task("tasks.remote")
    async def remote_task(value: "int") -> "int":
        return value + 1

    queue_backend = InMemoryQueueBackend()
    jobs_client = FakeJobsClient()
    backend = CloudRunExecutionBackend(
        execution_config=CloudRunExecutionConfig(project_id="test-project", job_name="worker", env_prefix="PREFIX"),
        jobs_client=cast("CloudRunJobsClient", jobs_client),
    )
    async with QueueService(
        QueueConfig(execution_backend="cloudrun"), queue_backend=queue_backend, execution_backend=backend
    ) as service:
        result = await service.enqueue(remote_task.using(execution_backend="cloudrun"), 41)
        record = result.record
        assert record is not None
        await backend.dispatch(service, record)

    env = env_map(jobs_client.requests[0])

    assert "PREFIX_DISPATCH_ENVELOPE" in env
    assert "LITESTAR_QUEUES_DISPATCH_ENVELOPE" not in env


async def test_cloudrun_dispatch_returns_without_waiting_for_operation_result() -> "None":
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend, CloudRunExecutionConfig

    @task("tasks.remote_nonblocking")
    async def remote_task() -> "str":
        return "ok"

    queue_backend = InMemoryQueueBackend()
    jobs_client = FakeJobsClient(block_result=True)
    backend = CloudRunExecutionBackend(
        execution_config=CloudRunExecutionConfig(project_id="test-project", job_name="worker"),
        jobs_client=cast("CloudRunJobsClient", jobs_client),
    )
    service = QueueService(
        QueueConfig(execution_backend="cloudrun"), queue_backend=queue_backend, execution_backend=backend
    )
    await service.open()
    try:
        record = await queue_backend.enqueue(remote_task.name, execution_backend="cloudrun")

        execution_ref = await asyncio.wait_for(backend.dispatch(service, record), timeout=0.05)
        stored = await queue_backend.get_task(record.id)
    finally:
        await service.close()

    assert execution_ref == "projects/test/locations/us-central1/jobs/worker/executions/run-1"
    assert jobs_client.operations[0].result_called is False
    assert stored is not None
    assert stored.execution_ref == execution_ref


async def test_cloudrun_dispatch_failure_default_surfaces_and_preserves_backend(
    caplog: "pytest.LogCaptureFixture",
) -> "None":
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend, CloudRunExecutionConfig

    @task("tasks.remote_default_failure")
    async def remote_task() -> "str":
        return "ok"

    queue_backend = InMemoryQueueBackend()
    backend = CloudRunExecutionBackend(
        execution_config=CloudRunExecutionConfig(project_id="test-project", job_name="worker"),
        jobs_client=cast("CloudRunJobsClient", FakeJobsClient(error=RuntimeError("api unavailable"))),
    )
    service = QueueService(
        QueueConfig(execution_backend="cloudrun"), queue_backend=queue_backend, execution_backend=backend
    )
    await service.open()
    try:
        record = await queue_backend.enqueue(remote_task.name, execution_backend="cloudrun")

        with (
            caplog.at_level(logging.WARNING, logger="litestar_queues.execution.cloudrun.backend"),
            pytest.raises(RuntimeError, match="api unavailable"),
        ):
            await backend.dispatch(service, record)
        stored = await queue_backend.get_task(record.id)
    finally:
        await service.close()

    assert "Cloud Run dispatch failed" in caplog.text
    assert stored is not None
    assert stored.execution_backend == "cloudrun"
    assert stored.execution_ref is None


async def test_cloudrun_dispatch_failure_falls_back_to_local_when_remote_has_not_taken_ownership() -> "None":
    from litestar_queues.events import EventConfig, InMemoryQueueEventSink
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend, CloudRunExecutionConfig

    @task("tasks.remote")
    async def remote_task() -> "str":
        return "ok"

    queue_backend = InMemoryQueueBackend()
    backend = CloudRunExecutionBackend(
        execution_config=CloudRunExecutionConfig(
            project_id="test-project", job_name="worker", fallback_execution_backend="local"
        ),
        jobs_client=cast("CloudRunJobsClient", FakeJobsClient(error=RuntimeError("api unavailable"))),
    )
    event_sink = InMemoryQueueEventSink()
    service = QueueService(
        QueueConfig(execution_backend="cloudrun", event=EventConfig(sink=event_sink)),
        queue_backend=queue_backend,
        execution_backend=backend,
    )
    await service.open()
    try:
        record = await queue_backend.enqueue(remote_task.name, execution_backend="cloudrun")

        execution_ref = await backend.dispatch(service, record)
        stored = await queue_backend.get_task(record.id)
    finally:
        await service.close()

    assert execution_ref is None
    assert stored is not None
    assert stored.execution_backend == "local"
    assert stored.execution_ref is None
    assert any(
        event.type == "task.event" and event.payload.get("phase") == "cloudrun.dispatch_fallback"
        for event in event_sink.events
    )


@pytest.mark.parametrize(
    ("execution", "expected_status", "expected_error"),
    [
        (FakeCloudRunExecution(succeeded_count=1), "completed", None),
        (
            FakeCloudRunExecution(
                failed_count=1, conditions=[type("Condition", (), {"message": "container failed"})()]
            ),
            "failed",
            "container failed",
        ),
        (FakeCloudRunExecution(cancelled_count=1), "failed", "Cloud Run execution cancelled"),
    ],
)
async def test_cloudrun_reconcile_updates_terminal_statuses(
    execution: "FakeCloudRunExecution", expected_status: "str", expected_error: "str | None"
) -> "None":
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend, CloudRunExecutionConfig

    queue_backend = InMemoryQueueBackend()
    backend = CloudRunExecutionBackend(
        execution_config=CloudRunExecutionConfig(project_id="test-project", job_name="worker"),
        executions_client=FakeExecutionsClient(execution),
    )
    service = QueueService(
        QueueConfig(execution_backend="cloudrun"), queue_backend=queue_backend, execution_backend=backend
    )
    await service.open()
    try:
        record = await queue_backend.enqueue("tasks.remote", execution_backend="cloudrun")
        await queue_backend.set_execution_ref(record.id, "cloudrun", "executions/run-1")
        claimed = await queue_backend.claim_task(record.id)
        assert claimed is not None

        updated = await backend.reconcile(service, claimed)
    finally:
        await service.close()

    assert updated is not None
    assert updated.status == expected_status
    assert updated.error == expected_error


async def test_cloudrun_reconcile_treats_transient_status_errors_as_running() -> "None":
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend, CloudRunExecutionConfig

    queue_backend = InMemoryQueueBackend()
    backend = CloudRunExecutionBackend(
        execution_config=CloudRunExecutionConfig(project_id="test-project", job_name="worker"),
        executions_client=FakeExecutionsClient(RuntimeError("temporary api failure")),
    )
    service = QueueService(
        QueueConfig(execution_backend="cloudrun"), queue_backend=queue_backend, execution_backend=backend
    )
    await service.open()
    try:
        record = await queue_backend.enqueue("tasks.remote", execution_backend="cloudrun")
        await queue_backend.set_execution_ref(record.id, "cloudrun", "executions/run-1")
        claimed = await queue_backend.claim_task(record.id)
        assert claimed is not None

        updated = await backend.reconcile(service, claimed)
    finally:
        await service.close()

    stored = await queue_backend.get_task(record.id)

    assert updated is None
    assert stored is not None
    assert stored.status == "running"


async def test_cloudrun_reconcile_retries_preclaim_not_found_and_clears_execution_ref() -> "None":
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend, CloudRunExecutionConfig

    queue_backend = InMemoryQueueBackend()
    backend = CloudRunExecutionBackend(
        execution_config=CloudRunExecutionConfig(project_id="test-project", job_name="worker"),
        executions_client=FakeExecutionsClient(NotFoundError("execution not found")),
    )
    service = QueueService(
        QueueConfig(execution_backend="cloudrun"), queue_backend=queue_backend, execution_backend=backend
    )
    await service.open()
    try:
        record = await queue_backend.enqueue("tasks.remote", execution_backend="cloudrun", max_retries=1)
        await queue_backend.set_execution_ref(record.id, "cloudrun", "executions/missing")

        updated = await backend.reconcile(service, record)
        stored = await queue_backend.get_task(record.id)
    finally:
        await service.close()

    assert updated is not None
    assert updated.status == "pending"
    assert updated.retry_count == 1
    assert updated.execution_ref is None
    assert stored is not None
    assert stored.status == "pending"
    assert stored.retry_count == 1
    assert stored.execution_ref is None


async def test_cloudrun_reconcile_does_not_terminal_write_after_stale_retry_reassigns_row() -> "None":
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend, CloudRunExecutionConfig

    queue_backend = InMemoryQueueBackend()
    backend = CloudRunExecutionBackend(
        execution_config=CloudRunExecutionConfig(project_id="test-project", job_name="worker"),
        executions_client=FakeExecutionsClient(FakeCloudRunExecution(succeeded_count=1)),
    )
    service = QueueService(
        QueueConfig(execution_backend="cloudrun"), queue_backend=queue_backend, execution_backend=backend
    )
    await service.open()
    try:
        record = await queue_backend.enqueue("tasks.remote", execution_backend="cloudrun", max_retries=1)
        await queue_backend.set_execution_ref(record.id, "cloudrun", "executions/run-1")
        claimed = await queue_backend.claim_task(record.id)
        assert claimed is not None
        claimed.heartbeat_at = datetime.now(timezone.utc) - timedelta(minutes=10)
        stale_result = await queue_backend.requeue_stale_running(stale_after=timedelta(seconds=1))

        updated = await backend.reconcile(service, claimed)
        stored = await queue_backend.get_task(record.id)
    finally:
        await service.close()

    assert stale_result.requeued == 1
    assert updated is None
    assert stored is not None
    assert stored.status == "pending"
    assert stored.retry_count == 1
    assert stored.result is None


async def test_worker_dispatches_external_records_without_claiming_them() -> "None":
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend, CloudRunExecutionConfig

    @task("tasks.remote")
    async def remote_task() -> "str":
        return "ok"

    queue_backend = InMemoryQueueBackend()
    backend = CloudRunExecutionBackend(
        execution_config=CloudRunExecutionConfig(project_id="test-project", job_name="worker"),
        jobs_client=cast("CloudRunJobsClient", FakeJobsClient()),
    )
    async with QueueService(
        QueueConfig(execution_backend="cloudrun"), queue_backend=queue_backend, execution_backend=backend
    ) as service:
        result = await service.enqueue(remote_task.using(execution_backend="cloudrun"))
        worker = Worker(service)

        processed = await worker.run_once()
        stored = await queue_backend.get_task(result.id)

    assert processed == 1
    assert stored is not None
    assert stored.status == "pending"
    assert stored.execution_ref == "projects/test/locations/us-central1/jobs/worker/executions/run-1"


async def test_worker_reconciles_running_external_records() -> "None":
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend, CloudRunExecutionConfig

    queue_backend = InMemoryQueueBackend()
    backend = CloudRunExecutionBackend(
        execution_config=CloudRunExecutionConfig(project_id="test-project", job_name="worker"),
        executions_client=FakeExecutionsClient(FakeCloudRunExecution(failed_count=1)),
    )
    async with QueueService(
        QueueConfig(execution_backend="cloudrun"), queue_backend=queue_backend, execution_backend=backend
    ) as service:
        record = await queue_backend.enqueue("tasks.remote", execution_backend="cloudrun")
        await queue_backend.set_execution_ref(record.id, "cloudrun", "executions/run-1")
        claimed = await queue_backend.claim_task(record.id)
        assert claimed is not None

        reconciled = await Worker(service).reconcile_external()
        stored = await queue_backend.get_task(record.id)

    assert reconciled == 1
    assert stored is not None
    assert stored.status == "failed"


def test_cloudrun_factory_registration_and_import_boundary() -> "None":
    command = [
        sys.executable,
        "-c",
        (
            "import sys; "
            "import litestar_queues; "
            "from litestar_queues.execution import get_execution_backend_class; "
            "from litestar_queues.execution.cloudrun import CloudRunExecutionBackend; "
            "assert get_execution_backend_class('cloudrun') is CloudRunExecutionBackend; "
            "loaded=[name for name in sys.modules if name == 'google' or name.startswith('google.cloud.run')]; "
            "print(loaded); "
            "raise SystemExit(1 if loaded else 0)"
        ),
    ]

    completed = subprocess.run(command, check=False, capture_output=True, text=True)

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert completed.stdout.strip() == "[]"

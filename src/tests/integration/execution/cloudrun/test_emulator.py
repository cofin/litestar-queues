import asyncio
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, TypedDict, cast

import pytest

from litestar_queues import QueueConfig, QueueService, Worker, task
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.task import clear_task_registry

if TYPE_CHECKING:
    from uuid import UUID

    from litestar_queues.execution.cloudrun._typing import CloudRunJobsClient

pytestmark = pytest.mark.anyio


class RunJobEnv(TypedDict):
    name: "str"
    value: "str"


class RunJobContainerOverride(TypedDict):
    env: "list[RunJobEnv]"


class RunJobOverrides(TypedDict):
    container_overrides: "list[RunJobContainerOverride]"
    timeout: "str"


class RunJobRequest(TypedDict):
    name: "str"
    overrides: "RunJobOverrides"


@pytest.fixture(autouse=True)
def clean_task_registry() -> "None":
    clear_task_registry()


@dataclass(slots=True)
class FakeCloudRunExecution:
    name: "str" = "projects/test/locations/us-central1/jobs/worker/executions/run-1"
    succeeded_count: "int" = 0
    failed_count: "int" = 0
    cancelled_count: "int" = 0
    conditions: "list[object] | None" = None


class FakeOperation:
    def __init__(self, execution: "FakeCloudRunExecution") -> "None":
        self.execution = execution

    async def result(self) -> "FakeCloudRunExecution":
        return self.execution


class FakeJobsClient:
    def __init__(self, execution: "FakeCloudRunExecution | None" = None, *, error: "Exception | None" = None) -> "None":
        self.execution = execution or FakeCloudRunExecution()
        self.error = error
        self.requests: "list[RunJobRequest]" = []

    async def run_job(self, *, request: "RunJobRequest") -> "FakeOperation":
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return FakeOperation(self.execution)


class FakeExecutionsClient:
    def __init__(self, execution: "FakeCloudRunExecution | Exception") -> "None":
        self.execution = execution
        self.names: "list[str]" = []

    async def get_execution(self, *, name: "str") -> "FakeCloudRunExecution":
        self.names.append(name)
        if isinstance(self.execution, Exception):
            raise self.execution
        return self.execution


def _env_map(request: "RunJobRequest") -> "dict[str, str]":
    env = request["overrides"]["container_overrides"][0]["env"]
    return {item["name"]: item["value"] for item in env}


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
    env = _env_map(request)

    assert execution_ref == "projects/test/locations/us-central1/jobs/worker/executions/run-1"
    assert request["name"] == "projects/test-project/locations/us-central1/jobs/heavy-worker"
    assert request["overrides"]["timeout"] == "120s"
    assert env["LITESTAR_QUEUES_TASK_ID"] == str(record.id)
    assert env["LITESTAR_QUEUES_TASK_NAME"] == "tasks.remote"
    assert env["LITESTAR_QUEUES_TASK_ARGS"] == "[41]"
    assert env["LITESTAR_QUEUES_TASK_KWARGS"] == "{}"
    assert env["EXTRA_SETTING"] == "enabled"
    assert stored is not None
    assert stored.execution_backend == "cloudrun"
    assert stored.execution_profile == "heavy"
    assert stored.execution_ref == execution_ref
    assert stored.status == "pending"


async def test_cloudrun_dispatch_failure_falls_back_to_local_when_remote_has_not_taken_ownership() -> "None":
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
    service = QueueService(
        QueueConfig(execution_backend="cloudrun"), queue_backend=queue_backend, execution_backend=backend
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


async def test_cloudrun_entrypoint_claims_and_executes_persisted_record() -> "None":
    from litestar_queues.execution.cloudrun.entrypoint import CloudRunExitCode, execute_cloudrun_task

    @task("tasks.entrypoint")
    async def entrypoint_task(value: "int") -> "int":
        return value + 1

    queue_backend = InMemoryQueueBackend()
    async with QueueService(QueueConfig(execution_backend="cloudrun"), queue_backend=queue_backend) as service:
        result = await service.enqueue(entrypoint_task.using(execution_backend="cloudrun"), 41)
        exit_code = await execute_cloudrun_task(service=service, env={"LITESTAR_QUEUES_TASK_ID": str(result.id)})
        await result.refresh()

    assert exit_code == CloudRunExitCode.SUCCESS
    assert result.status == "completed"
    assert result.result == 42


async def test_cloudrun_entrypoint_returns_claim_lost_when_heartbeat_loses_ownership() -> "None":
    from litestar_queues.execution.cloudrun.entrypoint import CloudRunExitCode, execute_cloudrun_task

    heartbeat_seen = asyncio.Event()
    release_task = asyncio.Event()
    task_id: "UUID | None" = None

    @task("tasks.entrypoint_claim_lost")
    async def entrypoint_claim_lost() -> "str":
        assert task_id is not None
        stored = await queue_backend.get_task(task_id)
        assert stored is not None
        stored.status = "pending"
        stored.retry_count += 1
        stored.started_at = None
        stored.heartbeat_at = None
        heartbeat_seen.set()
        await release_task.wait()
        return "too late"

    queue_backend = InMemoryQueueBackend()
    async with QueueService(
        QueueConfig(execution_backend="cloudrun", worker_heartbeat_interval=0.01), queue_backend=queue_backend
    ) as service:
        result = await service.enqueue(entrypoint_claim_lost.using(execution_backend="cloudrun"), retries=1)
        task_id = result.id
        runner = asyncio.create_task(
            execute_cloudrun_task(service=service, env={"LITESTAR_QUEUES_TASK_ID": str(result.id)})
        )
        await asyncio.wait_for(heartbeat_seen.wait(), timeout=1)
        try:
            exit_code = await runner
        finally:
            release_task.set()
        stored = await queue_backend.get_task(result.id)

    assert exit_code == CloudRunExitCode.CLAIM_LOST
    assert stored is not None
    assert stored.status == "pending"
    assert stored.retry_count == 1


async def test_cloudrun_entrypoint_returns_deterministic_error_codes() -> "None":
    from litestar_queues.execution.cloudrun.entrypoint import CloudRunExitCode, execute_cloudrun_task

    async with QueueService(QueueConfig()) as service:
        missing = await execute_cloudrun_task(service=service, env={})
        invalid = await execute_cloudrun_task(service=service, env={"LITESTAR_QUEUES_TASK_ID": "not-a-uuid"})

    assert missing == CloudRunExitCode.MISSING_TASK_ID
    assert invalid == CloudRunExitCode.INVALID_TASK_ID


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

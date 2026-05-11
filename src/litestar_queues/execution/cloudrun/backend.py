import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from litestar_queues.exceptions import MissingDependencyError
from litestar_queues.execution.base import BaseExecutionBackend
from litestar_queues.execution.cloudrun.config import CloudRunExecutionConfig, cloudrun_config_from_queue_config

if TYPE_CHECKING:
    from litestar_queues.execution.cloudrun._typing import (
        CloudRunExecutionLike,
        CloudRunExecutionsClient,
        CloudRunJobsClient,
    )
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.service import QueueService

__all__ = ("CloudRunExecutionBackend", "CloudRunExecutionStatus")

_GOOGLE_CLOUD_RUN_PACKAGE = "google-cloud-run"
_CLOUDRUN_EXTRA = "cloudrun"


@dataclass(frozen=True, slots=True)
class CloudRunExecutionStatus:
    """Backend-neutral status for a Cloud Run execution."""

    succeeded: bool = False
    failed: bool = False
    cancelled: bool = False
    running: bool = True
    error: str | None = None


class CloudRunExecutionBackend(BaseExecutionBackend):
    """Execution backend that dispatches queued records to Cloud Run Jobs."""

    __slots__ = ("_cloudrun_config", "executions_client", "jobs_client")

    def __init__(
        self,
        config: "Any | None" = None,
        *,
        cloudrun_config: CloudRunExecutionConfig | None = None,
        jobs_client: "CloudRunJobsClient | None" = None,
        executions_client: "CloudRunExecutionsClient | None" = None,
    ) -> None:
        super().__init__(config=config)
        self._cloudrun_config = cloudrun_config
        self.jobs_client = jobs_client
        self.executions_client = executions_client

    @property
    def is_external(self) -> bool:
        """Return whether this backend dispatches records to another process."""
        return True

    @property
    def cloudrun_config(self) -> CloudRunExecutionConfig:
        """Return the resolved Cloud Run execution config."""
        if self._cloudrun_config is None:
            self._cloudrun_config = cloudrun_config_from_queue_config(self.config)
        return self._cloudrun_config

    async def execute(
        self,
        service: "QueueService",
        record: "QueuedTaskRecord",
        *,
        worker_id: str | None = None,
    ) -> "QueuedTaskRecord":
        """Dispatch a record and return its persisted state.

        The ``worker_id`` argument is accepted for protocol parity but not
        forwarded: external dispatch does not run ``service.execute_record``
        locally, so the remote runner is responsible for its own worker
        identity binding.

        Returns:
            The persisted queue record after dispatch.
        """
        del worker_id
        await self.dispatch(service, record)
        return await service.get_queue_backend().get_task(record.id) or record

    async def dispatch(self, service: "QueueService", record: "QueuedTaskRecord") -> str | None:
        """Dispatch a queue record to Cloud Run Jobs.

        Returns:
            The Cloud Run execution reference, if dispatch succeeds.
        """
        request = self.build_run_job_request(service, record)
        client = await self._get_jobs_client()
        try:
            operation = await client.run_job(request=request)
            execution = await operation.result()
        except Exception:
            fallback = self.cloudrun_config.fallback_execution_backend
            if fallback is None:
                raise
            await service.get_queue_backend().set_execution_backend(record.id, fallback)
            return None

        execution_ref = str(execution.name)
        await service.get_queue_backend().set_execution_ref(
            record.id,
            "cloudrun",
            execution_ref,
            execution_profile=record.execution_profile,
        )
        return execution_ref

    async def reconcile(self, service: "QueueService", record: "QueuedTaskRecord") -> "QueuedTaskRecord | None":
        """Reconcile a Cloud Run execution with the queue record.

        Returns:
            The terminal queue record when reconciliation completed it.
        """
        if record.execution_ref is None:
            return None

        status = await self.check_execution_status(record.execution_ref)
        if status.running:
            return None

        queue_backend = service.get_queue_backend()
        if status.succeeded:
            return await queue_backend.complete_task(
                record.id,
                result=record.result
                if record.result is not None
                else {"cloudrun_execution": record.execution_ref, "status": "succeeded"},
            )

        if status.cancelled:
            return await queue_backend.fail_task(record.id, "Cloud Run execution cancelled", retry=False)

        if status.failed:
            return await queue_backend.fail_task(record.id, status.error or "Cloud Run execution failed")

        return None

    async def cancel(self, service: "QueueService", record: "QueuedTaskRecord") -> bool:
        """Cloud Run Jobs do not expose per-execution cancellation here.

        Returns:
            Always false because per-execution cancellation is not implemented.
        """
        return False

    async def check_execution_status(self, execution_ref: str) -> CloudRunExecutionStatus:
        """Return Cloud Run execution status.

        Transient API failures are treated as still running so reconciliation does
        not create false terminal queue states.
        """
        try:
            execution = await (await self._get_executions_client()).get_execution(name=execution_ref)
        except Exception as exc:  # noqa: BLE001
            return CloudRunExecutionStatus(running=True, error=str(exc))

        succeeded = int(getattr(execution, "succeeded_count", 0) or 0) > 0
        failed = int(getattr(execution, "failed_count", 0) or 0) > 0
        cancelled = int(getattr(execution, "cancelled_count", 0) or 0) > 0
        return CloudRunExecutionStatus(
            succeeded=succeeded,
            failed=failed,
            cancelled=cancelled,
            running=not (succeeded or failed or cancelled),
            error=_execution_error(execution) if failed else None,
        )

    def build_run_job_request(self, service: "QueueService", record: "QueuedTaskRecord") -> dict[str, Any]:
        """Build the Cloud Run Jobs API request for a queue record.

        Returns:
            Cloud Run Jobs API request data.
        """
        config = self.cloudrun_config
        task_obj = service.resolve_task(record.task_name)
        timeout = record.metadata.get("timeout", task_obj.timeout)
        timeout_seconds = int(timeout if isinstance(timeout, int | float) else config.timeout)
        job_name = config.resolve_job_name(record.execution_profile)
        env = self.build_environment(record)
        return {
            "name": f"projects/{config.project_id}/locations/{config.region}/jobs/{job_name}",
            "overrides": {
                "container_overrides": [{"env": [{"name": key, "value": value} for key, value in env.items()]}],
                "timeout": f"{timeout_seconds}s",
            },
        }

    def build_environment(self, record: "QueuedTaskRecord") -> dict[str, str]:
        """Build generic environment variables for a Cloud Run task process.

        Returns:
            Environment variables for the Cloud Run task process.
        """
        config = self.cloudrun_config
        env = {
            config.env_name("TASK_ID"): str(record.id),
            config.env_name("TASK_NAME"): record.task_name,
            config.env_name("TASK_ARGS"): json.dumps(list(record.args), separators=(",", ":")),
            config.env_name("TASK_KWARGS"): json.dumps(record.kwargs, separators=(",", ":")),
            config.env_name("EXECUTION_BACKEND"): "cloudrun",
        }
        if record.execution_profile is not None:
            env[config.env_name("EXECUTION_PROFILE")] = record.execution_profile
        env.update(config.extra_env)
        return env

    async def _get_jobs_client(self) -> "CloudRunJobsClient":
        if self.jobs_client is None:
            try:
                from google.cloud import run_v2  # pyright: ignore[reportMissingImports]
            except ImportError as exc:
                raise MissingDependencyError(_GOOGLE_CLOUD_RUN_PACKAGE, _CLOUDRUN_EXTRA) from exc
            self.jobs_client = cast("CloudRunJobsClient", run_v2.JobsAsyncClient())
        return self.jobs_client

    async def _get_executions_client(self) -> "CloudRunExecutionsClient":
        if self.executions_client is None:
            try:
                from google.cloud import run_v2  # pyright: ignore[reportMissingImports]
            except ImportError as exc:
                raise MissingDependencyError(_GOOGLE_CLOUD_RUN_PACKAGE, _CLOUDRUN_EXTRA) from exc
            self.executions_client = cast("CloudRunExecutionsClient", run_v2.ExecutionsAsyncClient())
        return self.executions_client


def _execution_error(execution: "CloudRunExecutionLike") -> str | None:
    conditions = getattr(execution, "conditions", None) or []
    for condition in reversed(conditions):
        message = getattr(condition, "message", None)
        if message:
            return str(message)
    return None

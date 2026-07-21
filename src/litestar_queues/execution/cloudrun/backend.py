import logging
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import TYPE_CHECKING, Any, cast

from litestar_queues.events import QueueEvent
from litestar_queues.exceptions import MissingDependencyError
from litestar_queues.execution.base import BaseExecutionBackend
from litestar_queues.execution.cloudrun.config import CloudRunExecutionConfig, _execution_config_from_queue_config
from litestar_queues.execution.dispatch import TaskDispatch

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig
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
_TASK_DISPATCH_ENV_SUFFIX = "TASK_DISPATCH"
_HTTP_NOT_FOUND = 404
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CloudRunExecutionStatus:
    """Backend-neutral status for a Cloud Run execution."""

    succeeded: "bool" = False
    failed: "bool" = False
    cancelled: "bool" = False
    running: "bool" = True
    error: "str | None" = None


class CloudRunExecutionBackend(BaseExecutionBackend):
    """Execution backend that dispatches queued records to Cloud Run Jobs."""

    __slots__ = ("_execution_config", "executions_client", "jobs_client")

    def __init__(
        self,
        config: "QueueConfig | None" = None,
        *,
        execution_config: "CloudRunExecutionConfig | None" = None,
        jobs_client: "CloudRunJobsClient | None" = None,
        executions_client: "CloudRunExecutionsClient | None" = None,
    ) -> "None":
        super().__init__(config=config)
        self._execution_config = execution_config
        self.jobs_client = jobs_client
        self.executions_client = executions_client

    @property
    def is_external(self) -> "bool":
        """Whether this backend dispatches records to another process."""
        return True

    @property
    def execution_config(self) -> "CloudRunExecutionConfig":
        """Resolved Cloud Run execution config."""
        if self._execution_config is None:
            self._execution_config = _execution_config_from_queue_config(self.config)
        return self._execution_config

    async def execute(
        self, service: "QueueService", record: "QueuedTaskRecord", *, worker_id: "str | None" = None
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

    async def dispatch(self, service: "QueueService", record: "QueuedTaskRecord") -> "str | None":
        """Dispatch a queue record to Cloud Run Jobs.

        Returns:
            The Cloud Run execution reference, if dispatch succeeds.
        """
        runtime = service.observability_runtime
        attributes = _queue_observability_attributes("dispatch", record)
        attributes["messaging.message.id"] = str(record.id)
        metric_attributes = _queue_metric_attributes(attributes)
        span = runtime.start_span("litestar_queues.dispatch", kind="producer", attributes=attributes)
        try:
            request = self.build_run_job_request(service, record)
            client = await self._get_jobs_client()
            operation = await client.run_job(request=request)
            execution_ref = _require_operation_execution_ref(operation)
        except Exception as exc:
            runtime.record_exception(span, exc)
            await self._publish_dispatch_failure(service, record, exc)
            fallback = self.execution_config.fallback_execution_backend
            if fallback is None:
                runtime.record_counter(
                    "litestar_queues.execution.dispatch.count",
                    attributes={**metric_attributes, "queue.execution.status": "error"},
                )
                raise
            await service.get_queue_backend().set_execution_backend(record.id, fallback)
            runtime.record_counter(
                "litestar_queues.execution.dispatch.count",
                attributes={**metric_attributes, "queue.execution.status": "fallback"},
            )
            return None
        finally:
            runtime.end_span(span)

        await service.get_queue_backend().set_execution_ref(
            record.id, "cloudrun", execution_ref, execution_profile=record.execution_profile
        )
        runtime.record_counter(
            "litestar_queues.execution.dispatch.count",
            attributes={**metric_attributes, "queue.execution.status": "dispatched"},
        )
        return execution_ref

    async def reconcile(self, service: "QueueService", record: "QueuedTaskRecord") -> "QueuedTaskRecord | None":
        """Reconcile a Cloud Run execution with the queue record.

        Returns:
            The terminal queue record when reconciliation completed it.
        """
        if record.execution_ref is None:
            return None
        queue_backend = service.get_queue_backend()
        current = await queue_backend.get_task(record.id) or record
        if current.status in {"completed", "failed", "cancelled"}:
            return None

        runtime = service.observability_runtime
        attributes = _queue_observability_attributes("reconcile", record)
        attributes["messaging.message.id"] = str(record.id)
        metric_attributes = _queue_metric_attributes(attributes)
        span = runtime.start_span("litestar_queues.reconcile", kind="consumer", attributes=attributes)
        try:
            status = await self.check_execution_status(record.execution_ref)
        except Exception as exc:
            runtime.record_exception(span, exc)
            runtime.record_counter(
                "litestar_queues.execution.reconcile.count",
                attributes={**metric_attributes, "queue.execution.status": "error"},
            )
            raise
        finally:
            runtime.end_span(span)
        if status.running:
            return None

        expected_retry_count = current.retry_count if current.status == "running" else None
        if status.succeeded:
            if current.status != "running":
                return None
            updated = await queue_backend.complete_task(
                current.id,
                result=current.result
                if current.result is not None
                else {"cloudrun_execution": current.execution_ref, "status": "succeeded"},
                expected_retry_count=expected_retry_count,
            )
            _record_reconcile_result(runtime, metric_attributes, updated)
            return updated

        if status.cancelled:
            updated = await queue_backend.fail_task(
                current.id, "Cloud Run execution cancelled", retry=False, expected_retry_count=expected_retry_count
            )
            _record_reconcile_result(runtime, metric_attributes, updated)
            return updated

        if status.failed:
            updated = await queue_backend.fail_task(
                current.id, status.error or "Cloud Run execution failed", expected_retry_count=expected_retry_count
            )
            _record_reconcile_result(runtime, metric_attributes, updated)
            if updated is not None and updated.status in {"pending", "scheduled"}:
                return await queue_backend.set_execution_backend(
                    updated.id, updated.execution_backend, execution_profile=updated.execution_profile
                )
            return updated

        return None

    async def cancel(self, service: "QueueService", record: "QueuedTaskRecord") -> "bool":
        """Cloud Run Jobs do not expose per-execution cancellation here.

        Returns:
            Always false because per-execution cancellation is not implemented.
        """
        return False

    async def check_execution_status(self, execution_ref: "str") -> "CloudRunExecutionStatus":
        """Return Cloud Run execution status.

        Transient API failures are treated as still running so reconciliation does
        not create false terminal queue states.
        """
        try:
            execution = await (await self._get_executions_client()).get_execution(name=execution_ref)
        except Exception as exc:
            if _is_not_found_error(exc):
                return CloudRunExecutionStatus(running=False, failed=True, error="Cloud Run execution not found")
            logger.warning(
                "Cloud Run status probe failed", exc_info=True, extra={"cloudrun_execution_ref": execution_ref}
            )
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

    def build_run_job_request(self, service: "QueueService", record: "QueuedTaskRecord") -> "dict[str, Any]":
        """Build the Cloud Run Jobs API request for a queue record.

        Returns:
            Cloud Run Jobs API request data.
        """
        config = self.execution_config
        task_obj = service.resolve_task(record.task_name)
        timeout = record.metadata.get("timeout", task_obj.timeout)
        timeout_seconds = int(timeout if isinstance(timeout, int | float) else config.timeout)
        job_name = config.resolve_job_name(record.execution_profile)
        env = self.build_dispatch_env(record)
        return {
            "name": f"projects/{config.project_id}/locations/{config.region}/jobs/{job_name}",
            "overrides": {
                "container_overrides": [{"env": [{"name": key, "value": value} for key, value in env.items()]}],
                "timeout": f"{timeout_seconds}s",
            },
        }

    def build_dispatch_env(self, record: "QueuedTaskRecord") -> "dict[str, str]":
        """Build the single-value task-dispatch environment for a Cloud Run task process.

        The record is serialized into one prefix-aware environment variable
        (``LITESTAR_QUEUES_TASK_DISPATCH`` by default) carrying the
        universal task dispatch. Adopter ``extra_env`` values are merged in.

        Returns:
            Environment variables for the Cloud Run task process.
        """
        config = self.execution_config
        dispatch = TaskDispatch.from_record(record)
        env = {config.env_name(_TASK_DISPATCH_ENV_SUFFIX): dispatch.to_json().decode()}
        env.update(config.extra_env)
        return env

    async def _get_jobs_client(self) -> "CloudRunJobsClient":
        if self.jobs_client is None:
            try:
                run_v2 = import_module("google.cloud.run_v2")
            except ImportError as exc:
                raise MissingDependencyError(_GOOGLE_CLOUD_RUN_PACKAGE, _CLOUDRUN_EXTRA) from exc
            self.jobs_client = cast("CloudRunJobsClient", run_v2.JobsAsyncClient())
        return self.jobs_client

    async def _get_executions_client(self) -> "CloudRunExecutionsClient":
        if self.executions_client is None:
            try:
                run_v2 = import_module("google.cloud.run_v2")
            except ImportError as exc:
                raise MissingDependencyError(_GOOGLE_CLOUD_RUN_PACKAGE, _CLOUDRUN_EXTRA) from exc
            self.executions_client = cast("CloudRunExecutionsClient", run_v2.ExecutionsAsyncClient())
        return self.executions_client

    async def _publish_dispatch_failure(
        self, service: "QueueService", record: "QueuedTaskRecord", exc: "BaseException"
    ) -> "None":
        fallback = self.execution_config.fallback_execution_backend
        logger.warning(
            "Cloud Run dispatch failed",
            exc_info=(type(exc), exc, exc.__traceback__),
            extra={
                "queue_task_id": str(record.id),
                "queue_task_name": record.task_name,
                "queue_task_queue": record.queue,
                "queue_task_execution_backend": record.execution_backend,
                "queue_task_execution_profile": record.execution_profile,
                "cloudrun_fallback_execution_backend": fallback,
            },
        )
        try:
            await service.get_event_publisher().publish(
                QueueEvent(
                    type="task.event",
                    scope="task",
                    task_id=str(record.id),
                    task_name=record.task_name,
                    queue=record.queue,
                    execution_backend=record.execution_backend,
                    execution_profile=record.execution_profile,
                    attempt=record.retry_count + 1,
                    level="warning",
                    message="Cloud Run dispatch failed",
                    payload={
                        "phase": "cloudrun.dispatch_fallback",
                        "error": str(exc),
                        "fallback_execution_backend": fallback,
                    },
                )
            )
        except Exception:
            logger.warning(
                "Cloud Run dispatch failure event publish failed",
                exc_info=True,
                extra={"queue_task_id": str(record.id)},
            )


def _execution_error(execution: "CloudRunExecutionLike") -> "str | None":
    conditions = getattr(execution, "conditions", None) or []
    for condition in reversed(conditions):
        message = getattr(condition, "message", None)
        if message:
            return str(message)
    return None


def _queue_observability_attributes(operation: "str", record: "QueuedTaskRecord") -> "dict[str, object]":
    return {
        "messaging.system": "litestar_queues",
        "messaging.operation.name": operation,
        "messaging.destination.name": record.queue,
        "queue.task.name": record.task_name,
        "queue.execution.backend": record.execution_backend,
        "queue.execution.profile": record.execution_profile or "",
    }


def _queue_metric_attributes(attributes: "Mapping[str, object]") -> "dict[str, str]":
    return {
        "messaging.destination.name": str(attributes["messaging.destination.name"]),
        "queue.task.name": str(attributes["queue.task.name"]),
        "queue.execution.backend": str(attributes["queue.execution.backend"]),
        "queue.execution.profile": str(attributes["queue.execution.profile"]),
    }


def _record_reconcile_result(
    runtime: "Any", metric_attributes: "dict[str, str]", record: "QueuedTaskRecord | None"
) -> "None":
    if record is not None:
        runtime.record_counter(
            "litestar_queues.execution.reconcile.count",
            attributes={**metric_attributes, "queue.execution.status": record.status},
        )


def _operation_execution_ref(operation: "object") -> "str | None":
    return _execution_ref_from_value(getattr(operation, "metadata", None))


def _require_operation_execution_ref(operation: "object") -> "str":
    execution_ref = _operation_execution_ref(operation)
    if execution_ref is None:
        msg = "Cloud Run run_job operation did not include execution metadata."
        raise RuntimeError(msg)
    return execution_ref


def _execution_ref_from_value(value: "object") -> "str | None":
    if value is None:
        return None
    if isinstance(value, str):
        return value or None
    name = getattr(value, "name", None)
    if name:
        return str(name)
    if isinstance(value, Mapping):
        mapped_name = value.get("name")
        if mapped_name:
            return str(mapped_name)
        mapped_execution = value.get("execution")
        if mapped_execution is not None:
            return _execution_ref_from_value(mapped_execution)
    execution = getattr(value, "execution", None)
    if execution is not None:
        return _execution_ref_from_value(execution)
    return None


def _is_not_found_error(exc: "BaseException") -> "bool":
    if exc.__class__.__name__ == "NotFound":
        return True
    status_code = getattr(exc, "status_code", None) or getattr(exc, "status", None)
    return status_code == _HTTP_NOT_FOUND

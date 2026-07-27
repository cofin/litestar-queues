"""Cloud Tasks execution backend.

Constructing the backend never builds a Google client: the client is resolved on
first use so an installation without the ``cloud-tasks`` extra can still import,
configure, and validate this backend.

Scheduling only ever runs against an already-durable record, and the delivery's
resource name is persisted before the create call so an ambiguous response still
leaves a handle to look the delivery up by.
"""

import logging
from datetime import datetime, timedelta, timezone
from importlib import import_module
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from litestar.serialization import encode_json

from litestar_queues.events import QueueEvent
from litestar_queues.exceptions import MissingDependencyError, QueueConfigurationError, QueueDispatchError
from litestar_queues.execution.base import BaseExecutionBackend, DispatchRepairResult
from litestar_queues.execution.cloudtasks.config import (
    CLOUD_TASKS_MAX_SCHEDULE_HORIZON,
    CLOUD_TASKS_PROTOCOL_VERSION,
    CloudTasksExecutionConfig,
    _execution_config_from_queue_config,
)

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig
    from litestar_queues.execution.cloudtasks._typing import CloudTasksClient
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.service import QueueService

__all__ = ("CloudTasksExecutionBackend",)

_GOOGLE_CLOUD_TASKS_PACKAGE = "google-cloud-tasks"
_CLOUD_TASKS_EXTRA = "cloud-tasks"
_BACKEND_NAME = "cloudtasks"
_DELIVERY_PREFIX = "lq-"
_HTTP_CONFLICT = 409
_HTTP_NOT_FOUND = 404
_SCHEDULE_FAILED_PHASE = "cloudtasks.schedule_failed"
_REPAIR_FAILED_PHASE = "cloudtasks.repair_failed"
_FAILURE_MESSAGES = {
    _SCHEDULE_FAILED_PHASE: "Cloud Tasks delivery creation failed",
    _REPAIR_FAILED_PHASE: "Cloud Tasks delivery repair failed",
}
# A record a consumer already holds is excluded: Cloud Tasks keeps that delivery
# open until the response, so re-creating it would run the task twice.
_REPAIRABLE_STATUSES = frozenset({"pending", "scheduled"})
logger = logging.getLogger(__name__)


class CloudTasksExecutionBackend(BaseExecutionBackend):
    """Execution backend that hands persisted records to Google Cloud Tasks."""

    __slots__ = ("_execution_config", "_owns_client", "client")

    def __init__(
        self,
        config: "QueueConfig | None" = None,
        *,
        execution_config: "CloudTasksExecutionConfig | None" = None,
        client: "CloudTasksClient | None" = None,
    ) -> "None":
        """Initialize the Cloud Tasks execution backend."""
        super().__init__(config=config)
        self._execution_config = execution_config
        self.client = client
        self._owns_client = False

    @property
    def is_external(self) -> "bool":
        """Whether this backend dispatches records to another process."""
        return True

    @property
    def schedules_on_enqueue(self) -> "bool":
        """Whether the backend schedules a persisted record without a Worker."""
        return True

    @property
    def max_schedule_horizon(self) -> "timedelta":
        """How far ahead Cloud Tasks will hold a scheduled delivery."""
        return CLOUD_TASKS_MAX_SCHEDULE_HORIZON

    @property
    def execution_config(self) -> "CloudTasksExecutionConfig":
        """Resolved Cloud Tasks execution config."""
        if self._execution_config is not None:
            return self._execution_config
        return _execution_config_from_queue_config(self.config)

    async def schedule(self, service: "QueueService", record: "QueuedTaskRecord") -> "str | None":
        """Create the Cloud Tasks delivery for an already-persisted record.

        The record is read back rather than trusted: between the caller's copy
        and this call it may have been cancelled, expired, or removed, and a
        delivery for an id storage no longer holds would retry against the
        consumer until it expired.

        Returns:
            The delivery's full resource name, or ``None`` when the record is no
            longer eligible.

        Raises:
            QueueDispatchError: If the delivery could not be created. The record
                is committed, so the caller must not retry the enqueue.
        """
        queue_backend = service.get_queue_backend()
        current = await queue_backend.get_task(record.id)
        if current is None or current.is_terminal:
            return None

        config = self.execution_config
        _validate_schedulable(current, config)

        task_name = current.execution_ref
        if task_name is None or not _is_current_delivery_name(task_name, config, current):
            task_name = await self._reserve_delivery_name(service, current, config)
            if task_name is None:
                return None
        return await self._create_delivery(service, current, config, task_name, phase=_SCHEDULE_FAILED_PHASE)

    async def repair(self, service: "QueueService", *, limit: "int") -> "DispatchRepairResult":
        """Recreate deliveries Cloud Tasks no longer holds for still-active records.

        A delivery can go missing while its record stays active: a create call
        that failed after Google had already accepted it, an operator purging
        the queue, a retention window closing before the schedule time arrived.
        Nothing polls these records, so without this pass they wait forever.

        Every candidate is attempted at most once per pass, so a queue whose
        target is broken cannot spin inside one maintenance window.

        Args:
            service: The queue service whose records to repair.
            limit: Ceiling on how many records this pass may examine.

        Returns:
            How many records the pass looked at, and how many it re-delivered.
        """
        candidates = await service.get_queue_backend().list_running_external(limit=limit)
        config = self.execution_config
        changed = 0
        for candidate in candidates:
            if await self._repair_one(service, candidate, config):
                changed += 1
        return DispatchRepairResult(examined=len(candidates), changed=changed)

    async def close(self) -> "None":
        """Release a client this backend created.

        An injected client belongs to whoever built it and is left untouched.
        """
        if self.client is not None and self._owns_client:
            await self.client.close()
            self.client = None
            self._owns_client = False

    async def _repair_one(
        self, service: "QueueService", record: "QueuedTaskRecord", config: "CloudTasksExecutionConfig"
    ) -> "bool":
        """Re-deliver one candidate when Cloud Tasks no longer holds its delivery.

        The candidate list is a snapshot, so the record is re-read: between the
        listing and here it may have been cancelled, or claimed by a consumer
        whose delivery Cloud Tasks is still holding open for the response.

        Returns:
            True when a new delivery was created.
        """
        current = await service.get_queue_backend().get_task(record.id)
        if (
            current is None
            or current.execution_ref is None
            or current.status not in _REPAIRABLE_STATUSES
            or current.execution_backend != _BACKEND_NAME
        ):
            return False
        try:
            if await self._delivery_exists(current.execution_ref, config):
                return False
            task_name = await self._reserve_delivery_name(service, current, config)
            if task_name is None:
                return False
            await self._create_delivery(service, current, config, task_name, phase=_REPAIR_FAILED_PHASE)
        except QueueDispatchError:
            # Already reported with a sanitized payload. The record keeps the
            # name it reserved, and the next maintenance pass tries again.
            return False
        except Exception as exc:  # noqa: BLE001 - one broken candidate must not end the pass.
            await self._publish_delivery_failure(service, current, exc, phase=_REPAIR_FAILED_PHASE)
            return False
        return True

    async def _delivery_exists(self, task_name: "str", config: "CloudTasksExecutionConfig") -> "bool":
        """Whether Cloud Tasks still holds the named delivery.

        Returns:
            True when the task is present.

        Raises:
            Exception: Any lookup error other than a definite absence, so the
                caller does not treat an unknown answer as a missing delivery.
        """
        client = await self._get_client()
        try:
            await client.get_task(name=task_name, timeout=config.api_timeout)
        except Exception as exc:
            if _is_not_found(exc):
                return False
            raise
        return True

    async def _reserve_delivery_name(
        self, service: "QueueService", record: "QueuedTaskRecord", config: "CloudTasksExecutionConfig"
    ) -> "str | None":
        """Assign a fresh delivery name and persist it before anything is created.

        Returns:
            The reserved name, or ``None`` when the record no longer exists.
        """
        task_name = _delivery_name(config, record)
        stored = await service.get_queue_backend().set_execution_ref(
            record.id, record.execution_backend, task_name, execution_profile=record.execution_profile
        )
        return None if stored is None else task_name

    async def _create_delivery(
        self,
        service: "QueueService",
        record: "QueuedTaskRecord",
        config: "CloudTasksExecutionConfig",
        task_name: "str",
        *,
        phase: "str",
    ) -> "str":
        """Create the named delivery on the queue.

        Returns:
            The delivery's full resource name.

        Raises:
            QueueDispatchError: If the delivery could not be created. The record
                is committed, so the caller must not retry the enqueue.
        """
        client = await self._get_client()
        try:
            await client.create_task(
                request=_create_task_request(config, record, task_name), timeout=config.api_timeout
            )
        except Exception as exc:
            if _is_already_exists(exc) and await self._owns_delivery(service, record, task_name):
                # This attempt's own name: a previous create reached Google after
                # all, so the delivery it describes is already in flight.
                return task_name
            await self._publish_delivery_failure(service, record, exc, phase=phase)
            msg = f"Cloud Tasks delivery could not be created for task {record.id}."
            raise QueueDispatchError(msg, task_id=record.id, committed=True) from exc
        return task_name

    async def _owns_delivery(self, service: "QueueService", record: "QueuedTaskRecord", task_name: "str") -> "bool":
        """Whether the record still names ``task_name`` as its delivery.

        Returns:
            True when the collision proves this attempt's delivery exists.
        """
        current = await service.get_queue_backend().get_task(record.id)
        return current is not None and current.execution_ref == task_name

    async def _get_client(self) -> "CloudTasksClient":
        """Return the Cloud Tasks client, creating it on first use.

        Returns:
            The Cloud Tasks async client.

        Raises:
            MissingDependencyError: If the ``cloud-tasks`` extra is not installed.
        """
        if self.client is None:
            try:
                tasks_v2 = import_module("google.cloud.tasks_v2")
            except ImportError as exc:
                raise MissingDependencyError(_GOOGLE_CLOUD_TASKS_PACKAGE, _CLOUD_TASKS_EXTRA) from exc
            self.client = cast("CloudTasksClient", tasks_v2.CloudTasksAsyncClient())
            self._owns_client = True
        return self.client

    async def _publish_delivery_failure(
        self, service: "QueueService", record: "QueuedTaskRecord", exc: "BaseException", *, phase: "str"
    ) -> "None":
        """Report a failed delivery without repeating the API's message.

        Google's error text routinely quotes the target URL and the calling
        service account, so only the phase reaches the event payload; the
        original exception stays on the log record and chained to the raise.
        """
        message = _FAILURE_MESSAGES[phase]
        logger.warning(
            message,
            exc_info=(type(exc), exc, exc.__traceback__),
            extra={
                "queue_task_id": str(record.id),
                "queue_task_name": record.task_name,
                "queue_task_queue": record.queue,
                "queue_task_execution_backend": record.execution_backend,
                "queue_task_execution_profile": record.execution_profile,
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
                    message=message,
                    payload={"phase": phase},
                )
            )
        except Exception:
            logger.warning(
                "Cloud Tasks delivery failure event publish failed",
                exc_info=True,
                extra={"queue_task_id": str(record.id)},
            )


def _validate_schedulable(record: "QueuedTaskRecord", config: "CloudTasksExecutionConfig") -> "None":
    """Refuse a record Cloud Tasks would reject or silently never run.

    The enqueue path already checks both of these, but a recurrence computes its
    next run long afterwards, so the horizon has to be re-checked where the
    delivery is actually created.

    Raises:
        QueueConfigurationError: If the record names another backend or is due
            beyond the Cloud Tasks scheduling horizon.
    """
    if record.execution_backend != _BACKEND_NAME:
        msg = (
            f"Task {record.id} names execution_backend={record.execution_backend!r}, which no worker "
            f"polls on a Cloud Tasks queue; it would never run."
        )
        raise QueueConfigurationError(msg)
    if record.scheduled_at is not None and record.scheduled_at > datetime.now(timezone.utc) + (
        CLOUD_TASKS_MAX_SCHEDULE_HORIZON
    ):
        msg = (
            f"Task {record.id} is scheduled beyond the "
            f"{CLOUD_TASKS_MAX_SCHEDULE_HORIZON.days}-day Cloud Tasks horizon; "
            f"the create call would be refused."
        )
        raise QueueConfigurationError(msg)


def _delivery_name(config: "CloudTasksExecutionConfig", record: "QueuedTaskRecord") -> "str":
    """Build a fresh full resource name for this record's current attempt.

    The random suffix keeps names unguessable and sidesteps the tombstone Cloud
    Tasks keeps for a completed or deleted task id.

    Returns:
        The full Cloud Tasks resource name.
    """
    return f"{_delivery_name_prefix(config, record)}{uuid4().hex}"


def _delivery_name_prefix(config: "CloudTasksExecutionConfig", record: "QueuedTaskRecord") -> "str":
    return f"{config.queue_path}/tasks/{_DELIVERY_PREFIX}{record.id.hex}-r{record.retry_count}-"


def _is_current_delivery_name(name: "str", config: "CloudTasksExecutionConfig", record: "QueuedTaskRecord") -> "bool":
    """Whether a persisted name belongs to this record's current attempt.

    Returns:
        True when the name can be reused instead of creating a second delivery.
    """
    return name.startswith(_delivery_name_prefix(config, record))


def _create_task_request(
    config: "CloudTasksExecutionConfig", record: "QueuedTaskRecord", task_name: "str"
) -> "dict[str, Any]":
    """Build the CreateTask request for one delivery.

    Plain data throughout: the Google client converts the mapping itself, so
    nothing here imports ``google-cloud-tasks``. Only the record id crosses the
    transport -- the consumer re-reads everything else from storage.

    Returns:
        The Cloud Tasks CreateTask request.
    """
    task: "dict[str, Any]" = {
        "name": task_name,
        "dispatch_deadline": timedelta(seconds=config.dispatch_deadline),
        "http_request": {
            "http_method": "POST",
            "url": config.target_url,
            "headers": {"Content-Type": "application/json"},
            "body": encode_json({"version": CLOUD_TASKS_PROTOCOL_VERSION, "task_id": str(record.id)}),
            "oidc_token": {"service_account_email": config.service_account_email, "audience": config.audience},
        },
    }
    if record.scheduled_at is not None:
        # An unset schedule time means "dispatch now" on Google's clock, which is
        # a better answer than stamping this process's possibly skewed one.
        task["schedule_time"] = record.scheduled_at
    return {"parent": config.queue_path, "task": task}


def _is_already_exists(exc: "BaseException") -> "bool":
    """Whether an API error reports the named task as already present.

    Matched structurally so the check works against an injected fake and does not
    drag ``google.api_core`` into the import graph.

    Returns:
        True for an already-exists error.
    """
    return exc.__class__.__name__ == "AlreadyExists" or getattr(exc, "code", None) == _HTTP_CONFLICT


def _is_not_found(exc: "BaseException") -> "bool":
    """Whether an API error reports the named task as absent.

    Returns:
        True for a not-found error.
    """
    return exc.__class__.__name__ == "NotFound" or getattr(exc, "code", None) == _HTTP_NOT_FOUND

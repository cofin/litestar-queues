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
from litestar_queues.execution.base import BaseExecutionBackend
from litestar_queues.execution.cloudtasks.config import (
    CLOUD_TASKS_MAX_SCHEDULE_HORIZON,
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
_DELIVERY_VERSION = 1
_HTTP_CONFLICT = 409
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

        existing = current.execution_ref
        if existing is not None and _is_current_delivery_name(existing, config, current):
            task_name = existing
        else:
            task_name = _delivery_name(config, current)
            stored = await queue_backend.set_execution_ref(
                current.id, current.execution_backend, task_name, execution_profile=current.execution_profile
            )
            if stored is None:
                return None

        client = await self._get_client()
        try:
            await client.create_task(
                request=_create_task_request(config, current, task_name), timeout=config.api_timeout
            )
        except Exception as exc:
            if _is_already_exists(exc) and await self._owns_delivery(service, current, task_name):
                # This attempt's own name: a previous create reached Google after
                # all, so the delivery it describes is already in flight.
                return task_name
            await self._publish_schedule_failure(service, current, exc)
            msg = f"Cloud Tasks delivery could not be created for task {current.id}."
            raise QueueDispatchError(msg, task_id=current.id, committed=True) from exc
        return task_name

    async def close(self) -> "None":
        """Release a client this backend created.

        An injected client belongs to whoever built it and is left untouched.
        """
        if self.client is not None and self._owns_client:
            await self.client.close()
            self.client = None
            self._owns_client = False

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

    async def _publish_schedule_failure(
        self, service: "QueueService", record: "QueuedTaskRecord", exc: "BaseException"
    ) -> "None":
        """Report a failed delivery creation without repeating the API's message.

        Google's error text routinely quotes the target URL and the calling
        service account, so only the phase reaches the event payload; the
        original exception stays on the log record and chained to the raise.
        """
        logger.warning(
            "Cloud Tasks delivery creation failed",
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
                    message="Cloud Tasks delivery creation failed",
                    payload={"phase": "cloudtasks.schedule_failed"},
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
            "body": encode_json({"version": _DELIVERY_VERSION, "task_id": str(record.id)}),
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

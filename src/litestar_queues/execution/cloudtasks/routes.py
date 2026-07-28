"""The private HTTP route Cloud Tasks delivers each record to.

Cloud Tasks treats any non-2xx response as "deliver this again". That is why
this route answers so few statuses: every outcome the queue reached durably is
acknowledged, whether the task succeeded, failed for the last time, was
cancelled, or was already owned by someone else. None of those change if the
same delivery arrives a second time.

Only two things earn a retryable answer -- the queue could not be reached, and
the retry this request just scheduled never made it to Google -- because in both
cases the redelivery is what repairs the record.

A request that cannot be authenticated or parsed is never acknowledged. That is
a deployment fault, and letting Cloud Tasks retry it is what makes it visible.
"""

import logging
from typing import TYPE_CHECKING, Any
from uuid import UUID  # noqa: TC003 - msgspec resolves the struct's annotations at runtime.

import msgspec
from litestar import Request, Response, post
from litestar.exceptions import ValidationException

from litestar_queues.consumer import TaskExitCode, consume_one
from litestar_queues.exceptions import QueueDispatchError
from litestar_queues.execution.cloudtasks.config import (
    CLOUD_TASKS_BACKEND_NAME,
    CLOUD_TASKS_PROTOCOL_VERSION,
    _execution_config_from_queue_config,
)

if TYPE_CHECKING:
    from litestar.handlers import HTTPRouteHandler

    from litestar_queues.config import QueueConfig
    from litestar_queues.observability import QueueObservabilityRuntimeProtocol
    from litestar_queues.service import QueueService

__all__ = ("CloudTasksDelivery", "build_cloud_tasks_route")

logger = logging.getLogger(__name__)

_ACKNOWLEDGED = 204
_REDELIVER = 503

_DELIVERY_METRIC = "litestar_queues.execution.delivery"
_DELIVERY_LABEL = "queue.delivery.outcome"
_DELIVERY_LABELS = {"queue.execution.backend": CLOUD_TASKS_BACKEND_NAME}
"""The only label a delivery carries besides its outcome.

The route holds an id, not a record. Labelling with the queue or task name would
mean reading storage again on the one path -- a plain success -- that currently
needs nothing, and the per-task breakdown already exists on the task execution
metrics the run itself emits.
"""

_OUTCOME_ACKNOWLEDGED = "acknowledged"
_OUTCOME_DUPLICATE = "duplicate"
_OUTCOME_RETRY_SCHEDULED = "retry_scheduled"
_OUTCOME_TRANSIENT_ERROR = "transient_error"


class CloudTasksDelivery(msgspec.Struct, forbid_unknown_fields=True, frozen=True):
    """Everything that crosses the transport for one delivery.

    Rejecting unknown fields is the point rather than strictness for its own
    sake: the record in storage is authoritative, so anything a caller adds here
    would either be ignored or, worse, believed.
    """

    version: "int"
    task_id: "UUID"


def build_cloud_tasks_route(queue_config: "QueueConfig") -> "HTTPRouteHandler":
    """Build the delivery route for a queue configured for Cloud Tasks.

    Returns:
        The route handler Cloud Tasks posts each record to.
    """
    config = _execution_config_from_queue_config(queue_config)

    @post(
        path=config.route_path,
        guards=list(config.guards),
        status_code=_ACKNOWLEDGED,
        include_in_schema=False,
        signature_types=[CloudTasksDelivery],
        summary="Cloud Tasks delivery",
    )
    async def consume_cloud_task(data: "CloudTasksDelivery", request: "Request[Any, Any, Any]") -> "Response[None]":
        """Run the delivered record and answer with whether to deliver it again.

        Returns:
            An empty response carrying only the acknowledgement decision.

        Raises:
            ValidationException: If the body is not a protocol version this
                build knows how to read.
        """
        if data.version != CLOUD_TASKS_PROTOCOL_VERSION:
            msg = f"Unsupported Cloud Tasks delivery version: {data.version!r}."
            raise ValidationException(msg)
        return await _run_delivered_record(queue_config.get_service(request.app.state), data.task_id)

    return consume_cloud_task


async def _run_delivered_record(service: "QueueService", task_id: "UUID") -> "Response[None]":
    """Execute one delivered record and map its outcome onto a delivery answer.

    Returns:
        An empty acknowledgement, or an empty retryable response.
    """
    runtime = service.observability_runtime
    runtime_logger = logging.getLogger(service.config.names.logger("execution", "cloudtasks", "delivery"))
    try:
        exit_code = await consume_one(service, task_id)
    except QueueDispatchError:
        # Already reported with a sanitized payload where it was raised. The
        # record is durable and active; this same delivery arriving again is
        # what gives it a new Cloud Task.
        runtime_logger.warning(
            "Cloud Tasks delivery could not schedule the next attempt", extra={"queue_task_id": str(task_id)}
        )
        _record_delivery(runtime, _OUTCOME_TRANSIENT_ERROR)
        return Response(content=None, status_code=_REDELIVER)
    except Exception:
        # Task failures never reach here -- the service records those durably.
        # What is left is the queue itself being unreachable or broken, which
        # says nothing about the task, so the delivery has to survive it.
        runtime_logger.exception("Cloud Tasks delivery failed", extra={"queue_task_id": str(task_id)})
        _record_delivery(runtime, _OUTCOME_TRANSIENT_ERROR)
        return Response(content=None, status_code=_REDELIVER)

    try:
        outcome = await _delivery_outcome(service, task_id, exit_code)
    except Exception:  # noqa: BLE001 - naming an outcome must never revoke one.
        # The run already earned this response. A label that cannot be worked
        # out is a worse signal than "acknowledged"; a 500 here would be a wrong
        # answer about work that is finished.
        outcome = _OUTCOME_ACKNOWLEDGED
    _record_delivery(runtime, outcome)
    return Response(content=None, status_code=_ACKNOWLEDGED)


async def _delivery_outcome(service: "QueueService", task_id: "UUID", exit_code: "TaskExitCode") -> "str":
    """Name what this delivery did, for a signal an operator reads by rate.

    A queue whose deliveries are nearly all retries is not idle and not broken --
    it is paying Google twice for every unit of work -- and that is invisible if
    a retry and a settled failure count as the same thing.

    Returns:
        One of the fixed delivery outcomes.
    """
    if exit_code is TaskExitCode.CLAIM_LOST:
        return _OUTCOME_DUPLICATE
    if exit_code is not TaskExitCode.FAILURE:
        return _OUTCOME_ACKNOWLEDGED
    # The only ambiguous code: the record either settled or went back to the
    # queue with a delivery of its own, and one read tells them apart.
    record = await service.get_task(task_id)
    return _OUTCOME_ACKNOWLEDGED if record is None or record.is_terminal else _OUTCOME_RETRY_SCHEDULED


def _record_delivery(runtime: "QueueObservabilityRuntimeProtocol", outcome: "str") -> "None":
    """Count one delivery under its fixed outcome vocabulary."""
    runtime.record_counter(_DELIVERY_METRIC, attributes={**_DELIVERY_LABELS, _DELIVERY_LABEL: outcome})

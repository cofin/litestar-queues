"""Strict test receiver for Floci-GCP Eventarc Standard Pub/Sub events."""

import binascii
from base64 import b64decode
from dataclasses import dataclass, field
from uuid import UUID

import msgspec
from litestar import Litestar, Request, Response, post
from litestar.di import NamedDependency, Provide

from litestar_queues.consumer import consume_one
from litestar_queues.exceptions import QueueError
from litestar_queues.execution.pubsub.backend import ATTEMPT_ATTRIBUTE, _parse_attempt
from litestar_queues.service import QueueService

__all__ = ("EVENT_TYPE", "EventarcDelivery", "EventarcReceiverProbe", "create_eventarc_receiver")

EVENT_TYPE = "google.cloud.pubsub.topic.v1.messagePublished"
_MAX_BODY_BYTES = 64 * 1024


class _PubSubMessage(msgspec.Struct, forbid_unknown_fields=True, frozen=True, rename="camel"):
    data: str
    attributes: dict[str, str]
    message_id: str
    publish_time: str


class _PubSubEnvelope(msgspec.Struct, forbid_unknown_fields=True, frozen=True):
    message: _PubSubMessage
    subscription: str | None = None


@dataclass(frozen=True)
class EventarcDelivery:
    """Validated values observed at the receiver boundary."""

    task_id: UUID
    attempt: str


@dataclass
class EventarcReceiverProbe:
    """Record validated deliveries without making request data authoritative."""

    deliveries: list[EventarcDelivery] = field(default_factory=list)
    rejections: list[str] = field(default_factory=list)


def create_eventarc_receiver(*, queue_service: QueueService, topic: str) -> Litestar:
    """Build an isolated receiver app for one exact Pub/Sub topic."""
    probe = EventarcReceiverProbe()

    async def provide_queue_service() -> QueueService:
        return queue_service

    @post(path="/eventarc/pubsub", include_in_schema=False)
    async def receive_event(request: Request, queue_service: NamedDependency[QueueService]) -> Response[None]:
        header_error = _headers_error(request, topic)
        if header_error is not None:
            probe.rejections.append(header_error)
            return Response(content=None, status_code=400)
        body = await request.body()
        if len(body) > _MAX_BODY_BYTES:
            probe.rejections.append("body_too_large")
            return Response(content=None, status_code=413)
        try:
            envelope = msgspec.json.decode(body, type=_PubSubEnvelope, strict=True)
            task_id = UUID(b64decode(envelope.message.data, validate=True).decode("utf-8"))
        except (msgspec.DecodeError, binascii.Error, UnicodeDecodeError, ValueError):
            probe.rejections.append("invalid_body")
            return Response(content=None, status_code=400)
        attempt = envelope.message.attributes.get(ATTEMPT_ATTRIBUTE)
        parsed = _parse_attempt(attempt)
        if not isinstance(attempt, str) or parsed is None or envelope.message.message_id != request.headers["ce-id"]:
            probe.rejections.append("invalid_attempt_or_message_id")
            return Response(content=None, status_code=400)
        retry_count, _created_at = parsed
        try:
            await consume_one(queue_service, task_id, expected_retry_count=retry_count, expected_execution_ref=attempt)
        except (OSError, QueueError):
            return Response(content=None, status_code=503)
        probe.deliveries.append(EventarcDelivery(task_id=task_id, attempt=attempt))
        return Response(content=None, status_code=204)

    app = Litestar(
        route_handlers=[receive_event],
        dependencies={"queue_service": Provide(provide_queue_service)},
        request_max_body_size=_MAX_BODY_BYTES,
    )
    app.state.eventarc_queue_service = queue_service
    app.state.eventarc_receiver_probe = probe
    return app


def _headers_error(request: Request, topic: str) -> str | None:
    headers = request.headers
    content_type = headers.get("content-type", "").partition(";")[0].strip().lower()
    expected = {
        "content-type": "application/json",
        "ce-specversion": "1.0",
        "ce-type": EVENT_TYPE,
        "ce-source": f"//pubsub.googleapis.com/{topic}",
    }
    actual = {**headers, "content-type": content_type}
    for name, value in expected.items():
        if actual.get(name) != value:
            return f"invalid_{name}"
    if not headers.get("ce-id"):
        return "missing_ce-id"
    if not headers.get("ce-time"):
        return "missing_ce-time"
    return None

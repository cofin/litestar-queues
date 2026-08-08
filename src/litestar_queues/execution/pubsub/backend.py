import asyncio
import inspect
import time
from contextlib import suppress
from importlib import import_module
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID, uuid4

from litestar_queues.consumer import TaskExitCode, consume_one
from litestar_queues.exceptions import MissingDependencyError, QueueDispatchError
from litestar_queues.execution.base import (
    BaseConsumerExecutionBackend,
    DispatchRepairResult,
    _queue_metric_attributes,
    _queue_observability_attributes,
)
from litestar_queues.execution.pubsub.config import PubSubExecutionConfig, _execution_config_from_queue_config

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig
    from litestar_queues.execution.pubsub._typing import PubSubPublisherClient, PubSubSubscriberClient
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.service import QueueService

__all__ = ("PubSubExecutionBackend",)

ATTEMPT_ATTRIBUTE = "litestar_queues_attempt"


class PubSubExecutionBackend(BaseConsumerExecutionBackend):
    """Dispatch bare task identifiers through Google Cloud Pub/Sub."""

    __slots__ = ("_execution_config", "_owns_publisher", "_owns_subscriber", "_publisher", "_subscriber")

    def __init__(
        self,
        config: "QueueConfig | None" = None,
        *,
        execution_config: "PubSubExecutionConfig | None" = None,
        publisher: "PubSubPublisherClient | None" = None,
        subscriber: "PubSubSubscriberClient | None" = None,
    ) -> "None":
        super().__init__(config=config)
        self._execution_config = execution_config
        self._publisher = publisher
        self._subscriber = subscriber
        self._owns_publisher = False
        self._owns_subscriber = False

    @property
    def is_external(self) -> "bool":
        """Return whether execution occurs in another process."""
        return True

    @property
    def execution_config(self) -> "PubSubExecutionConfig":
        """Return the effective Pub/Sub configuration."""
        if self._execution_config is None:
            self._execution_config = _execution_config_from_queue_config(self.config)
        return self._execution_config

    async def dispatch(self, service: "QueueService", record: "QueuedTaskRecord") -> "str | None":
        """Reserve and publish one task-id delivery."""
        runtime = service.observability_runtime
        attributes = _queue_observability_attributes("dispatch", record)
        attributes["messaging.message.id"] = str(record.id)
        span = runtime.start_span("litestar_queues.dispatch", kind="producer", attributes=attributes)
        attempt_ref = _new_attempt(record.retry_count)
        try:
            reserved = await service.get_queue_backend().reserve_external_dispatch(
                record.id,
                "pubsub",
                attempt_ref,
                execution_profile=record.execution_profile,
                expected_retry_count=record.retry_count,
            )
            if reserved is None:
                _record_metric(service, record, "dispatch", "skipped")
                return None
            await self._send(service, reserved, attempt_ref)
            _record_metric(service, reserved, "dispatch", "dispatched")
        except asyncio.CancelledError:
            _record_metric(service, record, "dispatch", "cancelled")
            raise
        except Exception:
            runtime.set_status_error(span, "pubsub.dispatch_failed")
            _record_metric(service, record, "dispatch", "error")
            raise
        else:
            return attempt_ref
        finally:
            runtime.end_span(span)

    async def _send(self, service: "QueueService", record: "QueuedTaskRecord", attempt_ref: "str") -> "None":
        request = {
            "topic": self.execution_config.topic_path,
            "messages": [{"data": str(record.id).encode(), "attributes": {ATTEMPT_ATTRIBUTE: attempt_ref}}],
        }
        try:
            publisher = await self._get_publisher()
            await publisher.publish(request=request, timeout=self.execution_config.api_timeout)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _definitive_publish_error(exc):
                with suppress(Exception):
                    await service.get_queue_backend().clear_execution_ref(record.id, record.retry_count, attempt_ref)
                raise
            msg = "Pub/Sub dispatch outcome is unknown"
            raise QueueDispatchError(msg, task_id=record.id, committed=True) from exc

    async def repair(self, service: "QueueService", *, limit: "int") -> "DispatchRepairResult":
        """Rotate and republish stale attempt references."""
        if limit <= 0:
            return DispatchRepairResult()
        records = await service.get_queue_backend().list_running_external(limit=limit)
        examined = len(records)
        changed = 0
        now_ms = int(time.time() * 1000)
        stale_ms = self.execution_config.dispatch_stale_after * 1000
        for record in records:
            old_ref = record.execution_ref
            parsed = _parse_attempt(old_ref)
            if old_ref is None or parsed is None:
                continue
            attempt_retry, attempt_ms = parsed
            if attempt_retry == record.retry_count and now_ms - attempt_ms < stale_ms:
                continue
            new_ref = _new_attempt(record.retry_count)
            rotated = await service.get_queue_backend().replace_execution_ref(
                record.id, record.retry_count, old_ref, new_ref
            )
            if rotated is None:
                continue
            try:
                await self._send(service, rotated, new_ref)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._logger.warning(
                    "Pub/Sub delivery repair failed",
                    exc_info=True,
                    extra={"queue_task_id": str(record.id), "queue_task_queue": record.queue},
                )
                _record_metric(service, rotated, "repair", "error")
            else:
                _record_metric(service, rotated, "repair", "republished")
                changed += 1
        return DispatchRepairResult(examined=examined, changed=changed)

    async def execute(
        self, service: "QueueService", record: "QueuedTaskRecord", *, worker_id: "str | None" = None
    ) -> "QueuedTaskRecord":
        """Dispatch a record and return its latest persisted state."""
        del worker_id
        await self.dispatch(service, record)
        return await service.get_queue_backend().get_task(record.id) or record

    async def _get_publisher(self) -> "PubSubPublisherClient":
        if self._publisher is None:
            publisher_module = _pubsub_module("google.pubsub_v1.services.publisher")
            self._publisher = cast(
                "PubSubPublisherClient",
                _create_async_client(publisher_module.PublisherAsyncClient, "Publisher", self.execution_config),
            )
            self._owns_publisher = True
        return self._publisher

    async def _get_subscriber(self) -> "PubSubSubscriberClient":
        if self._subscriber is None:
            subscriber_module = _pubsub_module("google.pubsub_v1.services.subscriber")
            self._subscriber = cast(
                "PubSubSubscriberClient",
                _create_async_client(subscriber_module.SubscriberAsyncClient, "Subscriber", self.execution_config),
            )
            self._owns_subscriber = True
        return self._subscriber

    async def close(self) -> "None":
        """Close clients created by this backend."""
        if self._owns_subscriber and self._subscriber is not None:
            await _close_client(self._subscriber)
        if self._owns_publisher and self._publisher is not None:
            await _close_client(self._publisher)
        self._subscriber = None
        self._publisher = None
        self._owns_subscriber = False
        self._owns_publisher = False

    async def run_consumer(self, service: "QueueService", *, max_concurrency: "int", drain_timeout: "float") -> "None":
        """Consume streaming-pull deliveries until cancelled."""
        request_stream = _StreamingPullRequests(self.execution_config, max_concurrency)
        subscriber = await self._get_subscriber()
        responses = await subscriber.streaming_pull(requests=request_stream)
        semaphore = asyncio.Semaphore(max_concurrency)
        running: "set[asyncio.Task[None]]" = set()
        try:
            async for response in responses:
                for received in response.received_messages:
                    task = asyncio.create_task(self._consume_message(service, received, request_stream, semaphore))
                    running.add(task)
                    task.add_done_callback(lambda completed: _consumer_task_done(running, completed, self._logger))
        except asyncio.CancelledError:
            await request_stream.close()
            if running:
                try:
                    done, pending = await asyncio.wait(running, timeout=drain_timeout)
                except asyncio.CancelledError:
                    for task in running:
                        task.cancel()
                    await asyncio.gather(*running, return_exceptions=True)
                    raise
                for task in pending:
                    task.cancel()
                await asyncio.gather(*done, *pending, return_exceptions=True)
            raise

    async def _consume_message(
        self,
        service: "QueueService",
        received: "Any",
        requests: "_StreamingPullRequests",
        semaphore: "asyncio.Semaphore",
    ) -> "None":
        async with semaphore:
            ack_id = received.ack_id
            message = received.message
            attempt = message.attributes.get(ATTEMPT_ATTRIBUTE)
            parsed = _parse_attempt(attempt)
            try:
                task_id = UUID(message.data.decode())
            except (ValueError, UnicodeDecodeError, AttributeError):
                task_id = None
            if task_id is None or parsed is None:
                _record_delivery_metric(service, "poison")
                await requests.ack(ack_id)
                return
            retry_count, _attempt_ms = parsed
            try:
                current = await service.get_task(task_id)
            except Exception:  # noqa: BLE001 -- storage failures must be retried by Pub/Sub
                _record_delivery_metric(service, "storage_error")
                await requests.nack(ack_id)
                return
            if current is None:
                _record_delivery_metric(service, "missing")
                await requests.ack(ack_id)
                return
            if current.is_terminal:
                _record_delivery_metric(service, "terminal")
                await requests.ack(ack_id)
                return
            extension_task = asyncio.create_task(requests.extend(ack_id))
            try:
                outcome = await consume_one(
                    service, task_id, expected_retry_count=retry_count, expected_execution_ref=attempt
                )
            except asyncio.CancelledError:
                _record_delivery_metric(service, "cancelled")
                await requests.nack(ack_id)
                raise
            except Exception:  # noqa: BLE001 -- infrastructure failures must be retried by Pub/Sub
                _record_delivery_metric(service, "execution_error")
                await requests.nack(ack_id)
                return
            finally:
                extension_task.cancel()
                await asyncio.gather(extension_task, return_exceptions=True)
            if outcome == TaskExitCode.FAILURE:
                latest = await service.get_task(task_id)
                if latest is None:
                    await requests.nack(ack_id)
                    return
                if not latest.is_terminal:
                    cleared = await service.get_queue_backend().clear_execution_ref(
                        task_id, latest.retry_count, attempt
                    )
                    if cleared is None:
                        _record_delivery_metric(service, "retry_clear_lost")
                        await requests.nack(ack_id)
                        return
            await requests.ack(ack_id)
            _record_delivery_metric(service, "acked")


def _pubsub_module(module: "str") -> "Any":
    try:
        return import_module(module)
    except ImportError as exc:
        package = "google-cloud-pubsub"
        extra = "pubsub"
        raise MissingDependencyError(package, extra) from exc


def _create_async_client(client_class: "Any", service_name: "str", config: "PubSubExecutionConfig") -> "Any":
    if config.api_endpoint is None:
        return client_class()
    if not config.api_insecure:
        return client_class(client_options={"api_endpoint": config.api_endpoint})
    grpc = _pubsub_module("grpc")
    transport_module = _pubsub_module(f"google.pubsub_v1.services.{service_name.lower()}.transports.grpc_asyncio")
    transport_class = getattr(transport_module, f"{service_name}GrpcAsyncIOTransport")
    channel = grpc.aio.insecure_channel(config.api_endpoint)
    return client_class(transport=transport_class(channel=channel))


def _definitive_publish_error(exc: "BaseException") -> "bool":
    return exc.__class__.__name__ in {
        "FailedPrecondition",
        "InvalidArgument",
        "NotFound",
        "PermissionDenied",
        "Unauthenticated",
    }


def _new_attempt(retry_count: "int") -> "str":
    return f"pubsub:{retry_count}:{int(time.time() * 1000)}:{uuid4()}"


def _parse_attempt(value: "object") -> "tuple[int, int] | None":
    if not isinstance(value, str):
        return None
    parts = value.split(":")
    if len(parts) != 4 or parts[0] != "pubsub":  # noqa: PLR2004
        return None
    try:
        retry_count = int(parts[1])
        timestamp = int(parts[2])
        UUID(parts[3])
    except (ValueError, TypeError):
        return None
    return (retry_count, timestamp) if retry_count >= 0 and timestamp >= 0 else None


def _record_metric(service: "QueueService", record: "QueuedTaskRecord", operation: "str", outcome: "str") -> "None":
    label = "queue.repair.outcome" if operation == "repair" else "queue.execution.status"
    service.observability_runtime.record_counter(
        f"litestar_queues.execution.{operation}", attributes={**_queue_metric_attributes(record), label: outcome}
    )


class _StreamingPullRequests:
    """Keep the bidirectional request stream alive for ack and deadline updates."""

    __slots__ = ("_config", "_max_concurrency", "_queue", "_started")

    def __init__(self, config: "PubSubExecutionConfig", max_concurrency: "int") -> "None":
        self._config = config
        self._max_concurrency = max_concurrency
        self._queue: "asyncio.Queue[Any | None]" = asyncio.Queue()
        self._started = False

    def __aiter__(self) -> "_StreamingPullRequests":
        return self

    async def __anext__(self) -> "Any":
        pubsub = _pubsub_module("google.pubsub_v1.types.pubsub")
        if not self._started:
            self._started = True
            return pubsub.StreamingPullRequest(
                subscription=self._config.subscription_path,
                stream_ack_deadline_seconds=self._config.ack_deadline,
                max_outstanding_messages=self._max_concurrency,
            )
        request = await self._queue.get()
        if request is None:
            raise StopAsyncIteration
        return request

    async def ack(self, ack_id: "str") -> "None":
        pubsub = _pubsub_module("google.pubsub_v1.types.pubsub")
        await self._queue.put(pubsub.StreamingPullRequest(ack_ids=[ack_id]))

    async def nack(self, ack_id: "str") -> "None":
        await self._modify_deadline(ack_id, 0)

    async def extend(self, ack_id: "str") -> "None":
        while True:
            await asyncio.sleep(self._config.ack_extension_interval)
            await self._modify_deadline(ack_id, self._config.ack_deadline)

    async def close(self) -> "None":
        await self._queue.put(None)

    async def _modify_deadline(self, ack_id: "str", seconds: "int") -> "None":
        pubsub = _pubsub_module("google.pubsub_v1.types.pubsub")
        await self._queue.put(
            pubsub.StreamingPullRequest(modify_deadline_ack_ids=[ack_id], modify_deadline_seconds=[seconds])
        )


def _consumer_task_done(running: "set[asyncio.Task[None]]", task: "asyncio.Task[None]", logger: "Any") -> "None":
    running.discard(task)
    if not task.cancelled() and task.exception() is not None:
        logger.error("Pub/Sub consumer task failed", exc_info=task.exception())


async def _close_client(client: "Any") -> "None":
    close = getattr(client, "close", None)
    if close is None:
        close = client.transport.close
    result = close()
    if inspect.isawaitable(result):
        await result


def _record_delivery_metric(service: "QueueService", outcome: "str") -> "None":
    service.observability_runtime.record_counter(
        "litestar_queues.execution.delivery", attributes={"queue.delivery.outcome": outcome}
    )

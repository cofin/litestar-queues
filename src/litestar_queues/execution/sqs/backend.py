import asyncio
import hashlib
import time
from contextlib import suppress
from importlib import import_module
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from litestar_queues.consumer import TaskExitCode, consume_one
from litestar_queues.exceptions import MissingDependencyError, QueueDispatchError
from litestar_queues.execution.base import (
    BaseConsumerExecutionBackend,
    DispatchRepairResult,
    _queue_metric_attributes,
    _queue_observability_attributes,
)
from litestar_queues.execution.sqs.config import SqsExecutionConfig, _execution_config_from_queue_config

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.service import QueueService

__all__ = ("SqsExecutionBackend",)

ATTEMPT_ATTRIBUTE = "litestar_queues_attempt"


class SqsExecutionBackend(BaseConsumerExecutionBackend):
    """Dispatch bare task identifiers through Amazon SQS."""

    __slots__ = ("_client", "_client_context", "_execution_config")

    def __init__(
        self,
        config: "QueueConfig | None" = None,
        *,
        execution_config: "SqsExecutionConfig | None" = None,
        client: "Any | None" = None,
    ) -> "None":
        super().__init__(config=config)
        self._execution_config = execution_config
        self._client = client
        self._client_context: "Any | None" = None

    @property
    def is_external(self) -> "bool":
        return True

    @property
    def execution_config(self) -> "SqsExecutionConfig":
        if self._execution_config is None:
            self._execution_config = _execution_config_from_queue_config(self.config)
        return self._execution_config

    async def dispatch(self, service: "QueueService", record: "QueuedTaskRecord") -> "str | None":
        runtime = service.observability_runtime
        attributes = _queue_observability_attributes("dispatch", record)
        attributes["messaging.message.id"] = str(record.id)
        span = runtime.start_span("litestar_queues.dispatch", kind="producer", attributes=attributes)
        attempt_ref = _new_attempt(record.retry_count)
        queue_backend = service.get_queue_backend()
        try:
            reserved = await queue_backend.reserve_external_dispatch(
                record.id,
                "sqs",
                attempt_ref,
                execution_profile=record.execution_profile,
                expected_retry_count=record.retry_count,
            )
            if reserved is None:
                _record_metric(service, record, "dispatch", "skipped")
                return None
            await self._send(service, reserved, attempt_ref)
            _record_metric(service, record, "dispatch", "dispatched")
            return attempt_ref
        except asyncio.CancelledError:
            _record_metric(service, record, "dispatch", "cancelled")
            raise
        except Exception:
            runtime.set_status_error(span, "sqs.dispatch_failed")
            _record_metric(service, record, "dispatch", "error")
            raise
        finally:
            runtime.end_span(span)

    async def _send(self, service: "QueueService", record: "QueuedTaskRecord", attempt_ref: "str") -> "None":
        """Publish an already-owned attempt, preserving ambiguous reservations."""
        queue_backend = service.get_queue_backend()
        request: "dict[str, Any]" = {
            "QueueUrl": self.execution_config.queue_url,
            "MessageBody": str(record.id),
            "MessageAttributes": {ATTEMPT_ATTRIBUTE: {"DataType": "String", "StringValue": attempt_ref}},
        }
        if self.execution_config.fifo:
            request["MessageGroupId"] = self.execution_config.message_group_id or _derived_group(record.queue)
            request["MessageDeduplicationId"] = attempt_ref
        try:
            client = await self._get_client()
            await asyncio.wait_for(client.send_message(**request), timeout=self.execution_config.api_timeout)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _definitive_client_error(exc):
                with suppress(Exception):
                    await queue_backend.clear_execution_ref(record.id, record.retry_count, attempt_ref)
                raise
            msg = "SQS dispatch outcome is unknown"
            raise QueueDispatchError(msg, task_id=record.id, committed=True) from exc

    async def repair(self, service: "QueueService", *, limit: "int") -> "DispatchRepairResult":
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
                    "SQS delivery repair failed",
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
        del worker_id
        await self.dispatch(service, record)
        return await service.get_queue_backend().get_task(record.id) or record

    async def _get_client(self) -> "Any":
        if self._client is not None:
            return self._client
        try:
            session_module = import_module("aiobotocore.session")
            config_module = import_module("botocore.config")
        except ImportError as exc:
            package = "aiobotocore"
            extra = "sqs"
            raise MissingDependencyError(package, extra) from exc
        session = session_module.get_session()
        client_context = session.create_client(
            "sqs",
            region_name=self.execution_config.region_name,
            endpoint_url=self.execution_config.endpoint_url,
            config=config_module.Config(
                connect_timeout=self.execution_config.api_timeout, read_timeout=self.execution_config.api_timeout
            ),
        )
        self._client_context = client_context
        self._client = await client_context.__aenter__()
        return self._client

    async def close(self) -> "None":
        if self._client_context is not None:
            await self._client_context.__aexit__(None, None, None)
            self._client_context = None
            self._client = None

    async def run_consumer(self, service: "QueueService", *, max_concurrency: "int", drain_timeout: "float") -> "None":
        semaphore = asyncio.Semaphore(max_concurrency)
        running: "set[asyncio.Task[None]]" = set()
        client = await self._get_client()
        try:
            while True:
                available = max_concurrency - len(running)
                if available <= 0:
                    done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
                    running.difference_update(done)
                    continue
                response = await client.receive_message(
                    QueueUrl=self.execution_config.queue_url,
                    MaxNumberOfMessages=min(self.execution_config.receive_batch_size, available),
                    WaitTimeSeconds=self.execution_config.wait_time_seconds,
                    VisibilityTimeout=self.execution_config.visibility_timeout,
                    MessageAttributeNames=[ATTEMPT_ATTRIBUTE],
                )
                for message in response.get("Messages", ()):
                    task = asyncio.create_task(self._consume_message(service, message, semaphore))
                    running.add(task)
                    task.add_done_callback(lambda completed: _consumer_task_done(running, completed, self._logger))
        except asyncio.CancelledError:
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
        self, service: "QueueService", message: "dict[str, Any]", semaphore: "asyncio.Semaphore"
    ) -> "None":
        async with semaphore:
            receipt = message.get("ReceiptHandle")
            attempt = message.get("MessageAttributes", {}).get(ATTEMPT_ATTRIBUTE, {}).get("StringValue")
            parsed = _parse_attempt(attempt)
            try:
                task_id = UUID(message.get("Body", ""))
            except (ValueError, TypeError, AttributeError):
                task_id = None
            if task_id is None or parsed is None:
                _record_delivery_metric(service, "poison")
                if receipt:
                    await self._delete(receipt)
                return
            retry_count, _attempt_ms = parsed
            try:
                current = await service.get_task(task_id)
            except Exception:
                _record_delivery_metric(service, "storage_error")
                return
            if current is None:
                _record_delivery_metric(service, "missing")
                if receipt:
                    await self._delete(receipt)
                return
            runtime = service.observability_runtime
            attributes = _queue_observability_attributes("deliver", current)
            attributes["messaging.message.id"] = str(task_id)
            span = runtime.start_span("litestar_queues.deliver", kind="consumer", attributes=attributes)
            visibility_task = (
                asyncio.create_task(self._extend_visibility(receipt)) if receipt and self.execution_config.visibility_timeout else None
            )
            try:
                outcome = await consume_one(
                    service, task_id, expected_retry_count=retry_count, expected_execution_ref=attempt
                )
            except asyncio.CancelledError:
                _record_delivery_metric(service, "cancelled")
                raise
            except Exception:  # noqa: BLE001 -- infrastructure failures must leave the delivery unacknowledged
                runtime.set_status_error(span, "sqs.delivery_failed")
                _record_delivery_metric(service, "execution_error")
                return
            finally:
                if visibility_task is not None:
                    visibility_task.cancel()
                    await asyncio.gather(visibility_task, return_exceptions=True)
                runtime.end_span(span)
            if outcome == TaskExitCode.FAILURE:
                current = await service.get_task(task_id)
                if current is None:
                    return
                if not current.is_terminal:
                    cleared = await service.get_queue_backend().clear_execution_ref(
                        task_id, current.retry_count, attempt
                    )
                    if cleared is None:
                        _record_delivery_metric(service, "retry_clear_lost")
                        return
            if receipt:
                await self._delete(receipt)
            _record_delivery_metric(service, "deleted")

    async def _delete(self, receipt: "str") -> "None":
        client = await self._get_client()
        await client.delete_message(QueueUrl=self.execution_config.queue_url, ReceiptHandle=receipt)

    async def _extend_visibility(self, receipt: "str") -> "None":
        client = await self._get_client()
        while True:
            await asyncio.sleep(self.execution_config.visibility_extension_interval)
            try:
                await client.change_message_visibility(
                    QueueUrl=self.execution_config.queue_url,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=self.execution_config.visibility_timeout,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                self._logger.warning("SQS visibility extension failed", exc_info=True)


def _derived_group(queue: "str") -> "str":
    return f"queue-{hashlib.sha256(queue.encode()).hexdigest()[:32]}"


def _definitive_client_error(exc: "BaseException") -> "bool":
    response = getattr(exc, "response", None)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode") if isinstance(response, dict) else None
    return isinstance(status, int) and 400 <= status < 500 and status not in {408, 429}  # noqa: PLR2004


def _new_attempt(retry_count: "int") -> "str":
    return f"sqs:{retry_count}:{int(time.time() * 1000)}:{uuid4()}"


def _parse_attempt(value: "object") -> "tuple[int, int] | None":
    if not isinstance(value, str):
        return None
    parts = value.split(":")
    if len(parts) != 4 or parts[0] != "sqs":  # noqa: PLR2004
        return None
    try:
        retry_count = int(parts[1])
        timestamp = int(parts[2])
        UUID(parts[3])
    except (ValueError, TypeError):
        return None
    return (retry_count, timestamp) if retry_count >= 0 and timestamp >= 0 else None


def _record_metric(
    service: "QueueService", record: "QueuedTaskRecord", operation: "str", outcome: "str"
) -> "None":
    label = "queue.repair.outcome" if operation == "repair" else "queue.execution.status"
    service.observability_runtime.record_counter(
        f"litestar_queues.execution.{operation}", attributes={**_queue_metric_attributes(record), label: outcome}
    )


def _consumer_task_done(
    running: "set[asyncio.Task[None]]", completed: "asyncio.Task[None]", logger: "Any"
) -> "None":
    running.discard(completed)
    if completed.cancelled():
        return
    error = completed.exception()
    if error is not None:
        logger.warning("SQS delivery processing failed", exc_info=(type(error), error, error.__traceback__))


def _record_delivery_metric(service: "QueueService", outcome: "str") -> "None":
    service.observability_runtime.record_counter(
        "litestar_queues.execution.delivery",
        attributes={"queue.execution.backend": "sqs", "queue.delivery.outcome": outcome},
    )

import asyncio
import time
from contextlib import suppress
from importlib import import_module
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from litestar_queues.consumer import TaskExitCode, consume_one
from litestar_queues.exceptions import MissingDependencyError, QueueConfigurationError, QueueDispatchError
from litestar_queues.execution.base import BaseConsumerExecutionBackend, DispatchRepairResult
from litestar_queues.execution.rabbitmq.config import RabbitMQExecutionConfig, _execution_config_from_queue_config

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.service import QueueService

__all__ = ("RabbitMQExecutionBackend",)

ATTEMPT_HEADER = "litestar_queues_attempt"


class RabbitMQExecutionBackend(BaseConsumerExecutionBackend):
    """Dispatch bare task identifiers through a RabbitMQ quorum queue."""

    __slots__ = ("_connection", "_consumer_channel", "_execution_config", "_publisher_channel", "_queue")

    def __init__(
        self,
        config: "QueueConfig | None" = None,
        *,
        execution_config: "RabbitMQExecutionConfig | None" = None,
        connection: "Any | None" = None,
    ) -> "None":
        super().__init__(config=config)
        self._execution_config = execution_config
        self._connection = connection
        self._publisher_channel: "Any | None" = None
        self._consumer_channel: "Any | None" = None
        self._queue: "Any | None" = None

    @property
    def is_external(self) -> "bool":
        return True

    @property
    def execution_config(self) -> "RabbitMQExecutionConfig":
        if self._execution_config is None or self._execution_config.queue_name is None:
            self._execution_config = _execution_config_from_queue_config(self.config)
        return self._execution_config

    async def dispatch(self, service: "QueueService", record: "QueuedTaskRecord") -> "str | None":
        attempt_ref = _new_attempt(record.retry_count)
        queue_backend = service.get_queue_backend()
        reserved = await queue_backend.reserve_external_dispatch(
            record.id,
            "rabbitmq",
            attempt_ref,
            execution_profile=record.execution_profile,
            expected_retry_count=record.retry_count,
        )
        if reserved is None:
            return None
        await self._send(service, reserved, attempt_ref)
        return attempt_ref

    async def _send(self, service: "QueueService", record: "QueuedTaskRecord", attempt_ref: "str") -> "None":
        aio_pika = _aio_pika()
        channel = await self._publisher()
        message = aio_pika.Message(
            str(record.id).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=str(record.id),
            priority=max(0, min(31, record.priority)),
            headers={ATTEMPT_HEADER: attempt_ref},
        )
        try:
            confirmed = await asyncio.wait_for(
                channel.default_exchange.publish(message, routing_key=self.execution_config.queue_name, mandatory=True),
                timeout=self.execution_config.api_timeout,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _is_definitive_publish_error(exc, aio_pika):
                with suppress(Exception):
                    await service.get_queue_backend().clear_execution_ref(record.id, record.retry_count, attempt_ref)
                raise
            msg = "RabbitMQ dispatch outcome is unknown"
            raise QueueDispatchError(msg, task_id=record.id, committed=True) from exc
        if confirmed is False:
            with suppress(Exception):
                await service.get_queue_backend().clear_execution_ref(record.id, record.retry_count, attempt_ref)
            msg = "RabbitMQ rejected the published routing slip."
            raise _NegativePublishError(msg)

    async def repair(self, service: "QueueService", *, limit: "int") -> "DispatchRepairResult":
        if limit <= 0:
            return DispatchRepairResult()
        records = await service.get_queue_backend().list_running_external(limit=limit)
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
                self._logger.warning("RabbitMQ delivery repair failed", exc_info=True)
            else:
                changed += 1
        return DispatchRepairResult(examined=len(records), changed=changed)

    async def execute(
        self, service: "QueueService", record: "QueuedTaskRecord", *, worker_id: "str | None" = None
    ) -> "QueuedTaskRecord":
        del worker_id
        await self.dispatch(service, record)
        return await service.get_queue_backend().get_task(record.id) or record

    async def _get_connection(self) -> "Any":
        if self._connection is None:
            aio_pika = _aio_pika()
            self._connection = await aio_pika.connect_robust(
                self.execution_config.amqp_url, timeout=self.execution_config.api_timeout
            )
        _validate_server_version(self._connection)
        return self._connection

    async def _publisher(self) -> "Any":
        if self._publisher_channel is None or getattr(self._publisher_channel, "is_closed", False):
            connection = await self._get_connection()
            self._publisher_channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
            await self._declare_queue(self._publisher_channel)
        return self._publisher_channel

    async def _consumer(self, max_concurrency: "int") -> "tuple[Any, Any]":
        connection = await self._get_connection()
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=max_concurrency)
        queue = await self._declare_queue(channel)
        self._consumer_channel = channel
        self._queue = queue
        return channel, queue

    async def _declare_queue(self, channel: "Any") -> "Any":
        arguments: "dict[str, object]" = {"x-queue-type": "quorum"}
        config = self.execution_config
        if config.delayed_retry_type != "disabled":
            arguments.update({
                "x-delayed-retry-type": config.delayed_retry_type,
                "x-delayed-retry-min": config.delayed_retry_min_ms,
                "x-delayed-retry-max": config.delayed_retry_max_ms,
            })
        if config.consumer_timeout_ms is not None:
            arguments["x-consumer-timeout"] = config.consumer_timeout_ms
        return await channel.declare_queue(
            config.queue_name,
            durable=True,
            exclusive=False,
            auto_delete=False,
            passive=not config.declare_queue,
            arguments=None if not config.declare_queue else arguments,
        )

    async def run_consumer(self, service: "QueueService", *, max_concurrency: "int", drain_timeout: "float") -> "None":
        _channel, queue = await self._consumer(max_concurrency)
        running: "set[asyncio.Task[None]]" = set()
        try:
            async with queue.iterator() as iterator:
                async for message in iterator:
                    task = asyncio.create_task(self._consume_message(service, message))
                    running.add(task)
                    task.add_done_callback(running.discard)
        except asyncio.CancelledError:
            if running:
                done, pending = await asyncio.wait(running, timeout=drain_timeout)
                for task in pending:
                    task.cancel()
                await asyncio.gather(*done, *pending, return_exceptions=True)
            raise

    async def _consume_message(self, service: "QueueService", message: "Any") -> "None":
        attempt = (message.headers or {}).get(ATTEMPT_HEADER)
        parsed = _parse_attempt(attempt)
        try:
            task_id = UUID(message.body.decode())
        except (ValueError, UnicodeDecodeError, AttributeError):
            task_id = None
        if task_id is None or not isinstance(attempt, str) or parsed is None:
            await message.ack()
            return
        retry_count, _timestamp = parsed
        try:
            current = await service.get_task(task_id)
            if current is None or current.is_terminal:
                await message.ack()
                return
            outcome = await consume_one(
                service, task_id, expected_retry_count=retry_count, expected_execution_ref=attempt
            )
            if outcome == TaskExitCode.FAILURE:
                latest = await service.get_task(task_id)
                if latest is not None and not latest.is_terminal:
                    await service.get_queue_backend().clear_execution_ref(task_id, latest.retry_count, attempt)
            await message.ack()
        except asyncio.CancelledError:
            with suppress(Exception):
                await message.nack(requeue=True)
            raise
        except Exception:  # noqa: BLE001 -- infrastructure failures must be returned to the broker
            with suppress(Exception):
                await message.nack(requeue=True)

    async def close(self) -> "None":
        for channel_name in ("_consumer_channel", "_publisher_channel"):
            channel = getattr(self, channel_name)
            close = getattr(channel, "close", None)
            if channel is not None and close is not None and not getattr(channel, "is_closed", False):
                await close()
            setattr(self, channel_name, None)
        if self._connection is not None and not getattr(self._connection, "is_closed", False):
            await self._connection.close()
        self._connection = None
        self._queue = None


class _NegativePublishError(Exception):
    pass


def _aio_pika() -> "Any":
    try:
        return import_module("aio_pika")
    except ImportError as exc:
        package = "aio-pika"
        extra = "rabbitmq"
        raise MissingDependencyError(package, extra) from exc


def _is_definitive_publish_error(exc: "BaseException", aio_pika: "Any") -> "bool":
    exceptions = getattr(aio_pika, "exceptions", None)
    definitive = tuple(
        error
        for name in ("DeliveryError", "PublishError", "NackError")
        if isinstance((error := getattr(exceptions, name, None)), type)
    )
    return bool(definitive) and isinstance(exc, definitive)


def _validate_server_version(connection: "Any") -> "None":
    properties = getattr(connection, "server_properties", None)
    version = properties.get("version") if isinstance(properties, dict) else None
    if isinstance(version, bytes):
        version = version.decode(errors="replace")
    if not isinstance(version, str):
        return
    try:
        parts = tuple(int(value) for value in version.split(".")[:2])
    except ValueError:
        return
    if parts < (4, 3):
        msg = f"RabbitMQ 4.3 or newer is required; server reported {version}."
        raise QueueConfigurationError(msg)


def _new_attempt(retry_count: "int") -> "str":
    return f"rabbitmq:{retry_count}:{int(time.time() * 1000)}:{uuid4()}"


def _parse_attempt(value: "object") -> "tuple[int, int] | None":
    if not isinstance(value, str):
        return None
    parts = value.split(":")
    if len(parts) != 4 or parts[0] != "rabbitmq":  # noqa: PLR2004
        return None
    try:
        retry_count = int(parts[1])
        timestamp = int(parts[2])
        UUID(parts[3])
    except (ValueError, TypeError):
        return None
    return (retry_count, timestamp) if retry_count >= 0 and timestamp >= 0 else None

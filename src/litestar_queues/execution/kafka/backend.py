import asyncio
import time
from contextlib import suppress
from importlib import import_module
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID, uuid4

from litestar_queues.consumer import TaskExitCode, consume_one
from litestar_queues.exceptions import MissingDependencyError, QueueDispatchError
from litestar_queues.execution.base import BaseConsumerExecutionBackend, DispatchRepairResult
from litestar_queues.execution.kafka.config import KafkaExecutionConfig, _execution_config_from_queue_config

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from litestar_queues.config import QueueConfig
    from litestar_queues.execution.kafka._typing import KafkaConsumer, KafkaProducer
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.service import QueueService

__all__ = ("KafkaExecutionBackend",)

ATTEMPT_HEADER = "litestar_queues_attempt"


class KafkaExecutionBackend(BaseConsumerExecutionBackend):
    """Dispatch bare task identifiers through a Kafka consumer group."""

    __slots__ = (
        "_consumer",
        "_drain_timeout",
        "_execution_config",
        "_inflight",
        "_listener",
        "_owns_consumer",
        "_owns_producer",
        "_producer",
    )

    def __init__(
        self,
        config: "QueueConfig | None" = None,
        *,
        execution_config: "KafkaExecutionConfig | None" = None,
        producer: "KafkaProducer | None" = None,
        consumer: "KafkaConsumer | None" = None,
    ) -> "None":
        super().__init__(config=config)
        self._execution_config = execution_config
        self._producer = producer
        self._consumer = consumer
        self._owns_producer = False
        self._owns_consumer = False
        self._inflight: "dict[Any, asyncio.Task[None]]" = {}
        self._listener: "Any | None" = None
        self._drain_timeout = 0.0

    @property
    def is_external(self) -> "bool":
        return True

    @property
    def execution_config(self) -> "KafkaExecutionConfig":
        if self._execution_config is None:
            self._execution_config = _execution_config_from_queue_config(self.config)
        return self._execution_config

    async def dispatch(self, service: "QueueService", record: "QueuedTaskRecord") -> "str | None":
        attempt_ref = _new_attempt(record.retry_count)
        reserved = await service.get_queue_backend().reserve_external_dispatch(
            record.id,
            "kafka",
            attempt_ref,
            execution_profile=record.execution_profile,
            expected_retry_count=record.retry_count,
        )
        if reserved is None:
            return None
        await self._send(service, reserved, attempt_ref)
        return attempt_ref

    async def _send(self, service: "QueueService", record: "QueuedTaskRecord", attempt_ref: "str") -> "None":
        producer = await self._get_producer()
        try:
            await asyncio.wait_for(
                producer.send_and_wait(
                    self.execution_config.topic,
                    str(record.id).encode(),
                    headers=[(ATTEMPT_HEADER, attempt_ref.encode())],
                ),
                timeout=self.execution_config.api_timeout,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _is_definitive_publish_error(exc):
                with suppress(Exception):
                    await service.get_queue_backend().clear_execution_ref(record.id, record.retry_count, attempt_ref)
                raise
            msg = "Kafka dispatch outcome is unknown"
            raise QueueDispatchError(msg, task_id=record.id, committed=True) from exc

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
                self._logger.warning("Kafka delivery repair failed", exc_info=True)
            else:
                changed += 1
        return DispatchRepairResult(examined=len(records), changed=changed)

    async def execute(
        self, service: "QueueService", record: "QueuedTaskRecord", *, worker_id: "str | None" = None
    ) -> "QueuedTaskRecord":
        del worker_id
        await self.dispatch(service, record)
        return await service.get_queue_backend().get_task(record.id) or record

    async def _get_producer(self) -> "KafkaProducer":
        if self._producer is None:
            aiokafka = _aiokafka()
            producer = cast(
                "KafkaProducer",
                aiokafka.AIOKafkaProducer(
                    bootstrap_servers=self.execution_config.bootstrap_servers,
                    acks="all",
                    **self.execution_config.producer_options,
                ),
            )
            try:
                await producer.start()
            except BaseException:
                with suppress(Exception):
                    await producer.stop()
                raise
            self._producer = producer
            self._owns_producer = True
        return self._producer

    async def _get_consumer(self) -> "KafkaConsumer":
        if self._consumer is None:
            aiokafka = _aiokafka()
            self._consumer = cast(
                "KafkaConsumer",
                aiokafka.AIOKafkaConsumer(
                    bootstrap_servers=self.execution_config.bootstrap_servers,
                    group_id=self.execution_config.consumer_group,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                    **self.execution_config.consumer_options,
                ),
            )
        return self._consumer

    async def run_consumer(self, service: "QueueService", *, max_concurrency: "int", drain_timeout: "float") -> "None":
        consumer = await self._get_consumer()
        self._drain_timeout = drain_timeout

        async def drain_revoked(revoked: "set[Any]") -> "None":
            await self._drain_inflight(self._drain_timeout, revoked)

        self._listener = _create_rebalance_listener(drain_revoked)
        consumer.subscribe([self.execution_config.topic], listener=self._listener)
        try:
            await consumer.start()
        except BaseException:
            with suppress(Exception):
                await consumer.stop()
            raise
        self._owns_consumer = True
        try:
            while True:
                batches = await consumer.getmany(timeout_ms=1_000, max_records=max_concurrency)
                tasks = [
                    self._start_partition(service, consumer, partition, messages)
                    for partition, messages in batches.items()
                ]
                if tasks:
                    results = await asyncio.shield(asyncio.gather(*tasks, return_exceptions=True))
                    for result in results:
                        if isinstance(result, asyncio.CancelledError):
                            continue
                        if isinstance(result, BaseException):
                            self._logger.error(
                                "Kafka partition consumer failed", exc_info=(type(result), result, result.__traceback__)
                            )
                            raise result
        except asyncio.CancelledError:
            await self._drain_inflight(drain_timeout)
            raise
        finally:
            if self._owns_consumer:
                await consumer.stop()

    def _start_partition(
        self, service: "QueueService", consumer: "KafkaConsumer", partition: "Any", messages: "list[Any]"
    ) -> "asyncio.Task[None]":
        task = asyncio.create_task(self._consume_partition(service, consumer, partition, messages))
        self._inflight[partition] = task

        def discard(completed: "asyncio.Task[None]") -> "None":
            if self._inflight.get(partition) is completed:
                self._inflight.pop(partition, None)

        task.add_done_callback(discard)
        return task

    async def _drain_inflight(self, timeout: "float", partitions: "set[Any] | None" = None) -> "None":
        tasks = {task for partition, task in self._inflight.items() if partitions is None or partition in partitions}
        if not tasks:
            return
        done, pending = await asyncio.wait(tasks, timeout=timeout)
        for task in pending:
            task.cancel()
        await asyncio.gather(*done, *pending, return_exceptions=True)

    async def _consume_partition(
        self, service: "QueueService", consumer: "KafkaConsumer", partition: "Any", messages: "list[Any]"
    ) -> "None":
        """Process one partition in order and commit only its contiguous durable prefix."""
        for message in messages:
            assignment = getattr(consumer, "assignment", None)
            if assignment is not None and partition not in assignment():
                return
            if not await self._consume_message(service, message):
                return
            try:
                await consumer.commit({partition: message.offset + 1})
            except asyncio.CancelledError:
                raise
            except Exception:  # Rebalance failures leave the offset uncommitted.
                self._logger.warning("Kafka offset commit failed", exc_info=True)
                return

    async def _consume_message(self, service: "QueueService", message: "Any") -> "bool":
        attempt = dict(message.headers or ()).get(ATTEMPT_HEADER)
        if isinstance(attempt, bytes):
            with suppress(UnicodeDecodeError):
                attempt = attempt.decode()
        parsed = _parse_attempt(attempt)
        try:
            task_id = UUID(message.value.decode())
        except (ValueError, UnicodeDecodeError, AttributeError):
            task_id = None
        if task_id is None or not isinstance(attempt, str) or parsed is None:
            return True
        retry_count, _timestamp = parsed
        try:
            current = await service.get_task(task_id)
            if current is None or current.is_terminal:
                return True
            outcome = await consume_one(
                service, task_id, expected_retry_count=retry_count, expected_execution_ref=attempt
            )
            if outcome == TaskExitCode.FAILURE:
                latest = await service.get_task(task_id)
                if latest is None:
                    return False
                if not latest.is_terminal:
                    cleared = await service.get_queue_backend().clear_execution_ref(
                        task_id, latest.retry_count, attempt
                    )
                    if cleared is None:
                        return False
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 -- leave the offset uncommitted for broker redelivery
            return False
        else:
            return True

    async def close(self) -> "None":
        if self._owns_consumer and self._consumer is not None:
            await self._consumer.stop()
        if self._owns_producer and self._producer is not None:
            await self._producer.stop()
        self._consumer = None
        self._producer = None
        self._owns_consumer = False
        self._owns_producer = False
        self._inflight.clear()
        self._listener = None


def _create_rebalance_listener(on_revoked: "Callable[[set[Any]], Awaitable[None]]") -> "Any":
    aiokafka = _aiokafka()

    async def revoked(_self: "object", partitions: "set[Any]") -> "None":
        await on_revoked(partitions)

    async def assigned(_self: "object", partitions: "set[Any]") -> "None":
        del partitions

    listener_type = type(
        "KafkaRebalanceListener",
        (aiokafka.ConsumerRebalanceListener,),
        {"on_partitions_revoked": revoked, "on_partitions_assigned": assigned},
    )
    return listener_type()


def _aiokafka() -> "Any":
    try:
        return import_module("aiokafka")
    except ImportError as exc:
        package = "aiokafka"
        extra = "kafka"
        raise MissingDependencyError(package, extra) from exc


def _is_definitive_publish_error(exc: "BaseException") -> "bool":
    return exc.__class__.__name__ in {
        "InvalidTopicError",
        "MessageSizeTooLargeError",
        "TopicAuthorizationFailedError",
        "UnsupportedVersionError",
    }


def _new_attempt(retry_count: "int") -> "str":
    return f"kafka:{retry_count}:{int(time.time() * 1000)}:{uuid4()}"


def _parse_attempt(value: "object") -> "tuple[int, int] | None":
    if not isinstance(value, str):
        return None
    parts = value.split(":")
    if len(parts) != 4 or parts[0] != "kafka":  # noqa: PLR2004
        return None
    try:
        retry_count = int(parts[1])
        timestamp = int(parts[2])
        UUID(parts[3])
    except (ValueError, TypeError):
        return None
    return (retry_count, timestamp) if retry_count >= 0 and timestamp >= 0 else None

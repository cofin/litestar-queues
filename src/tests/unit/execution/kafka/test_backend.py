import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, NamedTuple, cast
from uuid import UUID

import pytest

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.consumer import TaskExitCode
from litestar_queues.exceptions import QueueDispatchError
from litestar_queues.execution.kafka import KafkaExecutionBackend, KafkaExecutionConfig

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

pytestmark = pytest.mark.anyio


class TopicPartition(NamedTuple):
    topic: "str"
    partition: "int"


class FakeProducer:
    def __init__(self, error: "Exception | None" = None) -> "None":
        self.sent: "list[tuple[str, bytes, list[tuple[str, bytes]]]]" = []
        self.error = error

    async def start(self) -> "None":
        return None

    async def stop(self) -> "None":
        return None

    async def send_and_wait(self, topic: "str", value: "bytes", *, headers: "Sequence[tuple[str, bytes]]") -> "object":
        self.sent.append((topic, value, list(headers)))
        if self.error is not None:
            raise self.error
        return object()


class FakeConsumer:
    def __init__(self, messages: "list[Any]") -> "None":
        self.messages = messages
        self.commits: "list[dict[Any, int]]" = []
        self.started = False
        self.stopped = False
        self.committed = asyncio.Event()
        self.listener: "Any | None" = None
        self.assigned: "set[Any]" = set()
        self.commit_error: "Exception | None" = None
        self.fetched = asyncio.Event()

    def subscribe(self, topics: "Iterable[str]", listener: "Any") -> "None":
        assert list(topics) == ["litestar-queues"]
        self.listener = listener

    def assignment(self) -> "set[Any]":
        return self.assigned

    async def start(self) -> "None":
        self.started = True

    async def stop(self) -> "None":
        self.stopped = True

    async def getmany(self, *, timeout_ms: "int", max_records: "int") -> "dict[Any, list[Any]]":
        del timeout_ms, max_records
        if self.messages:
            message = self.messages.pop(0)
            self.assigned.add(message.topic_partition)
            self.fetched.set()
            return {message.topic_partition: [message]}
        await asyncio.sleep(10)
        return {}

    async def commit(self, offsets: "Mapping[Any, int]") -> "None":
        if self.commit_error is not None:
            raise self.commit_error
        self.commits.append(dict(offsets))
        self.committed.set()


async def test_dispatch_publishes_uuid_and_attempt_header() -> "None":
    @task("tasks.kafka-secret")
    async def secret_task(secret: "str") -> "None":
        del secret

    producer = FakeProducer()
    execution_config = KafkaExecutionConfig(bootstrap_servers="localhost:9092", topic="tasks")
    config = QueueConfig(
        namespace="tests",
        queue_backend="memory",
        execution_backend=execution_config,
        worker=WorkerConfig(placement="external"),
    )
    queue_backend = InMemoryQueueBackend()
    backend = KafkaExecutionBackend(config, execution_config=execution_config, producer=producer)
    async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
        result = await service.enqueue(secret_task.using(execution_backend="kafka"), "never-on-the-wire")
        record = await queue_backend.get_task(result.id)
        assert record is not None
        attempt_ref = await backend.dispatch(service, record)

    topic, value, headers = producer.sent[0]
    assert attempt_ref is not None
    assert topic == "tasks"
    assert value == str(result.id).encode()
    assert dict(headers)["litestar_queues_attempt"] == attempt_ref.encode()


@pytest.mark.parametrize(
    "outcome", [TaskExitCode.SUCCESS, TaskExitCode.CANCELLED, TaskExitCode.CLAIM_LOST, TaskExitCode.UNKNOWN_TASK]
)
async def test_durable_consume_one_outcomes_are_committable(
    monkeypatch: "pytest.MonkeyPatch", outcome: "TaskExitCode"
) -> "None":
    task_id = UUID(int=10)
    attempt = f"kafka:2:1:{UUID(int=11)}"
    current = SimpleNamespace(is_terminal=False)
    service = SimpleNamespace(get_task=lambda _task_id: current)

    async def get_task(_task_id: "UUID") -> "object":
        return current

    async def fake_consume(*_args: "object", **kwargs: "object") -> "TaskExitCode":
        assert kwargs == {"expected_retry_count": 2, "expected_execution_ref": attempt}
        return outcome

    service.get_task = get_task
    monkeypatch.setattr("litestar_queues.execution.kafka.backend.consume_one", fake_consume)
    backend = KafkaExecutionBackend(execution_config=KafkaExecutionConfig(bootstrap_servers="localhost:9092"))
    message = SimpleNamespace(value=str(task_id).encode(), headers=[("litestar_queues_attempt", attempt.encode())])

    assert await backend._consume_message(service, message) is True  # type: ignore[arg-type]


async def test_failure_retry_clears_exact_new_retry_fence(monkeypatch: "pytest.MonkeyPatch") -> "None":
    task_id = UUID(int=12)
    attempt = f"kafka:1:1:{UUID(int=13)}"
    records = iter([SimpleNamespace(is_terminal=False), SimpleNamespace(is_terminal=False, retry_count=2)])
    cleared: "list[tuple[UUID, int, str]]" = []

    async def get_task(_task_id: "UUID") -> "object":
        return next(records)

    async def clear(task: "UUID", retry: "int", reference: "str") -> "object":
        cleared.append((task, retry, reference))
        return object()

    async def failed(*_args: "object", **_kwargs: "object") -> "TaskExitCode":
        return TaskExitCode.FAILURE

    service = SimpleNamespace(get_task=get_task, get_queue_backend=lambda: SimpleNamespace(clear_execution_ref=clear))
    monkeypatch.setattr("litestar_queues.execution.kafka.backend.consume_one", failed)
    backend = KafkaExecutionBackend(execution_config=KafkaExecutionConfig(bootstrap_servers="localhost:9092"))
    message = SimpleNamespace(value=str(task_id).encode(), headers=[("litestar_queues_attempt", attempt.encode())])

    assert await backend._consume_message(service, message) is True  # type: ignore[arg-type]
    assert cleared == [(task_id, 2, attempt)]


async def test_failure_retry_clear_lost_remains_uncommitted(monkeypatch: "pytest.MonkeyPatch") -> "None":
    task_id = UUID(int=17)
    attempt = f"kafka:1:1:{UUID(int=18)}"
    records = iter([SimpleNamespace(is_terminal=False), SimpleNamespace(is_terminal=False, retry_count=2)])

    async def get_task(_task_id: "UUID") -> "object":
        return next(records)

    async def clear(*_args: "object") -> "None":
        return None

    async def failed(*_args: "object", **_kwargs: "object") -> "TaskExitCode":
        return TaskExitCode.FAILURE

    service = SimpleNamespace(get_task=get_task, get_queue_backend=lambda: SimpleNamespace(clear_execution_ref=clear))
    monkeypatch.setattr("litestar_queues.execution.kafka.backend.consume_one", failed)
    backend = KafkaExecutionBackend(execution_config=KafkaExecutionConfig(bootstrap_servers="localhost:9092"))
    message = SimpleNamespace(value=str(task_id).encode(), headers=[("litestar_queues_attempt", attempt.encode())])

    assert await backend._consume_message(service, message) is False  # type: ignore[arg-type]


async def test_missing_terminal_poison_and_transient_outcomes() -> "None":
    backend = KafkaExecutionBackend(execution_config=KafkaExecutionConfig(bootstrap_servers="localhost:9092"))
    attempt = f"kafka:0:1:{UUID(int=14)}"
    valid = SimpleNamespace(value=str(UUID(int=15)).encode(), headers=[("litestar_queues_attempt", attempt.encode())])
    poison = SimpleNamespace(value=b"bad", headers=[])

    async def missing(_task_id: "UUID") -> "None":
        return None

    async def terminal(_task_id: "UUID") -> "object":
        return SimpleNamespace(is_terminal=True)

    async def transient(_task_id: "UUID") -> "object":
        raise RuntimeError

    assert await backend._consume_message(SimpleNamespace(get_task=missing), valid) is True  # type: ignore[arg-type]
    assert await backend._consume_message(SimpleNamespace(get_task=terminal), valid) is True  # type: ignore[arg-type]
    assert await backend._consume_message(SimpleNamespace(get_task=transient), valid) is False  # type: ignore[arg-type]
    assert await backend._consume_message(object(), poison) is True  # type: ignore[arg-type]


async def test_actual_consume_one_unknown_and_stale_attempts_are_committable() -> "None":
    execution_config = KafkaExecutionConfig(bootstrap_servers="localhost:9092")
    config = QueueConfig(
        queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
    )
    queue_backend = InMemoryQueueBackend()
    backend = KafkaExecutionBackend(config, execution_config=execution_config)
    async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
        unknown = await queue_backend.enqueue("tasks.kafka-unknown", execution_backend="kafka")
        unknown_attempt = f"kafka:0:1:{UUID(int=19)}"
        await queue_backend.reserve_external_dispatch(
            unknown.id, "kafka", unknown_attempt, expected_retry_count=unknown.retry_count
        )
        unknown_message = SimpleNamespace(
            value=str(unknown.id).encode(), headers=[("litestar_queues_attempt", unknown_attempt.encode())]
        )
        assert await backend._consume_message(service, unknown_message) is True
        retired = await queue_backend.get_task(unknown.id)
        assert retired is not None and retired.status == "failed"

        stale = await queue_backend.enqueue("tasks.kafka-stale", execution_backend="kafka")
        live_attempt = f"kafka:0:1:{UUID(int=20)}"
        stale_attempt = f"kafka:0:1:{UUID(int=21)}"
        await queue_backend.reserve_external_dispatch(
            stale.id, "kafka", live_attempt, expected_retry_count=stale.retry_count
        )
        stale_message = SimpleNamespace(
            value=str(stale.id).encode(), headers=[("litestar_queues_attempt", stale_attempt.encode())]
        )
        assert await backend._consume_message(service, stale_message) is True
        untouched = await queue_backend.get_task(stale.id)
        assert untouched is not None and untouched.execution_ref == live_attempt


async def test_definitive_publish_clears_reservation_and_ambiguous_publish_keeps_it() -> "None":
    class InvalidTopicError(Exception):
        pass

    @task("tasks.kafka-publish-errors")
    async def queued() -> "None":
        return None

    config = QueueConfig(
        queue_backend="memory",
        execution_backend=KafkaExecutionConfig(bootstrap_servers="localhost:9092"),
        worker=WorkerConfig(placement="external"),
    )
    for error, definitive in ((InvalidTopicError(), True), (ConnectionError(), False)):
        queue_backend = InMemoryQueueBackend()
        backend = KafkaExecutionBackend(config, producer=FakeProducer(error))
        async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
            result = await service.enqueue(queued.using(execution_backend="kafka"))
            record = await queue_backend.get_task(result.id)
            assert record is not None
            expected = InvalidTopicError if definitive else QueueDispatchError
            with pytest.raises(expected) as caught:
                await backend.dispatch(service, record)
            stored = await queue_backend.get_task(result.id)
            assert stored is not None
            assert (stored.execution_ref is None) is definitive
            if not definitive:
                assert isinstance(caught.value, QueueDispatchError)
                assert caught.value.committed is True


async def test_repair_rotates_stale_attempt_and_republishes() -> "None":
    @task("tasks.kafka-repair")
    async def queued() -> "None":
        return None

    execution_config = KafkaExecutionConfig(bootstrap_servers="localhost:9092", dispatch_stale_after=1)
    config = QueueConfig(
        queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
    )
    queue_backend = InMemoryQueueBackend()
    producer = FakeProducer()
    backend = KafkaExecutionBackend(config, execution_config=execution_config, producer=producer)
    async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
        result = await service.enqueue(queued.using(execution_backend="kafka"))
        record = await queue_backend.get_task(result.id)
        assert record is not None
        old_ref = f"kafka:0:0:{UUID(int=16)}"
        await queue_backend.reserve_external_dispatch(record.id, "kafka", old_ref, expected_retry_count=0)
        repaired = await backend.repair(service, limit=1)
        stored = await queue_backend.get_task(record.id)

    assert repaired.changed == 1
    assert stored is not None and stored.execution_ref != old_ref
    assert dict(producer.sent[0][2])["litestar_queues_attempt"].decode() == stored.execution_ref


async def test_partition_commits_only_contiguous_prefix_and_stops_after_commit_failure(
    monkeypatch: "pytest.MonkeyPatch",
) -> "None":
    partition = TopicPartition("tasks", 0)
    messages = [SimpleNamespace(offset=value) for value in range(3)]
    consumer = FakeConsumer([])
    consumer.assigned.add(partition)
    outcomes = iter([True, False, True])

    async def consume(_self: "object", *_args: "object") -> "bool":
        return next(outcomes)

    backend = KafkaExecutionBackend(execution_config=KafkaExecutionConfig(bootstrap_servers="localhost:9092"))
    monkeypatch.setattr(KafkaExecutionBackend, "_consume_message", consume)
    await backend._consume_partition(cast("Any", object()), consumer, partition, messages)
    assert consumer.commits == [{partition: 1}]

    consumer.commits.clear()
    consumer.commit_error = RuntimeError()
    await backend._consume_partition(cast("Any", object()), consumer, partition, messages[:1])
    assert consumer.commits == []


async def test_multiple_partitions_commit_each_ordered_offset(monkeypatch: "pytest.MonkeyPatch") -> "None":
    first = TopicPartition("tasks", 0)
    second = TopicPartition("tasks", 1)
    consumer = FakeConsumer([])
    consumer.assigned.update({first, second})

    async def consume(_self: "object", *_args: "object") -> "bool":
        return True

    monkeypatch.setattr(KafkaExecutionBackend, "_consume_message", consume)
    backend = KafkaExecutionBackend(execution_config=KafkaExecutionConfig(bootstrap_servers="localhost:9092"))
    await asyncio.gather(
        backend._consume_partition(
            cast("Any", object()), consumer, first, [SimpleNamespace(offset=4), SimpleNamespace(offset=5)]
        ),
        backend._consume_partition(cast("Any", object()), consumer, second, [SimpleNamespace(offset=8)]),
    )

    assert [commit[first] for commit in consumer.commits if first in commit] == [5, 6]
    assert [commit[second] for commit in consumer.commits if second in commit] == [9]


async def test_consumer_commits_next_offset_only_after_durable_outcome(monkeypatch: "pytest.MonkeyPatch") -> "None":
    task_id = UUID(int=1)
    attempt = f"kafka:0:1:{UUID(int=2)}"
    partition = TopicPartition(topic="tasks", partition=0)
    message = SimpleNamespace(
        value=str(task_id).encode(),
        headers=[("litestar_queues_attempt", attempt.encode())],
        offset=4,
        topic_partition=partition,
    )
    consumer = FakeConsumer([message])
    backend = KafkaExecutionBackend(
        execution_config=KafkaExecutionConfig(bootstrap_servers="localhost:9092"), consumer=consumer
    )
    service = SimpleNamespace(get_task=lambda _task_id: None)

    async def get_task(_task_id: "UUID") -> "object":
        return SimpleNamespace(is_terminal=True)

    service.get_task = get_task
    consumer_task = asyncio.create_task(
        backend.run_consumer(service, max_concurrency=1, drain_timeout=0)  # type: ignore[arg-type]
    )
    await asyncio.wait_for(consumer.committed.wait(), timeout=0.5)
    consumer_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consumer_task

    assert consumer.commits == [{partition: 5}]


async def test_cancelled_delivery_is_not_committed(monkeypatch: "pytest.MonkeyPatch") -> "None":
    task_id = UUID(int=1)
    attempt = f"kafka:0:1:{UUID(int=2)}"
    partition = TopicPartition(topic="tasks", partition=0)
    message = SimpleNamespace(
        value=str(task_id).encode(),
        headers=[("litestar_queues_attempt", attempt.encode())],
        offset=4,
        topic_partition=partition,
    )
    consumer = FakeConsumer([message])
    backend = KafkaExecutionBackend(
        execution_config=KafkaExecutionConfig(bootstrap_servers="localhost:9092"), consumer=consumer
    )

    async def get_task(_task_id: "UUID") -> "object":
        raise asyncio.CancelledError

    service = SimpleNamespace(get_task=get_task)
    consumer_task = asyncio.create_task(
        backend.run_consumer(service, max_concurrency=1, drain_timeout=0)  # type: ignore[arg-type]
    )
    await asyncio.sleep(0)
    consumer_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consumer_task

    assert consumer.commits == []


async def test_shutdown_drains_inflight_partition_before_committing(monkeypatch: "pytest.MonkeyPatch") -> "None":
    partition = TopicPartition("tasks", 0)
    message = SimpleNamespace(offset=0, topic_partition=partition)
    consumer = FakeConsumer([message])
    entered = asyncio.Event()
    release = asyncio.Event()

    async def consume(_self: "object", *_args: "object") -> "bool":
        entered.set()
        await release.wait()
        return True

    monkeypatch.setattr(KafkaExecutionBackend, "_consume_message", consume)
    backend = KafkaExecutionBackend(
        execution_config=KafkaExecutionConfig(bootstrap_servers="localhost:9092"), consumer=consumer
    )
    runner = asyncio.create_task(backend.run_consumer(object(), max_concurrency=1, drain_timeout=1))  # type: ignore[arg-type]
    await entered.wait()
    runner.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await runner

    assert consumer.commits == [{partition: 1}]


async def test_shutdown_timeout_cancels_unfinished_partition_without_commit(
    monkeypatch: "pytest.MonkeyPatch",
) -> "None":
    partition = TopicPartition("tasks", 0)
    consumer = FakeConsumer([SimpleNamespace(offset=0, topic_partition=partition)])
    entered = asyncio.Event()

    async def consume(_self: "object", *_args: "object") -> "bool":
        entered.set()
        await asyncio.Event().wait()
        return True

    monkeypatch.setattr(KafkaExecutionBackend, "_consume_message", consume)
    backend = KafkaExecutionBackend(
        execution_config=KafkaExecutionConfig(bootstrap_servers="localhost:9092"), consumer=consumer
    )
    runner = asyncio.create_task(backend.run_consumer(object(), max_concurrency=1, drain_timeout=0))  # type: ignore[arg-type]
    await entered.wait()
    runner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await runner

    assert consumer.commits == []


async def test_rebalance_waits_for_inflight_partition_prefix(monkeypatch: "pytest.MonkeyPatch") -> "None":
    partition = TopicPartition("tasks", 0)
    message = SimpleNamespace(offset=0, topic_partition=partition)
    consumer = FakeConsumer([message])
    entered = asyncio.Event()
    release = asyncio.Event()

    async def consume(_self: "object", *_args: "object") -> "bool":
        entered.set()
        await release.wait()
        return True

    monkeypatch.setattr(KafkaExecutionBackend, "_consume_message", consume)
    backend = KafkaExecutionBackend(
        execution_config=KafkaExecutionConfig(bootstrap_servers="localhost:9092"), consumer=consumer
    )
    runner = asyncio.create_task(backend.run_consumer(object(), max_concurrency=1, drain_timeout=1))  # type: ignore[arg-type]
    await entered.wait()
    assert consumer.listener is not None
    revoked = asyncio.create_task(consumer.listener.on_partitions_revoked({partition}))
    await asyncio.sleep(0)
    assert not revoked.done()
    release.set()
    await revoked
    assert consumer.commits == [{partition: 1}]
    runner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await runner


async def test_rebalance_timeout_keeps_consumer_running_for_new_assignment(monkeypatch: "pytest.MonkeyPatch") -> "None":
    revoked_partition = TopicPartition("tasks", 0)
    retained_partition = TopicPartition("tasks", 1)
    consumer = FakeConsumer([
        SimpleNamespace(offset=0, topic_partition=revoked_partition),
        SimpleNamespace(offset=4, topic_partition=retained_partition),
    ])
    entered = asyncio.Event()

    async def consume(_self: "object", _service: "object", message: "object") -> "bool":
        if message.topic_partition == revoked_partition:
            entered.set()
            await asyncio.Event().wait()
        return True

    monkeypatch.setattr(KafkaExecutionBackend, "_consume_message", consume)
    backend = KafkaExecutionBackend(
        execution_config=KafkaExecutionConfig(bootstrap_servers="localhost:9092"), consumer=consumer
    )
    runner = asyncio.create_task(backend.run_consumer(object(), max_concurrency=1, drain_timeout=0))  # type: ignore[arg-type]
    await entered.wait()
    assert consumer.listener is not None
    await consumer.listener.on_partitions_revoked({revoked_partition})
    await asyncio.wait_for(consumer.committed.wait(), timeout=0.5)

    assert not runner.done()
    assert consumer.commits == [{retained_partition: 5}]
    runner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await runner


async def test_unexpected_partition_failure_is_reported_without_commit(
    monkeypatch: "pytest.MonkeyPatch", caplog: "pytest.LogCaptureFixture"
) -> "None":
    partition = TopicPartition("tasks", 0)
    consumer = FakeConsumer([SimpleNamespace(offset=0, topic_partition=partition)])
    partition_error = RuntimeError("partition consumer failed")

    async def fail(*_args: "object") -> "None":
        raise partition_error

    monkeypatch.setattr(KafkaExecutionBackend, "_consume_partition", fail)
    backend = KafkaExecutionBackend(
        execution_config=KafkaExecutionConfig(bootstrap_servers="localhost:9092"), consumer=consumer
    )

    with caplog.at_level("ERROR"), pytest.raises(RuntimeError, match="partition consumer failed"):
        await backend.run_consumer(object(), max_concurrency=1, drain_timeout=0)  # type: ignore[arg-type]

    assert "Kafka partition consumer failed" in caplog.text
    assert consumer.commits == []

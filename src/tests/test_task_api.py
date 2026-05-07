from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from litestar_queues import (
    QueueConfig,
    QueuedTaskRecord,
    QueueService,
    ScheduleConfig,
    Task,
    TaskExecutionContext,
    get_scheduled_tasks,
    get_task_registry,
    task,
)
from litestar_queues.events import NoopQueueEventSink, QueueEventPublisher
from litestar_queues.task import clear_task_registry


def _build_test_context(record: QueuedTaskRecord) -> TaskExecutionContext:
    return TaskExecutionContext(
        task_id=str(record.id),
        task_name=record.task_name,
        queue=record.queue,
        worker_id=None,
        execution_backend=record.execution_backend,
        execution_profile=record.execution_profile,
        attempt=record.retry_count + 1,
        event_publisher=QueueEventPublisher(NoopQueueEventSink()),
    )

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def clean_task_registry() -> None:
    clear_task_registry()


async def test_task_decorator_registers_and_calls_async_and_sync_functions() -> None:
    @task("math.add", queue="critical", priority=10, retries=2, timeout=5)
    async def add(left: int, right: int) -> int:
        return left + right

    @task
    def uppercase(value: str) -> str:
        return value.upper()

    registry = get_task_registry()

    assert registry["math.add"] is add
    assert registry["uppercase"] is uppercase
    assert isinstance(add, Task)
    assert add.name == "math.add"
    assert add.queue == "critical"
    assert add.priority == 10
    assert add.retries == 2
    assert add.timeout == 5
    assert await add(2, 3) == 5
    assert await uppercase("queue") == "QUEUE"


async def test_task_execute_record_merges_extra_kwargs_into_call() -> None:
    @task("inject.consume")
    async def consume(**kwargs: Any) -> dict[str, Any]:
        return dict(kwargs)

    record = QueuedTaskRecord(task_name="inject.consume", kwargs={"existing": "from_record"})
    context = _build_test_context(record)

    result = await consume.execute_record(
        record,
        task_context=context,
        extra_kwargs={"injected_service": "resolved", "another": 42},
    )

    assert result["existing"] == "from_record"
    assert result["injected_service"] == "resolved"
    assert result["another"] == 42


async def test_task_execute_record_extra_kwargs_cannot_override_sentinels() -> None:
    @task("inject.sentinels")
    async def sentinels(**kwargs: Any) -> dict[str, Any]:
        return dict(kwargs)

    record = QueuedTaskRecord(task_name="inject.sentinels")
    context = _build_test_context(record)

    result = await sentinels.execute_record(
        record,
        task_context=context,
        extra_kwargs={"_job_id": "hijacked", "_task_context": "hijacked"},
    )

    assert result["_job_id"] == record.id
    assert result["_task_context"] is context


async def test_task_using_returns_configured_copy_without_mutating_original() -> None:
    @task("email.send", priority=1, retries=1)
    async def send_email(address: str) -> str:
        return address

    overridden = send_email.using(priority=20, key="email:1", queue="high")

    assert overridden is not send_email
    assert overridden.name == send_email.name
    assert send_email.priority == 1
    assert send_email.queue == "default"
    assert overridden.priority == 20
    assert overridden.queue == "high"
    assert overridden.key == "email:1"
    assert await overridden("user@example.com") == "user@example.com"


async def test_task_enqueue_uses_immediate_memory_service_by_default() -> None:
    @task("tasks.double")
    async def double(value: int) -> int:
        return value * 2

    result = await double.enqueue(21)

    assert result.task_name == "tasks.double"
    assert result.status == "completed"
    assert result.result == 42
    assert result.error is None


async def test_queue_service_enqueue_by_name_executes_immediately_and_refreshes_result() -> None:
    @task("tasks.greet")
    async def greet(name: str) -> dict[str, str]:
        return {"message": f"hello {name}"}

    async with QueueService(QueueConfig(execution_backend="immediate")) as service:
        result = await service.enqueue("tasks.greet", "Ada")
        await result.refresh()

    assert result.status == "completed"
    assert result.result == {"message": "hello Ada"}


async def test_schedule_config_supports_interval_and_basic_cron_next_run() -> None:
    interval = ScheduleConfig(task_name="tasks.interval", interval=timedelta(minutes=5))
    base = datetime(2026, 5, 3, 12, 0, tzinfo=UTC)

    cron = ScheduleConfig(task_name="tasks.daily", cron="30 2 * * *")

    assert interval.get_next_run(base) == datetime(2026, 5, 3, 12, 5, tzinfo=UTC)
    assert cron.get_next_run(base) == datetime(2026, 5, 4, 2, 30, tzinfo=UTC)


async def test_task_decorator_registers_interval_schedule() -> None:
    @task("tasks.scheduled", interval=60)
    async def scheduled() -> str:
        return "ok"

    schedules = get_scheduled_tasks()

    assert "tasks.scheduled" in schedules
    assert schedules["tasks.scheduled"].interval == timedelta(seconds=60)

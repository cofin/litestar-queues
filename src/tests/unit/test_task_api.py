import asyncio
from datetime import datetime, timedelta, timezone
from importlib import import_module
from typing import Any, cast
from uuid import uuid4

import pytest

from litestar_queues import (
    QueueConfig,
    QueuedTaskRecord,
    QueueService,
    ScheduleConfig,
    Task,
    TaskExecutionContext,
    TaskResult,
    get_scheduled_tasks,
    get_task_registry,
    task,
)
from litestar_queues.events import NoopQueueEventSink, QueueEventPublisher
from litestar_queues.task import clear_task_registry

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def clean_task_registry() -> "None":
    clear_task_registry()


async def test_task_decorator_registers_and_calls_async_and_sync_functions() -> "None":
    @task("math.add", queue="critical", priority=10, retries=2, timeout=5)
    async def add(left: "int", right: "int") -> "int":
        return left + right

    @task
    def uppercase(value: "str") -> "str":
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


async def test_task_execute_record_merges_extra_kwargs_into_call() -> "None":
    @task("inject.consume")
    async def consume(**kwargs: "object") -> "dict[str, object]":
        return dict(kwargs)

    record = QueuedTaskRecord(task_name="inject.consume", kwargs={"existing": "from_record"})
    context = _build_test_context(record)

    result = await consume.execute_record(
        record, task_context=context, extra_kwargs={"injected_service": "resolved", "another": 42}
    )

    assert result["existing"] == "from_record"
    assert result["injected_service"] == "resolved"
    assert result["another"] == 42


async def test_task_execute_record_extra_kwargs_cannot_override_sentinels() -> "None":
    @task("inject.sentinels")
    async def sentinels(**kwargs: "object") -> "dict[str, object]":
        return dict(kwargs)

    record = QueuedTaskRecord(task_name="inject.sentinels")
    context = _build_test_context(record)

    result = await sentinels.execute_record(
        record, task_context=context, extra_kwargs={"_job_id": "hijacked", "_task_context": "hijacked"}
    )

    assert result["_job_id"] == str(record.id)
    assert result["_task_context"] is context


async def test_task_using_returns_configured_copy_without_mutating_original() -> "None":
    @task("email.send", priority=1, retries=1, requeue_on_stale=False)
    async def send_email(address: "str") -> "str":
        return address

    overridden = send_email.using(priority=20, key="email:1", queue="high", requeue_on_stale=True)

    assert overridden is not send_email
    assert overridden.name == send_email.name
    assert send_email.priority == 1
    assert send_email.queue == "default"
    assert send_email.requeue_on_stale is False
    assert overridden.priority == 20
    assert overridden.queue == "high"
    assert overridden.key == "email:1"
    assert overridden.requeue_on_stale is True
    assert send_email.metadata()["requeue_on_stale"] is False
    assert overridden.metadata()["requeue_on_stale"] is True
    assert await overridden("user@example.com") == "user@example.com"


async def test_task_enqueue_uses_immediate_memory_service_by_default() -> "None":
    @task("tasks.double")
    async def double(value: "int") -> "int":
        return value * 2

    result = await double.enqueue(21)

    assert result.task_name == "tasks.double"
    assert result.status == "completed"
    assert result.result == 42
    assert result.error is None


async def test_queue_service_enqueue_by_name_executes_immediately_and_refreshes_result() -> "None":
    @task("tasks.greet")
    async def greet(name: "str") -> "dict[str, str]":
        return {"message": f"hello {name}"}

    async with QueueService(QueueConfig(execution_backend="immediate")) as service:
        result = await service.enqueue("tasks.greet", "Ada")
        await result.refresh()

    assert result.status == "completed"
    assert result.result == {"message": "hello Ada"}


async def test_task_result_wait_raises_when_record_disappears() -> "None":
    result = TaskResult(uuid4(), "tasks.missing", service=cast("QueueService", _MissingTaskService()))

    with pytest.raises(RuntimeError, match="no longer exists"):
        await asyncio.wait_for(result.wait(poll_interval=0), timeout=0.05)


async def test_schedule_config_supports_interval_and_basic_cron_next_run() -> "None":
    interval = ScheduleConfig(task_name="tasks.interval", interval=timedelta(minutes=5))
    base = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)

    cron = ScheduleConfig(task_name="tasks.daily", cron="30 2 * * *")

    assert interval.get_next_run(base) == datetime(2026, 5, 3, 12, 5, tzinfo=timezone.utc)
    assert cron.get_next_run(base) == datetime(2026, 5, 4, 2, 30, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("cron", "expected"),
    [
        ("@hourly", datetime(2026, 5, 3, 13, 0, tzinfo=timezone.utc)),
        ("@daily", datetime(2026, 5, 4, 0, 0, tzinfo=timezone.utc)),
        ("@midnight", datetime(2026, 5, 4, 0, 0, tzinfo=timezone.utc)),
        ("@weekly", datetime(2026, 5, 10, 0, 0, tzinfo=timezone.utc)),
        ("@monthly", datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc)),
        ("@yearly", datetime(2027, 1, 1, 0, 0, tzinfo=timezone.utc)),
        ("@annually", datetime(2027, 1, 1, 0, 0, tzinfo=timezone.utc)),
    ],
)
async def test_schedule_config_supports_cron_aliases(cron: "str", expected: "datetime") -> "None":
    schedule = ScheduleConfig(task_name=f"tasks.{cron.removeprefix('@')}", cron=cron)

    assert schedule.get_next_run(datetime(2026, 5, 3, 12, 34, tzinfo=timezone.utc)) == expected


async def test_schedule_config_supports_cron_names_ranges_lists_steps_and_sunday_aliases() -> "None":
    business_hours = ScheduleConfig(task_name="tasks.business_hours", cron="*/15 9-17 * JAN,MAR MON-FRI")
    sunday_zero = ScheduleConfig(task_name="tasks.sunday_zero", cron="0 0 * * 0")
    sunday_seven = ScheduleConfig(task_name="tasks.sunday_seven", cron="0 0 * * 7")

    assert business_hours.get_next_run(datetime(2026, 1, 1, 8, 59, tzinfo=timezone.utc)) == datetime(
        2026, 1, 1, 9, 0, tzinfo=timezone.utc
    )
    assert sunday_zero.get_next_run(datetime(2026, 5, 2, 23, 59, tzinfo=timezone.utc)) == datetime(
        2026, 5, 3, 0, 0, tzinfo=timezone.utc
    )
    assert sunday_seven.get_next_run(datetime(2026, 5, 2, 23, 59, tzinfo=timezone.utc)) == datetime(
        2026, 5, 3, 0, 0, tzinfo=timezone.utc
    )


async def test_schedule_config_supports_question_mark_and_posix_day_matching() -> "None":
    monday_only = ScheduleConfig(task_name="tasks.monday_only", cron="0 0 ? * MON")
    first_of_month_or_monday = ScheduleConfig(task_name="tasks.dom_or_dow", cron="0 0 1 * MON")

    assert monday_only.get_next_run(datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)) == datetime(
        2026, 5, 4, 0, 0, tzinfo=timezone.utc
    )
    assert first_of_month_or_monday.get_next_run(datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)) == datetime(
        2026, 5, 4, 0, 0, tzinfo=timezone.utc
    )


async def test_schedule_config_searches_far_enough_for_leap_day_cron() -> "None":
    schedule = ScheduleConfig(task_name="tasks.leap_day", cron="0 0 29 2 *")

    assert schedule.get_next_run(datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)) == datetime(
        2028, 2, 29, 0, 0, tzinfo=timezone.utc
    )


async def test_schedule_config_skips_nonexistent_local_cron_times() -> "None":
    schedule = ScheduleConfig(task_name="tasks.dst_gap", cron="30 2 * * *", timezone="America/New_York")

    assert schedule.get_next_run(datetime(2026, 3, 7, 12, 0, tzinfo=timezone.utc)) == datetime(
        2026, 3, 9, 6, 30, tzinfo=timezone.utc
    )


async def test_schedule_config_returns_cron_runs_in_utc_from_configured_timezone() -> "None":
    schedule = ScheduleConfig(task_name="tasks.local_time", cron="0 3 * * *", timezone="America/New_York")

    assert schedule.get_next_run(datetime(2026, 1, 1, 7, 59, tzinfo=timezone.utc)) == datetime(
        2026, 1, 1, 8, 0, tzinfo=timezone.utc
    )


async def test_schedule_config_applies_deterministic_jitter_to_cron(monkeypatch: "pytest.MonkeyPatch") -> "None":
    class FixedRandom:
        def uniform(self, _lower: "float", _upper: "float") -> "float":
            return 2.5

    task_module = import_module("litestar_queues.task")
    monkeypatch.setattr(task_module, "_RANDOM", FixedRandom())
    schedule = ScheduleConfig(task_name="tasks.jittered_cron", cron="0 0 * * *", jitter=10)

    assert schedule.get_next_run(datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)) == datetime(
        2026, 5, 4, 0, 0, 2, 500000, tzinfo=timezone.utc
    )


@pytest.mark.parametrize("cron", ["0 0 1,,2 * *", "0 0 * * FRI-MON", "*/0 * * * *", "0 0 ? * ?"])
async def test_schedule_config_rejects_unsupported_or_invalid_cron_syntax(cron: "str") -> "None":
    with pytest.raises(ValueError, match="Invalid cron expression"):
        ScheduleConfig(task_name="tasks.invalid_cron", cron=cron)


@pytest.mark.parametrize("cron", ["0 0 L * *", "0 0 15W * *", "0 0 * * MON#2", "0 0 * * * 2027", "@reboot"])
async def test_schedule_config_rejects_unsupported_cron_syntax_with_documented_reason(cron: "str") -> "None":
    with pytest.raises(ValueError, match="Unsupported cron syntax"):
        ScheduleConfig(task_name="tasks.unsupported_cron", cron=cron)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"interval": 0},
        {"interval": timedelta(seconds=0)},
        {"interval": -1},
        {"interval": 60, "initial_delay": -1},
        {"interval": 60, "jitter": -1},
        {"cron": "0 0 * * *", "timezone": "Not/AZone"},
    ],
)
async def test_schedule_config_rejects_invalid_schedule_values(kwargs: "dict[str, Any]") -> "None":
    with pytest.raises(ValueError):
        ScheduleConfig(task_name="tasks.invalid_values", **kwargs)


async def test_task_decorator_registers_interval_schedule() -> "None":
    @task("tasks.scheduled", interval=60)
    async def scheduled() -> "str":
        return "ok"

    schedules = get_scheduled_tasks()

    assert "tasks.scheduled" in schedules
    assert schedules["tasks.scheduled"].interval == timedelta(seconds=60)


class _MissingTaskService:
    async def get_task(self, task_id: "object") -> "None":
        del task_id
        return None


def _build_test_context(record: "QueuedTaskRecord") -> "TaskExecutionContext":
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

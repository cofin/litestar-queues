from datetime import UTC, datetime, timedelta
from importlib import import_module

import pytest

from litestar_queues import QueueConfig, QueueService, task
from litestar_queues.task import clear_task_registry, get_scheduled_tasks

pytestmark = pytest.mark.anyio


def _use_upper_jitter(_lower: float, upper: float) -> float:
    return upper


async def test_downstream_style_schedules_preserve_task_metadata_and_jitter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_module = import_module("litestar_queues.task")
    monkeypatch.setattr(task_module.random, "uniform", _use_upper_jitter)

    @task(
        "jobs.weekly_report",
        cron="0 0 * * MON",
        timezone="America/New_York",
        description="Generate a weekly workspace report.",
        log_level="info",
    )
    async def weekly_report() -> dict[str, bool]:
        return {"ok": True}

    @task(
        "jobs.distributed_task",
        interval=300,
        initial_delay=60,
        jitter=30,
        max_instances=1,
        execution_backend="cloudrun",
        execution_profile="worker-heavy",
        description="Fan out external synchronization work.",
        log_level="debug",
        quiet_success=True,
    )
    async def distributed_task() -> dict[str, bool]:
        return {"ok": True}

    before = datetime.now(UTC)
    async with QueueService(QueueConfig(execution_backend="local")) as service:
        records = await service.initialize_schedules()
    after = datetime.now(UTC)

    by_task_name = {record.task_name: record for record in records}
    weekly_record = by_task_name["jobs.weekly_report"]
    distributed_record = by_task_name["jobs.distributed_task"]

    assert weekly_record.metadata["description"] == "Generate a weekly workspace report."
    assert weekly_record.metadata["log_level"] == "info"
    assert weekly_record.metadata["schedule"]["cron"] == "0 0 * * MON"
    assert weekly_record.metadata["schedule"]["timezone"] == "America/New_York"

    assert distributed_record.execution_backend == "cloudrun"
    assert distributed_record.execution_profile == "worker-heavy"
    assert distributed_record.metadata["description"] == "Fan out external synchronization work."
    assert distributed_record.metadata["log_level"] == "debug"
    assert distributed_record.metadata["quiet_success"] is True
    assert distributed_record.metadata["schedule"]["interval"] == pytest.approx(300.0)
    assert distributed_record.metadata["schedule"]["initial_delay"] == pytest.approx(60.0)
    assert distributed_record.metadata["schedule"]["jitter"] == pytest.approx(30.0)
    assert distributed_record.metadata["schedule"]["max_instances"] == 1
    assert distributed_record.scheduled_at is not None
    assert before + timedelta(seconds=90) <= distributed_record.scheduled_at <= after + timedelta(seconds=91)


async def test_initialize_schedules_replaces_changed_schedule_definition() -> None:
    @task("jobs.changed_schedule", interval=60)
    async def changed_schedule() -> str:
        return "old"

    async with QueueService(QueueConfig(execution_backend="local")) as service:
        old_record = await service.get_queue_backend().enqueue(
            "jobs.changed_schedule",
            key="scheduled:jobs.changed_schedule",
            scheduled_at=datetime.now(UTC) + timedelta(seconds=60),
            metadata={"schedule": get_scheduled_tasks()["jobs.changed_schedule"].as_metadata()},
        )

        clear_task_registry()

        @task("jobs.changed_schedule", interval=300)
        async def changed_schedule_new() -> str:
            return "new"

        records = await service.initialize_schedules()
        old_record_after_reinitialize = await service.get_task(old_record.id)
        active_record = await service.get_queue_backend().get_task_by_key("scheduled:jobs.changed_schedule")

    assert len(records) == 1
    assert old_record_after_reinitialize is not None
    assert old_record_after_reinitialize.status == "cancelled"
    assert active_record is records[0]
    assert active_record is not None
    assert active_record.id != old_record.id
    assert active_record.status == "scheduled"
    assert active_record.metadata["schedule"]["interval"] == pytest.approx(300.0)

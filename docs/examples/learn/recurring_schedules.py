"""Defining interval and cron recurring schedules with @task."""

from datetime import UTC, datetime, timedelta

from litestar_queues import QueueConfig, QueueService, ScheduleConfig, WorkerConfig, task

MIN_REGISTERED_SCHEDULES = 2


@task("maintenance.prune_audit_logs", queue="maintenance", cron="0 2 * * *", timezone="UTC")
async def prune_audit_logs() -> int:
    """Prune expired audit log entries at 02:00 UTC daily."""
    return 128


@task("metrics.collect_queue_depth", queue="metrics", interval=timedelta(minutes=5), jitter=timedelta(seconds=15))
async def collect_queue_depth() -> str:
    """Sample queue depth metrics every five minutes with bounded jitter."""
    return "sampled"


async def main() -> None:
    """Verify schedule calculation and startup schedule registration."""
    schedule = ScheduleConfig(task_name="maintenance.prune_audit_logs", cron="0 2 * * *", timezone="UTC")
    reference = datetime(2026, 11, 3, 1, 30, tzinfo=UTC)
    next_run = schedule.get_next_run(after=reference)
    assert next_run == datetime(2026, 11, 3, 2, 0, tzinfo=UTC)

    config = QueueConfig(
        queue_backend="memory",
        execution_backend="local",
        worker=WorkerConfig(placement="external"),
        initialize_schedules=True,
    )
    async with QueueService(config=config) as service:
        records = await service.initialize_schedules()
        assert len(records) >= MIN_REGISTERED_SCHEDULES

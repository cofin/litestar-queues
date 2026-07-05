from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from litestar_queues.events import QueueEvent, SQLiteQueueEventSink

pytestmark = pytest.mark.anyio


async def test_sqlite_event_sink_persists_logs_progress_summaries_and_retention(tmp_path: "Path") -> None:
    sink = SQLiteQueueEventSink(tmp_path / "queue-events.db", buffer_size=2, flush_interval=60)
    await sink.open()
    try:
        first = QueueEvent(
            type="task.log",
            scope="task",
            task_id="job-1",
            task_name="tasks.import",
            queue="default",
            sequence=1,
            level="info",
            message="loaded page",
            payload={"stage": "load", "duration_ms": 12, "page": 1},
            occurred_at=datetime.now(timezone.utc) - timedelta(seconds=2),
        )
        second = QueueEvent(
            type="task.progress",
            scope="task",
            task_id="job-1",
            task_name="tasks.import",
            queue="default",
            sequence=2,
            message="stored rows",
            progress_current=10,
            progress_total=20,
            progress_percent=50,
            payload={"stage": "store", "duration_ms": 25},
        )

        await sink.publish(first, channels=("task:job-1",))
        await sink.publish(second, channels=("task:job-1",))

        records = await sink.list_events(task_id="job-1")
        summaries = await sink.summarize_stages(task_name="tasks.import")
        deleted = await sink.cleanup_before(datetime.now(timezone.utc) + timedelta(seconds=1))

        assert [record.event_id for record in records] == [first.id, second.id]
        assert records[0].job_id == "job-1"
        assert records[0].stage == "load"
        assert records[0].detail["page"] == 1
        assert [(summary.stage, summary.event_count, summary.total_duration_ms) for summary in summaries] == [
            ("load", 1, 12),
            ("store", 1, 25),
        ]
        assert deleted == 2
        assert await sink.list_events(task_id="job-1") == []
    finally:
        await sink.close()

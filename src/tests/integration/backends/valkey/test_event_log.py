"""Valkey backend-managed queue event history tests."""

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest

pytest.importorskip("valkey")

from litestar_queues import EventLogConfig
from litestar_queues.backends.redis.event_log import RedisQueueEventLog
from litestar_queues.events import QueueEvent

if TYPE_CHECKING:
    from litestar_queues.backends.valkey import ValkeyQueueBackend

pytestmark = pytest.mark.anyio


async def test_valkey_event_log_reuses_redis_protocol_implementation(valkey_backend: "ValkeyQueueBackend") -> "None":
    event_log_config = EventLogConfig(buffer_size=10, flush_interval=60)
    event_log = valkey_backend.get_event_log(event_log_config)
    assert isinstance(event_log, RedisQueueEventLog)

    await event_log.publish_event(
        QueueEvent(
            id="valkey-event-1",
            type="task.event",
            scope="task",
            task_id="task-valkey-1",
            task_name="tasks.valkey.history",
            sequence=1,
            payload={"stage": "load", "duration_ms": 7},
            occurred_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
        )
    )
    await event_log.flush_events()

    records = await event_log.list_events(task_name="tasks.valkey.history")

    assert [record.event_id for record in records] == ["valkey-event-1"]
    assert records[0].detail == {"stage": "load", "duration_ms": 7}

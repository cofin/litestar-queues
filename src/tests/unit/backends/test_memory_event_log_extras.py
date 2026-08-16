import pytest

from litestar_queues.backends.memory.event_log import InMemoryQueueEventLog
from litestar_queues.events.query import QueueEventQuery
from litestar_queues.events import EventHistoryConfig, EventHistoryExtraColumn, QueueEvent
from litestar_queues.exceptions import QueueConfigurationError

pytestmark = pytest.mark.anyio


async def test_memory_filters_declared_extra() -> "None":
    config = EventHistoryConfig(extra_columns=(EventHistoryExtraColumn(name="tenant", source="tenant_id"),))
    log = InMemoryQueueEventLog(config)
    await log.publish_event(QueueEvent(type="task.started", scope="task", task_id="a", payload={"tenant_id": "acme"}))
    await log.publish_event(QueueEvent(type="task.started", scope="task", task_id="b", payload={"tenant_id": "other"}))

    events = (await log.query_events(QueueEventQuery(), extra={"tenant": "acme"})).items
    assert [r.task_id for r in events] == ["a"]
    with pytest.raises(QueueConfigurationError):
        (await log.query_events(QueueEventQuery(), extra={"project": "x"})).items

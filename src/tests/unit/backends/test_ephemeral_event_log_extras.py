import pytest

from litestar_queues.backends.ephemeral import EphemeralQueueBackend
from litestar_queues.backends.ephemeral.event_log import EphemeralQueueEventLog
from litestar_queues.backends.ephemeral.server import EphemeralServerContext
from litestar_queues.events import EventHistoryConfig, EventHistoryExtraColumn, QueueEvent
from litestar_queues.exceptions import QueueConfigurationError

pytestmark = pytest.mark.anyio


@pytest.fixture
def server_context() -> "EphemeralServerContext":
    with EphemeralServerContext(nonce="test-nonce") as context:
        yield context


async def test_ephemeral_filters_declared_extra(server_context: "EphemeralServerContext") -> "None":
    config = EventHistoryConfig(extra_columns=(EventHistoryExtraColumn(name="tenant", source="tenant_id"),))
    backend = EphemeralQueueBackend()
    await backend.open()
    try:
        log = EphemeralQueueEventLog(config, backend=backend)
        await log.publish_event(
            QueueEvent(type="task.started", scope="task", task_id="a", payload={"tenant_id": "acme"})
        )
        await log.publish_event(
            QueueEvent(type="task.started", scope="task", task_id="b", payload={"tenant_id": "other"})
        )

        events = await log.list_events(extra={"tenant": "acme"})
        assert [r.task_id for r in events] == ["a"]
        with pytest.raises(QueueConfigurationError):
            await log.list_events(extra={"project": "x"})
    finally:
        await backend.close()

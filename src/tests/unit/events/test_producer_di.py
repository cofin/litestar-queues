import pytest
from litestar import post
from litestar.di import NamedDependency
from litestar.testing import create_test_client

from litestar_queues import EventDeliveryConfig, QueueConfig, QueuePlugin
from litestar_queues.events import InMemoryQueueEventSink, QueueChannels, QueueEventProducer, QueueEventsConfig

pytestmark = pytest.mark.anyio


def test_queue_events_injects_producer() -> None:
    sink = InMemoryQueueEventSink()

    @post("/events")
    async def publish(queue_events: NamedDependency[QueueEventProducer]) -> dict[str, str]:
        await queue_events.task("t1").log("note")
        await queue_events.channel("c").publish("x")
        return {"status": "ok"}

    with create_test_client(
        route_handlers=[publish],
        plugins=[QueuePlugin(QueueConfig(events=QueueEventsConfig(delivery=EventDeliveryConfig(sinks=(sink,)))))],
        openapi_config=None,
    ) as client:
        response = client.post("/events")

    assert response.status_code == 201
    assert sink.events_for(QueueChannels.task("t1"))[0].type == "task.log"
    assert sink.events_for(QueueChannels.custom("c"))[0].type == "x"


def test_signature_namespace_has_producer() -> None:
    assert QueueConfig().signature_namespace["QueueEventProducer"] is QueueEventProducer


def test_dependency_key_default() -> None:
    config = QueueConfig()

    assert config.events_dependency_key == "queue_events"
    assert "queue_events" in config.dependencies


async def test_provider_raises_clear_error_without_publisher_state() -> None:
    config = QueueConfig()
    state = _State()

    provider = config.provide_event_producer_dependency(state)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="Queue event publisher is not available"):
        await provider.__anext__()


class _State(dict[str, object]):
    pass

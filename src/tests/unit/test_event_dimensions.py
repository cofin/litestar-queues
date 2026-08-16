"""Package-owned event scoping dimensions."""

import pytest

from litestar_queues.events import QueueEvent, QueueEventActor, QueueEventEntityRef, event_entity_key
from litestar_queues.events._log_records import event_log_record_from_event


def test_event_entity_key() -> "None":
    assert event_entity_key(None) is None
    assert event_entity_key(QueueEventEntityRef(type="invoice", id="42")) == "invoice:42"


def test_record_carries_the_four_dimensions() -> "None":
    event = QueueEvent(
        type="task.log",
        scope="task",
        scope_key="acme",
        task_id="t-1",
        actor=QueueEventActor(type="user", id="u-1"),
        entity=QueueEventEntityRef(type="invoice", id="42"),
    )

    record = event_log_record_from_event(event)

    assert (record.scope, record.scope_key, record.entity) == ("task", "acme", "invoice:42")


pytestmark = pytest.mark.anyio


async def test_context_defaults_and_per_call_overrides() -> "None":
    from litestar_queues.events import (
        InMemoryQueueEventSink,
        QueueEventPublisher,
        TaskExecutionContext,
        bind_task_context,
        publish_task_log,
    )

    sink = InMemoryQueueEventSink()
    context = TaskExecutionContext(
        task_id="t-1",
        task_name="tasks.demo",
        queue="default",
        worker_id=None,
        execution_backend="local",
        execution_profile=None,
        attempt=1,
        event_publisher=QueueEventPublisher(sink),
        scope_key="acme",
        entity=QueueEventEntityRef(type="invoice", id="42"),
    )

    with bind_task_context(context):
        await context.log("default scope")
        await publish_task_log("override", actor=QueueEventActor(type="user", id="u-9"))

    first, second = sink.events
    assert first.entity is not None
    assert second.actor is not None
    assert (first.scope, first.scope_key, first.entity.id) == ("task", "acme", "42")
    assert second.scope_key == "acme"  # inherited
    assert second.actor.id == "u-9"  # per-call override

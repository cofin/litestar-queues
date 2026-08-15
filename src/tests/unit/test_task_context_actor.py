import pytest

from litestar_queues.events import (
    EventBufferConfig,
    InMemoryQueueEventSink,
    QueueEventActor,
    QueueEventPublisher,
    TaskExecutionContext,
    bind_task_context,
    publish_task_event,
    publish_task_log,
    publish_task_progress,
)

pytestmark = pytest.mark.anyio


def _make_context() -> "TaskExecutionContext":
    sink = InMemoryQueueEventSink()
    publisher = QueueEventPublisher(sink, buffer_config=EventBufferConfig())
    return TaskExecutionContext(
        task_id="t-1",
        task_name="test_task",
        queue="default",
        worker_id="w-1",
        execution_backend="memory",
        execution_profile=None,
        attempt=1,
        event_publisher=publisher,
    )


def test_context_actor_defaults_to_none() -> "None":
    context = _make_context()
    assert context.actor is None


def test_context_actor_is_assignable() -> "None":
    context = _make_context()
    context.actor = QueueEventActor(type="user", id="u1")
    assert context.actor.id == "u1"


async def test_publish_stamps_per_call_actor() -> "None":
    context = _make_context()
    event = await context.publish("custom", actor=QueueEventActor(type="user", id="u1"))
    assert event.actor is not None and event.actor.id == "u1"


async def test_publish_falls_back_to_context_actor() -> "None":
    context = _make_context()
    context.actor = QueueEventActor(type="service", id="s1")
    event = await context.publish("custom")
    assert event.actor is not None and event.actor.id == "s1"


async def test_per_call_actor_overrides_context() -> "None":
    context = _make_context()
    context.actor = QueueEventActor(type="service", id="s1")
    event = await context.publish("custom", actor=QueueEventActor(type="user", id="u1"))
    assert event.actor is not None and event.actor.id == "u1"


async def test_no_actor_anywhere_leaves_it_unset() -> "None":
    assert (await _make_context().publish("custom")).actor is None


@pytest.mark.parametrize("method_name", ["progress", "log", "event"])
async def test_convenience_methods_forward_actor(method_name: "str") -> "None":
    context = _make_context()
    context.actor = QueueEventActor(type="service", id="s1")
    method = getattr(context, method_name)
    if method_name == "progress":
        await method(current=1, total=2, actor=QueueEventActor(type="user", id="u1"))
    elif method_name == "log":
        await method("log msg", actor=QueueEventActor(type="user", id="u1"))
    elif method_name == "event":
        await method("my.event", actor=QueueEventActor(type="user", id="u1"))

    # When actor is omitted, inherits context actor
    if method_name == "progress":
        await method(current=2, total=2)
    elif method_name == "log":
        await method("second msg")
    elif method_name == "event":
        await method("second.event")


async def test_module_helpers_accept_actor() -> "None":
    context = _make_context()
    sink = context.event_publisher._sink  # type: ignore[attr-defined]
    with bind_task_context(context):
        await publish_task_progress(current=1, total=2, actor=QueueEventActor(type="user", id="u1"), immediate=True)
        await publish_task_log("hello", actor=QueueEventActor(type="user", id="u2"), immediate=True)
        await publish_task_event("custom.done", actor=QueueEventActor(type="user", id="u3"), immediate=True)

    assert [e.actor.id for e in sink.events if e.actor is not None] == ["u1", "u2", "u3"]

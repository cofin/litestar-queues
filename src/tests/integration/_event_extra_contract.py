from typing import TYPE_CHECKING, Any

import pytest

from litestar_queues.events import EventHistoryConfig, EventHistoryExtraColumn, QueueEvent, QueueEventActor
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from litestar_queues.backends import BaseQueueBackend


def _event(
    *, task_id: "str", actor: "QueueEventActor | None" = None, payload: "dict[str, Any] | None" = None
) -> "QueueEvent":
    return QueueEvent(
        type="task.log",
        scope="task",
        task_id=task_id,
        task_name="test.task",
        queue="default",
        worker_id="w-1",
        execution_backend="local",
        execution_profile=None,
        attempt=1,
        sequence=1,
        actor=actor,
        payload=dict(payload or {}),
    )


async def _bootstrap_if_sqlspec(queue_backend: "BaseQueueBackend", config: "EventHistoryConfig") -> "None":
    if hasattr(queue_backend, "_session") and hasattr(queue_backend, "_get_event_log_store"):
        store = queue_backend._get_event_log_store(extra_columns=config.extra_columns)
        async with queue_backend._session() as driver:
            for stmt in store.create_statements():
                await driver.execute_script(stmt)
            await driver.commit()


async def assert_event_extra_contract(queue_backend: "BaseQueueBackend") -> "None":
    """Every backend filters declared extras identically and rejects undeclared names."""
    config = EventHistoryConfig(
        batch_size=1, flush_interval=0.1, extra_columns=(EventHistoryExtraColumn(name="tenant", source="tenant_id"),)
    )
    await _bootstrap_if_sqlspec(queue_backend, config)
    log = queue_backend.get_event_log(config)
    assert log is not None
    await log.publish_event(_event(task_id="a", payload={"tenant_id": "acme"}))
    await log.publish_event(_event(task_id="b", payload={"tenant_id": "other"}))
    await log.flush_events()

    matched = await log.list_events(extra={"tenant": "acme"})
    assert [r.task_id for r in matched] == ["a"]
    assert matched[0].extra == {"tenant": "acme"}

    assert await log.list_events(extra={"tenant": "missing"}) == []

    with pytest.raises(QueueConfigurationError):
        await log.list_events(extra={"undeclared": "x"})


async def assert_event_actor_contract(queue_backend: "BaseQueueBackend") -> "None":
    """Attach, persist, filter: the actor round-trips on every backend."""
    config = EventHistoryConfig(batch_size=1, flush_interval=0.1)
    await _bootstrap_if_sqlspec(queue_backend, config)
    log = queue_backend.get_event_log(config)
    assert log is not None
    await log.publish_event(_event(task_id="a", actor=QueueEventActor(type="user", id="u1")))
    await log.publish_event(_event(task_id="b", actor=QueueEventActor(type="user", id="u2")))
    await log.flush_events()

    matched_id = await log.list_events(actor_id="u1")
    assert [r.task_id for r in matched_id] == ["a"]
    assert matched_id[0].actor_type == "user"

    matched_type = await log.list_events(actor_type="user")
    assert {r.task_id for r in matched_type} >= {"a", "b"}

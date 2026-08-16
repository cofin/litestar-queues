"""Fresh-process proof for the ephemeral SQLite queue backend.

Children are started with ``spawn`` and ``forkserver`` so nothing is inherited
except the environment. Only the database path and invocation nonce cross the
process boundary; every result is read back out of the shared database.
"""

import asyncio
import multiprocessing
import os
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, cast
from uuid import UUID

import pytest

from litestar_queues import QueueConfig
from litestar_queues.backends.ephemeral import NONCE_ENV_VAR, PATH_ENV_VAR, EphemeralQueueBackend
from litestar_queues.backends.ephemeral.server import EphemeralServerContext
from litestar_queues.events.query import QueueEventQuery
from litestar_queues.events import EventHistoryConfig, QueueEvent

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterator
    from multiprocessing.context import SpawnContext

pytestmark = pytest.mark.anyio

PRODUCER_ID = UUID("00000000-0000-4000-8000-000000000001")
SCHEDULED_ID = UUID("00000000-0000-4000-8000-000000000002")
RETRIED_ID = UUID("00000000-0000-4000-8000-000000000003")
RESERVED_ID = UUID("00000000-0000-4000-8000-000000000004")
LOSER_ID = UUID("00000000-0000-4000-8000-000000000005")

_START_METHODS = [method for method in ("spawn", "forkserver") if method in multiprocessing.get_all_start_methods()]


async def _produce(backend: "EphemeralQueueBackend") -> "None":
    await backend.enqueue("tasks.crossing", args=("payload",), id=PRODUCER_ID)
    await backend.enqueue(
        "tasks.scheduled", scheduled_at=datetime.now(timezone.utc) - timedelta(seconds=1), id=SCHEDULED_ID
    )
    await backend.enqueue("tasks.retried", max_retries=1, id=RETRIED_ID)
    await backend.reserve_identity("forever-key", task_id=RESERVED_ID, task_name="tasks.once")
    log = backend.get_event_log(EventHistoryConfig())
    assert log is not None
    await log.publish_event(
        QueueEvent(type="task.started", scope="task", task_id=str(PRODUCER_ID), task_name="tasks.crossing")
    )


async def _consume(backend: "EphemeralQueueBackend") -> "None":
    claimed = await backend.claim_many(limit=10)
    assert {record.id for record in claimed} == {PRODUCER_ID, SCHEDULED_ID, RETRIED_ID}
    await backend.complete_task(PRODUCER_ID, result={"rows": 2})
    await backend.complete_task(SCHEDULED_ID)
    retried = next(record for record in claimed if record.id == RETRIED_ID)
    await backend.fail_task(RETRIED_ID, "transient", expected_retry_count=retried.retry_count)
    assert await backend.reserve_identity("forever-key", task_id=LOSER_ID, task_name="tasks.once") is not None
    log = backend.get_event_log(EventHistoryConfig())
    assert log is not None
    await log.publish_event(
        QueueEvent(type="task.completed", scope="task", task_id=str(PRODUCER_ID), task_name="tasks.crossing")
    )


_STEPS: "dict[str, Callable[[EphemeralQueueBackend], Awaitable[None]]]" = {"produce": _produce, "consume": _consume}


async def _child_main(step: "str") -> "None":
    backend = EphemeralQueueBackend(QueueConfig(queue_backend="ephemeral"))
    await backend.open()
    try:
        await _STEPS[step](backend)
    finally:
        await backend.close()


def run_step(step: "str") -> "None":
    """Run one child step in a fresh process.

    A non-zero exit code is the only failure channel back to the parent.
    """
    asyncio.run(_child_main(step))


@pytest.fixture(autouse=True)
def _clean_environment() -> "Iterator[None]":
    for name in (PATH_ENV_VAR, NONCE_ENV_VAR):
        os.environ.pop(name, None)
    yield
    for name in (PATH_ENV_VAR, NONCE_ENV_VAR):
        os.environ.pop(name, None)


@pytest.fixture
def server_context() -> "Iterator[EphemeralServerContext]":
    with EphemeralServerContext(nonce="multiprocess-nonce") as context:
        yield context


def _context(method: "str") -> "SpawnContext":
    return cast("SpawnContext", multiprocessing.get_context(method))


def _run_in_child(method: "str", step: "str") -> "None":
    process = _context(method).Process(target=run_step, args=(step,))
    process.start()
    process.join(timeout=60)

    assert process.exitcode == 0, f"{step} child exited with {process.exitcode}"


@pytest.mark.parametrize("start_method", _START_METHODS)
async def test_a_fresh_producer_and_consumer_share_one_database(
    server_context: "EphemeralServerContext", start_method: "str"
) -> "None":
    del server_context
    _run_in_child(start_method, "produce")
    _run_in_child(start_method, "consume")

    backend = EphemeralQueueBackend(QueueConfig(queue_backend="ephemeral"))
    await backend.open()
    try:
        completed = await backend.get_task(PRODUCER_ID)
        scheduled = await backend.get_task(SCHEDULED_ID)
        retried = await backend.get_task(RETRIED_ID)
        reservation = await backend.has_identity("forever-key")
        log = backend.get_event_log(EventHistoryConfig())
        assert log is not None
        events = (await log.query_events(QueueEventQuery(task_id=str(PRODUCER_ID)))).items
    finally:
        await backend.close()

    assert completed is not None
    assert completed.status == "completed"
    assert completed.result == {"rows": 2}
    assert completed.args == ("payload",)
    assert scheduled is not None
    assert scheduled.status == "completed"
    assert retried is not None
    assert retried.status == "pending"
    assert retried.retry_count == 1
    assert reservation is not None
    assert reservation.task_id == RESERVED_ID
    assert [event.event_type for event in events] == ["task.started", "task.completed"]


@pytest.mark.parametrize("start_method", _START_METHODS)
async def test_a_child_without_the_environment_cannot_open_the_database(start_method: "str") -> "None":
    process = _context(start_method).Process(target=run_step, args=("produce",))
    process.start()
    process.join(timeout=60)

    assert process.exitcode not in (0, None)


def test_the_database_is_removed_after_every_child_has_exited() -> "None":
    method = _START_METHODS[0]
    with EphemeralServerContext(nonce="cleanup-nonce") as context:
        path = context.path
        _run_in_child(method, "produce")
        assert path.exists()

    assert not path.exists()
    assert not path.with_name(f"{path.name}-wal").exists()
    assert not path.with_name(f"{path.name}-shm").exists()
    assert not path.parent.exists()


def test_the_ephemeral_backend_overrides_the_whole_memory_contract() -> "None":
    """A method memory implements locally must get an explicit ephemeral decision."""
    from litestar_queues.backends.memory import InMemoryQueueBackend

    memory = {name for name in vars(InMemoryQueueBackend) if not name.startswith("_")}
    ephemeral = {name for name in vars(EphemeralQueueBackend) if not name.startswith("_")}

    assert sorted(memory - ephemeral) == []


def test_only_composed_base_methods_are_inherited_unchanged() -> "None":
    """Both backends inherit only base methods composed from primitives they do override."""
    from litestar_queues.backends.base import BaseQueueBackend
    from litestar_queues.backends.memory import InMemoryQueueBackend

    base = {name for name in vars(BaseQueueBackend) if not name.startswith("_")}
    memory = {name for name in vars(InMemoryQueueBackend) if not name.startswith("_")}
    ephemeral = {name for name in vars(EphemeralQueueBackend) if not name.startswith("_")}

    assert sorted(base - memory - ephemeral) == [
        "acquire_worker_lock",
        "claim_next",
        "config",
        "notify_new_tasks",
        "wait_for_completion",
    ]

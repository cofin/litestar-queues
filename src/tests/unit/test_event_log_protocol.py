import inspect
from collections.abc import AsyncIterator

import pytest

from litestar_queues import QueueConfig
from litestar_queues.backends.ephemeral import EphemeralQueueBackend
from litestar_queues.backends.ephemeral.event_log import EphemeralQueueEventLog
from litestar_queues.backends.ephemeral.server import EphemeralServerContext
from litestar_queues.backends.memory.event_log import InMemoryQueueEventLog
from litestar_queues.events import EventHistoryConfig, QueueEvent
from litestar_queues.events.history import QueueEventLog
from litestar_queues.exceptions import QueueConfigurationError


def test_protocol_declares_extra() -> "None":
    signature = inspect.signature(QueueEventLog.query_events)
    assert "extra" in signature.parameters
    assert signature.parameters["extra"].default is None


def test_protocol_declares_explicit_commit_release_and_close() -> "None":
    signature = inspect.signature(QueueEventLog.publish_event_after_commit)
    assert signature.parameters["release"].kind is inspect.Parameter.KEYWORD_ONLY
    assert signature.parameters["release"].default is inspect.Parameter.empty
    assert signature.parameters["barrier"].default is False
    assert inspect.iscoroutinefunction(QueueEventLog.aclose)


@pytest.fixture(params=["memory", "ephemeral"])
async def immediate_logs(
    request: pytest.FixtureRequest,
) -> AsyncIterator[
    tuple[InMemoryQueueEventLog | EphemeralQueueEventLog, InMemoryQueueEventLog | EphemeralQueueEventLog]
]:
    config = EventHistoryConfig()
    if request.param == "memory":
        log = InMemoryQueueEventLog(config)
        yield log, log
        return
    with EphemeralServerContext(nonce="history-commit-test"):
        writer = EphemeralQueueBackend(QueueConfig(queue_backend="ephemeral"))
        reader = EphemeralQueueBackend(QueueConfig(queue_backend="ephemeral"))
        await writer.open()
        await reader.open()
        try:
            yield EphemeralQueueEventLog(config, backend=writer), EphemeralQueueEventLog(config, backend=reader)
        finally:
            await reader.close()
            await writer.close()


@pytest.mark.anyio
async def test_immediate_history_is_visible_before_release(immediate_logs: tuple) -> None:
    log, reader = immediate_logs
    event = QueueEvent(type="task.log", scope="task", payload={"nested": {"value": [1]}})
    released = []

    async def release() -> None:
        page = await reader.query_events()
        assert [record.event_id for record in page.items] == [event.id]
        released.append(event.id)

    await log.publish_event_after_commit(event, release=release, barrier=True)
    event.payload["nested"]["value"].append(2)
    await log.flush_events()
    await log.aclose()
    await log.aclose()
    assert released == [event.id]
    assert (await reader.query_events()).items[0].detail == {"nested": {"value": [1]}}


@pytest.mark.anyio
async def test_immediate_copy_failure_never_releases_or_accepts(immediate_logs: tuple) -> None:
    class Uncopyable:
        def __deepcopy__(self, memo: dict) -> None:
            msg = "cannot snapshot"
            raise ValueError(msg)

    log, reader = immediate_logs
    released = []

    async def release() -> None:
        released.append(True)

    with pytest.raises(ValueError, match="cannot snapshot"):
        await log.publish_event_after_commit(
            QueueEvent(type="task.log", scope="task", payload={"value": Uncopyable()}), release=release
        )
    assert not released
    assert (await reader.query_events()).total == 0


@pytest.mark.anyio
async def test_immediate_live_failure_does_not_repeat_history(immediate_logs: tuple) -> None:
    log, reader = immediate_logs

    async def release() -> None:
        msg = "live sink failed"
        raise RuntimeError(msg)

    with pytest.raises(RuntimeError, match="live sink failed"):
        await log.publish_event_after_commit(QueueEvent(type="task.log", scope="task"), release=release)
    await log.flush_events()
    await log.aclose()
    assert (await reader.query_events()).total == 1


@pytest.mark.parametrize("interval", [float("nan"), float("inf"), float("-inf")])
def test_history_requires_finite_deadline(interval: float) -> None:
    with pytest.raises(QueueConfigurationError, match="flush_interval"):
        EventHistoryConfig(flush_interval=interval)


@pytest.mark.parametrize("capacity", [0, 19])
def test_history_capacity_covers_one_batch(capacity: int) -> None:
    with pytest.raises(QueueConfigurationError, match="max_pending"):
        EventHistoryConfig(batch_size=20, max_pending=capacity)

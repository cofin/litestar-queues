"""Task uniqueness policy: decorator/``using`` validation and enqueue precedence."""

from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

from litestar_queues import QueueConfig, QueueService, task
from litestar_queues._identity import IDENTITY_VERSION, arguments_identity, task_identity
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.task import clear_task_registry

if TYPE_CHECKING:
    from litestar_queues.models import QueuedTaskRecord

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def _clean_registry() -> "None":
    clear_task_registry()


# --------------------------------------------------------------------------- #
# Decorator + using() validation and propagation
# --------------------------------------------------------------------------- #


def test_decorator_propagates_unique_fields() -> "None":
    @task("reports.refresh", unique_by="task")
    async def refresh() -> "None": ...

    assert refresh.unique_by == "task"
    assert refresh.unique_until == "terminal"


def test_decorator_forever_requires_identity_source() -> "None":
    @task("imports.once", unique_by="arguments", unique_until="forever")
    async def import_once(object_key: "str") -> "None": ...

    assert import_once.unique_until == "forever"


def test_configured_key_plus_unique_by_is_rejected() -> "None":
    with pytest.raises(ValueError, match="ambiguous"):

        @task("bad.ambiguous", key="fixed", unique_by="task")
        async def bad() -> "None": ...


def test_forever_without_identity_is_rejected() -> "None":
    with pytest.raises(ValueError, match="forever"):

        @task("bad.forever", unique_until="forever")
        async def bad() -> "None": ...


def test_invalid_unique_by_is_rejected() -> "None":
    with pytest.raises(ValueError, match="unique_by"):

        @task("bad.mode", unique_by="everything")  # type: ignore[arg-type]
        async def bad() -> "None": ...


def test_using_propagates_and_revalidates() -> "None":
    @task("reports.render", unique_by="arguments")
    async def render(report_id: "str") -> "None": ...

    forever = render.using(unique_until="forever")
    assert forever.unique_by == "arguments"
    assert forever.unique_until == "forever"

    with pytest.raises(ValueError, match="ambiguous"):
        render.using(key="fixed")


# --------------------------------------------------------------------------- #
# Enqueue precedence + fast-path spies
# --------------------------------------------------------------------------- #


def _service(backend: "InMemoryQueueBackend") -> "QueueService":
    return QueueService(QueueConfig(), queue_backend=backend)


async def _enqueue(
    service: "QueueService", task_ref: "object", *args: "object", **kwargs: "object"
) -> "QueuedTaskRecord":
    result = await service.enqueue(task_ref, *args, **kwargs)  # type: ignore[arg-type]
    assert result.record is not None
    return result.record


async def test_unkeyed_calls_are_distinct_and_never_bind_arguments() -> "None":
    @task("plain.unkeyed")
    async def plain(value: "str") -> "None": ...

    backend = InMemoryQueueBackend()
    async with _service(backend) as service:
        with patch("litestar_queues.service.arguments_identity") as spy:
            first = await _enqueue(service, plain, "a")
            second = await _enqueue(service, plain, "a")
        assert spy.call_count == 0
    assert first.key is None
    assert second.key is None
    assert first.id != second.id


async def test_explicit_key_wins_verbatim_without_binding() -> "None":
    @task("keyed.explicit", unique_by="arguments")
    async def keyed(value: "str") -> "None": ...

    backend = InMemoryQueueBackend()
    async with _service(backend) as service:
        with patch("litestar_queues.service.arguments_identity") as spy:
            record = await _enqueue(service, keyed, "a", key="explicit-key")
        assert spy.call_count == 0
    assert record.key == "explicit-key"


async def test_configured_key_wins_without_binding() -> "None":
    @task("keyed.configured", key="configured-key")
    async def keyed(value: "str") -> "None": ...

    backend = InMemoryQueueBackend()
    async with _service(backend) as service:
        with patch("litestar_queues.service.arguments_identity") as spy:
            record = await _enqueue(service, keyed, "a")
        assert spy.call_count == 0
    assert record.key == "configured-key"


async def test_task_identity_never_binds_arguments() -> "None":
    @task("keyed.task", unique_by="task")
    async def keyed(value: "str") -> "None": ...

    backend = InMemoryQueueBackend()
    async with _service(backend) as service:
        with patch("litestar_queues.service.arguments_identity") as spy:
            first = await _enqueue(service, keyed, "a")
            second = await _enqueue(service, keyed, "b")
        assert spy.call_count == 0
    assert first.key == task_identity("keyed.task")
    assert first.id == second.id  # active dedup on the task-name identity
    assert first.metadata["unique_by"] == "task"
    assert first.metadata["unique_version"] == IDENTITY_VERSION


async def test_arguments_identity_dedups_equal_calls() -> "None":
    @task("keyed.args", unique_by="arguments")
    async def keyed(value: "str") -> "None": ...

    backend = InMemoryQueueBackend()
    async with _service(backend) as service:
        same_a = await _enqueue(service, keyed, "a")
        same_a_kw = await _enqueue(service, keyed, value="a")
        other = await _enqueue(service, keyed, "b")

    expected = arguments_identity("keyed.args", keyed.signature, ("a",), {}).key
    assert same_a.key == expected
    assert same_a.id == same_a_kw.id
    assert other.id != same_a.id
    assert same_a.metadata["unique_by"] == "arguments"


async def test_forever_lifetime_is_recorded_in_metadata() -> "None":
    @task("keyed.forever", unique_by="arguments", unique_until="forever")
    async def keyed(value: "str") -> "None": ...

    backend = InMemoryQueueBackend()
    async with _service(backend) as service:
        record = await _enqueue(service, keyed, "a")
    assert record.metadata["unique_until"] == "forever"


async def test_forever_blocks_reenqueue_across_terminal_and_cleanup() -> "None":
    from datetime import datetime, timedelta, timezone

    @task("once.forever", unique_by="arguments", unique_until="forever")
    async def once(object_key: "str") -> "None": ...

    backend = InMemoryQueueBackend()
    async with _service(backend) as service:
        first = await _enqueue(service, once, "obj-1")
        key = arguments_identity("once.forever", once.signature, ("obj-1",), {}).key

        # A tombstone exists carrying only key/id/name/time.
        tombstone = await service.get_task_identity(key)
        assert tombstone is not None
        assert tombstone.key == key
        assert tombstone.task_id == first.id
        assert tombstone.task_name == "once.forever"

        # Drive the record terminal, then run cleanup: the tombstone must survive.
        claimed = await backend.claim_task(first.id)
        assert claimed is not None
        await backend.complete_task(first.id, expected_retry_count=claimed.retry_count)
        removed = await backend.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1))
        assert removed == 1
        assert await backend.get_task(first.id) is None
        assert await service.get_task_identity(key) is not None

        # Re-enqueue is still blocked and returns the original owner id.
        blocked = await service.enqueue(once, "obj-1")
        assert blocked.id == first.id

        # Reset is the only way to allow a new enqueue.
        assert await service.reset_task_identity(key) is True
        assert await service.get_task_identity(key) is None
        reopened = await _enqueue(service, once, "obj-1")
        assert reopened.id != first.id

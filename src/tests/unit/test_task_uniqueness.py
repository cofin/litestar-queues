"""Task uniqueness policy: decorator/``using`` validation and enqueue precedence."""

import asyncio
from typing import TYPE_CHECKING
from unittest.mock import patch
from uuid import uuid4

import pytest

from litestar_queues import QueueConfig, QueueConfigurationError, QueueService, task
from litestar_queues._identity import IDENTITY_VERSION, arguments_identity, task_identity
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.task import clear_task_registry

if TYPE_CHECKING:
    from uuid import UUID

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
    async def bad() -> "None": ...

    with pytest.raises(ValueError, match="unique_by"):
        task("bad.mode", unique_by="everything")(bad)  # type: ignore[call-overload]


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


class _PostPersistenceFailureBackend(InMemoryQueueBackend):
    __slots__ = ("fail_verification",)

    def __init__(self, *, fail_verification: "bool" = False) -> "None":
        super().__init__()
        self.fail_verification = fail_verification

    async def get_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        if self.fail_verification:
            msg = "verification failed"
            raise RuntimeError(msg)
        return await super().get_task(task_id)

    async def notify_new_task(self, record: "QueuedTaskRecord") -> "None":
        msg = "notification failed after persistence"
        raise RuntimeError(msg)


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


async def test_forever_retains_reservation_when_enqueue_raises_after_persistence() -> "None":
    @task("once.post-persistence", key="once:post-persistence", unique_until="forever")
    async def once() -> "None": ...

    backend = _PostPersistenceFailureBackend()
    service = _service(backend)

    with pytest.raises(RuntimeError, match="notification failed after persistence"):
        await service.enqueue(once)

    reservation = await backend.has_identity("once:post-persistence")
    assert reservation is not None
    assert await backend.get_task(reservation.task_id) is not None


async def test_forever_retains_reservation_when_persistence_verification_fails() -> "None":
    @task("once.verification-failure", key="once:verification-failure", unique_until="forever")
    async def once() -> "None": ...

    backend = _PostPersistenceFailureBackend(fail_verification=True)
    service = _service(backend)

    with pytest.raises(RuntimeError, match="notification failed after persistence"):
        await service.enqueue(once)

    backend.fail_verification = False
    reservation = await backend.has_identity("once:verification-failure")
    assert reservation is not None
    assert await backend.get_task(reservation.task_id) is not None


async def test_forever_releases_reservation_when_enqueue_fails_before_persistence() -> "None":
    @task("once.pre-persistence", key="once:pre-persistence", unique_until="forever")
    async def once() -> "None": ...

    backend = InMemoryQueueBackend()
    service = _service(backend)

    with (
        patch.object(InMemoryQueueBackend, "enqueue", side_effect=RuntimeError("persistence failed")),
        pytest.raises(RuntimeError, match="persistence failed"),
    ):
        await service.enqueue(once)

    assert await backend.has_identity("once:pre-persistence") is None


async def test_forever_dedup_collision_does_not_leave_reserved_id_reservation() -> "None":
    @task("once.cross-policy", key="shared:cross-policy", unique_until="forever")
    async def once() -> "None": ...

    backend = InMemoryQueueBackend()
    existing = await backend.enqueue("existing.terminal-policy", key="shared:cross-policy")
    service = _service(backend)

    with pytest.raises(QueueConfigurationError, match=r"unique_until='forever'.*active task"):
        await service.enqueue(once)

    assert await backend.get_task(existing.id) == existing
    assert await backend.has_identity("shared:cross-policy") is None


async def test_fenced_reset_never_deletes_a_successor_reservation() -> "None":
    backend = InMemoryQueueBackend()
    key = "once:fenced-reset"
    first_task_id = uuid4()
    successor_task_id = uuid4()
    assert await backend.reserve_identity(key, task_id=successor_task_id, task_name="successor") is None

    assert await backend.reset_identity(key, expected_task_id=first_task_id) is False

    owner = await backend.has_identity(key)
    assert owner is not None
    assert owner.task_id == successor_task_id


async def test_forever_blocks_reenqueue_across_terminal_and_cleanup() -> "None":
    from datetime import datetime, timedelta, timezone

    @task("once.forever", unique_by="arguments", unique_until="forever")
    async def once(object_key: "str") -> "None": ...

    backend = InMemoryQueueBackend()
    async with _service(backend) as service:
        first = await _enqueue(service, once, "obj-1")
        key = arguments_identity("once.forever", once.signature, ("obj-1",), {}).key

        # A reservation exists carrying only key/id/name/time.
        reservation = await service.get_task_identity(key)
        assert reservation is not None
        assert reservation.key == key
        assert reservation.task_id == first.id
        assert reservation.task_name == "once.forever"

        # Drive the record terminal, then run cleanup: the reservation must survive.
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


async def test_unkeyed_enqueues_are_distinct_and_concurrently_claimable() -> "None":
    @task("plain.parallel")
    async def plain(value: "str") -> "None": ...

    backend = InMemoryQueueBackend()
    async with _service(backend) as service:
        results = await asyncio.gather(*(service.enqueue(plain, "same") for _ in range(5)))
        ids = {result.id for result in results}
        assert len(ids) == 5
        claimed = await backend.claim_many(limit=5)
    assert {record.id for record in claimed} == ids
    assert all(record.status == "running" for record in claimed)


async def test_forever_concurrent_enqueue_produces_one_winner() -> "None":
    @task("once.parallel", unique_by="arguments", unique_until="forever")
    async def once(object_key: "str") -> "None": ...

    backend = InMemoryQueueBackend()
    async with _service(backend) as service:
        results = await asyncio.gather(*(service.enqueue(once, "obj") for _ in range(6)))
    assert len({result.id for result in results}) == 1


async def test_schedules_keep_scheduled_key_regardless_of_uniqueness() -> "None":
    @task("sched.job", interval=60, unique_by="task", unique_until="forever")
    async def job() -> "None": ...

    backend = InMemoryQueueBackend()
    async with _service(backend) as service:
        first = await service.initialize_schedules()
        second = await service.initialize_schedules()

    assert len(first) == 1
    assert first[0].key == "scheduled:sched.job"
    # Re-initialization reuses the schedule record; the key is never rehashed.
    assert second[0].id == first[0].id
    # The forever policy never created a reservation for the schedule identity.
    assert await backend.has_identity("scheduled:sched.job") is None

"""Backend-neutral forever-uniqueness tombstone contract.

Every queue backend family (memory, Redis, Valkey, SQLSpec, Advanced Alchemy)
runs the same reservation / survival / reset / concurrency assertions so
``unique_until="forever"`` behaves identically everywhere.
"""

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from litestar_queues.backends import BaseQueueBackend


def _unique_key(prefix: str) -> str:
    return f"lq:u:v1:arguments:{prefix}:{uuid.uuid4().hex}"


async def assert_reserve_returns_owner_on_conflict(backend: "BaseQueueBackend") -> None:
    """Reservation is atomic, idempotent-by-owner, and readable via ``has_identity``."""
    key = _unique_key("reserve")
    first_id = uuid.uuid4()
    second_id = uuid.uuid4()

    assert await backend.has_identity(key) is None
    assert await backend.reserve_identity(key, task_id=first_id, task_name="uniq.reserve") is None

    owner = await backend.has_identity(key)
    assert owner is not None
    assert owner.key == key
    assert owner.task_id == first_id
    assert owner.task_name == "uniq.reserve"
    assert owner.created_at.tzinfo is not None

    conflict = await backend.reserve_identity(key, task_id=second_id, task_name="uniq.reserve.other")
    assert conflict is not None
    assert conflict.task_id == first_id
    assert conflict.task_name == "uniq.reserve"


async def assert_reset_is_only_deletion_path(backend: "BaseQueueBackend") -> None:
    """Reset removes a tombstone and permits exactly one fresh reservation."""
    key = _unique_key("reset")
    original = uuid.uuid4()
    await backend.reserve_identity(key, task_id=original, task_name="uniq.reset")

    assert await backend.reset_identity(key) is True
    assert await backend.has_identity(key) is None
    assert await backend.reset_identity(key) is False

    fresh = uuid.uuid4()
    assert await backend.reserve_identity(key, task_id=fresh, task_name="uniq.reset") is None
    owner = await backend.has_identity(key)
    assert owner is not None
    assert owner.task_id == fresh


async def assert_tombstone_survives_terminal_cleanup(backend: "BaseQueueBackend") -> None:
    """A tombstone survives its record going terminal and being cleaned up."""
    key = _unique_key("survive")
    record = await backend.enqueue("uniq.survive", key=key)
    await backend.reserve_identity(key, task_id=record.id, task_name="uniq.survive")

    claimed = await backend.claim_task(record.id)
    assert claimed is not None
    await backend.complete_task(record.id, result=None, expected_retry_count=claimed.retry_count)

    removed = await backend.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1))
    assert removed >= 1
    assert await backend.get_task(record.id) is None

    owner = await backend.has_identity(key)
    assert owner is not None
    assert owner.task_id == record.id


async def assert_concurrent_reservation_has_single_winner(backend: "BaseQueueBackend") -> None:
    """Concurrent reservers of one key yield exactly one winner; the rest see it."""
    key = _unique_key("race")
    candidates = [uuid.uuid4() for _ in range(8)]

    outcomes = await asyncio.gather(
        *(backend.reserve_identity(key, task_id=candidate, task_name="uniq.race") for candidate in candidates)
    )
    winners = [candidate for candidate, outcome in zip(candidates, outcomes, strict=True) if outcome is None]
    assert len(winners) == 1

    winner_id = winners[0]
    owner = await backend.has_identity(key)
    assert owner is not None
    assert owner.task_id == winner_id
    for outcome in outcomes:
        if outcome is not None:
            assert outcome.task_id == winner_id

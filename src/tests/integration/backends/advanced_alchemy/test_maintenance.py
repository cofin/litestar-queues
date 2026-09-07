"""Advanced Alchemy distributed maintenance coordination and bounded-operation contract."""

import asyncio
from datetime import datetime, timedelta, timezone, tzinfo
from typing import TYPE_CHECKING, Any
from uuid import UUID

import pytest
from typing_extensions import Self

pytest.importorskip("aiosqlite")
pytest.importorskip("advanced_alchemy")

from tests.integration.backends._maintenance_asserts import (
    assert_bounded_cleanup_terminal,
    assert_bounded_stale_recovery,
    assert_coordination_expiry,
)

if TYPE_CHECKING:
    from litestar_queues.backends.advanced_alchemy import SQLAlchemyBackend

pytestmark = pytest.mark.anyio


async def test_advanced_alchemy_backend_bounded_operations(advanced_alchemy_backend: "SQLAlchemyBackend") -> "None":
    await assert_bounded_cleanup_terminal(advanced_alchemy_backend)
    await assert_bounded_stale_recovery(advanced_alchemy_backend)


async def test_advanced_alchemy_backend_maintenance_expires(
    advanced_alchemy_backend: "SQLAlchemyBackend", monkeypatch: "pytest.MonkeyPatch", request: "pytest.FixtureRequest"
) -> "None":
    """An expired ownership is reacquirable within the database timestamp precision."""
    sqlalchemy_config = advanced_alchemy_backend._sqlalchemy_config
    if sqlalchemy_config is not None and sqlalchemy_config.get_engine().dialect.name == "mysql":
        from litestar_queues.backends.advanced_alchemy import backend as backend_module

        current_times = iter((
            datetime(2026, 7, 22, 12, 0, 0, 600_000, tzinfo=timezone.utc),
            datetime(2026, 7, 22, 12, 0, 0, 800_000, tzinfo=timezone.utc),
        ))
        monkeypatch.setattr(backend_module, "_utc_now", lambda: next(current_times))
        request.node.add_marker(
            pytest.mark.xfail(
                reason="Advanced Alchemy has no UTC datetime type preserving MySQL fractional seconds; see AA#777",
                strict=True,
            )
        )
    await assert_coordination_expiry(advanced_alchemy_backend)


async def test_advanced_alchemy_backend_concurrent_coordination_has_one_token_fenced_winner(
    advanced_alchemy_backend: "SQLAlchemyBackend",
) -> "None":
    """Two independently opened backends race for one persisted ownership."""
    from litestar_queues.backends.advanced_alchemy import SQLAlchemyBackend, SQLAlchemyBackendConfig

    first = advanced_alchemy_backend
    second = SQLAlchemyBackend(
        backend_config=SQLAlchemyBackendConfig(
            sqlalchemy_config=first._sqlalchemy_config,
            model_class=first._model_class,
            event_history_model_class=first._event_history_model_class,
            maintenance_model_class=first._maintenance_model_class,
            task_reservation_model_class=first._task_reservation_model_class,
        )
    )
    await second.open()
    try:
        ttl = timedelta(seconds=60)
        tokens = ("token-a", "token-b")
        outcomes = await asyncio.gather(
            first.acquire_maintenance("queue-maintenance-race", tokens[0], ttl=ttl),
            second.acquire_maintenance("queue-maintenance-race", tokens[1], ttl=ttl),
        )

        assert outcomes.count(True) == 1
        winner_index = outcomes.index(True)
        loser_index = 1 - winner_index
        backends = (first, second)
        assert await backends[loser_index].release_maintenance("queue-maintenance-race", tokens[loser_index]) is False
        assert await backends[winner_index].release_maintenance("queue-maintenance-race", tokens[winner_index]) is True
        assert (
            await backends[loser_index].acquire_maintenance("queue-maintenance-race", tokens[loser_index], ttl=ttl)
            is True
        )
        assert await backends[loser_index].release_maintenance("queue-maintenance-race", tokens[loser_index]) is True
    finally:
        await second.close()


async def test_advanced_alchemy_external_limit_uses_id_tie_breaker(
    advanced_alchemy_backend: "SQLAlchemyBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """Equal-age external records are selected by timestamp and then record id."""
    from litestar_queues import models as models_module
    from litestar_queues.backends.advanced_alchemy import backend as backend_module
    from litestar_queues.backends.advanced_alchemy import service as service_module

    fixed_now = datetime(2026, 7, 22, 12, 0, 0, tzinfo=timezone.utc)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz: "tzinfo | None" = None) -> Self:
            value = fixed_now if tz is not None else fixed_now.replace(tzinfo=None)
            return cls(
                value.year,
                value.month,
                value.day,
                value.hour,
                value.minute,
                value.second,
                value.microsecond,
                tzinfo=value.tzinfo,
            )

    monkeypatch.setattr(models_module, "datetime", FixedDateTime)
    monkeypatch.setattr(backend_module, "_utc_now", lambda: fixed_now)
    monkeypatch.setattr(service_module, "_utc_now", lambda: fixed_now)

    high = await advanced_alchemy_backend.enqueue("tasks.external.high", execution_backend="cloudrun", id=UUID(int=2))
    low = await advanced_alchemy_backend.enqueue("tasks.external.low", execution_backend="cloudrun", id=UUID(int=1))
    await advanced_alchemy_backend.set_execution_ref(high.id, "cloudrun", "jobs/high")
    await advanced_alchemy_backend.set_execution_ref(low.id, "cloudrun", "jobs/low")

    assert [record.id for record in await advanced_alchemy_backend.list_running_external(limit=1)] == [low.id]


async def test_dispatch_repair_equal_timestamps_rotate_across_fresh_backends(
    advanced_alchemy_backend: "SQLAlchemyBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    from litestar_queues.backends.advanced_alchemy import SQLAlchemyBackend, SQLAlchemyBackendConfig
    from litestar_queues.backends.advanced_alchemy import service as service_module
    from tests.helpers._timing import MutableClock

    backend = advanced_alchemy_backend
    clock = MutableClock(datetime(2026, 9, 7, 12, 0, 0, 123456, tzinfo=timezone.utc))
    monkeypatch.setattr(service_module, "_utc_now", clock)
    for number in (3, 1, 2):
        record = await backend.enqueue("repair.equal", execution_backend="cloudtasks", id=UUID(int=number))
        await backend.set_execution_ref(record.id, "cloudtasks", f"healthy-{number}")
    clock.advance(timedelta(seconds=1))
    selected_ids = []
    for _ in range(3):
        fresh = SQLAlchemyBackend(
            backend_config=SQLAlchemyBackendConfig(
                sqlalchemy_config=backend._sqlalchemy_config, model_class=backend._model_class
            )
        )
        await fresh.open()
        try:
            result = await fresh.list_dispatch_repair_candidates("cloudtasks", limit=1)
            selected_ids.append(result.records[0].id)
            stored = await fresh.get_task(result.records[0].id)
            assert stored is not None
            assert stored.dispatch_checked_at == clock()
        finally:
            await fresh.close()
    assert selected_ids == [UUID(int=number) for number in (1, 2, 3)]


async def test_dispatch_repair_delayed_selector_preserves_newer_mark(
    advanced_alchemy_backend: "SQLAlchemyBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    from sqlalchemy import Select
    from sqlalchemy.ext.asyncio import AsyncSession

    from litestar_queues.backends.advanced_alchemy import service as service_module
    from tests.helpers._timing import MutableClock

    backend = advanced_alchemy_backend
    record = await backend.enqueue("repair.delayed", execution_backend="cloudtasks")
    clock = MutableClock(datetime.now(timezone.utc))
    monkeypatch.setattr(service_module, "_utc_now", clock)
    selected = asyncio.Event()
    release = asyncio.Event()
    original_execute = AsyncSession.execute

    async def execute_with_delay(session: "AsyncSession", statement: "Any", *args: "Any", **kwargs: "Any") -> "Any":
        result = await original_execute(session, statement, *args, **kwargs)
        if (
            isinstance(statement, Select)
            and tuple(column.name for column in statement.selected_columns) == ("id",)
            and not selected.is_set()
        ):
            selected.set()
            await asyncio.wait_for(release.wait(), timeout=10)
        return result

    monkeypatch.setattr(AsyncSession, "execute", execute_with_delay)
    delayed = asyncio.create_task(backend.list_dispatch_repair_candidates("cloudtasks", limit=1))
    try:
        await asyncio.wait_for(selected.wait(), timeout=10)
        clock.advance(timedelta(seconds=1))
        latest = await backend.list_dispatch_repair_candidates("cloudtasks", limit=1)
    finally:
        release.set()
    earlier = await delayed
    assert latest.records[0].dispatch_checked_at == earlier.records[0].dispatch_checked_at == clock()
    stored = await backend.get_task(record.id)
    assert stored is not None
    assert stored.dispatch_checked_at == clock()


async def test_dispatch_repair_counts_transitioned_selection_without_refill(
    advanced_alchemy_backend: "SQLAlchemyBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    from sqlalchemy import Select
    from sqlalchemy.ext.asyncio import AsyncSession

    backend = advanced_alchemy_backend
    first = await backend.enqueue("repair.transitioned", execution_backend="cloudtasks", id=UUID(int=1))
    remaining = await backend.enqueue("repair.unexamined", execution_backend="cloudtasks", id=UUID(int=2))
    original_execute = AsyncSession.execute
    transitioned = False

    async def execute_with_transition(
        session: "AsyncSession", statement: "Any", *args: "Any", **kwargs: "Any"
    ) -> "Any":
        nonlocal transitioned
        result = await original_execute(session, statement, *args, **kwargs)
        if (
            isinstance(statement, Select)
            and tuple(column.name for column in statement.selected_columns) == ("id",)
            and not transitioned
        ):
            transitioned = True
            await backend.claim_task(first.id)
            await backend.complete_task(first.id)
        return result

    monkeypatch.setattr(AsyncSession, "execute", execute_with_transition)
    result = await backend.list_dispatch_repair_candidates("cloudtasks", limit=1)
    assert transitioned is True
    assert result.records == ()
    assert result.examined == 1
    assert result.limit_reached is True
    stored = await backend.get_task(remaining.id)
    assert stored is not None
    assert stored.dispatch_checked_at is None


async def test_dispatch_repair_zero_does_not_open_a_session(monkeypatch: "pytest.MonkeyPatch") -> "None":
    from litestar_queues.backends.advanced_alchemy import SQLAlchemyBackend
    from litestar_queues.backends.base import DispatchRepairCandidates
    from litestar_queues.exceptions import QueueConfigurationError

    unopened = SQLAlchemyBackend()
    assert await unopened.list_dispatch_repair_candidates("cloudtasks", limit=0) == DispatchRepairCandidates()
    with pytest.raises(QueueConfigurationError, match="non-negative"):
        await unopened.list_dispatch_repair_candidates("cloudtasks", limit=-1)

"""Advanced Alchemy distributed maintenance coordination and bounded-operation contract."""

import asyncio
from datetime import datetime, timedelta, timezone, tzinfo
from typing import TYPE_CHECKING
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

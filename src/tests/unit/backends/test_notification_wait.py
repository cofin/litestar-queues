"""Ownership invariants for the shared native-notification read helper."""

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING

import pytest

from litestar_queues.backends._notification_wait import PendingNativeRead

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

pytestmark = pytest.mark.anyio


def _has_pending(read: "PendingNativeRead") -> "bool":
    # Read through a function so mypy does not narrow the property to a literal.
    return read.has_pending


def _event_read_factory(event: "asyncio.Event", reads: "list[int]") -> "Callable[[], Awaitable[bool]]":
    def factory() -> "asyncio.Future[bool]":
        reads[0] += 1
        return asyncio.ensure_future(event.wait())

    return factory


async def test_repeated_timeouts_reuse_one_read() -> "None":
    event = asyncio.Event()
    reads = [0]
    factory = _event_read_factory(event, reads)
    pending = PendingNativeRead()

    assert await pending.race(factory, timeout=0.01) is None
    assert await pending.race(factory, timeout=0.01) is None
    assert await pending.race(factory, timeout=0.01) is None
    assert _has_pending(pending) is True
    assert reads[0] == 1

    event.set()
    task = await pending.race(factory, timeout=0.01)
    assert task is not None
    assert task.result() is True
    assert _has_pending(pending) is False
    assert reads[0] == 1

    await pending.aclose()


async def test_completed_read_is_consumed_exactly_once() -> "None":
    event = asyncio.Event()
    reads = [0]
    factory = _event_read_factory(event, reads)
    pending = PendingNativeRead()

    assert await pending.race(factory, timeout=0.01) is None
    event.set()

    first = await pending.race(factory, timeout=0.01)
    assert first is not None
    assert first.result() is True
    assert reads[0] == 1

    # The next wait must build a fresh read rather than re-consume the old one.
    event.clear()
    assert await pending.race(factory, timeout=0.01) is None
    assert reads[0] == 2
    await pending.aclose()


async def test_aclose_cancels_and_awaits_retained_read() -> "None":
    event = asyncio.Event()
    reads = [0]
    factory = _event_read_factory(event, reads)
    pending = PendingNativeRead()

    assert await pending.race(factory, timeout=0.01) is None
    assert _has_pending(pending) is True

    await pending.aclose()
    assert _has_pending(pending) is False


async def test_outer_cancellation_retains_the_native_read() -> "None":
    event = asyncio.Event()
    reads = [0]
    factory = _event_read_factory(event, reads)
    pending = PendingNativeRead()

    async def waiter() -> "None":
        await pending.race(factory, timeout=None)

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.01)
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task

    # Poll-boundary/shutdown cancellation of the caller must not destroy the read.
    assert _has_pending(pending) is True
    assert reads[0] == 1

    event.set()
    completed = await pending.race(factory, timeout=0.5)
    assert completed is not None
    assert completed.result() is True
    # The retained read was reused rather than rebuilt.
    assert reads[0] == 1
    await pending.aclose()

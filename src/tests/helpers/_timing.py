"""Deterministic timing helpers for scheduler-sensitive tests."""

import asyncio
import inspect
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


class MutableClock:
    """Callable UTC clock that tests can advance without sleeping."""

    __slots__ = ("_current",)

    def __init__(self, current: "datetime | None" = None) -> "None":
        self._current = current or datetime.now(timezone.utc)

    def __call__(self) -> "datetime":
        return self._current

    def advance(self, delta: "timedelta") -> "None":
        self._current += delta


async def wait_until(
    predicate: "Callable[[], bool | Awaitable[bool]]",
    *,
    timeout: float = 2.0,
    message: str = "condition was not satisfied before timeout",
) -> "None":
    """Yield until an observable condition becomes true."""

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        result = predicate()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        if loop.time() >= deadline:
            raise AssertionError(message)
        await asyncio.sleep(0)

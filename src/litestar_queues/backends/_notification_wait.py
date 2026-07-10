"""Backend-native notification read ownership shared across queue backends.

A native backend (memory event, Redis/Valkey pub/sub, SQLSpec event stream,
Advanced Alchemy asyncpg listener) owns at most one in-flight notification
read. An ordinary worker poll timeout must retain that read so the next wait
resumes the same subscription instead of re-establishing driver resources.
Only backend close or worker shutdown cancels and awaits the pending read.

This helper carries no driver, decoding, or durability policy: it only owns
the single-task race/retain/cancel lifecycle that every native backend shares.
"""

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

__all__ = ("PendingNativeRead",)


class PendingNativeRead:
    """Owns at most one in-flight backend-native notification read."""

    __slots__ = ("_task",)

    def __init__(self) -> "None":
        self._task: "asyncio.Task[Any] | None" = None

    @property
    def has_pending(self) -> "bool":
        """Whether a native read is currently retained."""
        return self._task is not None

    async def race(
        self, factory: "Callable[[], Awaitable[Any]]", timeout: "float | None"
    ) -> "asyncio.Task[Any] | None":
        """Race the retained read against ``timeout``.

        A new read is created via ``factory`` only when none is retained;
        otherwise the existing read is reused. A timeout leaves the read
        pending for the next call.

        Returns:
            The completed read task, which the caller must consume via
            ``result()``/``exception()``, or ``None`` when the timeout
            expired with the read still pending.
        """
        task = self._task
        if task is None:
            task = asyncio.ensure_future(factory())
            self._task = task
        done, _ = await asyncio.wait({task}, timeout=timeout)
        if task not in done:
            return None
        self._task = None
        return task

    async def aclose(self) -> "None":
        """Cancel and await the retained read, leaving no task behind."""
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

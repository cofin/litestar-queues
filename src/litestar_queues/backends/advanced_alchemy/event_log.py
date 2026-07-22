"""Advanced Alchemy-backed queue event history."""

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from litestar_queues.events._log_records import event_log_record_from_event

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractAsyncContextManager
    from datetime import datetime

    from litestar_queues.backends.advanced_alchemy.service import QueueEventLogService
    from litestar_queues.events import EventLogConfig, QueueEvent, QueueEventLogRecord, QueueEventStageSummary

__all__ = ("AdvancedAlchemyQueueEventLog",)

logger = logging.getLogger(__name__)


class AdvancedAlchemyQueueEventLog:
    """Buffered Advanced Alchemy event-history writer and query interface."""

    __slots__ = ("_config", "_flush_lock", "_last_flush", "_pending", "_service_factory", "_transaction_factory")

    def __init__(
        self,
        *,
        config: "EventLogConfig",
        service_factory: 'Callable[[], AbstractAsyncContextManager["QueueEventLogService"]]',
        transaction_factory: 'Callable[[], AbstractAsyncContextManager["QueueEventLogService"]]',
    ) -> "None":
        self._config = config
        self._service_factory = service_factory
        self._transaction_factory = transaction_factory
        self._pending: "list[QueueEventLogRecord]" = []
        self._last_flush = time.monotonic()
        self._flush_lock = asyncio.Lock()

    async def publish_event(self, event: "QueueEvent") -> "None":
        """Buffer a queue event and flush when configured thresholds are reached."""
        should_flush = False
        async with self._flush_lock:
            self._pending.append(event_log_record_from_event(event))
            should_flush = len(self._pending) >= max(1, self._config.buffer_size) or self._flush_interval_elapsed()
        if should_flush:
            await self.flush_events()

    async def flush_events(self) -> "None":
        """Flush buffered queue events through an Advanced Alchemy session.

        Returns:
            None.
        """
        async with self._flush_lock:
            if not self._pending:
                return
            batch = list(self._pending)
            try:
                async with self._transaction_factory() as service:
                    await service.add_records(batch)
            except Exception:
                if self._config.strict:
                    raise
                logger.warning("Advanced Alchemy queue event history flush failed", exc_info=True)
                return
            del self._pending[: len(batch)]
            self._last_flush = time.monotonic()

    async def list_events(
        self, *, task_id: "str | None" = None, task_name: "str | None" = None, limit: "int | None" = None
    ) -> "list[QueueEventLogRecord]":
        """Return durable event history records."""
        await self.flush_events()
        async with self._service_factory() as service:
            return await service.list_events(task_id=task_id, task_name=task_name, limit=limit)

    async def summarize_stages(self, *, task_name: "str | None" = None) -> "list[QueueEventStageSummary]":
        """Return no aggregate summaries for the Advanced Alchemy event log."""
        del task_name
        return []

    async def cleanup_before(self, before: "datetime", *, limit: "int | None" = None) -> "int":
        """Delete event history older than ``before``.

        Returns:
            Number of deleted event-history rows.
        """
        await self.flush_events()
        async with self._transaction_factory() as service:
            return await service.cleanup_before(before, limit=limit)

    def _flush_interval_elapsed(self) -> "bool":
        return self._config.flush_interval <= 0 or time.monotonic() - self._last_flush >= self._config.flush_interval

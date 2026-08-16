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

    from litestar.pagination import OffsetPagination

    from litestar_queues.backends.advanced_alchemy.service import QueueEventLogService
    from litestar_queues.events import EventHistoryConfig, QueueEvent, QueueEventLogRecord, QueueEventStageSummary
    from litestar_queues.events.query import QueueEventQuery

__all__ = ("AdvancedAlchemyQueueEventLog",)

logger = logging.getLogger(__name__)


class AdvancedAlchemyQueueEventLog:
    """Buffered Advanced Alchemy event-history writer and query interface."""

    __slots__ = (
        "_config",
        "_flush_lock",
        "_last_flush",
        "_logger",
        "_pending",
        "_service_factory",
        "_transaction_factory",
    )

    def __init__(
        self,
        config: "EventHistoryConfig",
        *,
        service_factory: 'Callable[[], AbstractAsyncContextManager["QueueEventLogService"]]',
        transaction_factory: 'Callable[[], AbstractAsyncContextManager["QueueEventLogService"]]',
        runtime_logger: "logging.Logger | None" = None,
    ) -> "None":
        self._config = config
        self._service_factory = service_factory
        self._transaction_factory = transaction_factory
        self._pending: "list[QueueEventLogRecord]" = []
        self._last_flush = time.monotonic()
        self._flush_lock = asyncio.Lock()
        self._logger = runtime_logger or logger

    async def publish_event(self, event: "QueueEvent") -> "None":
        """Buffer a queue event and flush when configured thresholds are reached."""
        should_flush = False
        async with self._flush_lock:
            self._pending.append(event_log_record_from_event(event, extra_columns=self._config.extra_columns))
            should_flush = len(self._pending) >= max(1, self._config.batch_size) or self._flush_interval_elapsed()
        if should_flush:
            await self.flush_events()

    async def flush_events(self) -> "None":
        """Flush buffered queue events through an Advanced Alchemy session."""
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
                self._logger.warning("Advanced Alchemy queue event history flush failed", exc_info=True)
                return
            del self._pending[: len(batch)]
            self._last_flush = time.monotonic()

    async def query_events(self, query: "QueueEventQuery | None" = None) -> "OffsetPagination[QueueEventLogRecord]":
        """Query durable event history records."""
        from litestar.pagination import OffsetPagination

        from litestar_queues.events import QueueEventQuery

        query = query or QueueEventQuery()

        await self.flush_events()
        async with self._service_factory() as service:
            total, items = await service.query_events(query)

            page_items = items[: query.limit] if query.limit else items

            return OffsetPagination(
                items=page_items, total=total, offset=query.offset, limit=query.limit or len(page_items) or 1
            )

    async def summarize_stages(self, query: "QueueEventQuery | None" = None) -> "list[QueueEventStageSummary]":
        """Return per-stage event history aggregates."""
        await self.flush_events()
        async with self._service_factory() as service:
            return await service.summarize_stages(query)


    async def cleanup_events(
        self,
        *,
        before: "datetime",
        match: "QueueEventQuery | None" = None,
        exclude: "tuple[QueueEventQuery, ...] | None" = None,
        limit: "int | None" = None,
    ) -> "int":
        """Delete event history older than ``before``.

        Returns:
            Number of deleted event-history rows.
        """
        await self.flush_events()
        async with self._transaction_factory() as service:
            return await service.cleanup_events(before=before, limit=limit, match=match, exclude=exclude)
    def _flush_interval_elapsed(self) -> "bool":
        return self._config.flush_interval <= 0 or time.monotonic() - self._last_flush >= self._config.flush_interval

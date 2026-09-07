"""Advanced Alchemy-backed queue event history."""

import asyncio
import logging
from contextlib import suppress
from sqlite3 import IntegrityError as SQLiteIntegrityError
from typing import TYPE_CHECKING

from advanced_alchemy.exceptions import DuplicateKeyError
from sqlalchemy.exc import IntegrityError as SQLAlchemyIntegrityError

from litestar_queues.events._history_buffer import _HistoryBuffer
from litestar_queues.events._log_records import event_log_record_from_event
from litestar_queues.events.query import QueueEventQuery
from litestar_queues.events.typing import OffsetPagination

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence
    from contextlib import AbstractAsyncContextManager
    from datetime import datetime

    from litestar_queues.backends.advanced_alchemy.service import QueueEventLogService
    from litestar_queues.events import EventHistoryConfig, QueueEvent, QueueEventLogRecord, QueueEventStageSummary

__all__ = ("AdvancedAlchemyQueueEventLog",)

logger = logging.getLogger(__name__)
_MYSQL_DUPLICATE_KEY = 1062


class AdvancedAlchemyQueueEventLog:
    """Buffered Advanced Alchemy event-history writer and query interface."""

    __slots__ = ("_buffer", "_config", "_logger", "_service_factory", "_transaction_factory")

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
        self._logger = runtime_logger or logger
        self._buffer = _HistoryBuffer(config, self._write_history_batch)

    async def publish_event(self, event: "QueueEvent") -> "None":
        """Accept a bounded snapshot for size- or deadline-triggered persistence."""
        await self._buffer.enqueue(event_log_record_from_event(event, extra_columns=self._config.extra_columns))

    async def publish_event_after_commit(
        self, event: "QueueEvent", *, release: "Callable[[], Awaitable[None]]", barrier: "bool" = False
    ) -> "None":
        """Release live delivery only after the history transaction commits."""
        await self._buffer.enqueue(
            event_log_record_from_event(event, extra_columns=self._config.extra_columns),
            release=release,
            barrier=barrier,
        )

    async def aclose(self) -> "None":
        """Stop admission and finish the owned history coordinator."""
        await self._buffer.stop()

    async def flush_events(self) -> "None":
        """Attempt persistence and live release of previously accepted events."""
        await self._buffer.flush()

    async def _write_history_batch(self, records: "Sequence[QueueEventLogRecord]") -> "None":
        for attempt in range(2):
            try:
                await self._write_history_transaction(records)
            except Exception as error:  # noqa: PERF203 - one fresh transaction retries a unique-key race.
                if attempt or not _is_duplicate_event_error(error):
                    raise
            else:
                return

    async def _write_history_transaction(self, records: "Sequence[QueueEventLogRecord]") -> "None":
        primary: BaseException | None = None
        try:
            async with self._transaction_factory() as service:
                try:
                    await service.add_records(records)
                except BaseException as error:
                    primary = error
                    raise
        except BaseException as cleanup:
            if isinstance(cleanup, asyncio.CancelledError):
                raise
            if primary is not None and cleanup is not primary:
                with suppress(Exception):
                    self._logger.warning("Advanced Alchemy history transaction cleanup also failed", exc_info=True)
                raise primary from None
            raise

        if primary is not None:
            raise primary

    async def query_events(
        self, query: "QueueEventQuery | None" = None, *, extra: "Mapping[str, str] | None" = None
    ) -> "OffsetPagination[QueueEventLogRecord]":
        """Query durable event history records."""
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
        exclude: "Sequence[QueueEventQuery]" = (),
        limit: "int | None" = None,
    ) -> "int":
        """Delete event history older than ``before``.

        Returns:
            Number of deleted event-history rows.
        """
        await self.flush_events()
        async with self._transaction_factory() as service:
            return await service.cleanup_events(before=before, limit=limit, match=match, exclude=tuple(exclude))


def _is_duplicate_event_error(error: "BaseException") -> "bool":
    seen: set[int] = set()
    native_integrity = False
    while id(error) not in seen:
        seen.add(id(error))
        if isinstance(error, DuplicateKeyError):
            return True
        if isinstance(error, SQLAlchemyIntegrityError) and isinstance(error.orig, BaseException):
            native_integrity = True
            error = error.orig
            continue
        if isinstance(error, SQLiteIntegrityError):
            code = getattr(error, "sqlite_errorcode", None)
            if code is not None:
                return code in {1555, 2067}
            return str(error).startswith(("UNIQUE constraint failed:", "PRIMARY KEY must be unique"))
        if getattr(error, "sqlstate", None) == "23505" or getattr(error, "pgcode", None) == "23505":
            return True
        if native_integrity and error.args and error.args[0] == _MYSQL_DUPLICATE_KEY:
            return True
        if native_integrity and error.args and getattr(error.args[0], "code", None) == 1:
            return True
        cause = error.__cause__
        if cause is None:
            return False
        error = cause
    return False

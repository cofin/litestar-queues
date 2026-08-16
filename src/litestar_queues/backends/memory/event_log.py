"""In-memory queue event history."""

import asyncio
from typing import TYPE_CHECKING

from litestar_queues.events._log_records import event_log_record_from_event, event_log_record_sort_key
from litestar_queues.events.history import validate_event_extra_filter
from litestar_queues.events.query import (
    match_event_record,
    paginate_event_records,
    require_unpaginated_query,
    sort_event_records,
    summarize_event_records,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import datetime

    from litestar_queues.events import EventHistoryConfig, QueueEvent, QueueEventLogRecord, QueueEventStageSummary
    from litestar_queues.events.query import QueueEventQuery
    from litestar_queues.events.typing import OffsetPagination

__all__ = ("InMemoryQueueEventLog",)


class InMemoryQueueEventLog:
    """Process-local, bounded queue event history for tests and local usage."""

    __slots__ = ("_config", "_lock", "_records")

    def __init__(self, config: "EventHistoryConfig") -> "None":
        self._config = config
        self._records: "list[QueueEventLogRecord]" = []
        self._lock = asyncio.Lock()

    async def publish_event(self, event: "QueueEvent") -> "None":
        """Append an event history record and prune the oldest records."""
        record = event_log_record_from_event(event, extra_columns=self._config.extra_columns)
        async with self._lock:
            self._records.append(record)
            overflow = len(self._records) - self._config.memory_capacity
            if overflow > 0:
                del self._records[:overflow]

    async def flush_events(self) -> "None":
        """Flush buffered events.

        The memory event log writes immediately, so this is intentionally a no-op.
        """

    async def list_events(
        self,
        *,
        task_id: "str | None" = None,
        task_name: "str | None" = None,
        actor_id: "str | None" = None,
        actor_type: "str | None" = None,
        extra: "Mapping[str, str] | None" = None,
        limit: "int | None" = None,
    ) -> "list[QueueEventLogRecord]":
        """Return matching event history records in ascending event order."""
        resolved_extra = validate_event_extra_filter(extra, self._config.extra_columns)
        async with self._lock:
            records = [
                record
                for record in self._records
                if (task_id is None or record.task_id == task_id)
                and (task_name is None or record.task_name == task_name)
                and (actor_id is None or record.actor_id == actor_id)
                and (actor_type is None or record.actor_type == actor_type)
                and (not resolved_extra or all(record.extra.get(k) == v for k, v in resolved_extra.items()))
            ]
        records.sort(key=event_log_record_sort_key)
        return records[:limit] if limit is not None else records

    async def query_events(self, query: "QueueEventQuery | None" = None) -> "OffsetPagination[QueueEventLogRecord]":
        """Return a filtered, ordered page of event history records.

        Returns:
            The matching page.
        """
        async with self._lock:
            matched = [record for record in self._records if match_event_record(record, query)]
        ordered = sort_event_records(matched, order="asc" if query is None else query.order)
        return paginate_event_records(ordered, query)

    async def summarize_stages(self, query: "QueueEventQuery | None" = None) -> "list[QueueEventStageSummary]":
        """Return per-stage aggregates for the matching records.

        Raises:
            QueueConfigurationError: If ``query`` sets ordering or pagination.

        Returns:
            One summary per distinct stage.
        """
        require_unpaginated_query(query)
        async with self._lock:
            matched = [record for record in self._records if match_event_record(record, query)]
        return summarize_event_records(matched)

    async def cleanup_events(
        self,
        *,
        before: "datetime",
        match: "QueueEventQuery | None" = None,
        exclude: "Sequence[QueueEventQuery]" = (),
        limit: "int | None" = None,
    ) -> "int":
        """Delete the oldest matching records occurring before ``before``.

        A record is deleted only when it matches ``match`` and matches none of
        ``exclude``. Deletion order is the ascending stable order key, so
        repeated bounded calls converge.

        Returns:
            Number of deleted records.
        """
        async with self._lock:
            doomed = [
                record
                for record in self._records
                if record.occurred_at < before
                and match_event_record(record, match)
                and not any(match_event_record(record, other) for other in exclude)
            ]
            doomed = sort_event_records(doomed)
            if limit is not None:
                doomed = doomed[:limit]
            if not doomed:
                return 0
            targets = {id(record) for record in doomed}
            self._records = [record for record in self._records if id(record) not in targets]
            return len(doomed)

    async def cleanup_before(self, before: "datetime", *, limit: "int | None" = None) -> "int":
        """Delete records older than ``before``.

        ``limit`` bounds one batch, deleting the oldest matching records first
        (oldest ``occurred_at``, then event id).

        Returns:
            Number of deleted records.
        """
        async with self._lock:
            stale = [record for record in self._records if record.occurred_at < before]
            if limit is not None:
                stale.sort(key=lambda record: (record.occurred_at, record.event_id))
                stale = stale[:limit]
            if not stale:
                return 0
            doomed = {id(record) for record in stale}
            self._records = [record for record in self._records if id(record) not in doomed]
            return len(stale)

    async def clear(self) -> "None":
        """Clear all memory event-history records."""
        async with self._lock:
            self._records.clear()

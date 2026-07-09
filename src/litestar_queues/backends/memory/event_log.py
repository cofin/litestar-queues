"""In-memory queue event history."""

import asyncio
from typing import TYPE_CHECKING

from litestar_queues.events._log_records import event_log_record_from_event, event_log_record_sort_key

if TYPE_CHECKING:
    from datetime import datetime

    from litestar_queues.events import EventLogConfig, QueueEvent, QueueEventLogRecord, QueueEventStageSummary

__all__ = ("InMemoryQueueEventLog",)


class InMemoryQueueEventLog:
    """Process-local, bounded queue event history for tests and local usage."""

    __slots__ = ("_config", "_lock", "_records")

    def __init__(self, config: "EventLogConfig") -> "None":
        self._config = config
        self._records: "list[QueueEventLogRecord]" = []
        self._lock = asyncio.Lock()

    async def publish_event(self, event: "QueueEvent") -> "None":
        """Append an event history record and prune the oldest records."""
        record = event_log_record_from_event(event)
        async with self._lock:
            self._records.append(record)
            overflow = len(self._records) - self._config.max_records
            if overflow > 0:
                del self._records[:overflow]

    async def flush_events(self) -> "None":
        """Flush buffered events.

        The memory event log writes immediately, so this is intentionally a no-op.
        """

    async def list_events(
        self, *, task_id: "str | None" = None, task_name: "str | None" = None, limit: "int | None" = None
    ) -> "list[QueueEventLogRecord]":
        """Return matching event history records in ascending event order."""
        async with self._lock:
            records = [
                record
                for record in self._records
                if (task_id is None or record.task_id == task_id)
                and (task_name is None or record.task_name == task_name)
            ]
        records.sort(key=event_log_record_sort_key)
        return records[:limit] if limit is not None else records

    async def summarize_stages(self, *, task_name: "str | None" = None) -> "list[QueueEventStageSummary]":
        """Return no aggregate summaries for the memory event log."""
        del task_name
        return []

    async def cleanup_before(self, before: "datetime") -> "int":
        """Delete records older than ``before``.

        Returns:
            Number of deleted records.
        """
        async with self._lock:
            original_count = len(self._records)
            self._records = [record for record in self._records if record.occurred_at >= before]
            return original_count - len(self._records)

    async def clear(self) -> "None":
        """Clear all memory event-history records."""
        async with self._lock:
            self._records.clear()

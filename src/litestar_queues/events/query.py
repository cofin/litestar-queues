"""Public query, page, and retention models for durable queue event history."""

from dataclasses import dataclass, field
from math import isfinite
from typing import TYPE_CHECKING, Literal

from litestar_queues.events._log_records import event_entity_key, event_log_record_sort_key
from litestar_queues.events._typing import OffsetPagination
from litestar_queues.events.history import QueueEventStageSummary
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from litestar_queues.events.history import QueueEventLogRecord

__all__ = (
    "EVENT_LEVEL_RANKS",
    "QueueEventOrder",
    "QueueEventQuery",
    "QueueEventRetentionRule",
    "event_level_for_rank",
    "event_level_rank",
    "match_event_record",
    "paginate_event_records",
    "require_unpaginated_query",
    "sort_event_records",
    "summarize_event_records",
)

QueueEventOrder = Literal["asc", "desc"]
"""Stable ordering direction for an event-history query."""

EVENT_LEVEL_RANKS: "dict[str, int]" = {"debug": 10, "info": 20, "warning": 30, "error": 40, "critical": 50}
"""Severity ranks for worst-level aggregation. Unknown non-empty levels rank 1."""


def event_level_rank(level: "str | None") -> "int":
    """Return the severity rank of an event level.

    ``None`` ranks 0, an unrecognized non-empty level ranks 1, and known levels
    use :data:`EVENT_LEVEL_RANKS`. Unknown levels are never dropped; they simply
    rank below ``debug``.

    Returns:
        The severity rank.
    """
    if not level:
        return 0
    return EVENT_LEVEL_RANKS.get(level.lower(), 1)


def event_level_for_rank(rank: "int") -> "str | None":
    """Map a severity rank back to a level name.

    Returns:
        The level name, or ``None`` if the rank is 0. Unrecognized ranks default
        to ``unknown``.
    """
    if not rank:
        return None
    for level, r in EVENT_LEVEL_RANKS.items():
        if r == rank:
            return level
    return "unknown"


@dataclass(frozen=True, slots=True)
class QueueEventQuery:
    """Immutable filter, order, and pagination request for durable event history.

    Every filter is equality and every set filter is ANDed. ``None`` means the
    dimension is unconstrained.
    """

    task_id: "str | None" = None
    task_name: "str | None" = None
    event_type: "str | None" = None
    scope: "str | None" = None
    scope_key: "str | None" = None
    entity: "str | None" = None
    level: "str | None" = None
    order: "QueueEventOrder" = "asc"
    limit: "int | None" = None
    offset: "int" = 0

    def __post_init__(self) -> "None":
        """Validate and canonicalize the query.

        Raises:
            QueueConfigurationError: If ``order`` is not ``asc``/``desc``,
                ``limit`` is not a positive integer when set, or ``offset`` is
                negative.
        """
        entity = self.entity
        if entity is not None and not isinstance(entity, str):
            object.__setattr__(self, "entity", event_entity_key(entity))  # type: ignore[unreachable]
        if self.order not in {"asc", "desc"}:
            msg = f"QueueEventQuery.order must be 'asc' or 'desc', got {self.order!r}."
            raise QueueConfigurationError(msg)
        if self.limit is not None and (
            isinstance(self.limit, bool) or not isinstance(self.limit, int) or self.limit <= 0
        ):
            msg = "QueueEventQuery.limit must be a positive integer when set."
            raise QueueConfigurationError(msg)
        if isinstance(self.offset, bool) or not isinstance(self.offset, int) or self.offset < 0:
            msg = "QueueEventQuery.offset must be a non-negative integer."
            raise QueueConfigurationError(msg)

    @property
    def is_paginated(self) -> "bool":
        """Whether the query constrains ordering output size or position."""
        return self.limit is not None or self.offset != 0

    def filters(self) -> "tuple[tuple[str, str], ...]":
        """Return the set equality filters as ``(record attribute, value)`` pairs.

        Returns:
            The set filters, in a stable declaration order.
        """
        pairs = (
            ("task_id", self.task_id),
            ("task_name", self.task_name),
            ("event_type", self.event_type),
            ("scope", self.scope),
            ("scope_key", self.scope_key),
            ("entity", self.entity),
            ("level", self.level),
        )
        return tuple((name, value) for name, value in pairs if value is not None)


def require_unpaginated_query(query: "QueueEventQuery | None") -> "None":
    """Reject a summary query that sets ordering or pagination.

    Raises:
        QueueConfigurationError: If ``query`` sets order, limit, or offset.
    """
    if query is not None and (query.is_paginated or query.order != "asc"):
        msg = "summarize_stages() does not accept order, limit, or offset on its query."
        raise QueueConfigurationError(msg)


@dataclass(frozen=True, slots=True)
class QueueEventRetentionRule:
    """One ordered, filtered retention rule for durable event history."""

    max_age: "float"
    """Retention age in seconds; must be finite and greater than 0."""

    match: "QueueEventQuery" = field(default_factory=QueueEventQuery)
    """Filter selecting the records this rule governs; an empty query matches all."""

    def __post_init__(self) -> "None":
        """Validate the rule.

        Raises:
            QueueConfigurationError: If ``max_age`` is not a finite positive
                number, or ``match`` sets ordering or pagination.
        """
        if (
            isinstance(self.max_age, bool)
            or not isinstance(self.max_age, (int, float))
            or not isfinite(self.max_age)
            or self.max_age <= 0
        ):
            msg = "QueueEventRetentionRule.max_age must be a finite number greater than 0."
            raise QueueConfigurationError(msg)
        if self.match.is_paginated or self.match.order != "asc":
            msg = "QueueEventRetentionRule.match must not set order, limit, or offset."
            raise QueueConfigurationError(msg)


def match_event_record(record: "QueueEventLogRecord", query: "QueueEventQuery | None") -> "bool":
    """Return whether ``record`` satisfies every set filter on ``query``."""
    if query is None:
        return True
    return all(getattr(record, name) == value for name, value in query.filters())


def sort_event_records(
    records: "Iterable[QueueEventLogRecord]", *, order: "QueueEventOrder" = "asc"
) -> "list[QueueEventLogRecord]":
    """Return records in the package-wide stable total order."""
    ordered = sorted(records, key=event_log_record_sort_key)
    if order == "desc":
        ordered.reverse()
    return ordered


def paginate_event_records(
    records: "Sequence[QueueEventLogRecord]", query: "QueueEventQuery | None"
) -> "OffsetPagination[QueueEventLogRecord]":
    """Slice already-ordered records into a page.

    ``total`` is the count of records matching the filter before pagination, so
    a caller can compute page counts without a second call. ``records`` is
    already filtered and ordered by the time it reaches here.
    """
    matched = len(records)
    if query is None:
        return OffsetPagination(items=list(records), limit=matched, offset=0, total=matched)
    window = records[query.offset :]
    if query.limit is None:
        return OffsetPagination(items=list(window), limit=matched, offset=query.offset, total=matched)
    items = list(window[: query.limit])
    # `records` is the already-filtered sequence, so the true match count is free
    # here. A SQL backend that cannot say the same passes `total=len(items)`.
    return OffsetPagination(items=items, limit=query.limit, offset=query.offset, total=matched)


def summarize_event_records(records: "Iterable[QueueEventLogRecord]") -> "list[QueueEventStageSummary]":
    """Aggregate already-filtered records into per-stage summaries.

    Groups by ``stage``, sums ``duration_ms``, tracks first/last ``occurred_at``,
    and derives ``latest_sequence``/``latest_message`` from the record with the
    greatest stable order key and ``worst_level`` from the greatest
    :func:`event_level_rank` (ties resolved by the latest record).

    Returns:
        Summaries ordered by stage, ``None`` stage first.
    """
    ordered = sort_event_records(records)

    stages: "dict[str | None, dict[str, object]]" = {}
    for record in ordered:
        stage = record.stage
        if stage not in stages:
            stages[stage] = {
                "event_count": 0,
                "total_duration_ms": 0.0,
                "first_event_at": None,
                "last_event_at": None,
                "latest_sequence": None,
                "latest_message": None,
                "best_rank": 0,
                "worst_level": None,
            }

        entry = stages[stage]
        entry["event_count"] += 1  # type: ignore[operator]

        if record.duration_ms is not None:
            entry["total_duration_ms"] += record.duration_ms  # type: ignore[operator]

        if entry["first_event_at"] is None or record.occurred_at < entry["first_event_at"]:  # type: ignore[operator]
            entry["first_event_at"] = record.occurred_at

        if entry["last_event_at"] is None or record.occurred_at >= entry["last_event_at"]:  # type: ignore[operator]
            entry["last_event_at"] = record.occurred_at

        # ordered list means later records always have a greater or equal sort key
        entry["latest_sequence"] = record.sequence
        entry["latest_message"] = record.message

        rank = event_level_rank(record.level)
        if rank >= entry["best_rank"]:  # type: ignore[operator]
            entry["best_rank"] = rank
            entry["worst_level"] = record.level

    result = []
    for stage, data in stages.items():
        summary = QueueEventStageSummary(
            stage=stage,
            event_count=data["event_count"],  # type: ignore[arg-type]
            total_duration_ms=data["total_duration_ms"],  # type: ignore[arg-type]
            first_event_at=data["first_event_at"],  # type: ignore[arg-type]
            last_event_at=data["last_event_at"],  # type: ignore[arg-type]
            latest_sequence=data["latest_sequence"],  # type: ignore[arg-type]
            latest_message=data["latest_message"],  # type: ignore[arg-type]
            worst_level=data["worst_level"],  # type: ignore[arg-type]
        )
        result.append(summary)

    result.sort(key=lambda s: (s.stage is not None, s.stage or ""))
    return result

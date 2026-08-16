"""Bounded, backend-neutral queue maintenance.

The maintenance service runs a small, predictable amount of repair and
retention work under token-fenced distributed coordination and a wall-clock time
budget, then returns. It never starts a worker, executes due work, or loops to
drain a backlog. Phases always run in the fixed order external-execution
reconciliation, stale-running recovery, terminal-task retention, and
durable-event retention.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from math import isfinite
from time import perf_counter
from typing import TYPE_CHECKING, Literal, cast
from uuid import uuid4

from litestar_queues.events import QueueEventLog, QueueEventRetentionRule
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import Callable, Collection

    from litestar_queues.service import QueueService

__all__ = (
    "MaintenancePhase",
    "MaintenancePhaseStatus",
    "QueueMaintenanceConfig",
    "QueueMaintenancePhaseResult",
    "QueueMaintenanceService",
    "QueueMaintenanceSummary",
)

MaintenancePhase = Literal["external", "stale", "terminal", "events"]
"""One bounded maintenance phase, in fixed execution order."""

MaintenancePhaseStatus = Literal["completed", "skipped", "failed", "partial"]
"""Outcome of a single maintenance phase."""

MaintenanceOutcome = Literal["completed", "failed", "partial", "already_running"]
"""Outcome of a whole maintenance run."""

PHASE_ORDER: "tuple[MaintenancePhase, ...]" = ("external", "stale", "terminal", "events")
"""Stable phase order. Never reordered; drift gates depend on this."""

MAINTENANCE_NAME = "queue-maintenance"
"""Distributed maintenance coordination name shared by every process."""

PHASE_ERROR_CODE = "maintenance_phase_failed"
"""Package-owned error code prefix for a failed phase.

A failed phase records ``maintenance_phase_failed:<ExceptionType>`` only. The
exception message, arguments, results, DSNs, and credentials are never included.
"""


@dataclass(slots=True)
class QueueMaintenanceConfig:
    """Bounded maintenance thresholds and limits.

    Durations and retention values are seconds. Every limit and duration must be
    positive and ``coordination_timeout`` must exceed ``time_budget`` so ownership outlives
    the whole run. ``stale_after``,
    ``terminal_retention``, and ``event_retention_rules`` default to ``None`` (or empty) which
    disables their phase; there are no destructive defaults.
    """

    time_budget: "float" = 300.0
    """Maximum wall-clock duration of one maintenance run in seconds."""

    coordination_timeout: "float" = 360.0
    """Distributed ownership duration in seconds; must exceed ``time_budget``."""

    external_limit: "int" = 100
    """Maximum external executions reconciled in one run."""

    stale_after: "float | None" = None
    """Running-task age threshold in seconds; ``None`` disables stale recovery."""

    stale_limit: "int" = 100
    """Maximum stale running tasks recovered in one run."""

    terminal_retention: "float | None" = None
    """Terminal-task retention age in seconds; ``None`` disables deletion."""

    terminal_limit: "int" = 1000
    """Maximum expired terminal tasks deleted in one run."""

    event_retention_rules: "tuple[QueueEventRetentionRule, ...]" = ()
    """Ordered event-history retention rules; empty disables the events phase.

    Rules are evaluated in order and the first rule whose ``match`` matches a
    record decides that record's retention age. A record matching no rule is
    never deleted.
    """

    event_limit: "int" = 1000
    """Maximum expired task-event records deleted in one run."""

    def __post_init__(self) -> "None":
        """Validate durations, retention thresholds, and limits.

        Raises:
            QueueConfigurationError: If a duration, retention threshold, or limit
                is not positive, or ``coordination_timeout`` does not exceed ``time_budget``.
        """
        for name, value in (("time_budget", self.time_budget), ("coordination_timeout", self.coordination_timeout)):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value) or value <= 0:
                msg = f"QueueMaintenanceConfig.{name} must be a finite number greater than 0."
                raise QueueConfigurationError(msg)
        for name, limit in (
            ("external_limit", self.external_limit),
            ("stale_limit", self.stale_limit),
            ("terminal_limit", self.terminal_limit),
            ("event_limit", self.event_limit),
        ):
            if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
                msg = f"QueueMaintenanceConfig.{name} must be a positive integer."
                raise QueueConfigurationError(msg)
        for name, retention in (("stale_after", self.stale_after), ("terminal_retention", self.terminal_retention)):
            if retention is not None and (
                isinstance(retention, bool)
                or not isinstance(retention, (int, float))
                or not isfinite(retention)
                or retention <= 0
            ):
                msg = f"QueueMaintenanceConfig.{name} must be a finite number greater than 0 when set."
                raise QueueConfigurationError(msg)
        if not isinstance(self.event_retention_rules, (tuple, list)):
            msg = "QueueMaintenanceConfig.event_retention_rules must be a tuple of QueueEventRetentionRule."  # type: ignore[unreachable]
            raise QueueConfigurationError(msg)
        rules = tuple(self.event_retention_rules)
        for rule in rules:
            if not isinstance(rule, QueueEventRetentionRule):
                msg = "QueueMaintenanceConfig.event_retention_rules must be a tuple of QueueEventRetentionRule."  # type: ignore[unreachable]
                raise QueueConfigurationError(msg)
        object.__setattr__(self, "event_retention_rules", rules)
        if self.coordination_timeout <= self.time_budget:
            msg = (
                "QueueMaintenanceConfig.coordination_timeout must be greater than time_budget "
                "so ownership outlives the run."
            )
            raise QueueConfigurationError(msg)


@dataclass(slots=True)
class QueueMaintenancePhaseResult:
    """Result of one bounded maintenance phase."""

    phase: "MaintenancePhase"
    status: "MaintenancePhaseStatus"
    changed: "int" = 0
    duration_ms: "float" = 0.0
    error: "str | None" = None

    def to_payload(self) -> "dict[str, object]":
        """Return a JSON-native mapping of this phase result."""
        return {
            "phase": self.phase,
            "status": self.status,
            "changed": self.changed,
            "duration_ms": self.duration_ms,
            "error": self.error,
        }


@dataclass(slots=True)
class QueueMaintenanceSummary:
    """Result of a whole maintenance run."""

    outcome: "MaintenanceOutcome"
    acquired: "bool"
    duration_ms: "float"
    phases: "list[QueueMaintenancePhaseResult]" = field(default_factory=list)

    def to_payload(self) -> "dict[str, object]":
        """Return a JSON-native mapping of the whole summary."""
        return {
            "outcome": self.outcome,
            "acquired": self.acquired,
            "duration_ms": self.duration_ms,
            "phases": [phase.to_payload() for phase in self.phases],
        }


def _default_utcnow() -> "datetime":
    return datetime.now(timezone.utc)


class QueueMaintenanceService:
    """Run bounded maintenance phases under token-fenced coordination and a time budget."""

    __slots__ = ("_config", "_monotonic", "_service", "_utcnow")

    def __init__(
        self,
        service: "QueueService",
        config: "QueueMaintenanceConfig",
        *,
        monotonic: "Callable[[], float]" = perf_counter,
        utcnow: "Callable[[], datetime]" = _default_utcnow,
    ) -> "None":
        """Initialize the maintenance service.

        Args:
            service: An opened queue service whose backend advertises
                ``supports_maintenance``.
            config: Bounded maintenance thresholds and limits.
            monotonic: Injected monotonic clock for budget/duration accounting.
            utcnow: Injected UTC clock used to compute stable retention cutoffs.
        """
        self._service = service
        self._config = config
        self._monotonic = monotonic
        self._utcnow = utcnow

    async def run(self, phases: "Collection[MaintenancePhase] | None" = None) -> "QueueMaintenanceSummary":
        """Claim maintenance ownership and run each selected phase once.

        Args:
            phases: Optional narrowing of the phases to run. Filtering only
                narrows configuration; it never enables a disabled retention
                threshold. ``None`` considers every phase in the fixed order.

        Returns:
            A summary whose outcome is ``already_running`` when ownership is denied,
            ``failed`` when any phase failed, ``partial`` when the budget skipped
            an enabled phase, else ``completed``.

        Raises:
            QueueConfigurationError: If a requested phase name is unknown or the
                backend does not support distributed maintenance coordination.
        """
        selected = self._select_phases(phases)
        started_monotonic = self._monotonic()
        started_at = self._utcnow()
        backend = self._service.get_queue_backend()
        if not backend.capabilities.supports_maintenance:
            msg = (
                f"{type(backend).__name__} does not support distributed maintenance coordination; "
                "use a persistent backend (Redis/Valkey, SQLSpec, or Advanced Alchemy) for cross-process maintenance."
            )
            raise QueueConfigurationError(msg)

        token = uuid4().hex
        maintenance_name = getattr(getattr(self._service, "config", None), "maintenance_name", MAINTENANCE_NAME)
        acquired = await backend.acquire_maintenance(
            maintenance_name, token, ttl=timedelta(seconds=self._config.coordination_timeout)
        )
        if not acquired:
            results = [QueueMaintenancePhaseResult(phase=phase, status="skipped") for phase in selected]
            return QueueMaintenanceSummary(
                outcome="already_running",
                acquired=False,
                duration_ms=self._elapsed_ms(started_monotonic),
                phases=results,
            )

        results = []
        try:
            cutoffs = self._cutoffs(started_at)
            budget_exhausted = False
            for phase in selected:
                if not self._phase_enabled(phase):
                    results.append(QueueMaintenancePhaseResult(phase=phase, status="skipped"))
                    continue
                if budget_exhausted or (self._monotonic() - started_monotonic) >= self._config.time_budget:
                    budget_exhausted = True
                    results.append(QueueMaintenancePhaseResult(phase=phase, status="partial"))
                    continue
                results.append(await self._run_phase(phase, cutoffs, started_at))
        finally:
            await backend.release_maintenance(maintenance_name, token)

        return QueueMaintenanceSummary(
            outcome=self._final_outcome(results),
            acquired=True,
            duration_ms=self._elapsed_ms(started_monotonic),
            phases=results,
        )

    def _select_phases(self, phases: "Collection[MaintenancePhase] | None") -> "tuple[MaintenancePhase, ...]":
        if phases is None:
            return PHASE_ORDER
        requested = set(phases)
        unknown = requested - set(PHASE_ORDER)
        if unknown:
            valid = ", ".join(PHASE_ORDER)
            msg = f"Unknown maintenance phase(s): {sorted(unknown)!r}; expected any of: {valid}."
            raise QueueConfigurationError(msg)
        return tuple(phase for phase in PHASE_ORDER if phase in requested)

    def _phase_enabled(self, phase: "MaintenancePhase") -> "bool":
        if phase == "external":
            return self._service.get_execution_backend().is_external
        if phase == "stale":
            return self._config.stale_after is not None
        if phase == "terminal":
            return self._config.terminal_retention is not None
        return bool(self._config.event_retention_rules) and self._service.get_event_log() is not None

    def _cutoffs(self, started_at: "datetime") -> "dict[str, datetime]":
        cutoffs: "dict[str, datetime]" = {}
        if self._config.terminal_retention is not None:
            cutoffs["terminal"] = started_at - timedelta(seconds=self._config.terminal_retention)
        return cutoffs

    async def _run_phase(
        self, phase: "MaintenancePhase", cutoffs: "dict[str, datetime]", started_at: "datetime"
    ) -> "QueueMaintenancePhaseResult":
        phase_start = self._monotonic()
        try:
            changed = await self._execute_phase(phase, cutoffs, started_at)
        except Exception as exc:  # noqa: BLE001 - phase failures are contained and sanitized.
            return QueueMaintenancePhaseResult(
                phase=phase,
                status="failed",
                changed=0,
                duration_ms=self._elapsed_ms(phase_start),
                error=f"{PHASE_ERROR_CODE}:{type(exc).__name__}",
            )
        return QueueMaintenancePhaseResult(
            phase=phase, status="completed", changed=changed, duration_ms=self._elapsed_ms(phase_start)
        )

    async def _execute_phase(
        self, phase: "MaintenancePhase", cutoffs: "dict[str, datetime]", started_at: "datetime"
    ) -> "int":
        if phase == "external":
            return await self._service.reconcile_external(limit=self._config.external_limit)
        if phase == "stale":
            result = await self._service.recover_stale_tasks(
                stale_after=timedelta(seconds=cast("float", self._config.stale_after)), limit=self._config.stale_limit
            )
            return result.requeued + result.failed
        if phase == "terminal":
            return await self._service.get_queue_backend().cleanup_terminal(
                cutoffs["terminal"], limit=self._config.terminal_limit
            )
        event_log = self._service.get_event_log()
        if event_log is None:  # pragma: no cover - guarded by _phase_enabled.
            return 0
        return await self._run_event_retention_rules(event_log, started_at)

    async def _run_event_retention_rules(self, event_log: "QueueEventLog", started_at: "datetime") -> "int":
        """Apply ordered retention rules under one shared row budget.

        Rule ``i`` deletes records matching its own filter and matching none of
        the filters of rules ``0..i-1``, so the first matching rule owns the
        record. Deletion is oldest-first and bounded, so repeated runs converge
        and never touch a record no rule matched.

        Returns:
            Total rows deleted across every rule.
        """
        rules = self._config.event_retention_rules
        remaining = self._config.event_limit
        deleted = 0
        for index, rule in enumerate(rules):
            if remaining <= 0:
                break
            removed = await event_log.cleanup_events(
                before=started_at - timedelta(seconds=rule.max_age),
                match=rule.match,
                exclude=tuple(earlier.match for earlier in rules[:index]),
                limit=remaining,
            )
            deleted += removed
            remaining -= removed
        return deleted

    def _elapsed_ms(self, start: "float") -> "float":
        return (self._monotonic() - start) * 1000.0

    @staticmethod
    def _final_outcome(results: "list[QueueMaintenancePhaseResult]") -> "MaintenanceOutcome":
        if any(result.status == "failed" for result in results):
            return "failed"
        if any(result.status == "partial" for result in results):
            return "partial"
        return "completed"

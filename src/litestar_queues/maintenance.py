"""Bounded, backend-neutral queue maintenance.

The maintenance service runs a small, predictable amount of repair and
retention work under token-fenced distributed coordination and a wall-clock time
budget, then returns. It never starts a worker, executes due work, or loops to
drain a backlog. Phases always run in the fixed order overdue-record expiration,
external-execution reconciliation, stale-running recovery, recurring-schedule
assertion, terminal-task retention, and durable-event retention.
"""

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from math import isfinite
from time import perf_counter
from typing import TYPE_CHECKING, Any, Literal, cast
from urllib.parse import urlsplit
from uuid import uuid4

from litestar import Request, Response, post

from litestar_queues.events import QueueEventLog, QueueEventRetentionRule
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.execution.base import DispatchRepairResult, ExternalReconciliationResult
from litestar_queues.execution.cloudtasks.oidc import CloudTasksOIDCGuard
from litestar_queues.models import ScheduleAssertionResult, StaleTaskRecoveryResult
from litestar_queues.namespace import QueueNamespace

if TYPE_CHECKING:
    from collections.abc import Callable, Collection

    from litestar.handlers import HTTPRouteHandler
    from litestar.types import Guard

    from litestar_queues.config import QueueConfig
    from litestar_queues.service import QueueService

__all__ = (
    "MaintenancePhase",
    "MaintenancePhaseStatus",
    "QueueMaintenanceConfig",
    "QueueMaintenancePhaseResult",
    "QueueMaintenanceService",
    "QueueMaintenanceSummary",
    "build_maintenance_route",
)

MaintenancePhase = Literal["expire", "external", "stale", "schedules", "terminal", "events"]
"""One bounded maintenance phase, in fixed execution order."""

MaintenancePhaseStatus = Literal["completed", "skipped", "failed", "partial"]
"""Outcome of a single maintenance phase."""

MaintenanceOutcome = Literal["completed", "failed", "partial", "already_running"]
"""Outcome of a whole maintenance run."""

PHASE_ORDER: "tuple[MaintenancePhase, ...]" = ("expire", "external", "stale", "schedules", "terminal", "events")
"""Stable phase order. Never reordered; drift gates depend on this."""

MAINTENANCE_NAME = "queue-maintenance"
"""Distributed maintenance coordination name shared by every process."""

PHASE_ERROR_CODE = "maintenance_phase_failed"
"""Package-owned error code prefix for a failed phase.

A failed phase records ``maintenance_phase_failed:<ExceptionType>`` only. The
exception message, arguments, results, DSNs, and credentials are never included.
"""

REPAIR_ERROR_CODE = "maintenance_repair_failed"
"""Bounded error code for a phase containing failed delivery repairs."""

_DEFAULT_MAINTENANCE_ROUTE_PATH = cast("str", object())


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
    """Budget checked before each phase; in-flight operations can run longer."""

    coordination_timeout: "float" = 360.0
    """Distributed ownership duration in seconds; must exceed ``time_budget``."""

    expire_limit: "int" = 1000
    """Maximum overdue pending or scheduled tasks expired in one run."""

    external_limit: "int" = 100
    """Maximum external executions reconciled in one run."""

    stale_after: "float | None" = None
    """Running-task age threshold in seconds; ``None`` disables stale recovery."""

    stale_limit: "int" = 100
    """Maximum stale running tasks recovered in one run."""

    schedules_limit: "int" = 100
    """Maximum recurring schedules asserted in one run."""

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

    mount_route: "bool" = False
    """Whether ``QueuePlugin`` mounts an HTTP endpoint that runs maintenance once."""

    route_path: "str" = _DEFAULT_MAINTENANCE_ROUTE_PATH
    """Path the HTTP maintenance endpoint is mounted on."""

    _route_path_derived: "bool" = field(init=False, repr=False)

    guards: "tuple[Guard, ...]" = ()
    """Application guards applied to the HTTP maintenance endpoint."""

    trust_platform_auth: "bool" = False
    """Whether platform IAM alone is accepted as protection for the HTTP maintenance endpoint."""

    verify_oidc: "bool" = False
    """Whether the HTTP maintenance endpoint verifies Google OIDC Bearer tokens in-process."""

    oidc_service_account_email: "str | None" = None
    """Expected Google OIDC service account email when ``verify_oidc=True``."""

    oidc_audience: "str | None" = None
    """Expected Google OIDC audience when ``verify_oidc=True``."""

    def __post_init__(self) -> "None":
        """Validate durations, retention thresholds, limits, and route protection.

        Raises:
            QueueConfigurationError: If a duration, retention threshold, limit,
                or route option is invalid, or ``coordination_timeout`` does not
                exceed ``time_budget``.
        """
        self._route_path_derived = self.route_path is _DEFAULT_MAINTENANCE_ROUTE_PATH
        if self._route_path_derived:
            self.route_path = "/_litestar-queues/maintenance"
        for name, value in (("time_budget", self.time_budget), ("coordination_timeout", self.coordination_timeout)):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value) or value <= 0:
                msg = f"QueueMaintenanceConfig.{name} must be a finite number greater than 0."
                raise QueueConfigurationError(msg)
        for name, limit in (
            ("expire_limit", self.expire_limit),
            ("external_limit", self.external_limit),
            ("stale_limit", self.stale_limit),
            ("schedules_limit", self.schedules_limit),
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
        self._validate_route_options()

    def _validate_route_options(self) -> "None":
        route = urlsplit(self.route_path if isinstance(self.route_path, str) else "")
        if not route.path.startswith("/") or route.path == "/" or route.query or route.fragment:
            msg = (
                "QueueMaintenanceConfig.route_path must be an absolute path below the root, "
                "such as '/_litestar-queues/maintenance'."
            )
            raise QueueConfigurationError(msg)
        if not isinstance(self.guards, (tuple, list)):
            msg = "QueueMaintenanceConfig.guards must be a tuple or list of Guard callables."  # type: ignore[unreachable]
            raise QueueConfigurationError(msg)
        object.__setattr__(self, "guards", tuple(self.guards))
        if self.oidc_service_account_email is not None and (
            not isinstance(self.oidc_service_account_email, str) or not self.oidc_service_account_email.strip()
        ):
            msg = "QueueMaintenanceConfig.oidc_service_account_email must be a non-empty string when set."
            raise QueueConfigurationError(msg)
        if self.oidc_audience is not None and (
            not isinstance(self.oidc_audience, str) or not self.oidc_audience.strip()
        ):
            msg = "QueueMaintenanceConfig.oidc_audience must be a non-empty string when set."
            raise QueueConfigurationError(msg)
        if self.verify_oidc and self.oidc_audience is None:
            msg = "QueueMaintenanceConfig.oidc_audience is required when verify_oidc=True."
            raise QueueConfigurationError(msg)
        if self.mount_route and not self.trust_platform_auth and not self.verify_oidc and not self.guards:
            msg = (
                "QueueMaintenanceConfig(mount_route=True) requires route protection. "
                "Set verify_oidc=True with oidc_audience, trust_platform_auth=True when platform IAM "
                "already requires an authenticated caller, or pass guards."
            )
            raise QueueConfigurationError(msg)

    def resolve(self, namespace: "QueueNamespace | None" = None) -> "QueueMaintenanceConfig":
        """Resolve namespace-owned defaults without mutating this reusable config."""
        if not self._route_path_derived:
            return self
        names = namespace or QueueNamespace()
        route_path = f"/_{names.resource()}/maintenance"
        return replace(self, route_path=route_path)


@dataclass(slots=True)
class QueueMaintenancePhaseResult:
    """Result of one bounded maintenance phase."""

    phase: "MaintenancePhase"
    """Name of the maintenance phase executed."""

    status: "MaintenancePhaseStatus"
    """Outcome status of this phase (`"completed"`, `"skipped"`, `"failed"`, or `"budget_exhausted"`)."""

    changed: "int" = 0
    """Count of records created, updated, or deleted during this phase."""

    duration_ms: "float" = 0.0
    """Elapsed execution duration of this phase in milliseconds."""

    error: "str | None" = None
    """Sanitized error message if the phase failed, otherwise `None`."""

    repair: "DispatchRepairResult | None" = None
    """Structured external dispatch repair summary produced by this phase, if applicable."""

    def to_payload(self) -> "dict[str, object]":
        """Return a JSON-native mapping of this phase result."""
        return {
            "phase": self.phase,
            "status": self.status,
            "changed": self.changed,
            "duration_ms": self.duration_ms,
            "error": self.error,
            "repair": {
                "examined": self.repair.examined,
                "changed": self.repair.changed,
                "failed": self.repair.failed,
                "unchanged": self.repair.unchanged,
                "limit_reached": self.repair.limit_reached,
            }
            if self.repair is not None
            else None,
        }


@dataclass(slots=True)
class QueueMaintenanceSummary:
    """Result of a whole maintenance run."""

    outcome: "MaintenanceOutcome"
    """Overall maintenance run outcome (`"completed"`, `"busy"`, `"partial"`, or `"failed"`)."""

    acquired: "bool"
    """Whether this caller acquired the distributed maintenance coordination lease."""

    duration_ms: "float"
    """Total elapsed maintenance execution time in milliseconds."""

    phases: "list[QueueMaintenancePhaseResult]" = field(default_factory=list)
    """Ordered per-phase execution summaries."""

    def to_payload(self) -> "dict[str, object]":
        """Return a JSON-native mapping of the whole summary."""
        return {
            "outcome": self.outcome,
            "acquired": self.acquired,
            "duration_ms": self.duration_ms,
            "phases": [phase.to_payload() for phase in self.phases],
        }


def _default_utcnow() -> "datetime":
    return datetime.now(UTC)


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
            ``failed`` when any phase or repair failed, ``partial`` when a scan
            exhausted its allowance or the time budget skipped an enabled phase,
            else ``completed``.

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
        if phase == "expire":
            return True
        if phase == "external":
            if hasattr(self._service, "get_execution_backends"):
                return any(backend.is_external for backend in self._service.get_execution_backends().values())
            return self._service.get_execution_backend().is_external
        if phase == "stale":
            return self._config.stale_after is not None
        if phase == "schedules":
            if hasattr(self._service, "has_registered_schedules"):
                return self._service.has_registered_schedules()
            return False
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
            result = await self._execute_phase(phase, cutoffs, started_at)
        except Exception as exc:  # noqa: BLE001 - phase failures are contained and sanitized.
            return QueueMaintenancePhaseResult(
                phase=phase,
                status="failed",
                changed=0,
                duration_ms=self._elapsed_ms(phase_start),
                error=f"{PHASE_ERROR_CODE}:{type(exc).__name__}",
            )
        if isinstance(result, ExternalReconciliationResult):
            repair = result.repair
            status: "MaintenancePhaseStatus" = (
                "failed" if repair.failed else "partial" if repair.limit_reached else "completed"
            )
            return QueueMaintenancePhaseResult(
                phase=phase,
                status=status,
                changed=result.changed,
                duration_ms=self._elapsed_ms(phase_start),
                error=REPAIR_ERROR_CODE if repair.failed else None,
                repair=repair,
            )
        if isinstance(result, StaleTaskRecoveryResult):
            stale_repair = result.repair
            repair_failed = stale_repair.failed > 0 if stale_repair is not None else False
            stale_status: "MaintenancePhaseStatus" = "failed" if repair_failed else "completed"
            return QueueMaintenancePhaseResult(
                phase=phase,
                status=stale_status,
                changed=result.requeued + result.failed,
                duration_ms=self._elapsed_ms(phase_start),
                error=REPAIR_ERROR_CODE if repair_failed else None,
                repair=stale_repair,
            )
        if isinstance(result, ScheduleAssertionResult):
            sched_repair = result.repair
            sched_failed = sched_repair.failed > 0 if sched_repair is not None else False
            sched_partial = sched_repair.limit_reached if sched_repair is not None else False
            sched_status: "MaintenancePhaseStatus" = (
                "failed" if sched_failed else "partial" if sched_partial else "completed"
            )
            return QueueMaintenancePhaseResult(
                phase=phase,
                status=sched_status,
                changed=result.changed,
                duration_ms=self._elapsed_ms(phase_start),
                error=REPAIR_ERROR_CODE if sched_failed else None,
                repair=sched_repair,
            )
        return QueueMaintenancePhaseResult(
            phase=phase, status="completed", changed=result, duration_ms=self._elapsed_ms(phase_start)
        )

    async def _execute_phase(
        self, phase: "MaintenancePhase", cutoffs: "dict[str, datetime]", started_at: "datetime"
    ) -> "int | ExternalReconciliationResult | StaleTaskRecoveryResult | ScheduleAssertionResult":
        if phase == "expire":
            expired = await self._service.expire_overdue_tasks(limit=self._config.expire_limit)
            return len(expired)
        if phase == "external":
            return await self._service.reconcile_external_result(limit=self._config.external_limit)
        if phase == "stale":
            return await self._service.recover_stale_tasks(
                stale_after=timedelta(seconds=cast("float", self._config.stale_after)), limit=self._config.stale_limit
            )
        if phase == "schedules":
            return await self._service.reassert_schedules(limit=self._config.schedules_limit)
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


def build_maintenance_route(queue_config: "QueueConfig") -> "HTTPRouteHandler":
    """Build the HTTP endpoint that runs one bounded maintenance pass.

    Returns:
        The POST route handler for queue maintenance.

    Raises:
        QueueConfigurationError: If ``queue_config.maintenance`` is not configured.
    """
    if queue_config.maintenance is None:
        msg = "build_maintenance_route requires QueueConfig.maintenance to be configured."
        raise QueueConfigurationError(msg)
    config = queue_config.maintenance.resolve(queue_config.names)
    guards: "list[Guard]" = []
    if config.verify_oidc and not any(isinstance(guard, CloudTasksOIDCGuard) for guard in config.guards):
        guards.append(
            CloudTasksOIDCGuard(
                audience=cast("str", config.oidc_audience), service_account_email=config.oidc_service_account_email
            )
        )
    guards.extend(config.guards)

    @post(
        path=config.route_path,
        guards=guards,
        opt={"exclude_from_csrf": True, "exclude_from_auth": True},
        include_in_schema=False,
        summary="Queue maintenance",
    )
    async def run_queue_maintenance(request: "Request[Any, Any, Any]") -> "Response[dict[str, object]]":
        """Execute one bounded maintenance pass and return its structured summary.

        Returns:
            A JSON response with status 200 on completion/partial/already_running
            or 503 when a phase or repair failed.
        """
        service = queue_config.get_service(request.app.state)
        summary = await QueueMaintenanceService(service, config).run()
        status_code = 503 if summary.outcome == "failed" else 200
        return Response(content=summary.to_payload(), status_code=status_code)

    return run_queue_maintenance

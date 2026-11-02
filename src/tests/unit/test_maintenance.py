"""Unit tests for the bounded queue maintenance contract and orchestration."""

import json
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, cast

import pytest

from litestar_queues import (
    QueueMaintenanceConfig,
    QueueMaintenancePhaseResult,
    QueueMaintenanceService,
    QueueMaintenanceSummary,
)
from litestar_queues.events import QueueEventRetentionRule
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.execution.base import DispatchRepairResult, ExternalReconciliationResult
from litestar_queues.maintenance import MAINTENANCE_NAME, PHASE_ORDER, build_maintenance_route
from litestar_queues.models import QueueBackendCapabilities, ScheduleAssertionResult, StaleTaskRecoveryResult

if TYPE_CHECKING:
    from collections.abc import Collection

    from litestar_queues.events.query import QueueEventQuery
    from litestar_queues.maintenance import MaintenancePhase
    from litestar_queues.service import QueueService

pytestmark = pytest.mark.anyio

FIXED_NOW = datetime(2026, 7, 21, 12, 0, 0, tzinfo=timezone.utc)


class _Clock:
    """Manually controlled monotonic clock; phases advance it via side effects."""

    def __init__(self, value: "float" = 0.0) -> "None":
        self.value = value

    def __call__(self) -> "float":
        return self.value


class _StubEventLog:
    """Event-log double recording ``cleanup_events`` calls."""

    def __init__(self, *, deleted: "int" = 0, error: "Exception | None" = None) -> "None":
        self.deleted = deleted
        self.error = error
        self.calls: "list[tuple[datetime, int | None]]" = []

    async def cleanup_events(
        self,
        before: "datetime",
        *,
        match: "QueueEventQuery | None" = None,
        exclude: "tuple[QueueEventQuery, ...] | None" = None,
        limit: "int | None" = None,
    ) -> "int":
        self.calls.append((before, limit))
        if self.error is not None:
            raise self.error
        return self.deleted


class _StubBackend:
    """Queue-backend double implementing the ownership + terminal-cleanup surface."""

    def __init__(self, *, supports_maintenance: "bool" = True, ownership_granted: "bool" = True) -> "None":
        self._capabilities = QueueBackendCapabilities(supports_maintenance=supports_maintenance)
        self.ownership_granted = ownership_granted
        self.acquire_calls: "list[tuple[str, str]]" = []
        self.release_calls: "list[tuple[str, str]]" = []
        self.cleanup_calls: "list[tuple[datetime, int | None]]" = []
        self.cleanup_deleted = 0
        self.cleanup_error: "Exception | None" = None
        self.clock: "_Clock | None" = None
        self.cleanup_advance = 0.0

    @property
    def capabilities(self) -> "QueueBackendCapabilities":
        return self._capabilities

    async def acquire_maintenance(self, name: "str", token: "str", *, ttl: "timedelta") -> "bool":
        self.acquire_calls.append((name, token))
        return self.ownership_granted

    async def release_maintenance(self, name: "str", token: "str") -> "bool":
        self.release_calls.append((name, token))
        return True

    async def cleanup_terminal(self, before: "datetime", *, limit: "int | None" = None) -> "int":
        self.cleanup_calls.append((before, limit))
        if self.clock is not None:
            self.clock.value += self.cleanup_advance
        if self.cleanup_error is not None:
            raise self.cleanup_error
        return self.cleanup_deleted


class _StubExecutionBackend:
    def __init__(self, *, is_external: "bool") -> "None":
        self._is_external = is_external

    @property
    def is_external(self) -> "bool":
        return self._is_external


class _StubService:
    """Queue-service double exposing exactly the maintenance dependencies."""

    def __init__(
        self,
        *,
        backend: "_StubBackend",
        is_external: "bool" = False,
        has_schedules: "bool" = False,
        event_log: "_StubEventLog | None" = None,
    ) -> "None":
        self._backend = backend
        self._execution_backend = _StubExecutionBackend(is_external=is_external)
        self._has_schedules = has_schedules
        self._event_log = event_log
        self.expire_calls: "list[int | None]" = []
        self.reconcile_calls: "list[int | None]" = []
        self.recover_calls: "list[tuple[timedelta, int | None]]" = []
        self.schedules_calls: "list[int | None]" = []
        self.expired_count = 0
        self.expire_error: "Exception | None" = None
        self.reconcile_result = 0
        self.repair_result = DispatchRepairResult()
        self.reconcile_error: "Exception | None" = None
        self.recover_result = StaleTaskRecoveryResult()
        self.recover_error: "Exception | None" = None
        self.schedules_result = ScheduleAssertionResult()
        self.schedules_error: "Exception | None" = None
        self.clock: "_Clock | None" = None
        self.expire_advance = 0.0
        self.reconcile_advance = 0.0
        self.recover_advance = 0.0
        self.schedules_advance = 0.0

    def get_queue_backend(self) -> "_StubBackend":
        return self._backend

    def get_execution_backend(self) -> "_StubExecutionBackend":
        return self._execution_backend

    def get_execution_backends(self) -> "dict[str, _StubExecutionBackend]":
        return {"default": self._execution_backend}

    def get_event_log(self) -> "_StubEventLog | None":
        return self._event_log

    def has_registered_schedules(self) -> "bool":
        return self._has_schedules

    async def expire_overdue_tasks(self, *, limit: "int | None" = None) -> "list[object]":
        self.expire_calls.append(limit)
        if self.clock is not None:
            self.clock.value += self.expire_advance
        if self.expire_error is not None:
            raise self.expire_error
        return [object() for _ in range(self.expired_count)]

    async def reconcile_external(self, limit: "int | None" = None) -> "int":
        self.reconcile_calls.append(limit)
        if self.clock is not None:
            self.clock.value += self.reconcile_advance
        if self.reconcile_error is not None:
            raise self.reconcile_error
        return self.reconcile_result

    async def recover_stale_tasks(
        self, *, stale_after: "timedelta", worker_id: "str | None" = None, limit: "int | None" = None
    ) -> "StaleTaskRecoveryResult":
        self.recover_calls.append((stale_after, limit))
        if self.clock is not None:
            self.clock.value += self.recover_advance
        if self.recover_error is not None:
            raise self.recover_error
        return self.recover_result

    async def reassert_schedules(self, *, limit: "int | None" = None) -> "ScheduleAssertionResult":
        self.schedules_calls.append(limit)
        if self.clock is not None:
            self.clock.value += self.schedules_advance
        if self.schedules_error is not None:
            raise self.schedules_error
        return self.schedules_result

    async def reconcile_external_result(self, *, limit: "int") -> "ExternalReconciliationResult":
        reconciled = await self.reconcile_external(limit=limit)
        return ExternalReconciliationResult(repair=self.repair_result, reconciled=reconciled)


async def _run(
    service: "_StubService",
    config: "QueueMaintenanceConfig",
    *,
    monotonic: "_Clock | None" = None,
    phases: "Collection[MaintenancePhase] | None" = None,
) -> "QueueMaintenanceSummary":
    maintenance = QueueMaintenanceService(
        cast("QueueService", service),
        config,
        monotonic=monotonic if monotonic is not None else _Clock(),
        utcnow=lambda: FIXED_NOW,
    )
    return await maintenance.run(phases)


# --------------------------------------------------------------------------- #
# Configuration validation
# --------------------------------------------------------------------------- #


def test_config_defaults_disable_retention_phases() -> "None":
    config = QueueMaintenanceConfig()
    assert config.stale_after is None
    assert config.terminal_retention is None
    assert config.event_retention_rules == ()
    assert config.coordination_timeout > config.time_budget


@pytest.mark.parametrize(
    "kwargs",
    [
        {"time_budget": 0},
        {"coordination_timeout": 0},
        {"external_limit": 0},
        {"stale_limit": 0},
        {"terminal_limit": 0},
        {"event_limit": 0},
        {"stale_after": 0},
        {"terminal_retention": -1},
        {"event_retention_rules": (0,)},
        {"coordination_timeout": 300.0, "time_budget": 300.0},
        {"coordination_timeout": 100.0, "time_budget": 300.0},
        {"time_budget": float("nan")},
        {"time_budget": float("inf")},
        {"coordination_timeout": float("nan")},
        {"coordination_timeout": float("inf")},
        {"stale_after": float("nan")},
        {"terminal_retention": float("inf")},
        {"event_retention_rules": (QueueEventRetentionRule,)},
        {"time_budget": True},
        {"stale_after": True},
        {"external_limit": 1.5},
        {"stale_limit": True},
        {"terminal_limit": 2.5},
        {"event_limit": False},
    ],
)
def test_config_rejects_invalid_values(kwargs: "dict[str, object]") -> "None":
    with pytest.raises(QueueConfigurationError):
        QueueMaintenanceConfig(**kwargs)  # type: ignore[arg-type]


def test_config_allows_positive_retention() -> "None":
    config = QueueMaintenanceConfig(
        stale_after=60.0, terminal_retention=3600.0, event_retention_rules=(QueueEventRetentionRule(max_age=7200.0),)
    )
    assert config.stale_after == 60.0


# --------------------------------------------------------------------------- #
# Ownership + capability
# --------------------------------------------------------------------------- #


async def test_run_rejects_backend_without_maintenance_capability() -> "None":
    backend = _StubBackend(supports_maintenance=False)
    service = _StubService(backend=backend)
    with pytest.raises(QueueConfigurationError):
        await _run(service, QueueMaintenanceConfig())
    assert backend.acquire_calls == []


async def test_coordination_denial_is_a_no_op_with_skipped_phases() -> "None":
    backend = _StubBackend(ownership_granted=False)
    stub = _StubService(backend=backend, is_external=True)
    stub.reconcile_result = 5
    summary = await _run(stub, QueueMaintenanceConfig(stale_after=60, terminal_retention=60))

    assert summary.outcome == "already_running"
    assert summary.acquired is False
    assert [phase.status for phase in summary.phases] == [
        "skipped",
        "skipped",
        "skipped",
        "skipped",
        "skipped",
        "skipped",
    ]
    assert stub.expire_calls == []
    assert stub.reconcile_calls == []
    assert stub.recover_calls == []
    assert stub.schedules_calls == []
    assert backend.cleanup_calls == []
    assert backend.release_calls == []


async def test_coordination_uses_shared_name_and_is_released() -> "None":
    backend = _StubBackend()
    stub = _StubService(backend=backend)
    await _run(stub, QueueMaintenanceConfig())
    assert [name for name, _ in backend.acquire_calls] == [MAINTENANCE_NAME]
    assert [name for name, _ in backend.release_calls] == [MAINTENANCE_NAME]
    assert backend.acquire_calls[0][1] == backend.release_calls[0][1]


# --------------------------------------------------------------------------- #
# Phase order, enablement, one-call-per-phase
# --------------------------------------------------------------------------- #


def test_phase_order_includes_expire_and_schedules() -> "None":
    assert PHASE_ORDER == ("expire", "external", "stale", "schedules", "terminal", "events")


async def test_all_enabled_phases_run_once_in_fixed_order() -> "None":
    backend = _StubBackend()
    backend.cleanup_deleted = 3
    event_log = _StubEventLog(deleted=7)
    stub = _StubService(backend=backend, is_external=True, has_schedules=True, event_log=event_log)
    stub.expired_count = 4
    stub.reconcile_result = 2
    stub.recover_result = StaleTaskRecoveryResult(requeued=1, failed=1)
    stub.schedules_result = ScheduleAssertionResult(created=1, repaired=1)
    summary = await _run(
        stub,
        QueueMaintenanceConfig(
            stale_after=60, terminal_retention=3600, event_retention_rules=(QueueEventRetentionRule(max_age=7200),)
        ),
    )

    assert [phase.phase for phase in summary.phases] == list(PHASE_ORDER)
    assert [phase.status for phase in summary.phases] == [
        "completed",
        "completed",
        "completed",
        "completed",
        "completed",
        "completed",
    ]
    assert [phase.changed for phase in summary.phases] == [4, 2, 2, 2, 3, 7]
    assert summary.outcome == "completed"
    assert stub.expire_calls == [1000]
    assert stub.reconcile_calls == [100]
    assert stub.recover_calls == [(timedelta(seconds=60), 100)]
    assert stub.schedules_calls == [100]
    assert backend.cleanup_calls == [(FIXED_NOW - timedelta(seconds=3600), 1000)]
    assert event_log.calls == [(FIXED_NOW - timedelta(seconds=7200), 1000)]


async def test_disabled_phases_are_skipped_without_backend_calls() -> "None":
    backend = _StubBackend()
    stub = _StubService(backend=backend, is_external=False, has_schedules=False)
    summary = await _run(stub, QueueMaintenanceConfig())

    assert [phase.status for phase in summary.phases] == [
        "completed",
        "skipped",
        "skipped",
        "skipped",
        "skipped",
        "skipped",
    ]
    assert summary.outcome == "completed"
    assert stub.expire_calls == [1000]
    assert stub.reconcile_calls == []
    assert stub.recover_calls == []
    assert stub.schedules_calls == []
    assert backend.cleanup_calls == []


async def test_events_phase_skipped_when_no_event_log_configured() -> "None":
    backend = _StubBackend()
    stub = _StubService(backend=backend, event_log=None)
    summary = await _run(stub, QueueMaintenanceConfig(event_retention_rules=(QueueEventRetentionRule(max_age=3600),)))
    events_result = next(phase for phase in summary.phases if phase.phase == "events")
    assert events_result.status == "skipped"


async def test_external_phase_disabled_for_local_execution() -> "None":
    backend = _StubBackend()
    stub = _StubService(backend=backend, is_external=False)
    summary = await _run(stub, QueueMaintenanceConfig())
    external_result = next(phase for phase in summary.phases if phase.phase == "external")
    assert external_result.status == "skipped"
    assert stub.reconcile_calls == []


# --------------------------------------------------------------------------- #
# Cutoffs computed once from injected UTC clock
# --------------------------------------------------------------------------- #


async def test_retention_cutoffs_are_computed_from_start_boundary() -> "None":
    backend = _StubBackend()
    event_log = _StubEventLog()
    stub = _StubService(backend=backend, event_log=event_log)
    await _run(
        stub,
        QueueMaintenanceConfig(terminal_retention=120, event_retention_rules=(QueueEventRetentionRule(max_age=600),)),
    )
    assert backend.cleanup_calls[0][0] == FIXED_NOW - timedelta(seconds=120)
    assert event_log.calls[0][0] == FIXED_NOW - timedelta(seconds=600)


# --------------------------------------------------------------------------- #
# Phase failure continuation + sanitized error
# --------------------------------------------------------------------------- #


async def test_phase_failure_continues_and_marks_outcome_failed() -> "None":
    backend = _StubBackend()
    backend.cleanup_deleted = 4
    stub = _StubService(backend=backend, is_external=True)
    stub.reconcile_error = RuntimeError("connect postgres://admin:s3cret@db:5432/app failed")
    summary = await _run(stub, QueueMaintenanceConfig(terminal_retention=60))

    external = next(phase for phase in summary.phases if phase.phase == "external")
    terminal = next(phase for phase in summary.phases if phase.phase == "terminal")
    assert external.status == "failed"
    assert external.error == "maintenance_phase_failed:RuntimeError"
    assert "postgres" not in (external.error or "")
    assert "s3cret" not in (external.error or "")
    assert external.changed == 0
    assert terminal.status == "completed"
    assert terminal.changed == 4
    assert summary.outcome == "failed"
    assert len(backend.release_calls) == 1


async def test_only_failed_phase_overrides_completed_outcome() -> "None":
    backend = _StubBackend()
    event_log = _StubEventLog(error=ValueError("boom"))
    stub = _StubService(backend=backend, event_log=event_log)
    summary = await _run(
        stub,
        QueueMaintenanceConfig(terminal_retention=60, event_retention_rules=(QueueEventRetentionRule(max_age=60),)),
    )
    assert summary.outcome == "failed"
    events = next(phase for phase in summary.phases if phase.phase == "events")
    assert events.error == "maintenance_phase_failed:ValueError"


@pytest.mark.parametrize(
    ("repair", "status"),
    [
        (DispatchRepairResult(examined=2, changed=2), "completed"),
        (DispatchRepairResult(examined=2, unchanged=2), "completed"),
        (DispatchRepairResult(examined=2, failed=2), "failed"),
        (DispatchRepairResult(examined=3, changed=1, failed=1, unchanged=1, limit_reached=True), "failed"),
        (DispatchRepairResult(examined=3, unchanged=3, limit_reached=True), "partial"),
    ],
)
async def test_repair_mixed_and_budget_outcomes_survive_maintenance(
    repair: "DispatchRepairResult", status: "str"
) -> "None":
    backend = _StubBackend()
    backend.cleanup_deleted = 4
    stub = _StubService(backend=backend, is_external=True)
    stub.repair_result = repair
    summary = await _run(stub, QueueMaintenanceConfig(external_limit=3, terminal_retention=60))
    external = next(phase for phase in summary.phases if phase.phase == "external")
    assert external.status == status
    assert external.changed == repair.changed
    assert external.repair == repair
    assert external.error == ("maintenance_repair_failed" if repair.failed else None)
    assert summary.outcome == status
    terminal = next(phase for phase in summary.phases if phase.phase == "terminal")
    assert terminal.status == "completed" and terminal.changed == 4
    assert len(backend.release_calls) == 1


async def test_repair_mixed_preserves_reconciliation_changes() -> "None":
    stub = _StubService(backend=_StubBackend(), is_external=True)
    stub.repair_result = DispatchRepairResult(examined=2, changed=1, failed=1)
    stub.reconcile_result = 2
    summary = await _run(stub, QueueMaintenanceConfig(external_limit=10))
    external = next(phase for phase in summary.phases if phase.phase == "external")
    assert summary.outcome == "failed"
    assert external.changed == 3


# --------------------------------------------------------------------------- #
# Budget exhaustion with injected monotonic clock
# --------------------------------------------------------------------------- #


async def test_budget_exhaustion_marks_later_phases_partial_without_running_them() -> "None":
    clock = _Clock()
    backend = _StubBackend()
    event_log = _StubEventLog()
    stub = _StubService(backend=backend, is_external=True, event_log=event_log)
    stub.clock = clock
    stub.reconcile_advance = 500.0  # first phase blows the 300s budget
    backend.clock = clock

    summary = await _run(
        stub,
        QueueMaintenanceConfig(
            stale_after=60, terminal_retention=60, event_retention_rules=(QueueEventRetentionRule(max_age=60),)
        ),
        monotonic=clock,
    )

    statuses = {phase.phase: phase.status for phase in summary.phases}
    assert statuses["external"] == "completed"
    assert statuses["stale"] == "partial"
    assert statuses["terminal"] == "partial"
    assert statuses["events"] == "partial"
    assert summary.outcome == "partial"
    assert stub.recover_calls == []
    assert backend.cleanup_calls == []
    assert event_log.calls == []
    assert len(backend.release_calls) == 1


async def test_budget_partial_does_not_override_failed_outcome() -> "None":
    clock = _Clock()
    backend = _StubBackend()
    stub = _StubService(backend=backend, is_external=True)
    stub.clock = clock
    stub.reconcile_error = RuntimeError("x")
    stub.reconcile_advance = 500.0
    summary = await _run(stub, QueueMaintenanceConfig(terminal_retention=60), monotonic=clock)
    assert summary.outcome == "failed"


# --------------------------------------------------------------------------- #
# Phase filtering only narrows
# --------------------------------------------------------------------------- #


async def test_phase_filter_narrows_selected_phases() -> "None":
    backend = _StubBackend()
    backend.cleanup_deleted = 9
    stub = _StubService(backend=backend, is_external=True)
    summary = await _run(stub, QueueMaintenanceConfig(stale_after=60, terminal_retention=60), phases=["terminal"])
    assert [phase.phase for phase in summary.phases] == ["terminal"]
    assert stub.reconcile_calls == []
    assert stub.recover_calls == []
    assert backend.cleanup_calls == [(FIXED_NOW - timedelta(seconds=60), 1000)]


async def test_phase_filter_never_enables_disabled_threshold() -> "None":
    backend = _StubBackend()
    stub = _StubService(backend=backend)
    summary = await _run(stub, QueueMaintenanceConfig(), phases=["terminal"])
    assert [phase.status for phase in summary.phases] == ["skipped"]
    assert backend.cleanup_calls == []


async def test_unknown_phase_name_is_rejected() -> "None":
    backend = _StubBackend()
    stub = _StubService(backend=backend)
    with pytest.raises(QueueConfigurationError):
        await _run(stub, QueueMaintenanceConfig(), phases=cast("list[MaintenancePhase]", ["bogus"]))


# --------------------------------------------------------------------------- #
# JSON-native summaries
# --------------------------------------------------------------------------- #


async def test_summary_payload_is_json_native() -> "None":
    backend = _StubBackend()
    backend.cleanup_deleted = 2
    stub = _StubService(backend=backend, is_external=True)
    stub.reconcile_result = 1
    summary = await _run(stub, QueueMaintenanceConfig(terminal_retention=60))

    payload = summary.to_payload()
    restored = json.loads(json.dumps(payload))
    assert restored["outcome"] == "completed"
    assert restored["acquired"] is True
    assert isinstance(restored["duration_ms"], (int, float))
    assert {phase["phase"] for phase in restored["phases"]} == set(PHASE_ORDER)
    for phase in restored["phases"]:
        assert set(phase) == {"phase", "status", "changed", "duration_ms", "error", "repair"}


def test_phase_result_payload_shape() -> "None":
    result = QueueMaintenancePhaseResult(phase="stale", status="completed", changed=3, duration_ms=1.5)
    assert result.to_payload() == {
        "phase": "stale",
        "status": "completed",
        "changed": 3,
        "duration_ms": 1.5,
        "error": None,
        "repair": None,
    }


async def test_stale_phase_propagates_repair_result_and_fails_when_repair_fails() -> "None":
    backend = _StubBackend()
    stub = _StubService(backend=backend, is_external=False)
    stub.recover_result = StaleTaskRecoveryResult(
        requeued=2,
        failed=0,
        repair=DispatchRepairResult(examined=2, changed=1, failed=1, unchanged=0),
    )
    summary = await _run(stub, QueueMaintenanceConfig(stale_after=60.0), phases=["stale"])
    assert summary.outcome == "failed"
    assert len(summary.phases) == 1
    stale_phase = summary.phases[0]
    assert stale_phase.phase == "stale"
    assert stale_phase.status == "failed"
    assert stale_phase.changed == 2
    assert stale_phase.error == "maintenance_repair_failed"
    assert stale_phase.repair == DispatchRepairResult(examined=2, changed=1, failed=1, unchanged=0)


async def test_schedules_phase_propagates_repair_result_and_fails_when_repair_fails() -> "None":
    backend = _StubBackend()
    stub = _StubService(backend=backend, is_external=False, has_schedules=True)
    stub.schedules_result = ScheduleAssertionResult(
        created=1,
        repaired=1,
        repair=DispatchRepairResult(examined=2, changed=1, failed=1, unchanged=0),
    )
    summary = await _run(stub, QueueMaintenanceConfig(schedules_limit=50), phases=["schedules"])
    assert summary.outcome == "failed"
    assert len(summary.phases) == 1
    schedules_phase = summary.phases[0]
    assert schedules_phase.phase == "schedules"
    assert schedules_phase.status == "failed"
    assert schedules_phase.changed == 2
    assert schedules_phase.error == "maintenance_repair_failed"
    assert schedules_phase.repair == DispatchRepairResult(examined=2, changed=1, failed=1, unchanged=0)
    assert stub.schedules_calls == [50]


def test_maintenance_route_config_validation() -> "None":
    with pytest.raises(QueueConfigurationError, match="requires route protection"):
        QueueMaintenanceConfig(mount_route=True)

    with pytest.raises(QueueConfigurationError, match="oidc_audience is required"):
        QueueMaintenanceConfig(mount_route=True, verify_oidc=True)

    with pytest.raises(QueueConfigurationError, match="route_path"):
        QueueMaintenanceConfig(route_path="/")

    valid_platform = QueueMaintenanceConfig(mount_route=True, trust_platform_auth=True)
    assert valid_platform.mount_route is True
    assert valid_platform.route_path == "/_litestar-queues/maintenance"

    valid_oidc = QueueMaintenanceConfig(
        mount_route=True,
        verify_oidc=True,
        oidc_audience="https://worker.example.run.app",
        oidc_service_account_email="scheduler@example.iam.gserviceaccount.com",
    )
    assert valid_oidc.verify_oidc is True
    assert valid_oidc.oidc_audience == "https://worker.example.run.app"


async def test_maintenance_http_route_executes_six_phase_pass_and_returns_summary() -> "None":
    from litestar import Litestar
    from litestar.testing import AsyncTestClient

    from litestar_queues import QueueConfig, QueuePlugin, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    try:

        @task("tests.maintenance.recurring", interval=60)
        async def recurring_job() -> "None":
            return None

        @task("tests.maintenance.expiring")
        async def expiring_job() -> "None":
            return None

        config = QueueConfig(
            queue_backend="memory",
            execution_backend="local",
            worker=WorkerConfig(placement="external"),
            maintenance=QueueMaintenanceConfig(
                mount_route=True,
                trust_platform_auth=True,
                stale_after=60.0,
            ),
        )
        plugin = QueuePlugin(config)
        route = build_maintenance_route(config)
        assert route.opt.get("exclude_from_csrf") is True
        assert route.opt.get("exclude_from_auth") is True

        app = Litestar(plugins=[plugin])
        async with AsyncTestClient(app=app) as client:
            service = app.state[config.service_state_key]
            await service.get_queue_backend().enqueue(
                "tests.maintenance.expiring",
                expires_at=datetime.now(timezone.utc) - timedelta(seconds=10),
            )
            response = await client.post("/_litestar-queues/maintenance")
            assert response.status_code == 200
            body = response.json()
            assert body["outcome"] == "completed"
            assert body["acquired"] is True
            phases_by_name = {item["phase"]: item for item in body["phases"]}
            assert list(phases_by_name) == list(PHASE_ORDER)
            assert phases_by_name["expire"]["status"] == "completed"
            assert phases_by_name["expire"]["changed"] == 1
            assert phases_by_name["schedules"]["status"] == "completed"
            assert phases_by_name["schedules"]["changed"] == 1
            scheduled = await service.get_queue_backend().get_task_by_key("scheduled:tests.maintenance.recurring")
            assert scheduled is not None
    finally:
        clear_task_registry()



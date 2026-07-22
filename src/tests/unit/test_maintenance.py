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
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.maintenance import LEASE_NAME, PHASE_ORDER
from litestar_queues.models import QueueBackendCapabilities, StaleTaskRecoveryResult

if TYPE_CHECKING:
    from collections.abc import Collection
    from datetime import timedelta as _TimeDelta

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
    """Event-log double recording ``cleanup_before`` calls."""

    def __init__(self, *, deleted: "int" = 0, error: "Exception | None" = None) -> "None":
        self.deleted = deleted
        self.error = error
        self.calls: "list[tuple[datetime, int | None]]" = []

    async def cleanup_before(self, before: "datetime", *, limit: "int | None" = None) -> "int":
        self.calls.append((before, limit))
        if self.error is not None:
            raise self.error
        return self.deleted


class _StubBackend:
    """Queue-backend double implementing the lease + terminal-cleanup surface."""

    def __init__(self, *, supports_lease: "bool" = True, lease_granted: "bool" = True) -> "None":
        self._capabilities = QueueBackendCapabilities(supports_maintenance_lease=supports_lease)
        self.lease_granted = lease_granted
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

    async def acquire_maintenance_lease(self, name: "str", token: "str", *, ttl: "_TimeDelta") -> "bool":
        self.acquire_calls.append((name, token))
        return self.lease_granted

    async def release_maintenance_lease(self, name: "str", token: "str") -> "bool":
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
        event_log: "_StubEventLog | None" = None,
    ) -> "None":
        self._backend = backend
        self._execution_backend = _StubExecutionBackend(is_external=is_external)
        self._event_log = event_log
        self.reconcile_calls: "list[int | None]" = []
        self.recover_calls: "list[tuple[_TimeDelta, int | None]]" = []
        self.reconcile_result = 0
        self.reconcile_error: "Exception | None" = None
        self.recover_result = StaleTaskRecoveryResult()
        self.recover_error: "Exception | None" = None
        self.clock: "_Clock | None" = None
        self.reconcile_advance = 0.0
        self.recover_advance = 0.0

    def get_queue_backend(self) -> "_StubBackend":
        return self._backend

    def get_execution_backend(self) -> "_StubExecutionBackend":
        return self._execution_backend

    def get_event_log(self) -> "_StubEventLog | None":
        return self._event_log

    async def reconcile_external(self, limit: "int | None" = None) -> "int":
        self.reconcile_calls.append(limit)
        if self.clock is not None:
            self.clock.value += self.reconcile_advance
        if self.reconcile_error is not None:
            raise self.reconcile_error
        return self.reconcile_result

    async def recover_stale_tasks(
        self, *, stale_after: "_TimeDelta", worker_id: "str | None" = None, limit: "int | None" = None
    ) -> "StaleTaskRecoveryResult":
        self.recover_calls.append((stale_after, limit))
        if self.clock is not None:
            self.clock.value += self.recover_advance
        if self.recover_error is not None:
            raise self.recover_error
        return self.recover_result


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
    assert config.event_retention is None
    assert config.lease_ttl > config.time_budget


@pytest.mark.parametrize(
    "kwargs",
    [
        {"time_budget": 0},
        {"lease_ttl": 0},
        {"external_limit": 0},
        {"stale_limit": 0},
        {"terminal_limit": 0},
        {"event_limit": 0},
        {"stale_after": 0},
        {"terminal_retention": -1},
        {"event_retention": 0},
        {"lease_ttl": 300.0, "time_budget": 300.0},
        {"lease_ttl": 100.0, "time_budget": 300.0},
    ],
)
def test_config_rejects_invalid_values(kwargs: "dict[str, float]") -> "None":
    with pytest.raises(QueueConfigurationError):
        QueueMaintenanceConfig(**kwargs)  # type: ignore[arg-type]


def test_config_allows_positive_retention() -> "None":
    config = QueueMaintenanceConfig(stale_after=60.0, terminal_retention=3600.0, event_retention=7200.0)
    assert config.stale_after == 60.0


# --------------------------------------------------------------------------- #
# Lease + capability
# --------------------------------------------------------------------------- #


async def test_run_rejects_backend_without_lease_capability() -> "None":
    backend = _StubBackend(supports_lease=False)
    service = _StubService(backend=backend)
    with pytest.raises(QueueConfigurationError):
        await _run(service, QueueMaintenanceConfig())
    assert backend.acquire_calls == []


async def test_lease_denial_is_a_no_op_with_skipped_phases() -> "None":
    backend = _StubBackend(lease_granted=False)
    stub = _StubService(backend=backend, is_external=True)
    stub.reconcile_result = 5
    summary = await _run(stub, QueueMaintenanceConfig(stale_after=60, terminal_retention=60))

    assert summary.outcome == "lease_held"
    assert summary.lease_acquired is False
    assert [phase.status for phase in summary.phases] == ["skipped", "skipped", "skipped", "skipped"]
    assert stub.reconcile_calls == []
    assert stub.recover_calls == []
    assert backend.cleanup_calls == []
    assert backend.release_calls == []


async def test_lease_uses_shared_name_and_is_released() -> "None":
    backend = _StubBackend()
    stub = _StubService(backend=backend)
    await _run(stub, QueueMaintenanceConfig())
    assert [name for name, _ in backend.acquire_calls] == [LEASE_NAME]
    assert [name for name, _ in backend.release_calls] == [LEASE_NAME]
    assert backend.acquire_calls[0][1] == backend.release_calls[0][1]


# --------------------------------------------------------------------------- #
# Phase order, enablement, one-call-per-phase
# --------------------------------------------------------------------------- #


async def test_all_enabled_phases_run_once_in_fixed_order() -> "None":
    backend = _StubBackend()
    backend.cleanup_deleted = 3
    event_log = _StubEventLog(deleted=7)
    stub = _StubService(backend=backend, is_external=True, event_log=event_log)
    stub.reconcile_result = 2
    stub.recover_result = StaleTaskRecoveryResult(requeued=1, failed=1)
    summary = await _run(
        stub,
        QueueMaintenanceConfig(stale_after=60, terminal_retention=3600, event_retention=7200),
    )

    assert [phase.phase for phase in summary.phases] == list(PHASE_ORDER)
    assert [phase.status for phase in summary.phases] == ["completed", "completed", "completed", "completed"]
    assert [phase.changed for phase in summary.phases] == [2, 2, 3, 7]
    assert summary.outcome == "completed"
    assert stub.reconcile_calls == [100]
    assert stub.recover_calls == [(timedelta(seconds=60), 100)]
    assert backend.cleanup_calls == [(FIXED_NOW - timedelta(seconds=3600), 1000)]
    assert event_log.calls == [(FIXED_NOW - timedelta(seconds=7200), 1000)]


async def test_disabled_phases_are_skipped_without_backend_calls() -> "None":
    backend = _StubBackend()
    stub = _StubService(backend=backend, is_external=False)
    summary = await _run(stub, QueueMaintenanceConfig())

    assert [phase.status for phase in summary.phases] == ["skipped", "skipped", "skipped", "skipped"]
    assert summary.outcome == "completed"
    assert stub.reconcile_calls == []
    assert stub.recover_calls == []
    assert backend.cleanup_calls == []


async def test_events_phase_skipped_when_no_event_log_configured() -> "None":
    backend = _StubBackend()
    stub = _StubService(backend=backend, event_log=None)
    summary = await _run(stub, QueueMaintenanceConfig(event_retention=3600))
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
    await _run(stub, QueueMaintenanceConfig(terminal_retention=120, event_retention=600))
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
    summary = await _run(stub, QueueMaintenanceConfig(terminal_retention=60, event_retention=60))
    assert summary.outcome == "failed"
    events = next(phase for phase in summary.phases if phase.phase == "events")
    assert events.error == "maintenance_phase_failed:ValueError"


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
        QueueMaintenanceConfig(stale_after=60, terminal_retention=60, event_retention=60),
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
    summary = await _run(
        stub,
        QueueMaintenanceConfig(stale_after=60, terminal_retention=60),
        phases=["terminal"],
    )
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
    assert restored["lease_acquired"] is True
    assert isinstance(restored["duration_ms"], (int, float))
    assert {phase["phase"] for phase in restored["phases"]} == set(PHASE_ORDER)
    for phase in restored["phases"]:
        assert set(phase) == {"phase", "status", "changed", "duration_ms", "error"}


def test_phase_result_payload_shape() -> "None":
    result = QueueMaintenancePhaseResult(phase="stale", status="completed", changed=3, duration_ms=1.5)
    assert result.to_payload() == {
        "phase": "stale",
        "status": "completed",
        "changed": 3,
        "duration_ms": 1.5,
        "error": None,
    }

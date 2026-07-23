"""Tests for the ``litestar queues`` CLI subcommand group.

The ``run`` subcommand is exercised by ``test_cli_run.py`` via subprocess
because ``CliRunner`` cannot deliver real signals.
"""

import json
import os
import sys
from typing import TYPE_CHECKING, Any, cast

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator
    from uuid import UUID

    from click.testing import Result

    from litestar_queues.models import QueuedTaskRecord

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def _evict_support_modules() -> "Iterator[None]":
    """Force re-import of tests.support.* between tests so task_modules re-register cleanly.

    ``clean_task_registry`` (autouse, project conftest) clears the registry but
    ``load_task_modules`` short-circuits on ``_loaded_modules``; evicting
    forces re-import so the canary task is registered for each invocation.

    Yields:
        Control to the test before clearing cached support modules.
    """
    yield
    for name in list(sys.modules):
        if name == "tests._factories.queue_tasks":
            del sys.modules[name]
    from litestar_queues.task import _loaded_modules

    _loaded_modules.discard("tests._factories.queue_tasks")


def _runner_invoke(app_target: "str", args: "list[str]", monkeypatch: "pytest.MonkeyPatch") -> "Result":
    """Invoke ``litestar`` CLI with the configured app target via ``CliRunner``.

    Returns:
        The ``Result`` object so callers can assert on ``exit_code``, ``stdout``, and ``stderr``.
    """
    from click.testing import CliRunner
    from litestar.cli.main import litestar_group

    monkeypatch.setenv("LITESTAR_APP", app_target)
    runner = CliRunner()
    return runner.invoke(litestar_group, args, catch_exceptions=False)


def test_litestar_queues_help_lists_subcommands(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.support.cli_app:app", ["queues", "--help"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    assert "run" in result.stdout
    assert "status" in result.stdout
    assert "scheduler-health" in result.stdout
    assert "run-task" in result.stdout


def test_queues_run_task_consumes_record_via_config_factory(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """``litestar queues run-task`` resolves CONFIG_FACTORY, claims the record, and runs it."""
    from types import ModuleType

    import anyio

    from litestar_queues import QueueConfig, QueueService, task
    from litestar_queues.backends import InMemoryQueueBackend

    queue_backend = InMemoryQueueBackend()

    @task("tasks.cli_execute")
    async def cli_execute(value: "int") -> "int":
        return value + 1

    task_id: "UUID | None" = None

    async def _seed() -> "None":
        nonlocal task_id
        async with QueueService(QueueConfig(execution_backend="cloudrun"), queue_backend=queue_backend) as service:
            result = await service.enqueue(cli_execute.using(execution_backend="cloudrun"), 41)
            task_id = result.id

    anyio.run(_seed)
    assert task_id is not None

    factory_module = ModuleType("cli_execute_config_factory")
    factory_module.create_service = lambda: QueueService(  # type: ignore[attr-defined]
        QueueConfig(execution_backend="cloudrun"), queue_backend=queue_backend
    )
    sys.modules[factory_module.__name__] = factory_module
    monkeypatch.setenv("LITESTAR_QUEUES_CONFIG_FACTORY", f"{factory_module.__name__}:create_service")
    monkeypatch.setenv("LITESTAR_QUEUES_TASK_ID", str(task_id))
    try:
        result = _runner_invoke("tests.support.cli_app:app", ["queues", "run-task"], monkeypatch)
    finally:
        sys.modules.pop(factory_module.__name__, None)

    assert result.exit_code == 0, result.output

    async def _fetch() -> "QueuedTaskRecord | None":
        assert task_id is not None
        return await queue_backend.get_task(task_id)

    stored = anyio.run(_fetch)
    assert stored is not None
    assert stored.status == "completed"
    assert stored.result == 42


def test_run_task_by_id_flag_runs_existing_record_without_env(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """``run-task --task-id <id> --config-factory mod:call`` runs a local one-shot with NO env vars."""
    from types import ModuleType

    import anyio

    from litestar_queues import QueueConfig, QueueService, task
    from litestar_queues.backends import InMemoryQueueBackend

    queue_backend = InMemoryQueueBackend()

    @task("tasks.cli_by_id")
    async def cli_by_id(value: "int") -> "int":
        return value + 1

    task_id: "UUID | None" = None

    async def _seed() -> "None":
        nonlocal task_id
        async with QueueService(QueueConfig(execution_backend="cloudrun"), queue_backend=queue_backend) as service:
            result = await service.enqueue(cli_by_id.using(execution_backend="cloudrun"), 41)
            task_id = result.id

    anyio.run(_seed)
    assert task_id is not None

    factory_module = ModuleType("cli_by_id_config_factory")
    factory_module.create_service = lambda: QueueService(  # type: ignore[attr-defined]
        QueueConfig(execution_backend="cloudrun"), queue_backend=queue_backend
    )
    sys.modules[factory_module.__name__] = factory_module
    for name in [key for key in os.environ if key.startswith("LITESTAR_QUEUES_")]:
        monkeypatch.delenv(name, raising=False)
    try:
        result = _runner_invoke(
            "tests.support.cli_app:app",
            [
                "queues",
                "run-task",
                "--task-id",
                str(task_id),
                "--config-factory",
                f"{factory_module.__name__}:create_service",
            ],
            monkeypatch,
        )
    finally:
        sys.modules.pop(factory_module.__name__, None)

    assert result.exit_code == 0, result.output

    async def _fetch() -> "QueuedTaskRecord | None":
        assert task_id is not None
        return await queue_backend.get_task(task_id)

    stored = anyio.run(_fetch)
    assert stored is not None
    assert stored.status == "completed"
    assert stored.result == 42


def test_cli_module_exposes_public_command_callbacks() -> "None":
    from litestar_queues import _cli

    expected_callbacks = {
        "run": "run_command",
        "run-maintenance": "run_maintenance_command",
        "scheduler-health": "scheduler_health_command",
        "status": "status_command",
    }

    for command_name, callback_name in expected_callbacks.items():
        command = _cli.queues_group.commands[command_name]
        assert command.callback is not None
        assert command.callback.__name__ == callback_name
        assert hasattr(_cli, callback_name)
        assert not hasattr(_cli, f"_{callback_name}")


def test_status_subcommand_default_table(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.support.cli_app:app", ["queues", "status"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    assert "pending" in result.stdout
    assert "total" in result.stdout


def test_status_subcommand_json(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.support.cli_app:app", ["queues", "status", "--json"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    payload = json.loads(result.stdout)
    expected_keys = {"pending", "scheduled", "running", "completed", "failed", "cancelled", "total"}
    assert expected_keys.issubset(payload.keys())
    assert all(isinstance(payload[key], int) for key in expected_keys)


def test_status_subcommand_advisory_queue_filter(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.support.cli_app:app", ["queues", "status", "--queue", "billing"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    assert "advisory" in result.stderr.lower()


def test_scheduler_health_returns_4_when_no_canary_runs(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """Canary task is registered but never executed, so the health check is stale."""
    result = _runner_invoke("tests.support.cli_app:app", ["queues", "scheduler-health", "--minutes", "5"], monkeypatch)

    assert result.exit_code == 4
    assert "stale" in result.stderr.lower()


def test_scheduler_health_returns_3_when_canary_not_registered(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.support.cli_app_missing_canary:app", ["queues", "scheduler-health"], monkeypatch)

    assert result.exit_code == 3
    assert "canary" in result.stderr.lower()


def test_litestar_queues_help_lists_run_maintenance(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.support.cli_app:app", ["queues", "--help"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    assert "run-maintenance" in result.stdout


def test_run_maintenance_help_exposes_only_phase_and_json(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.support.cli_app:app", ["queues", "run-maintenance", "--help"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    assert "--phase" in result.stdout
    assert "--json" in result.stdout
    # No destructive threshold/limit flags are exposed.
    for forbidden in ("--limit", "--retention", "--stale-after", "--terminal", "--budget"):
        assert forbidden not in result.stdout


def test_run_maintenance_reports_missing_maintenance_config(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.support.cli_app:app", ["queues", "run-maintenance"], monkeypatch)

    assert result.exit_code == 1
    assert "not configured" in result.stderr.lower()


# --------------------------------------------------------------------------- #
# ``_maintain_run`` behavior with fakes (no real backend required).
# --------------------------------------------------------------------------- #


class _FakeMaintenanceBackend:
    def __init__(self, *, supports_maintenance: "bool" = True) -> "None":
        from litestar_queues.models import QueueBackendCapabilities

        self._capabilities = QueueBackendCapabilities(supports_maintenance=supports_maintenance)

    @property
    def capabilities(self) -> "object":
        return self._capabilities


class _FakeMaintenanceServiceHost:
    def __init__(
        self,
        backend: "_FakeMaintenanceBackend",
        *,
        open_error: "Exception | None" = None,
        close_error: "Exception | None" = None,
    ) -> "None":
        self._backend = backend
        self._close_error = close_error
        self._open_error = open_error
        self.open_calls = 0
        self.close_calls = 0

    async def open(self) -> "None":
        self.open_calls += 1
        if self._open_error is not None:
            raise self._open_error

    async def close(self) -> "None":
        self.close_calls += 1
        if self._close_error is not None:
            raise self._close_error

    def get_queue_backend(self) -> "_FakeMaintenanceBackend":
        return self._backend


class _FakePlugin:
    def __init__(self, config: "object", service: "_FakeMaintenanceServiceHost") -> "None":
        self._config = config
        self._service = service

    @property
    def config(self) -> "object":
        return self._config

    def get_service(self, state: "object | None" = None) -> "_FakeMaintenanceServiceHost":
        return self._service


def _install_fake_maintenance(monkeypatch: "pytest.MonkeyPatch", summary: "object") -> "dict[str, object]":
    captured: "dict[str, object]" = {}
    from litestar_queues import _cli

    class _FakeMaintenanceService:
        def __init__(self, service: "object", config: "object") -> "None":
            captured["service"] = service
            captured["config"] = config

        async def run(self, phases: "object" = None) -> "object":
            captured["phases"] = phases
            return summary

    monkeypatch.setattr(_cli, "QueueMaintenanceService", _FakeMaintenanceService)
    return captured


def _install_failing_maintenance(monkeypatch: "pytest.MonkeyPatch", error: "Exception") -> "None":
    from litestar_queues import _cli

    class _FailingMaintenanceService:
        def __init__(self, service: "object", config: "object") -> "None":
            pass

        async def run(self, phases: "object" = None) -> "object":
            raise error

    monkeypatch.setattr(_cli, "QueueMaintenanceService", _FailingMaintenanceService)


def _maintenance_config(**kwargs: "object") -> "object":
    from litestar_queues import QueueConfig, QueueMaintenanceConfig

    return QueueConfig(queue_backend="redis", maintenance=QueueMaintenanceConfig(**kwargs))  # type: ignore[arg-type]


def _summary(outcome: "str", *, error: "str | None" = None) -> "object":
    from litestar_queues.maintenance import QueueMaintenancePhaseResult, QueueMaintenanceSummary

    return QueueMaintenanceSummary(
        outcome=outcome,  # type: ignore[arg-type]
        acquired=outcome != "already_running",
        duration_ms=12.5,
        phases=[
            QueueMaintenancePhaseResult(
                phase="terminal",
                status="failed" if error is not None else "completed",
                changed=3,
                duration_ms=4.0,
                error=error,
            )
        ],
    )


async def test_maintain_completed_exits_0_and_emits_json(
    monkeypatch: "pytest.MonkeyPatch", capsys: "pytest.CaptureFixture[str]"
) -> "None":
    from litestar_queues import _cli

    service = _FakeMaintenanceServiceHost(_FakeMaintenanceBackend())
    plugin = _FakePlugin(_maintenance_config(terminal_retention=60), service)
    _install_fake_maintenance(monkeypatch, _summary("completed"))

    code = await _cli._maintain_run(cast("Any", plugin), (), True)

    assert code == 0
    assert service.open_calls == 1
    assert service.close_calls == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["outcome"] == "completed"
    assert payload["acquired"] is True
    assert payload["phases"][0]["phase"] == "terminal"
    assert set(payload) == {"outcome", "acquired", "duration_ms", "phases"}


async def test_maintain_human_output_is_one_summary_table(
    monkeypatch: "pytest.MonkeyPatch", capsys: "pytest.CaptureFixture[str]"
) -> "None":
    from litestar_queues import _cli

    service = _FakeMaintenanceServiceHost(_FakeMaintenanceBackend())
    plugin = _FakePlugin(_maintenance_config(terminal_retention=60), service)
    _install_fake_maintenance(monkeypatch, _summary("completed"))

    code = await _cli._maintain_run(cast("Any", plugin), (), False)

    out = capsys.readouterr().out
    assert code == 0
    assert "outcome: completed" in out
    assert "terminal" in out


async def test_maintain_human_output_includes_sanitized_phase_error(
    monkeypatch: "pytest.MonkeyPatch", capsys: "pytest.CaptureFixture[str]"
) -> "None":
    from litestar_queues import _cli

    service = _FakeMaintenanceServiceHost(_FakeMaintenanceBackend())
    plugin = _FakePlugin(_maintenance_config(terminal_retention=60), service)
    _install_fake_maintenance(monkeypatch, _summary("failed", error="maintenance_phase_failed:RuntimeError"))

    code = await _cli._maintain_run(cast("Any", plugin), (), False)

    out = capsys.readouterr().out
    assert code == 1
    assert "maintenance_phase_failed:RuntimeError" in out


async def test_maintain_narrows_selected_phases(monkeypatch: "pytest.MonkeyPatch") -> "None":
    from litestar_queues import _cli

    service = _FakeMaintenanceServiceHost(_FakeMaintenanceBackend())
    plugin = _FakePlugin(_maintenance_config(terminal_retention=60, event_retention=60), service)
    captured = _install_fake_maintenance(monkeypatch, _summary("completed"))

    await _cli._maintain_run(cast("Any", plugin), ("terminal", "events"), False)

    assert captured["phases"] == ("terminal", "events")


async def test_maintain_already_running_exits_0(monkeypatch: "pytest.MonkeyPatch") -> "None":
    from litestar_queues import _cli

    service = _FakeMaintenanceServiceHost(_FakeMaintenanceBackend())
    plugin = _FakePlugin(_maintenance_config(terminal_retention=60), service)
    _install_fake_maintenance(monkeypatch, _summary("already_running"))

    code = await _cli._maintain_run(cast("Any", plugin), (), False)

    assert code == 0


async def test_maintain_partial_exits_2(monkeypatch: "pytest.MonkeyPatch") -> "None":
    from litestar_queues import _cli

    service = _FakeMaintenanceServiceHost(_FakeMaintenanceBackend())
    plugin = _FakePlugin(_maintenance_config(terminal_retention=60), service)
    _install_fake_maintenance(monkeypatch, _summary("partial"))

    code = await _cli._maintain_run(cast("Any", plugin), (), False)

    assert code == 2


async def test_maintain_failed_exits_1(monkeypatch: "pytest.MonkeyPatch") -> "None":
    from litestar_queues import _cli

    service = _FakeMaintenanceServiceHost(_FakeMaintenanceBackend())
    plugin = _FakePlugin(_maintenance_config(terminal_retention=60), service)
    _install_fake_maintenance(monkeypatch, _summary("failed"))

    code = await _cli._maintain_run(cast("Any", plugin), (), False)

    assert code == 1


async def test_maintain_rejects_memory_backend_before_opening(monkeypatch: "pytest.MonkeyPatch") -> "None":
    from litestar_queues import QueueConfig, QueueMaintenanceConfig, _cli

    service = _FakeMaintenanceServiceHost(_FakeMaintenanceBackend())
    config = QueueConfig(queue_backend="memory", maintenance=QueueMaintenanceConfig(terminal_retention=60))
    plugin = _FakePlugin(config, service)

    code = await _cli._maintain_run(cast("Any", plugin), (), False)

    assert code == 1
    assert service.open_calls == 0


async def test_maintain_rejects_backend_without_maintenance_capability(monkeypatch: "pytest.MonkeyPatch") -> "None":
    from litestar_queues import _cli

    service = _FakeMaintenanceServiceHost(_FakeMaintenanceBackend(supports_maintenance=False))
    plugin = _FakePlugin(_maintenance_config(terminal_retention=60), service)

    code = await _cli._maintain_run(cast("Any", plugin), (), False)

    assert code == 1
    # The service is opened for the capability check, then cleanly shut down.
    assert service.open_calls == 1
    assert service.close_calls == 1


@pytest.mark.parametrize(
    ("failure_stage", "expected_code"),
    [
        ("open", "maintenance_open_failed:RuntimeError"),
        ("run", "maintenance_run_failed:RuntimeError"),
        ("close", "maintenance_close_failed:RuntimeError"),
    ],
)
async def test_maintain_sanitizes_lifecycle_errors_and_closes_once(
    monkeypatch: "pytest.MonkeyPatch", capsys: "pytest.CaptureFixture[str]", failure_stage: "str", expected_code: "str"
) -> "None":
    from litestar_queues import _cli

    secret = "postgresql://admin:super-secret@example.invalid/queues"
    error = RuntimeError(secret)
    service = _FakeMaintenanceServiceHost(
        _FakeMaintenanceBackend(),
        open_error=error if failure_stage == "open" else None,
        close_error=error if failure_stage == "close" else None,
    )
    plugin = _FakePlugin(_maintenance_config(terminal_retention=60), service)
    if failure_stage == "run":
        _install_failing_maintenance(monkeypatch, error)
    else:
        _install_fake_maintenance(monkeypatch, _summary("completed"))

    code = await _cli._maintain_run(cast("Any", plugin), (), False)

    captured = capsys.readouterr()
    assert code == 1
    assert service.open_calls == 1
    assert service.close_calls == 1
    assert expected_code in captured.err
    assert secret not in captured.err
    assert secret not in captured.out
    assert "Apply the current queue backend migrations" in captured.err


def test_scheduler_health_returns_0_when_canary_completed_recently(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """Seed a completed canary record and assert exit 0 with healthy message."""
    import anyio

    async def _seed_canary() -> "None":
        import importlib

        cli_app = importlib.import_module("tests.support.cli_app")
        from litestar_queues import QueuePlugin
        from litestar_queues.task import get_task_registry, load_task_modules

        plugin = next(p for p in cli_app.app.plugins if isinstance(p, QueuePlugin))
        config = plugin.config

        load_task_modules(config.task_modules)
        service = plugin.get_service()
        await service.open()
        try:
            canary_task = get_task_registry()[config.scheduler_canary_task]
            record = await service.enqueue(canary_task)
            await service.get_queue_backend().complete_task(record.id, result=None)
        finally:
            await service.close()

    anyio.run(_seed_canary)

    result = _runner_invoke("tests.support.cli_app:app", ["queues", "scheduler-health"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    assert "healthy" in result.stdout.lower()

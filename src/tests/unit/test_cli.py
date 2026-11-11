"""Tests for the ``litestar queues`` CLI subcommand group.

The ``run`` subcommand is exercised by ``test_cli_run.py`` via subprocess
because ``CliRunner`` cannot deliver real signals.
"""

import json
import os
import sys
from types import ModuleType
from typing import TYPE_CHECKING, Any, cast

import anyio
import pytest
from litestar import Litestar

from litestar_queues import QueueConfig, QueuePlugin, WorkerConfig, _cli, task
from litestar_queues.events import QueueEventRetentionRule
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import Iterator
    from uuid import UUID

    from click.testing import Result

    from litestar_queues.models import QueuedTaskRecord

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def _evict_support_modules() -> "Iterator[None]":
    """Force re-import of tests.helpers.support.* between tests so task_modules re-register cleanly.

    ``clean_task_registry`` (autouse, project conftest) clears the registry but
    ``load_task_modules`` short-circuits on ``_loaded_modules``; evicting
    forces re-import so the canary task is registered for each invocation.

    Yields:
        Control to the test before clearing cached support modules.
    """
    yield
    for name in list(sys.modules):
        if name == "tests.helpers.queue_tasks":
            del sys.modules[name]
    from litestar_queues.task import _loaded_modules

    _loaded_modules.discard("tests.helpers.queue_tasks")


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
    result = _runner_invoke("tests.helpers.support.cli_app:app", ["queues", "--help"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    assert "list" in result.stdout
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
        async with QueueService(
            QueueConfig(
                worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="cloudrun"
            ),
            queue_backend=queue_backend,
        ) as service:
            result = await service.enqueue(cli_execute.using(execution_backend="cloudrun"), 41)
            task_id = result.id

    anyio.run(_seed)
    assert task_id is not None

    factory_module = ModuleType("cli_execute_config_factory")
    factory_module.create_service = lambda: QueueService(  # type: ignore[attr-defined]
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="cloudrun"),
        queue_backend=queue_backend,
    )
    sys.modules[factory_module.__name__] = factory_module
    monkeypatch.setenv("QUEUES_CONFIG_FACTORY", f"{factory_module.__name__}:create_service")
    monkeypatch.setenv("QUEUES_TASK_ID", str(task_id))
    try:
        result = _runner_invoke("tests.helpers.support.cli_app:app", ["queues", "run-task"], monkeypatch)
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
        async with QueueService(
            QueueConfig(
                worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="cloudrun"
            ),
            queue_backend=queue_backend,
        ) as service:
            result = await service.enqueue(cli_by_id.using(execution_backend="cloudrun"), 41)
            task_id = result.id

    anyio.run(_seed)
    assert task_id is not None

    factory_module = ModuleType("cli_by_id_config_factory")
    factory_module.create_service = lambda: QueueService(  # type: ignore[attr-defined]
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="cloudrun"),
        queue_backend=queue_backend,
    )
    sys.modules[factory_module.__name__] = factory_module
    for name in [key for key in os.environ if key.startswith("LITESTAR_QUEUES_")]:
        monkeypatch.delenv(name, raising=False)
    try:
        result = _runner_invoke(
            "tests.helpers.support.cli_app:app",
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
        "list": "list_command",
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
    result = _runner_invoke("tests.helpers.support.cli_app:app", ["queues", "status"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    assert "pending" in result.stdout
    assert "expired" in result.stdout
    assert "total" in result.stdout


def test_status_subcommand_json(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.helpers.support.cli_app:app", ["queues", "status", "--json"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    payload = json.loads(result.stdout)
    expected_keys = {"pending", "scheduled", "running", "completed", "failed", "cancelled", "expired", "total"}
    assert expected_keys.issubset(payload.keys())
    assert all(isinstance(payload[key], int) for key in expected_keys)


def test_status_subcommand_enforces_queue_filter_without_advisory(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke(
        "tests.helpers.support.cli_app:app", ["queues", "status", "--queue", "billing"], monkeypatch
    )

    assert result.exit_code == 0, result.stderr
    assert result.stderr == ""


def test_scheduler_health_returns_4_when_no_canary_runs(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """Canary task is registered but never executed, so the health check is stale."""
    result = _runner_invoke(
        "tests.helpers.support.cli_app:app", ["queues", "scheduler-health", "--minutes", "5"], monkeypatch
    )

    assert result.exit_code == 4
    assert "stale" in result.stderr.lower()


def test_scheduler_health_returns_3_when_canary_not_registered(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke(
        "tests.helpers.support.cli_app_missing_canary:app", ["queues", "scheduler-health"], monkeypatch
    )

    assert result.exit_code == 3
    assert "canary" in result.stderr.lower()


def test_litestar_queues_help_lists_run_maintenance(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.helpers.support.cli_app:app", ["queues", "--help"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    assert "run-maintenance" in result.stdout


def test_run_maintenance_help_exposes_only_phase_and_json(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.helpers.support.cli_app:app", ["queues", "run-maintenance", "--help"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    assert "--phase" in result.stdout
    assert "--json" in result.stdout
    for forbidden in ("--limit", "--retention", "--stale-after", "--terminal", "--budget"):
        assert forbidden not in result.stdout


def test_run_maintenance_reports_missing_maintenance_config(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.helpers.support.cli_app:app", ["queues", "run-maintenance"], monkeypatch)

    assert result.exit_code == 1
    assert "not configured" in result.stderr.lower()


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


@pytest.mark.parametrize("mixed", [False, True])
@pytest.mark.parametrize("as_json", [False, True])
async def test_repair_failure_real_maintenance_exits_1(
    capsys: "pytest.CaptureFixture[str]", mixed: "bool", as_json: "bool"
) -> "None":
    """Provider repair failures reach the actual maintenance CLI with partial progress."""
    from litestar_queues import QueueConfig, QueueMaintenanceConfig, QueueService, _cli
    from litestar_queues.backends.memory import InMemoryQueueBackend
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend, CloudTasksExecutionConfig
    from tests.unit.execution.cloudtasks._fakes import CreateCall, FakeCloudTasksClient, ServiceUnavailable

    execution_config = CloudTasksExecutionConfig(
        project_id="example-project",
        location="us-central1",
        queue_id="queue-consumer",
        service_url="https://queue-consumer-abcdef-uc.a.run.app",
        service_account_email="queues@example-project.iam.gserviceaccount.com",
        trust_platform_auth=True,
    )
    storage = InMemoryQueueBackend()
    client = FakeCloudTasksClient()
    execution = CloudTasksExecutionBackend(execution_config=execution_config, client=client)
    config = QueueConfig(
        queue_backend="redis",
        execution_backend=execution_config,
        worker=WorkerConfig(placement="external"),
        maintenance=QueueMaintenanceConfig(external_limit=10),
    )
    service = QueueService(config, queue_backend=storage, execution_backend=execution)
    first = await storage.enqueue("repair.first", execution_backend="cloudtasks")
    await storage.enqueue("repair.second", execution_backend="cloudtasks")

    async def fail_delivery(call: "CreateCall") -> "None":
        if not mixed or json.loads(call.body)["task_id"] == str(first.id):
            msg = "secret-provider-credential"
            raise ServiceUnavailable(msg)

    client.on_create = fail_delivery
    plugin = _FakePlugin(config, cast("Any", service))
    code = await _cli._maintain_run(cast("Any", plugin), (), as_json)
    output = capsys.readouterr().out
    assert code == 1
    assert "secret-provider-credential" not in output
    failed = 1 if mixed else 2
    changed = 1 if mixed else 0
    if as_json:
        payload = json.loads(output)
        assert payload["outcome"] == "failed"
        external = next(phase for phase in payload["phases"] if phase["phase"] == "external")
        assert external["changed"] == changed
        assert external["repair"] == {
            "examined": 2,
            "changed": changed,
            "failed": failed,
            "unchanged": 0,
            "limit_reached": False,
        }
    else:
        assert "outcome: failed" in output
        assert f"failed={failed}" in output
        assert f"changed={changed}" in output


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
    plugin = _FakePlugin(
        _maintenance_config(terminal_retention=60, event_retention_rules=(QueueEventRetentionRule(max_age=60),)),
        service,
    )
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
    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        maintenance=QueueMaintenanceConfig(terminal_retention=60),
    )
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

        cli_app = importlib.import_module("tests.helpers.support.cli_app")
        from litestar_queues import QueuePlugin
        from litestar_queues.task import get_task_registry, load_task_modules

        plugin = next(p for p in cli_app.app.plugins if isinstance(p, QueuePlugin))
        config = plugin.config

        load_task_modules(config.task_modules)
        service = plugin.get_service()
        await service.open()
        try:
            canary_task = get_task_registry()[cast("str", config.scheduler_canary_task)]
            record = await service.enqueue(canary_task)
            await service.get_queue_backend().complete_task(record.id, result=None)
        finally:
            await service.close()

    anyio.run(_seed_canary)

    result = _runner_invoke("tests.helpers.support.cli_app:app", ["queues", "scheduler-health"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    assert "healthy" in result.stdout.lower()


async def test_cli_maintain_run_enters_app_lifespan_when_configured(
    monkeypatch: "pytest.MonkeyPatch", capsys: "pytest.CaptureFixture[str]"
) -> "None":
    from litestar import Litestar

    from litestar_queues import QueueConfig, QueueMaintenanceConfig, QueuePlugin, _cli
    from litestar_queues.task import get_default_service

    events: "list[str]" = []

    async def on_startup() -> "None":
        events.append(f"startup:{get_default_service() is not None}")

    async def on_shutdown() -> "None":
        events.append("shutdown")

    service = _FakeMaintenanceServiceHost(_FakeMaintenanceBackend())
    plugin = QueuePlugin(
        QueueConfig(
            queue_backend="redis",
            worker=WorkerConfig(placement="external", run_app_lifespan=True),
            maintenance=QueueMaintenanceConfig(terminal_retention=60),
        )
    )
    app = Litestar(plugins=[plugin], on_startup=[on_startup], on_shutdown=[on_shutdown])
    monkeypatch.setattr(_cli, "_open_service", lambda _plugin: service)
    _install_fake_maintenance(monkeypatch, _summary("completed"))

    code = await _cli._maintain_run(plugin, (), True, app=app)
    assert code == 0
    assert events == ["startup:True", "shutdown"]
    assert get_default_service() is None
    assert json.loads(capsys.readouterr().out)["outcome"] == "completed"


def _invoke_litestar_app(app: "Litestar", args: "list[str]", monkeypatch: "pytest.MonkeyPatch") -> "Result":
    """Invoke ``litestar`` CLI against an isolated in-process ``Litestar`` instance."""
    module_name = "tests_cli_isolated_app"
    app_module = ModuleType(module_name)
    setattr(app_module, "app", app)
    sys.modules[module_name] = app_module
    try:
        return _runner_invoke(f"{module_name}:app", args, monkeypatch)
    finally:
        sys.modules.pop(module_name, None)


def _seed_cli_list_app() -> "tuple[Litestar, dict[str, UUID]]":
    """Create an in-memory ``Litestar`` app seeded with four tasks across queues and statuses."""
    plugin = QueuePlugin(QueueConfig(queue_backend="memory", worker=WorkerConfig(placement="asgi")))
    app = Litestar(plugins=[plugin])

    @task("tasks.cli_a", queue="default")
    async def cli_a() -> "str":
        return "a"

    @task("tasks.cli_b", queue="emails")
    async def cli_b() -> "str":
        return "b"

    @task("tasks.cli_c", queue="emails")
    async def cli_c() -> "str":
        return "c"

    @task("tasks.cli_d", queue="billing")
    async def cli_d() -> "str":
        return "d"

    ids: "dict[str, UUID]" = {}

    async def _seed() -> "None":
        service = plugin.get_service()
        await service.open()
        try:
            backend = service.get_queue_backend()
            res_a = await service.enqueue(cli_a, key="k1")
            await anyio.sleep(0.002)
            res_b = await service.enqueue(cli_b, key="k2")
            await backend.complete_task(res_b.id, result="ok")
            await anyio.sleep(0.002)
            res_c = await service.enqueue(cli_c, key="k3")
            await backend.fail_task(res_c.id, "boom", retry=False)
            await anyio.sleep(0.002)
            res_d = await service.enqueue(cli_d, key="k4")
            await backend.complete_task(res_d.id, result="ok")
            ids.update({"a": res_a.id, "b": res_b.id, "c": res_c.id, "d": res_d.id})
        finally:
            await service.close()

    anyio.run(_seed)
    return app, ids


def test_queues_list_help_exposes_options(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.helpers.support.cli_app:app", ["queues", "list", "--help"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    assert "--status" in result.stdout
    assert "--queue" in result.stdout
    assert "--task-name" in result.stdout
    assert "--key" in result.stdout
    assert "--limit" in result.stdout
    assert "--offset" in result.stdout
    assert "--json" in result.stdout


def test_queues_list_empty_table_output(monkeypatch: "pytest.MonkeyPatch") -> "None":
    app = Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend="memory", worker=WorkerConfig(placement="asgi")))])

    result = _invoke_litestar_app(app, ["queues", "list"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    assert "no tasks found (total=0, limit=50, offset=0, has_more=false)" in result.stdout


def test_queues_list_filtered_paginated_json(monkeypatch: "pytest.MonkeyPatch") -> "None":
    app, ids = _seed_cli_list_app()

    result = _invoke_litestar_app(
        app,
        [
            "queues",
            "list",
            "--status",
            "completed",
            "--status",
            "failed",
            "--queue",
            "emails",
            "--limit",
            "1",
            "--offset",
            "1",
            "--json",
        ],
        monkeypatch,
    )

    assert result.exit_code == 0, result.stderr
    lines = result.stdout.strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["total"] == 2
    assert payload["limit"] == 1
    assert payload["offset"] == 1
    assert payload["has_more"] is False
    assert len(payload["items"]) == 1
    item = payload["items"][0]
    assert item["id"] == str(ids["b"])
    assert item["status"] == "completed"
    assert item["queue"] == "emails"
    assert item["key"] == "k2"
    expected_keys = {
        "id",
        "task_name",
        "queue",
        "status",
        "key",
        "priority",
        "retry_count",
        "max_retries",
        "execution_backend",
        "execution_profile",
        "execution_ref",
        "worker_id",
        "created_at",
        "queued_at",
        "scheduled_at",
        "started_at",
        "completed_at",
        "heartbeat_at",
        "expires_at",
        "error",
    }
    assert set(item) == expected_keys
    assert {"args", "kwargs", "result", "metadata"}.isdisjoint(item)


def test_queues_list_filters_by_task_name_and_key_and_renders_table(monkeypatch: "pytest.MonkeyPatch") -> "None":
    app, ids = _seed_cli_list_app()

    json_result = _invoke_litestar_app(
        app, ["queues", "list", "--task-name", "tasks.cli_a", "--key", "k1", "--json"], monkeypatch
    )

    assert json_result.exit_code == 0, json_result.stderr
    payload = json.loads(json_result.stdout)
    assert payload["total"] == 1
    assert len(payload["items"]) == 1
    assert payload["items"][0]["id"] == str(ids["a"])
    assert payload["items"][0]["key"] == "k1"
    assert payload["items"][0]["task_name"] == "tasks.cli_a"

    table_result = _invoke_litestar_app(app, ["queues", "list", "--queue", "emails"], monkeypatch)

    assert table_result.exit_code == 0, table_result.stderr
    assert "ID" in table_result.stdout
    assert "Status" in table_result.stdout
    assert "Queue" in table_result.stdout
    assert "Task" in table_result.stdout
    assert "Created" in table_result.stdout
    assert "Key" in table_result.stdout
    assert str(ids["b"]) in table_result.stdout
    assert str(ids["c"]) in table_result.stdout
    assert "total=2 limit=50 offset=0 has_more=false" in table_result.stdout


def test_queues_list_json_handles_unserializable_task_payloads(monkeypatch: "pytest.MonkeyPatch") -> "None":
    plugin = QueuePlugin(QueueConfig(queue_backend="memory", worker=WorkerConfig(placement="asgi")))
    app = Litestar(plugins=[plugin])

    @task("tasks.non_json")
    async def non_json(payload: "object") -> "object":
        return payload

    async def _seed() -> "None":
        service = plugin.get_service()
        await service.open()
        try:
            enqueued = await service.enqueue(non_json, object(), metadata={"raw": object()})
            await service.get_queue_backend().complete_task(enqueued.id, result=object())
        finally:
            await service.close()

    anyio.run(_seed)

    result = _invoke_litestar_app(app, ["queues", "list", "--json"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["total"] == 1
    assert len(payload["items"]) == 1
    assert payload["items"][0]["task_name"] == "tasks.non_json"
    assert {"args", "kwargs", "result", "metadata"}.isdisjoint(payload["items"][0])


def test_queues_list_rejects_ephemeral_backend(monkeypatch: "pytest.MonkeyPatch") -> "None":
    app = Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend="ephemeral"))])

    result = _invoke_litestar_app(app, ["queues", "list"], monkeypatch)

    normalized_output = " ".join(result.output.replace("│", " ").split())
    assert result.exit_code != 0
    assert "ephemeral" in normalized_output.lower()
    assert "litestar queues list" in normalized_output


@pytest.mark.parametrize(
    "args",
    [["queues", "list", "--limit", "0"], ["queues", "list", "--offset", "-1"], ["queues", "list", "--status", "bogus"]],
)
def test_queues_list_rejects_invalid_options(args: "list[str]", monkeypatch: "pytest.MonkeyPatch") -> "None":
    app = Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend="memory", worker=WorkerConfig(placement="asgi")))])

    result = _invoke_litestar_app(app, args, monkeypatch)

    assert result.exit_code == 2


class _FakeListServiceHost:
    def __init__(self, *, open_error: "Exception | None" = None, list_error: "Exception | None" = None) -> "None":
        self._open_error = open_error
        self._list_error = list_error
        self.open_calls = 0
        self.close_calls = 0

    async def open(self) -> "None":
        self.open_calls += 1
        if self._open_error is not None:
            raise self._open_error

    async def close(self) -> "None":
        self.close_calls += 1

    async def list_tasks(self, **kwargs: "object") -> "object":
        if self._list_error is not None:
            raise self._list_error
        return None


class _FakeListPlugin:
    def __init__(self, service: "_FakeListServiceHost") -> "None":
        self._service = service

    def get_service(self, state: "object | None" = None) -> "_FakeListServiceHost":
        return self._service


@pytest.mark.parametrize(("failure_stage", "expected_error"), [("open", "open failed"), ("list", "bad limit")])
async def test_list_run_closes_service_cleanly_on_error(
    capsys: "pytest.CaptureFixture[str]", failure_stage: "str", expected_error: "str"
) -> "None":
    service = _FakeListServiceHost(
        open_error=QueueConfigurationError(expected_error) if failure_stage == "open" else None,
        list_error=QueueConfigurationError(expected_error) if failure_stage == "list" else None,
    )
    plugin = _FakeListPlugin(service)

    code = await _cli._list_run(
        cast("Any", plugin), statuses=(), queue_filter=None, task_name=None, key=None, limit=50, offset=0, as_json=False
    )

    captured = capsys.readouterr()
    assert code == 1
    assert service.open_calls == 1
    assert service.close_calls == 1
    assert f"error: {expected_error}" in captured.err

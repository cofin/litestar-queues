"""Explicit worker placement: config matrix, launch proof, and server ownership."""

import json
import os
import signal
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from litestar import Litestar
from litestar.plugins import CLIPlugin, InitPlugin

from litestar_queues import QueueConfig, QueuePlugin, WorkerConfig
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import Iterator

    from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig

pytestmark = pytest.mark.anyio


# --------------------------------------------------------------------------- defaults


def test_zero_argument_config_is_ephemeral_local_server() -> "None":
    """The zero-setup contract stays background execution, now server-owned."""
    config = QueueConfig()

    assert config.queue_backend == "ephemeral"
    assert config.execution_backend == "local"
    assert config.worker.placement == "server"


def test_low_level_backend_factory_keeps_its_explicit_memory_default() -> "None":
    """``get_queue_backend()`` has no server lifecycle, so its default is unchanged."""
    from litestar_queues.backends.factory import get_queue_backend
    from litestar_queues.backends.memory import InMemoryQueueBackend

    assert isinstance(get_queue_backend(), InMemoryQueueBackend)
    assert QueueConfig().queue_backend != "memory"


def test_run_in_app_is_deleted_not_aliased() -> "None":
    assert not hasattr(WorkerConfig(), "run_in_app")
    with pytest.raises(TypeError):
        WorkerConfig(run_in_app=False)  # type: ignore[call-arg]


def test_worker_placement_is_publicly_exported() -> "None":
    import litestar_queues
    from litestar_queues.config import WorkerPlacement

    assert litestar_queues.WorkerPlacement is WorkerPlacement
    assert "WorkerPlacement" in litestar_queues.__all__


# --------------------------------------------------------------------------- matrix

_VALID = [
    ("ephemeral", "local", "server"),
    ("ephemeral", "local", "asgi"),
    ("ephemeral", "local", "external"),
    ("ephemeral", "immediate", "external"),
    ("memory", "local", "asgi"),
    ("memory", "immediate", "external"),
    ("memory", "local", "external"),
    ("redis", "local", "server"),
    ("redis", "local", "asgi"),
    ("redis", "local", "external"),
]

_INVALID = [
    ("ephemeral", "immediate", "server"),
    ("ephemeral", "immediate", "asgi"),
    ("memory", "local", "server"),
    ("memory", "immediate", "server"),
    ("memory", "immediate", "asgi"),
    ("redis", "immediate", "server"),
    ("redis", "immediate", "asgi"),
]


@pytest.mark.parametrize(("backend", "execution", "placement"), _VALID)
def test_valid_placement_combinations(backend: "str", execution: "str", placement: "str") -> "None":
    config = QueueConfig(queue_backend=backend, execution_backend=execution, worker=WorkerConfig(placement=placement))  # type: ignore[arg-type]

    assert config.worker.placement == placement


@pytest.mark.parametrize(("backend", "execution", "placement"), _INVALID)
def test_invalid_placement_combinations(backend: "str", execution: "str", placement: "str") -> "None":
    with pytest.raises(QueueConfigurationError):
        QueueConfig(queue_backend=backend, execution_backend=execution, worker=WorkerConfig(placement=placement))  # type: ignore[arg-type]


def test_unknown_placement_is_rejected() -> "None":
    with pytest.raises(QueueConfigurationError):
        WorkerConfig(placement="inline")  # type: ignore[arg-type]


async def test_ephemeral_backend_refuses_to_open_without_a_prepared_database() -> "None":
    """Placement does not decide whether an ephemeral database exists.

    The database is created by whatever entered
    :class:`EphemeralServerContext`, so the backend's own open path is what
    enforces its presence. Rejecting a placement at configuration time
    restated that check as a proxy and blocked embedders that create the
    database themselves.
    """
    from litestar_queues.backends.ephemeral import EphemeralQueueBackend

    config = QueueConfig(queue_backend="ephemeral", worker=WorkerConfig(placement="external"))

    with pytest.raises(QueueConfigurationError, match="Litestar CLI server lifespan"):
        await EphemeralQueueBackend(config).open()


def test_validation_uses_backend_names_not_backend_instances() -> "None":
    """A typed selector is validated by name so no driver extra is imported."""
    import sys

    class _Selector:
        backend_name = "memory"

    before = set(sys.modules)
    with pytest.raises(QueueConfigurationError):
        QueueConfig(queue_backend=_Selector(), worker=WorkerConfig(placement="server"))  # type: ignore[arg-type]
    assert not {name for name in set(sys.modules) - before if name.startswith(("redis", "sqlspec"))}


# --------------------------------------------------------------------------- self-dispatching execution


def _cloud_tasks_config() -> "CloudTasksExecutionConfig":
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig

    return CloudTasksExecutionConfig(
        project_id="example-project",
        location="us-central1",
        queue_id="queue-consumer",
        service_url="https://queue-consumer-abcdef-uc.a.run.app",
        service_account_email="queues@example-project.iam.gserviceaccount.com",
        trust_platform_auth=True,
    )


@pytest.mark.parametrize("placement", ["server", "asgi"])
def test_cloud_tasks_requires_external_placement(placement: "str") -> "None":
    """Cloud Tasks schedules delivery itself, so a managed worker would double-dispatch."""
    with pytest.raises(QueueConfigurationError):
        QueueConfig(
            queue_backend="redis",
            execution_backend=_cloud_tasks_config(),
            worker=WorkerConfig(placement=placement),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("backend", ["memory", "ephemeral"])
def test_cloud_tasks_requires_shared_persistent_storage(backend: "str") -> "None":
    """Delivery arrives in a different process, which cannot see process-local storage."""
    with pytest.raises(QueueConfigurationError):
        QueueConfig(
            queue_backend=backend, execution_backend=_cloud_tasks_config(), worker=WorkerConfig(placement="external")
        )


def test_cloud_tasks_with_shared_storage_and_external_placement_is_valid() -> "None":
    config = QueueConfig(
        queue_backend="redis", execution_backend=_cloud_tasks_config(), worker=WorkerConfig(placement="external")
    )

    assert config.worker.placement == "external"


def test_cloud_tasks_placement_validation_imports_no_google_package() -> "None":
    """The rule is keyed on the selector name, so the extra stays optional."""
    import sys

    before = set(sys.modules)
    with pytest.raises(QueueConfigurationError):
        QueueConfig(queue_backend="redis", execution_backend="cloudtasks", worker=WorkerConfig(placement="server"))
    assert not {name for name in set(sys.modules) - before if name.startswith("google.cloud.tasks")}


# --------------------------------------------------------------------------- invocation marker


def _write_marker(directory: "Path", *, nonce: "str", owner_pid: "int", version: "int" = 1) -> "Path":
    marker = directory / "server.json"
    marker.write_text(json.dumps({"version": version, "owner_pid": owner_pid, "nonce": nonce}), encoding="utf-8")
    marker.chmod(0o600)
    return marker


@pytest.fixture
def clean_proof_environment() -> "Iterator[None]":
    from litestar_queues.worker.invocation import _ACTIVE_SERVER_CONTEXTS, MARKER_ENV_VAR, NONCE_ENV_VAR

    saved = {name: os.environ.get(name) for name in (MARKER_ENV_VAR, NONCE_ENV_VAR)}
    for name in saved:
        os.environ.pop(name, None)
    active = set(_ACTIVE_SERVER_CONTEXTS)
    _ACTIVE_SERVER_CONTEXTS.clear()
    yield
    _ACTIVE_SERVER_CONTEXTS.clear()
    _ACTIVE_SERVER_CONTEXTS.update(active)
    for name, value in saved.items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value


@pytest.mark.usefixtures("clean_proof_environment")
def test_server_context_sets_two_separate_variables_and_clears_them() -> "None":
    from litestar_queues.worker.invocation import MARKER_ENV_VAR, NONCE_ENV_VAR, server_context, server_context_active

    with server_context() as nonce:
        marker = Path(os.environ[MARKER_ENV_VAR])
        assert os.environ[NONCE_ENV_VAR] == nonce
        assert marker.is_absolute()
        assert marker.is_file()
        assert server_context_active() is True
        directory = marker.parent

    assert NONCE_ENV_VAR not in os.environ
    assert MARKER_ENV_VAR not in os.environ
    assert not marker.exists()
    assert not directory.exists()
    assert server_context_active() is False


@pytest.mark.usefixtures("clean_proof_environment")
def test_server_context_derives_private_environment_and_resource_names() -> "None":
    from litestar_queues import QueueNamespace
    from litestar_queues.worker.invocation import server_context, server_context_active

    names = QueueNamespace("myapp")
    nonce_env = names.environment("server", "nonce")
    marker_env = names.environment("server", "marker")

    with server_context(names) as nonce:
        marker = Path(os.environ[marker_env])
        assert os.environ[nonce_env] == nonce
        assert marker.parent.name.startswith("myapp-server-")
        assert server_context_active(names) is True

    assert nonce_env not in os.environ
    assert marker_env not in os.environ
    assert server_context_active(names) is False


@pytest.mark.usefixtures("clean_proof_environment")
def test_marker_holds_the_exact_json_shape() -> "None":
    from litestar_queues.worker.invocation import MARKER_ENV_VAR, server_context

    with server_context() as nonce:
        document = json.loads(Path(os.environ[MARKER_ENV_VAR]).read_text(encoding="utf-8"))

    assert document == {"version": 1, "owner_pid": os.getpid(), "nonce": nonce}
    assert len(nonce) == 32


@pytest.mark.usefixtures("clean_proof_environment")
def test_nested_server_contexts_are_rejected() -> "None":
    from litestar_queues.worker.invocation import MARKER_ENV_VAR, NONCE_ENV_VAR, server_context

    with server_context():
        outer_marker = os.environ[MARKER_ENV_VAR]
        with pytest.raises(QueueConfigurationError), server_context():
            pytest.fail("a nested server context must not be entered")
        assert os.environ[MARKER_ENV_VAR] == outer_marker

    assert NONCE_ENV_VAR not in os.environ


@pytest.mark.usefixtures("clean_proof_environment")
def test_preexisting_environment_is_refused_not_trusted() -> "None":
    from litestar_queues.worker.invocation import NONCE_ENV_VAR, server_context

    os.environ[NONCE_ENV_VAR] = "inherited-and-stale"
    with pytest.raises(QueueConfigurationError), server_context():
        pytest.fail("a pre-existing marker must never be reused")


@pytest.mark.usefixtures("clean_proof_environment")
def test_sequential_contexts_use_fresh_nonces_and_leave_nothing_behind() -> "None":
    from litestar_queues.worker.invocation import MARKER_ENV_VAR, NONCE_ENV_VAR, server_context

    seen = []
    for _ in range(3):
        with server_context() as nonce:
            seen.append((nonce, Path(os.environ[MARKER_ENV_VAR])))
        assert NONCE_ENV_VAR not in os.environ

    assert len({nonce for nonce, _ in seen}) == 3
    assert not any(marker.exists() or marker.parent.exists() for _, marker in seen)


@pytest.mark.usefixtures("clean_proof_environment")
def test_same_process_marker_requires_the_entered_context(tmp_path: "Path") -> "None":
    """Hand-set environment in the owning process does not prove ownership."""
    from litestar_queues.worker.invocation import MARKER_ENV_VAR, NONCE_ENV_VAR, server_context_active

    marker = _write_marker(tmp_path, nonce="forged", owner_pid=os.getpid())
    os.environ[NONCE_ENV_VAR] = "forged"
    os.environ[MARKER_ENV_VAR] = str(marker)

    assert server_context_active() is False


@pytest.mark.usefixtures("clean_proof_environment")
def test_descendant_process_accepts_the_inherited_marker(tmp_path: "Path") -> "None":
    """Uvicorn reload and Granian insert intermediate processes, so no parent check."""
    from litestar_queues.worker.invocation import MARKER_ENV_VAR, NONCE_ENV_VAR, server_context_active

    marker = _write_marker(tmp_path, nonce="inherited", owner_pid=os.getpid() + 1)
    os.environ[NONCE_ENV_VAR] = "inherited"
    os.environ[MARKER_ENV_VAR] = str(marker)

    assert server_context_active() is True


@pytest.mark.usefixtures("clean_proof_environment")
def test_marker_never_probes_owner_liveness() -> "None":
    """A descendant must not require its recorded owner pid to still be running."""
    import inspect

    from litestar_queues.worker import invocation

    source = inspect.getsource(invocation)
    assert "os.kill" not in source
    assert "getppid" not in source


@pytest.mark.usefixtures("clean_proof_environment")
@pytest.mark.parametrize(
    "document",
    [
        {"version": 2, "owner_pid": 1, "nonce": "inherited"},
        {"version": 1, "owner_pid": "1", "nonce": "inherited"},
        {"version": 1, "nonce": "inherited"},
        {"version": 1, "owner_pid": 1, "nonce": "inherited", "extra": True},
        {"version": 1, "owner_pid": 1, "nonce": "a-different-invocation"},
    ],
)
def test_malformed_or_mismatched_markers_are_rejected(tmp_path: "Path", document: "dict[str, object]") -> "None":
    from litestar_queues.worker.invocation import MARKER_ENV_VAR, NONCE_ENV_VAR, server_context_active

    marker = tmp_path / "server.json"
    marker.write_text(json.dumps(document), encoding="utf-8")
    os.environ[NONCE_ENV_VAR] = "inherited"
    os.environ[MARKER_ENV_VAR] = str(marker)

    assert server_context_active() is False


@pytest.mark.usefixtures("clean_proof_environment")
def test_unparsable_and_missing_markers_are_rejected(tmp_path: "Path") -> "None":
    from litestar_queues.worker.invocation import MARKER_ENV_VAR, NONCE_ENV_VAR, server_context_active

    os.environ[NONCE_ENV_VAR] = "inherited"
    os.environ[MARKER_ENV_VAR] = str(tmp_path / "absent.json")
    assert server_context_active() is False

    broken = tmp_path / "broken.json"
    broken.write_text("{not json", encoding="utf-8")
    os.environ[MARKER_ENV_VAR] = str(broken)
    assert server_context_active() is False

    os.environ[MARKER_ENV_VAR] = str(tmp_path)
    assert server_context_active() is False


@pytest.mark.usefixtures("clean_proof_environment")
def test_relative_marker_paths_are_rejected(tmp_path: "Path") -> "None":
    from litestar_queues.worker.invocation import MARKER_ENV_VAR, NONCE_ENV_VAR, server_context_active

    _write_marker(tmp_path, nonce="inherited", owner_pid=os.getpid() + 1)
    os.environ[NONCE_ENV_VAR] = "inherited"
    os.environ[MARKER_ENV_VAR] = "server.json"

    assert server_context_active() is False


@pytest.mark.usefixtures("clean_proof_environment")
def test_marker_path_with_spaces_and_unicode_is_not_delimiter_parsed(tmp_path: "Path") -> "None":
    """Two variables instead of one encoded value keeps odd paths intact."""
    from litestar_queues.worker.invocation import MARKER_ENV_VAR, NONCE_ENV_VAR, server_context_active

    directory = tmp_path / "C: Program Files" / "ünïcödé queue:1"
    directory.mkdir(parents=True)
    marker = _write_marker(directory, nonce="inherited", owner_pid=os.getpid() + 1)
    os.environ[NONCE_ENV_VAR] = "inherited"
    os.environ[MARKER_ENV_VAR] = str(marker)

    assert server_context_active() is True


# --------------------------------------------------------------------------- server lifespan


class _FakeSupervisor:
    """Records supervisor ownership without starting a process."""

    events: "list[str]" = []
    databases: "list[str]" = []
    fail_on_start = False

    def __init__(self, config: "QueueConfig") -> "None":
        self.config = config
        type(self).events.append("construct")

    @classmethod
    def from_plugin(cls, plugin: "QueuePlugin") -> "_FakeSupervisor":
        return cls(plugin.config)

    def start(self) -> "None":
        from litestar_queues.backends.ephemeral import PATH_ENV_VAR

        type(self).events.append("start")
        path = os.environ.get(PATH_ENV_VAR)
        if path is not None:
            type(self).databases.append(path)
        if type(self).fail_on_start:
            msg = "Server queue worker failed during bootstrap (TimeoutError)."
            raise QueueConfigurationError(msg)

    def close(self) -> "None":
        type(self).events.append("close")


@pytest.fixture
def fake_supervisor(monkeypatch: "pytest.MonkeyPatch") -> "Iterator[type[_FakeSupervisor]]":
    from litestar_queues.worker import supervisor

    _FakeSupervisor.events = []
    _FakeSupervisor.databases = []
    _FakeSupervisor.fail_on_start = False
    monkeypatch.setattr(supervisor, "ServerWorkerSupervisor", _FakeSupervisor)
    monkeypatch.setenv("LITESTAR_APP", "tests.helpers.support.cli_app:app")
    yield _FakeSupervisor


@pytest.mark.usefixtures("clean_proof_environment")
@pytest.mark.parametrize("placement", ["asgi", "external"])
def test_non_server_placements_own_no_supervisor(fake_supervisor: "type[_FakeSupervisor]", placement: "str") -> "None":
    plugin = QueuePlugin(QueueConfig(queue_backend="redis", worker=WorkerConfig(placement=placement)))  # type: ignore[arg-type]
    app = Litestar(plugins=[plugin], route_handlers=[])

    with plugin.server_lifespan(app):
        pass

    assert fake_supervisor.events == []


@pytest.mark.usefixtures("clean_proof_environment")
def test_server_placement_routes_a_console_break_through_the_interpreter(
    fake_supervisor: "type[_FakeSupervisor]", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """The storage teardown only runs if Ctrl+Break leaves the interpreter unwinding.

    Uvicorn re-raises the signal that stopped it, and the Windows default for a
    console break terminates the process without unwinding, taking the private
    database removal with it.
    """
    from litestar_queues.worker import invocation

    spare = signal.SIGBREAK if os.name == "nt" else signal.SIGUSR1  # type: ignore[attr-defined]
    monkeypatch.setattr(invocation, "_sys_platform", lambda: "win32")
    monkeypatch.setattr(signal, "SIGBREAK", spare, raising=False)
    original = signal.getsignal(spare)
    plugin = QueuePlugin(QueueConfig(queue_backend="redis", worker=WorkerConfig(placement="server")))
    app = Litestar(plugins=[plugin], route_handlers=[])

    with plugin.server_lifespan(app):
        installed = signal.getsignal(spare)

    assert installed is invocation._raise_keyboard_interrupt
    assert signal.getsignal(spare) is original


@pytest.mark.usefixtures("clean_proof_environment")
def test_server_placement_starts_before_yield_and_closes_after(fake_supervisor: "type[_FakeSupervisor]") -> "None":
    from litestar_queues.worker.invocation import server_context_active

    plugin = QueuePlugin(QueueConfig(queue_backend="redis", worker=WorkerConfig(placement="server")))
    app = Litestar(plugins=[plugin], route_handlers=[])

    with plugin.server_lifespan(app):
        assert fake_supervisor.events == ["construct", "start"]
        assert server_context_active() is True

    assert fake_supervisor.events == ["construct", "start", "close"]
    assert server_context_active() is False


@pytest.mark.usefixtures("clean_proof_environment")
def test_body_exception_still_closes_the_supervisor(fake_supervisor: "type[_FakeSupervisor]") -> "None":
    from litestar_queues.worker.invocation import MARKER_ENV_VAR

    plugin = QueuePlugin(QueueConfig(queue_backend="redis", worker=WorkerConfig(placement="server")))
    app = Litestar(plugins=[plugin], route_handlers=[])

    with pytest.raises(RuntimeError), plugin.server_lifespan(app):
        msg = "server crashed"
        raise RuntimeError(msg)

    assert fake_supervisor.events == ["construct", "start", "close"]
    assert MARKER_ENV_VAR not in os.environ


@pytest.mark.usefixtures("clean_proof_environment")
def test_supervisor_start_failure_does_not_yield_and_removes_the_marker(
    fake_supervisor: "type[_FakeSupervisor]",
) -> "None":
    from litestar_queues.worker.invocation import MARKER_ENV_VAR, NONCE_ENV_VAR

    fake_supervisor.fail_on_start = True
    plugin = QueuePlugin(QueueConfig(queue_backend="redis", worker=WorkerConfig(placement="server")))
    app = Litestar(plugins=[plugin], route_handlers=[])

    with pytest.raises(QueueConfigurationError), plugin.server_lifespan(app):
        pytest.fail("a failed supervisor start must not yield to the server")

    assert fake_supervisor.events == ["construct", "start"]
    assert MARKER_ENV_VAR not in os.environ
    assert NONCE_ENV_VAR not in os.environ


@pytest.mark.usefixtures("clean_proof_environment")
def test_missing_app_path_fails_before_any_supervisor_or_marker(
    fake_supervisor: "type[_FakeSupervisor]", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    from litestar_queues.worker.invocation import MARKER_ENV_VAR

    monkeypatch.delenv("LITESTAR_APP", raising=False)
    plugin = QueuePlugin(QueueConfig(queue_backend="redis", worker=WorkerConfig(placement="server")))
    app = Litestar(plugins=[plugin], route_handlers=[])

    with pytest.raises(QueueConfigurationError, match="--app"), plugin.server_lifespan(app):
        pytest.fail("autodiscovery is not supported for server placement")

    assert fake_supervisor.events == []
    assert MARKER_ENV_VAR not in os.environ


@pytest.mark.usefixtures("clean_proof_environment")
@pytest.mark.parametrize(("field", "value"), [("queue_backend", "memory"), ("execution_backend", "immediate")])
def test_server_lifespan_revalidates_backend_selection(
    fake_supervisor: "type[_FakeSupervisor]", field: "str", value: "str"
) -> "None":
    """Post-construction mutation must not reach a supervisor or a database."""
    from litestar_queues.worker.invocation import MARKER_ENV_VAR

    config = QueueConfig(queue_backend="redis", worker=WorkerConfig(placement="server"))
    app = Litestar(plugins=[QueuePlugin(config)], route_handlers=[])
    plugin = next(p for p in app.plugins if isinstance(p, QueuePlugin))
    setattr(config, field, value)

    with pytest.raises(QueueConfigurationError), plugin.server_lifespan(app):
        pytest.fail("an invalid server configuration must not start a worker")

    assert fake_supervisor.events == []
    assert MARKER_ENV_VAR not in os.environ


@pytest.mark.usefixtures("clean_proof_environment")
def test_default_placement_owns_one_private_database(fake_supervisor: "type[_FakeSupervisor]") -> "None":
    """The ephemeral database exists before the child starts and is gone after close."""
    from litestar_queues.backends.ephemeral import NONCE_ENV_VAR as EPHEMERAL_NONCE
    from litestar_queues.backends.ephemeral import PATH_ENV_VAR

    plugin = QueuePlugin(QueueConfig())
    app = Litestar(plugins=[plugin], route_handlers=[])

    with plugin.server_lifespan(app):
        database = Path(os.environ[PATH_ENV_VAR])
        assert database.is_file()
        assert os.environ[EPHEMERAL_NONCE]
        assert fake_supervisor.databases == [str(database)]

    assert PATH_ENV_VAR not in os.environ
    assert EPHEMERAL_NONCE not in os.environ
    for suffix in ("", "-wal", "-shm"):
        assert not database.with_name(f"{database.name}{suffix}").exists()
    assert not database.parent.exists()


@pytest.mark.usefixtures("clean_proof_environment")
def test_ephemeral_database_shares_the_invocation_nonce(fake_supervisor: "type[_FakeSupervisor]") -> "None":
    from litestar_queues.backends.ephemeral import NONCE_ENV_VAR as EPHEMERAL_NONCE
    from litestar_queues.backends.ephemeral import PATH_ENV_VAR
    from litestar_queues.backends.ephemeral.schema import SCHEMA_VERSION, read_runtime
    from litestar_queues.worker.invocation import NONCE_ENV_VAR

    plugin = QueuePlugin(QueueConfig())
    app = Litestar(plugins=[plugin], route_handlers=[])

    with plugin.server_lifespan(app):
        assert os.environ[EPHEMERAL_NONCE] == os.environ[NONCE_ENV_VAR]
        assert read_runtime(os.environ[PATH_ENV_VAR]) == (SCHEMA_VERSION, os.environ[NONCE_ENV_VAR])

    assert fake_supervisor.events == ["construct", "start", "close"]


@pytest.mark.usefixtures("clean_proof_environment")
def test_persistent_backend_creates_no_database(fake_supervisor: "type[_FakeSupervisor]") -> "None":
    from litestar_queues.backends.ephemeral import PATH_ENV_VAR

    plugin = QueuePlugin(QueueConfig(queue_backend="redis", worker=WorkerConfig(placement="server")))
    app = Litestar(plugins=[plugin], route_handlers=[])

    with plugin.server_lifespan(app):
        assert PATH_ENV_VAR not in os.environ

    assert fake_supervisor.databases == []


# --------------------------------------------------------------------------- CLI integration


def test_plugin_is_a_concrete_cli_plugin() -> "None":
    """Litestar only registers server lifespans for concrete ``CLIPlugin`` instances."""
    plugin = QueuePlugin(QueueConfig(queue_backend="redis", worker=WorkerConfig(placement="external")))
    app = Litestar(plugins=[plugin], route_handlers=[])

    assert isinstance(plugin, InitPlugin)
    assert isinstance(plugin, CLIPlugin)
    assert any(getattr(manager, "__self__", None) is plugin for manager in app._server_lifespan_managers)


@pytest.mark.usefixtures("clean_proof_environment")
def test_litestar_server_lifespan_enters_the_plugin_manager_once(fake_supervisor: "type[_FakeSupervisor]") -> "None":
    from litestar.cli.commands.core import _server_lifespan

    plugin = QueuePlugin(QueueConfig(queue_backend="redis", worker=WorkerConfig(placement="server")))
    app = Litestar(plugins=[plugin], route_handlers=[])

    with _server_lifespan(app):
        assert fake_supervisor.events == ["construct", "start"]

    assert fake_supervisor.events == ["construct", "start", "close"]


def test_package_has_no_server_specific_branching() -> "None":
    """``CLIPlugin.server_lifespan`` is the entire integration contract."""
    root = Path(__file__).resolve().parents[2] / "litestar_queues"
    offenders = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*.py")
        if any(marker in path.read_text(encoding="utf-8") for marker in ("litestar_granian", "GranianPlugin"))
    }

    assert offenders == set()


def test_package_never_parses_server_command_flags() -> "None":
    root = Path(__file__).resolve().parents[2] / "litestar_queues"
    offenders = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*.py")
        if any(marker in path.read_text(encoding="utf-8") for marker in ("sys.argv", "--workers", "--reload"))
    }

    assert offenders == set()


# --------------------------------------------------------------------------- ASGI ownership


@pytest.mark.usefixtures("clean_proof_environment")
async def test_server_placement_opens_the_service_but_owns_no_asgi_worker(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """With a valid marker the ASGI process enqueues only; the worker lives elsewhere."""
    from litestar.testing import AsyncTestClient

    from litestar_queues import QueueService
    from litestar_queues.backends.redis import RedisQueueBackend
    from litestar_queues.worker.invocation import server_context

    async def skip_redis_readiness(_self: "RedisQueueBackend") -> "None":
        return None

    monkeypatch.setattr(RedisQueueBackend, "_require_maintenance_indexes", skip_redis_readiness)
    plugin = QueuePlugin(QueueConfig(queue_backend="redis", worker=WorkerConfig(placement="server")))
    app = Litestar(plugins=[plugin], route_handlers=[])

    with server_context():
        async with AsyncTestClient(app=app):
            assert isinstance(app.state["queue_service"], QueueService)
            assert "queue_worker" not in app.state


@pytest.mark.usefixtures("clean_proof_environment")
async def test_server_placement_without_a_marker_fails_before_serving() -> "None":
    from litestar.testing import AsyncTestClient

    plugin = QueuePlugin(QueueConfig(queue_backend="redis", worker=WorkerConfig(placement="server")))
    app = Litestar(plugins=[plugin], route_handlers=[])

    with pytest.raises(BaseException) as failure:
        async with AsyncTestClient(app=app):
            pytest.fail("a raw ASGI launch must not accept traffic under server placement")

    assert "litestar run" in str(failure.value) or any(
        "litestar run" in str(inner) for inner in getattr(failure.value, "exceptions", ())
    )


@pytest.mark.usefixtures("clean_proof_environment")
async def test_external_placement_opens_the_service_and_starts_nothing() -> "None":
    from litestar.testing import AsyncTestClient

    from litestar_queues import QueueService

    plugin = QueuePlugin(QueueConfig(queue_backend="memory", worker=WorkerConfig(placement="external")))
    app = Litestar(plugins=[plugin], route_handlers=[])

    async with AsyncTestClient(app=app):
        assert isinstance(app.state["queue_service"], QueueService)
        assert "queue_worker" not in app.state


async def test_asgi_placement_is_explicitly_multiplicative() -> "None":
    """Two application instances mean two workers; that is the documented trade-off."""
    from litestar.testing import AsyncTestClient

    workers = []
    for index in range(2):
        plugin = QueuePlugin(
            QueueConfig(
                queue_backend="memory",
                worker=WorkerConfig(placement="asgi", poll_interval=0.01, id=f"asgi-worker-{index}"),
            )
        )
        app = Litestar(plugins=[plugin], route_handlers=[])
        async with AsyncTestClient(app=app):
            worker = app.state["queue_worker"]
            assert worker.is_running
            workers.append(worker)

    assert len({id(worker) for worker in workers}) == 2
    assert [worker.worker_id for worker in workers] == ["asgi-worker-0", "asgi-worker-1"]
    assert not any(worker.is_running for worker in workers)


@pytest.mark.usefixtures("clean_proof_environment")
def test_server_placement_rejects_a_process_local_channels_backend(fake_supervisor: "type[_FakeSupervisor]") -> "None":
    """A worker in its own process cannot publish into this process's memory Channels."""
    from litestar.channels import ChannelsPlugin
    from litestar.channels.backends.memory import MemoryChannelsBackend

    from litestar_queues.events import EventDeliveryConfig, QueueEventsConfig

    channels = ChannelsPlugin(backend=MemoryChannelsBackend(), arbitrary_channels_allowed=True)
    plugin = QueuePlugin(
        QueueConfig(
            queue_backend="redis",
            worker=WorkerConfig(placement="server"),
            events=QueueEventsConfig(channels=channels, delivery=EventDeliveryConfig()),
        )
    )
    app = Litestar(plugins=[channels, plugin], route_handlers=[])

    with pytest.raises(QueueConfigurationError, match="MemoryChannelsBackend"), plugin.server_lifespan(app):
        pytest.fail("live delivery through a process-local backend must not start a worker")

    assert fake_supervisor.events == []


@pytest.mark.usefixtures("clean_proof_environment")
def test_server_placement_accepts_a_shared_channels_backend(fake_supervisor: "type[_FakeSupervisor]") -> "None":
    """Any backend that can cross a process boundary is accepted; only memory is rejected."""
    from litestar.channels import ChannelsPlugin

    from litestar_queues.events import EventDeliveryConfig, QueueEventsConfig

    class SharedChannelsBackend:
        """Stands in for a broker-backed Channels backend."""

    channels = ChannelsPlugin(backend=SharedChannelsBackend(), arbitrary_channels_allowed=True)  # type: ignore[arg-type]
    plugin = QueuePlugin(
        QueueConfig(
            queue_backend="redis",
            worker=WorkerConfig(placement="server"),
            events=QueueEventsConfig(channels=channels, delivery=EventDeliveryConfig()),
        )
    )
    app = Litestar(plugins=[plugin], route_handlers=[])

    with plugin.server_lifespan(app):
        pass

    assert fake_supervisor.events == ["construct", "start", "close"]


@pytest.mark.usefixtures("clean_proof_environment")
def test_streaming_without_live_delivery_allows_memory_channels(fake_supervisor: "type[_FakeSupervisor]") -> "None":
    """Only live delivery fans out from the worker; a read-only stream mount is fine."""
    from litestar.channels import ChannelsPlugin
    from litestar.channels.backends.memory import MemoryChannelsBackend

    from litestar_queues.events import EventStreamConfig, QueueEventsConfig

    channels = ChannelsPlugin(backend=MemoryChannelsBackend(), arbitrary_channels_allowed=True)
    plugin = QueuePlugin(
        QueueConfig(
            queue_backend="redis",
            worker=WorkerConfig(placement="server"),
            events=QueueEventsConfig(channels=channels, stream=EventStreamConfig(unauthenticated_access="allow")),
        )
    )
    app = Litestar(plugins=[channels, plugin], route_handlers=[])

    with plugin.server_lifespan(app):
        pass

    assert fake_supervisor.events == ["construct", "start", "close"]

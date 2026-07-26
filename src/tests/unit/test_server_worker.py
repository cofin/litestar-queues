"""Tests for the private Litestar server-worker supervisor."""

import asyncio
import contextlib
import logging
import os
import signal
import subprocess
import sys
import threading
from collections.abc import Callable, Sequence
from dataclasses import fields
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest
from pytest import MonkeyPatch

from litestar_queues import QueueConfig, WorkerConfig
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.worker.runtime import _WorkerStage, _WorkerStageError
from litestar_queues.worker.supervisor import (
    ServerWorkerSupervisor,
    _apply_launch_spec,
    _build_launch_spec,
    _force_stop_process,
    _kill_windows_process_tree,
    _parent_loss_bridge,
    _QueueProcessCleanupError,
    _request_server_shutdown,
    _reset_child_logging,
    _resolve_queue_plugin,
    _run_child,
    _select_process_context,
    _verified_kill_process_group,
    _worker_process_main,
    _WorkerLaunchSpec,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


@contextlib.contextmanager
def _restored_logging_state() -> "Iterator[None]":
    """Snapshot and restore every logger's handlers and level.

    ``_reset_child_logging`` tears down all inherited handlers, which is correct in a
    fresh worker child but destructive here: run inside the pytest process it also
    removes pytest's own ``caplog`` handlers, so later tests in the same process
    silently observe empty log output.

    Yields:
        None: with the logging state restored on exit.
    """
    loggers = [
        logging.getLogger(),
        *(logger for logger in logging.root.manager.loggerDict.values() if isinstance(logger, logging.Logger)),
    ]
    snapshot = [(logger, logger.handlers[:], logger.level) for logger in loggers]
    try:
        yield
    finally:
        for logger, handlers, level in snapshot:
            for installed in tuple(logger.handlers):
                logger.removeHandler(installed)
                if installed not in handlers:
                    installed.close()
            for handler in handlers:
                logger.addHandler(handler)
            logger.setLevel(level)


class _FakeConnection:
    def __init__(self, messages: list[object] | None = None, *, close_failures: int = 0) -> None:
        self.messages = messages or []
        self.sent: list[object] = []
        self.closed = False
        self.close_calls = 0
        self.close_failures = close_failures

    def recv(self) -> object:
        if not self.messages:
            raise EOFError
        return self.messages.pop(0)

    def send(self, message: object) -> None:
        self.sent.append(message)

    def close(self) -> None:
        self.close_calls += 1
        if self.close_failures:
            self.close_failures -= 1
            message = "connection close failed"
            raise OSError(message)
        self.closed = True


class _FakeEvent:
    def __init__(self, *, set_failures: int = 0) -> None:
        self.set_calls = 0
        self.set_failures = set_failures

    def set(self) -> None:
        self.set_calls += 1
        if self.set_failures:
            self.set_failures -= 1
            message = "event set failed"
            raise OSError(message)

    def is_set(self) -> bool:
        return self.set_calls > 0

    def wait(self, timeout: float | None = None) -> bool:
        del timeout
        return self.is_set()


class _FakeProcess:
    def __init__(self, *, pid: int = 41, alive: bool = True) -> None:
        self.pid = pid
        self.exitcode: int | None = None
        self.sentinel = object()
        self.alive = alive
        self.started = False
        self.closed = False
        self.terminated = False
        self.killed = False
        self.join_timeouts: list[float | None] = []
        self.stop_after_terminate_join = True
        self.join_failures = 0
        self.terminate_failures = 0
        self.kill_failures = 0
        self.close_failures = 0
        self.close_calls = 0

    def start(self) -> None:
        self.started = True

    def is_alive(self) -> bool:
        return self.alive

    def join(self, timeout: float | None = None) -> None:
        self.join_timeouts.append(timeout)
        if self.join_failures:
            self.join_failures -= 1
            message = "process join failed"
            raise OSError(message)
        if self.terminated and self.stop_after_terminate_join:
            self.alive = False
        if self.killed:
            self.alive = False

    def terminate(self) -> None:
        if self.terminate_failures:
            self.terminate_failures -= 1
            message = "process terminate failed"
            raise OSError(message)
        self.terminated = True

    def kill(self) -> None:
        if self.kill_failures:
            self.kill_failures -= 1
            message = "process kill failed"
            raise OSError(message)
        self.killed = True

    def close(self) -> None:
        self.close_calls += 1
        if self.close_failures:
            self.close_failures -= 1
            message = "process close failed"
            raise OSError(message)
        self.closed = True


class _FakeThread:
    def __init__(self, *, target: Callable[[], None], **kwargs: object) -> None:
        del kwargs
        self.target = target
        self.started = False

    def start(self) -> None:
        self.started = True


class _FakeContext:
    def __init__(self, process: _FakeProcess, receive: _FakeConnection, send: _FakeConnection) -> None:
        self.process = process
        self.receive = receive
        self.send = send
        self.stop_event = _FakeEvent()
        self.process_kwargs: dict[str, object] = {}

    def Pipe(self, *, duplex: bool) -> tuple[_FakeConnection, _FakeConnection]:  # noqa: N802
        assert duplex is False
        return self.receive, self.send

    def Event(self) -> _FakeEvent:  # noqa: N802
        return self.stop_event

    def Process(self, **kwargs: object) -> _FakeProcess:  # noqa: N802
        self.process_kwargs = kwargs
        return self.process


def _config() -> QueueConfig:
    return QueueConfig(
        queue_backend="memory",
        worker=WorkerConfig(
            placement="external", startup_timeout=0.1, graceful_shutdown_timeout=0.1, final_cancel_timeout=0.1
        ),
    )


def _spec(secret: str = "credential") -> _WorkerLaunchSpec:
    return _WorkerLaunchSpec(
        app_path="example:app",
        app_dir="/tmp/example",
        sys_path=("/tmp/example",),
        environment=(("SECRET_TOKEN", secret), ("LITESTAR_APP", "example:app")),
        log_level=logging.WARNING,
    )


def _select_connection(objects: Sequence[object], timeout: float | None) -> list[object]:
    del timeout
    return [objects[0]]


def _supervisor(
    *,
    process: _FakeProcess | None = None,
    messages: list[object] | None = None,
    wait_result: Callable[[Sequence[object], float | None], list[object]] | None = None,
    request_shutdown: Callable[[], None] = lambda: None,
) -> tuple[ServerWorkerSupervisor, _FakeContext]:
    process = process or _FakeProcess()
    receive = _FakeConnection(messages)
    context = _FakeContext(process, receive, _FakeConnection())
    wait = wait_result or _select_connection
    supervisor = ServerWorkerSupervisor(
        _config(),
        launch_spec=_spec(),
        _context=context,  # type: ignore[arg-type]
        _wait=wait,
        _request_parent_shutdown=request_shutdown,
        _thread_factory=_FakeThread,  # type: ignore[arg-type]
    )
    return supervisor, context


@pytest.mark.parametrize(
    ("methods", "expected"), [(["fork", "spawn", "forkserver"], "forkserver"), (["fork", "spawn"], "spawn")]
)
def test_process_context_never_selects_fork(methods: list[str], expected: str) -> None:
    selected: list[str] = []

    def get_context(method: str) -> object:
        selected.append(method)
        return SimpleNamespace(get_start_method=lambda: method)

    context = _select_process_context(_available_methods=lambda: methods, _get_context=get_context)  # type: ignore[arg-type]

    assert context.get_start_method() == expected
    assert selected == [expected]


def test_launch_spec_contains_only_primitive_data_and_hides_environment() -> None:
    spec = _spec("postgres://user:credential@example")

    assert tuple(item.name for item in fields(spec)) == ("app_path", "app_dir", "sys_path", "environment", "log_level")
    assert "credential" not in repr(spec)
    assert all(
        isinstance(value, (str, int, tuple))
        for value in (spec.app_path, spec.app_dir, spec.sys_path, spec.environment, spec.log_level)
    )


def test_launch_spec_captures_complete_environment_cwd_and_sys_path(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    environment = {"LITESTAR_APP": "example:app", "SECRET_TOKEN": "credential", "EXTRA": "preserved"}
    monkeypatch.setattr(os, "environ", environment)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "path", ["one", "two"])

    spec = _build_launch_spec(log_level=12)

    assert spec.app_path == "example:app"
    assert spec.app_dir == str(tmp_path)
    assert spec.sys_path == ("one", "two")
    assert spec.environment == tuple(environment.items())
    assert spec.log_level == 12


def test_launch_spec_requires_explicit_litestar_app(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(os, "environ", {})

    with pytest.raises(QueueConfigurationError, match="LITESTAR_APP"):
        _build_launch_spec()


def _server_placement_plugin() -> SimpleNamespace:
    """A plugin-like stand-in whose configuration passes the child validate stage."""
    return SimpleNamespace(config=QueueConfig(queue_backend="redis", worker=WorkerConfig(placement="server")))


def test_child_applies_environment_and_sys_path_before_loading_app(monkeypatch: MonkeyPatch) -> None:
    from litestar.cli._utils import LitestarEnv

    captured: dict[str, object] = {}
    order: list[str] = []
    fake_environment: dict[str, str] = {"STALE": "value"}
    monkeypatch.setattr(os, "environ", fake_environment)
    monkeypatch.setattr(sys, "path", ["stale"])
    monkeypatch.setattr(os, "setsid", lambda: order.append("setsid"))
    monkeypatch.setattr("multiprocessing.parent_process", lambda: SimpleNamespace(sentinel=1, close=lambda: None))

    def from_env(app_path: str, app_dir: Path) -> object:
        order.append("load_app")
        captured["environment"] = dict(os.environ)
        captured["sys_path"] = tuple(sys.path)
        captured["app_path"] = app_path
        captured["app_dir"] = str(app_dir)
        return SimpleNamespace(app=object())

    monkeypatch.setattr(LitestarEnv, "from_env", from_env)

    def ignore_log_level(level: int) -> None:
        del level

    def fake_plugin(app: object) -> object:
        del app
        return _server_placement_plugin()

    monkeypatch.setattr("litestar_queues.worker.supervisor._reset_child_logging", ignore_log_level)
    monkeypatch.setattr("litestar_queues.worker.supervisor._resolve_queue_plugin", fake_plugin)

    def fake_asyncio_run(coroutine: object) -> None:
        order.append("readiness")
        coroutine.close()  # type: ignore[attr-defined]

    monkeypatch.setattr("asyncio.run", fake_asyncio_run)
    connection = _FakeConnection()
    stop = _FakeEvent()

    _worker_process_main(_spec(), stop, connection)  # type: ignore[arg-type]

    assert captured == {
        "environment": {
            "SECRET_TOKEN": "credential",
            "LITESTAR_APP": "example:app",
            "LITESTAR_QUEUES_PROCESS_ROLE": "server-worker",
        },
        "sys_path": ("/tmp/example",),
        "app_path": "example:app",
        "app_dir": "/tmp/example",
    }
    assert stop.set_calls == 1
    assert connection.closed is True
    assert order == ["setsid", "load_app", "readiness"]


def test_child_error_is_stage_and_type_only(monkeypatch: MonkeyPatch) -> None:
    class CredentialError(RuntimeError):
        pass

    def fail(spec: _WorkerLaunchSpec) -> None:
        del spec
        message = "postgres://user:credential@example"
        raise CredentialError(message)

    monkeypatch.setattr("litestar_queues.worker.supervisor._apply_launch_spec", fail)
    connection = _FakeConnection()

    _worker_process_main(_spec(), _FakeEvent(), connection)  # type: ignore[arg-type]

    assert connection.sent == [("error", "bootstrap", "CredentialError")]
    assert "credential" not in repr(connection.sent)


def test_child_cleanup_failures_never_escape_and_always_attempt_pipe_close(monkeypatch: MonkeyPatch) -> None:
    class CredentialError(RuntimeError):
        pass

    def fail(spec: _WorkerLaunchSpec) -> None:
        del spec
        message = "postgres://user:credential@example"
        raise CredentialError(message)

    monkeypatch.setattr("litestar_queues.worker.supervisor._apply_launch_spec", fail)
    connection = _FakeConnection(close_failures=1)
    stop = _FakeEvent(set_failures=1)

    _worker_process_main(_spec(), cast("Any", stop), cast("Any", connection))
    assert connection.sent == [("error", "bootstrap", "CredentialError")]
    assert stop.set_calls == 1
    assert connection.close_calls == 1


def test_child_sanitizes_runner_stage_error(monkeypatch: MonkeyPatch) -> None:
    from litestar.cli._utils import LitestarEnv

    class CredentialError(RuntimeError):
        pass

    credential_message = "postgres://user:credential@example"
    credential_error = CredentialError(credential_message)

    def raise_stage_error(coroutine: object) -> None:
        coroutine.close()  # type: ignore[attr-defined]
        raise _WorkerStageError(_WorkerStage.OPEN_SERVICE, credential_error)

    def ignore(value: object) -> None:
        del value

    def fake_parent() -> object:
        return SimpleNamespace(sentinel=1)

    def fake_environment(app_path: str, app_dir: Path) -> object:
        del app_path, app_dir
        return SimpleNamespace(app=object())

    def fake_plugin(app: object) -> object:
        del app
        return _server_placement_plugin()

    monkeypatch.setattr("litestar_queues.worker.supervisor._apply_launch_spec", ignore)
    monkeypatch.setattr(os, "setsid", lambda: None)
    monkeypatch.setattr("multiprocessing.parent_process", fake_parent)
    monkeypatch.setattr(LitestarEnv, "from_env", fake_environment)
    monkeypatch.setattr("litestar_queues.worker.supervisor._reset_child_logging", ignore)
    monkeypatch.setattr("litestar_queues.worker.supervisor._resolve_queue_plugin", fake_plugin)
    monkeypatch.setattr("asyncio.run", raise_stage_error)
    connection = _FakeConnection()

    _worker_process_main(_spec(), _FakeEvent(), connection)  # type: ignore[arg-type]

    assert connection.sent == [("error", "open_service", "CredentialError")]
    assert "credential" not in repr(connection.sent)


def test_missing_parent_process_is_sanitized_bootstrap_failure(monkeypatch: MonkeyPatch) -> None:
    def ignore(value: object) -> None:
        del value

    def no_parent() -> None:
        return None

    monkeypatch.setattr("litestar_queues.worker.supervisor._apply_launch_spec", ignore)
    monkeypatch.setattr(os, "setsid", lambda: None)
    monkeypatch.setattr("multiprocessing.parent_process", no_parent)
    connection = _FakeConnection()

    _worker_process_main(_spec(), _FakeEvent(), connection)  # type: ignore[arg-type]

    assert connection.sent == [("error", "bootstrap", "RuntimeError")]


def test_child_requires_exactly_one_queue_plugin() -> None:
    from litestar_queues import QueuePlugin

    plugin = QueuePlugin(_config())

    assert _resolve_queue_plugin(SimpleNamespace(plugins=[plugin])) is plugin
    with pytest.raises(QueueConfigurationError, match="exactly one"):
        _resolve_queue_plugin(SimpleNamespace(plugins=[]))
    with pytest.raises(QueueConfigurationError, match="exactly one"):
        _resolve_queue_plugin(SimpleNamespace(plugins=[plugin, QueuePlugin(_config())]))


@pytest.mark.parametrize(
    ("messages", "wait_choice", "expected"),
    [
        ([("error", "load_app", "CredentialError")], "connection", "load_app \\(CredentialError\\)"),
        ([], "connection", "bootstrap \\(EOFError\\)"),
        ([], "sentinel", "bootstrap \\(ChildProcessError\\)"),
        ([("surprise", "credential")], "connection", "bootstrap \\(ProtocolError\\)"),
        ([("error", "load_app", "CredentialError credential")], "connection", "bootstrap \\(ProtocolError\\)"),
        ([], "timeout", "bootstrap \\(TimeoutError\\)"),
    ],
)
def test_startup_failures_are_sanitized(messages: list[object], wait_choice: str, expected: str) -> None:
    process = _FakeProcess()

    def choose_wait_result(objects: Sequence[object], timeout: float | None) -> list[object]:
        del timeout
        if wait_choice == "connection":
            return [objects[0]]
        if wait_choice == "sentinel":
            return [objects[1]]
        return []

    supervisor, context = _supervisor(process=process, messages=messages, wait_result=choose_wait_result)

    with pytest.raises(QueueConfigurationError, match=expected) as exc_info:
        supervisor.start()

    assert "credential@example" not in str(exc_info.value)
    assert context.receive.closed is True
    assert context.send.closed is True
    assert process.closed is True


def test_process_start_failure_closes_every_created_handle() -> None:
    class StartErrorProcess(_FakeProcess):
        def start(self) -> None:
            message = "could not spawn"
            raise RuntimeError(message)

    process = StartErrorProcess()
    supervisor, context = _supervisor(process=process)

    with pytest.raises(RuntimeError, match="could not spawn"):
        supervisor.start()

    assert context.stop_event.is_set() is True
    assert context.receive.closed is True
    assert context.send.closed is True
    assert process.closed is True


def test_startup_primary_error_survives_force_and_handle_cleanup_failures() -> None:
    process = _FakeProcess()
    process.terminate_failures = 1
    process.close_failures = 1
    supervisor, context = _supervisor(process=process, messages=[("error", "load_app", "CredentialError")])
    context.receive.close_failures = 1

    with pytest.raises(QueueConfigurationError, match="load_app \\(CredentialError\\)"):
        supervisor.start()

    assert context.receive.close_calls == 1
    assert context.send.close_calls >= 1
    assert process.close_calls == 1
    process.alive = False
    supervisor.close()
    assert context.receive.close_calls == 2
    assert process.close_calls == 2


def test_process_construction_failure_closes_both_pipe_ends() -> None:
    class ConstructionErrorContext(_FakeContext):
        def Event(self) -> _FakeEvent:  # noqa: N802
            message = "could not create event"
            raise RuntimeError(message)

    process = _FakeProcess()
    receive = _FakeConnection()
    send = _FakeConnection()
    context = ConstructionErrorContext(process, receive, send)
    supervisor = ServerWorkerSupervisor(
        _config(),
        launch_spec=_spec(),
        _context=context,  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="could not create event"):
        supervisor.start()

    assert receive.closed is True
    assert send.closed is True


def test_watchdog_creation_failure_stops_child_and_closes_handles() -> None:
    process = _FakeProcess()
    supervisor, context = _supervisor(process=process, messages=[("ready", process.pid)])

    def fail_thread(**kwargs: object) -> None:
        del kwargs
        message = "could not create watchdog"
        raise RuntimeError(message)

    supervisor._thread_factory = fail_thread  # type: ignore[assignment]  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(RuntimeError, match="could not create watchdog"):
        supervisor.start()

    assert context.stop_event.is_set() is True
    assert process.terminated is True
    assert process.closed is True
    assert context.receive.closed is True


def test_ready_starts_watchdog_and_crosses_only_launch_spec_event_and_pipe() -> None:
    process = _FakeProcess()
    observed_timeouts: list[float | None] = []

    def capture_timeout(objects: Sequence[object], timeout: float | None) -> list[object]:
        observed_timeouts.append(timeout)
        return [objects[0]]

    supervisor, context = _supervisor(process=process, messages=[("ready", process.pid)], wait_result=capture_timeout)

    supervisor.start()

    args = context.process_kwargs["args"]
    assert isinstance(args, tuple)
    assert isinstance(args[0], _WorkerLaunchSpec)
    assert args[1] is context.stop_event
    assert args[2] is context.send
    assert len(args) == 3
    assert process.started is True
    assert observed_timeouts == [0.1]
    assert context.send.closed is True
    assert supervisor._watchdog is not None  # pyright: ignore[reportPrivateUsage]
    assert supervisor._watchdog.started is True  # type: ignore[attr-defined]  # pyright: ignore[reportPrivateUsage]


def test_first_close_sets_expected_stop_before_event_and_joins_then_repeated_close_is_noop() -> None:
    order: list[str] = []
    process = _FakeProcess()
    supervisor, context = _supervisor(process=process, messages=[("ready", process.pid)])
    supervisor.start()
    original_set = context.stop_event.set

    def set_stop() -> None:
        assert supervisor._expected_stop.is_set()  # pyright: ignore[reportPrivateUsage]
        order.append("set")
        original_set()

    context.stop_event.set = set_stop  # type: ignore[method-assign]
    process.alive = False

    supervisor.close()
    supervisor.close()

    assert order == ["set"]
    assert process.join_timeouts == [1.2]
    assert process.closed is True
    assert context.receive.closed is True


def test_join_timeout_terminates_then_kills(monkeypatch: MonkeyPatch) -> None:
    process = _FakeProcess()
    process.stop_after_terminate_join = False
    supervisor, _ = _supervisor(process=process, messages=[("ready", process.pid)])
    monkeypatch.setattr("litestar_queues.worker.supervisor.os.name", "nt")
    monkeypatch.setattr("litestar_queues.worker.supervisor._kill_windows_process_tree", lambda process: process.kill())
    supervisor.start()

    supervisor.close()

    assert process.terminated is False
    assert process.killed is True
    assert process.join_timeouts == [1.2, 5.0]


def test_windows_tree_cleanup_uses_bounded_taskkill() -> None:
    process = _FakeProcess(pid=81)
    calls: "list[tuple[list[str], bool, bool, float]]" = []

    def run(command: list[str], *, check: bool, capture_output: bool, timeout: float) -> "Any":
        calls.append((command, check, capture_output, timeout))
        process.alive = False
        return SimpleNamespace(returncode=0)

    _kill_windows_process_tree(cast("Any", process), _which=lambda _name: "C:/Windows/taskkill.exe", _run=run)

    assert calls == [(["C:/Windows/taskkill.exe", "/PID", "81", "/T", "/F"], False, True, 5.0)]


@pytest.mark.parametrize("failure", ["missing", "timeout", "status"])
def test_windows_tree_cleanup_failure_is_sanitized_and_never_claimed_clean(failure: str) -> None:
    process = _FakeProcess(pid=82)

    def run(command: list[str], **kwargs: "Any") -> "Any":
        del command, kwargs
        if failure == "missing":
            raise FileNotFoundError
        if failure == "timeout":
            command = "taskkill"
            raise subprocess.TimeoutExpired(command, 5)
        return SimpleNamespace(returncode=1)

    with pytest.raises(_QueueProcessCleanupError, match="Windows process-tree cleanup failed"):
        _kill_windows_process_tree(cast("Any", process), _which=lambda _name: None, _run=run)

    assert process.killed is True


def test_windows_force_stop_joins_after_tree_cleanup_failure(monkeypatch: MonkeyPatch) -> None:
    process = _FakeProcess(pid=83)
    message = "Windows process-tree cleanup failed"

    def fail_tree_cleanup(process: object) -> None:
        del process
        raise _QueueProcessCleanupError(message)

    monkeypatch.setattr("litestar_queues.worker.supervisor._kill_windows_process_tree", fail_tree_cleanup)

    with pytest.raises(_QueueProcessCleanupError, match="Windows process-tree cleanup failed"):
        _force_stop_process(cast("Any", process), _platform="nt")

    assert process.join_timeouts == [5.0]


def test_server_shutdown_request_is_portable(monkeypatch: MonkeyPatch) -> None:
    signals: "list[tuple[str, int]]" = []
    monkeypatch.setattr(
        "litestar_queues.worker.supervisor.signal.raise_signal", lambda sig: signals.append(("raise", sig))
    )
    monkeypatch.setattr("litestar_queues.worker.supervisor.os.kill", lambda _pid, sig: signals.append(("kill", sig)))

    monkeypatch.setattr("litestar_queues.worker.supervisor.sys.platform", "win32")
    _request_server_shutdown()
    monkeypatch.setattr("litestar_queues.worker.supervisor.sys.platform", "linux")
    _request_server_shutdown()

    assert signals == [("raise", signal.SIGINT), ("kill", signal.SIGTERM)]


def test_posix_force_stop_uses_verified_group_term_then_stops_when_child_exits() -> None:
    process = _FakeProcess(pid=73)
    signals: list[tuple[int, int]] = []

    def killpg(pgid: int, sig: int) -> None:
        signals.append((pgid, sig))
        process.alive = False

    _force_stop_process(cast("Any", process), _platform="posix", _getpgid=lambda pid: pid, _killpg=killpg)

    assert signals == [(73, signal.SIGTERM)]
    assert process.terminated is False
    assert process.killed is False
    assert process.join_timeouts == [5.0]


def test_posix_force_stop_uses_group_term_then_group_kill() -> None:
    process = _FakeProcess(pid=74)
    signals: list[tuple[int, int]] = []

    def killpg(pgid: int, sig: int) -> None:
        signals.append((pgid, sig))
        if sig == signal.SIGKILL:
            process.alive = False

    _force_stop_process(cast("Any", process), _platform="posix", _getpgid=lambda pid: pid, _killpg=killpg)

    assert signals == [(74, signal.SIGTERM), (74, signal.SIGKILL)]
    assert process.terminated is False
    assert process.killed is False
    assert process.join_timeouts == [5.0, 5.0]


def test_posix_force_stop_falls_back_to_direct_process_for_unverified_group() -> None:
    process = _FakeProcess(pid=75)
    process.stop_after_terminate_join = False
    signals: list[tuple[int, int]] = []

    _force_stop_process(
        cast("Any", process),
        _platform="posix",
        _getpgid=lambda pid: pid + 1,
        _killpg=lambda pgid, sig: signals.append((pgid, sig)),
    )

    assert signals == []
    assert process.terminated is True
    assert process.killed is True
    assert process.join_timeouts == [5.0, 5.0]


def test_close_join_failure_still_attempts_all_handle_closes() -> None:
    process = _FakeProcess(alive=False)
    supervisor, context = _supervisor(process=process, messages=[("ready", process.pid)])
    supervisor.start()
    process.join_failures = 1

    with pytest.raises(OSError, match="process join failed"):
        supervisor.close()

    assert context.receive.close_calls == 1
    assert process.close_calls == 1


def test_failed_handle_closes_remain_retryable_and_supervisor_is_single_use() -> None:
    process = _FakeProcess(alive=False)
    process.close_failures = 1
    supervisor, context = _supervisor(process=process, messages=[("ready", process.pid)])
    context.receive.close_failures = 1
    supervisor.start()

    with pytest.raises(OSError, match="connection close failed"):
        supervisor.close()

    assert context.receive.close_calls == 1
    assert process.close_calls == 1
    supervisor.close()
    assert context.receive.close_calls == 2
    assert process.close_calls == 2
    with pytest.raises(QueueConfigurationError, match="already started"):
        supervisor.start()


def test_unexpected_post_ready_exit_requests_shutdown_once(caplog: pytest.LogCaptureFixture) -> None:
    requests: list[str] = []
    process = _FakeProcess(alive=False)
    process.exitcode = 7
    supervisor, _ = _supervisor(
        process=process, messages=[("ready", process.pid)], request_shutdown=lambda: requests.append("shutdown")
    )
    supervisor.start()

    supervisor._watch_child()  # pyright: ignore[reportPrivateUsage]
    supervisor._watch_child()  # pyright: ignore[reportPrivateUsage]

    assert requests == ["shutdown"]
    assert "7" in caplog.text


def test_watchdog_join_and_callback_failures_still_close_handles_without_unsafe_traceback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    process = _FakeProcess(alive=False)
    process.exitcode = 8
    process.join_failures = 1

    def fail_shutdown() -> None:
        message = "postgres://user:credential@example"
        raise RuntimeError(message)

    supervisor, context = _supervisor(
        process=process, messages=[("ready", process.pid)], request_shutdown=fail_shutdown
    )
    supervisor.start()

    supervisor._watch_child()  # pyright: ignore[reportPrivateUsage]
    assert context.receive.close_calls == 1
    assert process.close_calls == 1
    assert "credential" not in caplog.text


def test_expected_exit_after_close_never_requests_shutdown() -> None:
    requests: list[str] = []
    process = _FakeProcess(alive=False)
    supervisor, _ = _supervisor(
        process=process, messages=[("ready", process.pid)], request_shutdown=lambda: requests.append("shutdown")
    )
    supervisor.start()
    supervisor.close()

    supervisor._watch_child()  # pyright: ignore[reportPrivateUsage]

    assert requests == []


def test_parent_sentinel_bridge_requests_graceful_stop_once() -> None:
    calls: list[Callable[[], None]] = []
    graceful = _FakeEvent()
    completed = threading.Event()
    loop = SimpleNamespace(call_soon_threadsafe=calls.append)
    parent = SimpleNamespace(sentinel=object())

    _parent_loss_bridge(parent, loop, graceful, completed, _wait=list)  # type: ignore[arg-type]
    for callback in calls:
        callback()

    assert graceful.set_calls == 1


def test_completed_parent_bridge_does_not_request_stop() -> None:
    calls: list[Callable[[], None]] = []
    completed = threading.Event()
    completed.set()

    _parent_loss_bridge(
        cast("Any", SimpleNamespace(sentinel=object())),
        cast("Any", SimpleNamespace(call_soon_threadsafe=calls.append)),
        cast("Any", _FakeEvent()),
        completed,
        _wait=list,
    )

    assert calls == []


@pytest.mark.parametrize("wait_raises", [False, True])
def test_parent_bridge_suppresses_closed_loop_scheduling_race(
    wait_raises: bool, caplog: pytest.LogCaptureFixture
) -> None:
    secret = "postgres://user:credential@example"

    class ClosedLoop:
        def call_soon_threadsafe(self, callback: Callable[[], None]) -> None:
            del callback
            raise RuntimeError(secret)

    def wait_for_parent(objects: Sequence[object]) -> list[object]:
        if wait_raises:
            raise OSError(secret)
        return list(objects)

    _parent_loss_bridge(
        cast("Any", SimpleNamespace(sentinel=object())),
        cast("Any", ClosedLoop()),
        cast("Any", _FakeEvent()),
        threading.Event(),
        _wait=wait_for_parent,
    )

    assert secret not in caplog.text


@pytest.mark.anyio
async def test_run_child_bridges_parent_loss_independently_and_releases_ipc_wait() -> None:
    from litestar_queues.worker.runtime import WorkerRunResult

    class BlockingEvent:
        def __init__(self) -> None:
            self.event = threading.Event()
            self.set_calls = 0

        def set(self) -> None:
            self.set_calls += 1
            self.event.set()

        def is_set(self) -> bool:
            return self.event.is_set()

        def wait(self, timeout: float | None = None) -> bool:
            return self.event.wait(timeout)

    parent_lost = threading.Event()
    parent_wait_started = threading.Event()
    runner_ready = threading.Event()
    runner_stopped = threading.Event()
    parent_closed: list[str] = []
    stop_event = BlockingEvent()
    connection = _FakeConnection()

    def create_service() -> object:
        return object()

    plugin = SimpleNamespace(config=object(), create_worker_service=create_service)
    parent = SimpleNamespace(sentinel=object(), close=lambda: parent_closed.append("closed"))

    def wait_for_parent(objects: Sequence[object]) -> list[object]:
        parent_wait_started.set()
        assert parent_lost.wait(1)
        return list(objects)

    async def fake_runner(
        service: object,
        config: object,
        *,
        graceful_stop: asyncio.Event,
        force_stop: asyncio.Event,
        ready: Callable[[], None],
    ) -> WorkerRunResult:
        del service, config, force_stop
        ready()
        runner_ready.set()
        await graceful_stop.wait()
        runner_stopped.set()
        return WorkerRunResult.CLEAN

    child = asyncio.create_task(
        _run_child(
            plugin,  # type: ignore[arg-type]
            cast("Any", stop_event),
            cast("Any", connection),
            99,
            parent,  # type: ignore[arg-type]
            _runner=fake_runner,
            _parent_wait=wait_for_parent,
        )
    )
    assert await asyncio.to_thread(runner_ready.wait, 1)
    assert await asyncio.to_thread(parent_wait_started.wait, 1)
    assert stop_event.is_set() is False

    parent_lost.set()
    await asyncio.wait_for(child, 1)

    assert runner_stopped.is_set() is True
    assert stop_event.set_calls == 1
    assert connection.sent == [("ready", 99)]
    assert parent_closed == ["closed"]


@pytest.mark.anyio
async def test_run_child_finishes_when_stop_event_set_fails() -> None:
    from litestar_queues.worker.runtime import WorkerRunResult

    class FailingReleaseEvent:
        def __init__(self) -> None:
            self.release = threading.Event()
            self.set_calls = 0

        def set(self) -> None:
            self.set_calls += 1
            message = "stop event set failed"
            raise OSError(message)

        def is_set(self) -> bool:
            return self.release.is_set()

        def wait(self, timeout: float | None = None) -> bool:
            return self.release.wait(timeout)

    async def finished_runner(
        service: object,
        config: object,
        *,
        graceful_stop: asyncio.Event,
        force_stop: asyncio.Event,
        ready: Callable[[], None],
    ) -> WorkerRunResult:
        del service, config, graceful_stop, force_stop, ready
        return WorkerRunResult.CLEAN

    def create_service() -> object:
        return object()

    parent_closed: list[str] = []
    stop_event = FailingReleaseEvent()
    child = asyncio.create_task(
        _run_child(
            SimpleNamespace(config=object(), create_worker_service=create_service),  # type: ignore[arg-type]
            cast("Any", stop_event),
            cast("Any", _FakeConnection()),
            100,
            SimpleNamespace(sentinel=object(), close=lambda: parent_closed.append("closed")),  # type: ignore[arg-type]
            _runner=finished_runner,
        )
    )
    try:
        await asyncio.wait_for(asyncio.shield(child), 0.2)
    except TimeoutError:
        stop_event.release.set()
        await child
        pytest.fail("child cleanup hung after stop_event.set() failed")
    finally:
        stop_event.release.set()

    assert stop_event.set_calls == 1
    assert parent_closed == ["closed"]


def test_posix_group_cleanup_requires_child_owned_process_group() -> None:
    process = _FakeProcess(pid=73)
    killed: list[tuple[int, int]] = []

    unverified = _verified_kill_process_group(
        cast("Any", process),
        signal.SIGKILL,
        _getpgid=lambda pid: pid + 1,
        _killpg=lambda pgid, sig: killed.append((pgid, sig)),
    )
    verified = _verified_kill_process_group(
        cast("Any", process),
        signal.SIGKILL,
        _getpgid=lambda pid: pid,
        _killpg=lambda pgid, sig: killed.append((pgid, sig)),
    )

    assert unverified is False
    assert verified is True
    assert killed == [(73, signal.SIGKILL)]


def test_logging_reset_stops_queue_listeners_and_leaves_one_direct_stderr_handler() -> None:
    with _restored_logging_state():
        root = logging.getLogger()
        named = logging.getLogger("litestar_queues.test.server_worker_logging")
        listener_stops: list[str] = []
        named_handler = logging.NullHandler()
        setattr(named_handler, "listener", SimpleNamespace(stop=lambda: listener_stops.append("stopped")))
        root.addHandler(logging.NullHandler())
        named.addHandler(named_handler)

        _reset_child_logging(logging.ERROR)

        assert listener_stops == ["stopped"]
        assert named.handlers == []
        assert len(root.handlers) == 1
        assert type(root.handlers[0]) is logging.StreamHandler
        assert root.handlers[0].stream is sys.stderr
        assert root.level == logging.ERROR


def test_apply_launch_spec_replaces_not_merges_environment(monkeypatch: MonkeyPatch) -> None:
    environment = {"STALE": "discard"}
    path = ["stale"]
    monkeypatch.setattr(os, "environ", environment)
    monkeypatch.setattr(sys, "path", path)

    _apply_launch_spec(_spec())

    assert environment == {"SECRET_TOKEN": "credential", "LITESTAR_APP": "example:app"}
    assert path == ["/tmp/example"]


def test_server_worker_module_import_is_click_free() -> None:
    script = (
        "import sys; import litestar_queues.worker.supervisor; raise SystemExit(1 if 'click' in sys.modules else 0)"
    )

    result = subprocess.run([sys.executable, "-c", script], check=False, capture_output=True, text=True)

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    ("config", "reason"),
    [
        (QueueConfig(queue_backend="redis", worker=WorkerConfig(placement="external")), "placement"),
        (QueueConfig(queue_backend="memory", worker=WorkerConfig(placement="asgi")), "memory"),
        (QueueConfig(), "ephemeral"),
    ],
)
def test_child_validate_stage_rejects_a_non_server_application(config: QueueConfig, reason: str) -> None:
    """The child re-reads configuration from source, so a mismatch must not start a worker."""
    from litestar.cli._utils import LitestarEnv

    del reason
    started = False

    def never_run(coroutine: object) -> None:
        nonlocal started
        started = True
        coroutine.close()  # type: ignore[attr-defined]

    def ignore(value: object) -> None:
        del value

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("litestar_queues.worker.supervisor._apply_launch_spec", ignore)
        patch.setattr(os, "setsid", lambda: None)
        patch.setattr("multiprocessing.parent_process", lambda: SimpleNamespace(sentinel=1))
        patch.setattr(LitestarEnv, "from_env", lambda *_: SimpleNamespace(app=object()))
        patch.setattr("litestar_queues.worker.supervisor._reset_child_logging", ignore)
        patch.setattr(
            "litestar_queues.worker.supervisor._resolve_queue_plugin", lambda *_: SimpleNamespace(config=config)
        )
        patch.setattr("asyncio.run", never_run)
        connection = _FakeConnection()

        _worker_process_main(_spec(), _FakeEvent(), connection)  # type: ignore[arg-type]

    assert started is False
    assert connection.sent == [("error", "validate", "QueueConfigurationError")]

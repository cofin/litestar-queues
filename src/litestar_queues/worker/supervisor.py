"""Private supervision for a fresh queue worker process."""

import asyncio
import contextlib
import logging
import multiprocessing
import os
import shutil
import signal
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn, Protocol, cast

from litestar_queues.exceptions import QueueConfigurationError, QueueError
from litestar_queues.worker.runtime import WorkerRunResult, _WorkerStageError
from litestar_queues.worker.runtime import run_worker as _run_worker

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Sequence
    from multiprocessing.connection import Connection
    from multiprocessing.context import BaseContext
    from multiprocessing.process import BaseProcess
    from typing import Any

    from litestar_queues.config import QueueConfig
    from litestar_queues.plugin import QueuePlugin
    from litestar_queues.service import QueueService

__all__ = ()

logger = logging.getLogger(__name__)

_FORCE_STOP_TIMEOUT = 5.0
_STOP_EVENT_POLL_INTERVAL = 0.05
_READY_MESSAGE_LENGTH = 2
_ERROR_MESSAGE_LENGTH = 3
_RUNNER_STAGES = frozenset(("load_tasks", "open_service", "initialize_schedules", "start_worker"))
_BOOTSTRAP_STAGES = frozenset(("bootstrap", "load_app", "resolve_plugin", "validate"))
_SAFE_STAGES = _RUNNER_STAGES | _BOOTSTRAP_STAGES
_PROCESS_ROLE_ENV_VAR = "LITESTAR_QUEUES_PROCESS_ROLE"
_WINDOWS_CLEANUP_ERROR = "Windows process-tree cleanup failed."
_POSIX_SIGKILL = cast("int", getattr(signal, "SIGKILL", 9))


class _QueueProcessCleanupError(QueueError):
    """Raised when the supervisor cannot prove descendant cleanup."""


class _EventLike(Protocol):
    def is_set(self) -> "bool": ...

    def set(self) -> "None": ...

    def wait(self, timeout: "float | None" = None) -> "bool": ...


class _ProcessLike(Protocol):
    pid: "int | None"
    exitcode: "int | None"
    sentinel: "int"

    def start(self) -> "None": ...

    def is_alive(self) -> "bool": ...

    def join(self, timeout: "float | None" = None) -> "None": ...

    def terminate(self) -> "None": ...

    def kill(self) -> "None": ...

    def close(self) -> "None": ...


class _WorkerRunner(Protocol):
    def __call__(
        self,
        service: "QueueService",
        config: "QueueConfig",
        *,
        graceful_stop: "asyncio.Event",
        force_stop: "asyncio.Event",
        ready: "Callable[[], None]",
    ) -> "Awaitable[WorkerRunResult]": ...


@dataclass(frozen=True, slots=True)
class _WorkerLaunchSpec:
    app_path: "str"
    app_dir: "str"
    sys_path: "tuple[str, ...]"
    environment: "tuple[tuple[str, str], ...]" = field(repr=False)
    log_level: "int"


def _select_process_context(
    *,
    _available_methods: "Callable[[], Sequence[str]]" = multiprocessing.get_all_start_methods,
    _get_context: "Callable[[str], BaseContext]" = multiprocessing.get_context,
) -> "BaseContext":
    methods = _available_methods()
    return _get_context("forkserver" if "forkserver" in methods else "spawn")


def _build_launch_spec(*, log_level: "int" = logging.INFO) -> "_WorkerLaunchSpec":
    try:
        app_path = os.environ["LITESTAR_APP"]
    except KeyError as exc:
        msg = "LITESTAR_APP is required to start the server queue worker."
        raise QueueConfigurationError(msg) from exc
    if not app_path:
        msg = "LITESTAR_APP is required to start the server queue worker."
        raise QueueConfigurationError(msg)
    return _WorkerLaunchSpec(
        app_path=app_path,
        app_dir=str(Path.cwd()),
        sys_path=tuple(sys.path),
        environment=tuple(os.environ.items()),
        log_level=log_level,
    )


def _safe_send(connection: "Connection", message: "tuple[object, ...]") -> "None":
    with contextlib.suppress(BaseException):
        connection.send(message)


def _close_handlers(log: "logging.Logger") -> "None":
    for handler in tuple(log.handlers):
        listener = getattr(handler, "listener", None)
        if listener is not None:
            with contextlib.suppress(BaseException):
                listener.stop()
        log.removeHandler(handler)
        with contextlib.suppress(BaseException):
            handler.close()


def _reset_child_logging(log_level: "int") -> "None":
    logging.shutdown()
    _close_handlers(logging.getLogger())
    for candidate in tuple(logging.root.manager.loggerDict.values()):
        if isinstance(candidate, logging.Logger):
            _close_handlers(candidate)
    handler = logging.StreamHandler(sys.stderr)
    logging.basicConfig(level=log_level, handlers=[handler], force=True)


def _apply_launch_spec(spec: "_WorkerLaunchSpec") -> "None":
    os.environ.clear()
    os.environ.update(spec.environment)
    sys.path[:] = spec.sys_path


def _get_parent_process() -> "BaseProcess":
    parent = multiprocessing.parent_process()
    if parent is None:
        msg = "Worker process has no parent process handle."
        raise RuntimeError(msg)
    return parent


def _resolve_queue_plugin(app: "object") -> "QueuePlugin":
    from litestar_queues.plugin import QueuePlugin

    plugins = tuple(
        plugin for plugin in cast("Iterable[object]", getattr(app, "plugins", ())) if isinstance(plugin, QueuePlugin)
    )
    if len(plugins) != 1:
        msg = "The loaded Litestar app must contain exactly one QueuePlugin."
        raise QueueConfigurationError(msg)
    return plugins[0]


def _validate_child_plugin(plugin: "QueuePlugin") -> "None":
    """Confirm the freshly loaded application still describes a server-owned worker.

    The child imports the application itself, so its configuration is read from
    source rather than inherited. A parent that started under one configuration
    must not produce a child that would claim from somewhere else.

    Raises:
        QueueConfigurationError: If placement is not server-owned, or the
            configured storage is not reachable from this process.
    """
    from litestar_queues.backends.ephemeral.schema import SCHEMA_VERSION, read_environment, read_runtime
    from litestar_queues.config import queue_backend_name

    config = plugin.config
    if config.worker.placement != "server":
        msg = "The server queue worker requires an application configured with WorkerConfig(placement='server')."
        raise QueueConfigurationError(msg)
    backend = queue_backend_name(config.queue_backend)
    if backend == "memory":
        msg = "queue_backend='memory' is process-local and cannot be claimed by a server-owned worker process."
        raise QueueConfigurationError(msg)
    if backend != "ephemeral":
        return
    resolved = read_environment()
    if resolved is None:
        msg = "The server-owned ephemeral queue database is not available to this worker process."
        raise QueueConfigurationError(msg)
    path, nonce = resolved
    if read_runtime(path) != (SCHEMA_VERSION, nonce):
        msg = "The ephemeral queue database does not belong to this server invocation."
        raise QueueConfigurationError(msg)


def _parent_loss_bridge(
    parent: "BaseProcess",
    loop: "asyncio.AbstractEventLoop",
    graceful_stop: "asyncio.Event",
    completed: "threading.Event",
    *,
    _wait: "Callable[[Sequence[object]], list[object]] | None" = None,
) -> "None":
    if _wait is None:
        from multiprocessing.connection import wait

        wait_for_parent = cast("Callable[[Sequence[object]], list[object]]", wait)
    else:
        wait_for_parent = _wait
    with contextlib.suppress(BaseException):
        wait_for_parent([parent.sentinel])
    if not completed.is_set():
        with contextlib.suppress(BaseException):
            loop.call_soon_threadsafe(graceful_stop.set)


async def _bridge_stop_event(
    stop_event: "_EventLike", graceful_stop: "asyncio.Event", completed: "threading.Event"
) -> "None":
    while not completed.is_set():
        try:
            requested = await asyncio.to_thread(stop_event.wait, _STOP_EVENT_POLL_INTERVAL)
        except BaseException:  # noqa: BLE001 - bridge failures must not strand child cleanup.
            return
        if requested:
            graceful_stop.set()
            return


async def _run_child(
    plugin: "QueuePlugin",
    stop_event: "_EventLike",
    connection: "Connection",
    child_pid: "int",
    parent: "BaseProcess",
    *,
    _runner: "_WorkerRunner" = _run_worker,
    _parent_wait: "Callable[[Sequence[object]], list[object]] | None" = None,
    _thread_factory: "Callable[..., threading.Thread]" = threading.Thread,
) -> "None":
    graceful_stop = asyncio.Event()
    force_stop = asyncio.Event()
    bridge_completed = threading.Event()
    parent_bridge: "threading.Thread | None" = None

    def signal_ready() -> "None":
        nonlocal parent_bridge
        _safe_send(connection, ("ready", child_pid))
        loop = asyncio.get_running_loop()
        parent_bridge = _thread_factory(
            target=_parent_loss_bridge,
            args=(parent, loop, graceful_stop, bridge_completed),
            kwargs={"_wait": _parent_wait},
            name="litestar-queues-parent-watch",
            daemon=True,
        )
        parent_bridge.start()

    stop_translation = asyncio.create_task(_bridge_stop_event(stop_event, graceful_stop, bridge_completed))
    try:
        await _runner(
            plugin.create_worker_service(),
            plugin.config,
            graceful_stop=graceful_stop,
            force_stop=force_stop,
            ready=signal_ready,
        )
    finally:
        bridge_completed.set()
        with contextlib.suppress(BaseException):
            stop_event.set()
        try:
            await asyncio.shield(stop_translation)
        except BaseException:  # noqa: BLE001 - cleanup still owns the bridge task and parent handle.
            stop_translation.cancel()
        finally:
            await asyncio.gather(stop_translation, return_exceptions=True)
            with contextlib.suppress(BaseException):
                parent.close()


def _os_name() -> "str":
    """Return the OS family through a seam tests can replace.

    Tests must never assign to ``os.name`` itself: it is process-global, and
    with it set to ``"posix"`` on Windows ``pathlib.Path`` builds a
    ``PosixPath`` and raises. That breaks pytest's own failure reporting, which
    turns an ordinary assertion into an INTERNALERROR before monkeypatch can
    restore it.

    Returns:
        The value of :data:`os.name`.
    """
    return os.name


def _sys_platform() -> "str":
    """Return the platform identifier through a seam tests can replace.

    Returns:
        The value of :data:`sys.platform`.
    """
    return sys.platform


def _worker_process_main(spec: "_WorkerLaunchSpec", stop_event: "_EventLike", connection: "Connection") -> "None":
    stage = "bootstrap"
    try:
        _apply_launch_spec(spec)
        os.environ[_PROCESS_ROLE_ENV_VAR] = "server-worker"
        if _os_name() == "posix":
            os.setsid()
        parent = _get_parent_process()
        stage = "load_app"
        from litestar.cli._utils import LitestarEnv

        env = LitestarEnv.from_env(spec.app_path, Path(spec.app_dir))
        _reset_child_logging(spec.log_level)
        stage = "resolve_plugin"
        plugin = _resolve_queue_plugin(env.app)
        stage = "validate"
        _validate_child_plugin(plugin)
        asyncio.run(_run_child(plugin, stop_event, connection, os.getpid(), parent))
    except BaseException as exc:  # noqa: BLE001 - this boundary must suppress unsafe multiprocessing tracebacks.
        if isinstance(exc, _WorkerStageError):
            _safe_send(connection, ("error", exc.stage.value, exc.exception_type))
        else:
            _safe_send(connection, ("error", stage, type(exc).__name__))
    finally:
        os.environ.pop(_PROCESS_ROLE_ENV_VAR, None)
        with contextlib.suppress(BaseException):
            stop_event.set()
        with contextlib.suppress(BaseException):
            connection.close()


def _request_server_shutdown() -> "None":
    if _sys_platform() == "win32":
        signal.raise_signal(signal.SIGINT)
    else:
        os.kill(os.getpid(), signal.SIGTERM)


def _connection_wait(objects: "Sequence[object]", timeout: "float | None" = None) -> "list[object]":
    from multiprocessing.connection import wait

    return cast("list[object]", wait(cast("Any", objects), timeout))


def _is_safe_exception_type(value: "object") -> "bool":
    return isinstance(value, str) and value.isidentifier()


def _get_process_group(pid: "int") -> "int":
    return os.getpgid(pid)


def _kill_process_group(pgid: "int", sig: "int") -> "None":
    os.killpg(pgid, sig)


def _kill_windows_process_tree(
    process: "_ProcessLike",
    *,
    _which: "Callable[[str], str | None]" = shutil.which,
    _run: "Callable[..., subprocess.CompletedProcess[bytes]]" = subprocess.run,
) -> "None":
    pid = process.pid
    if pid is None:
        raise _QueueProcessCleanupError(_WINDOWS_CLEANUP_ERROR)
    executable = _which("taskkill")
    if executable is None:
        executable = str(Path(os.environ.get("SYSTEMROOT", r"C:\Windows")) / "System32" / "taskkill.exe")
    try:
        completed = _run(
            [executable, "/PID", str(pid), "/T", "/F"], check=False, capture_output=True, timeout=_FORCE_STOP_TIMEOUT
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        if process.is_alive():
            process.kill()
        raise _QueueProcessCleanupError(_WINDOWS_CLEANUP_ERROR) from None
    if completed.returncode != 0:
        if process.is_alive():
            process.kill()
        raise _QueueProcessCleanupError(_WINDOWS_CLEANUP_ERROR)


def _verified_kill_process_group(
    process: "_ProcessLike",
    sig: "int",
    *,
    _getpgid: "Callable[[int], int]" = _get_process_group,
    _killpg: "Callable[[int, int], None]" = _kill_process_group,
) -> "bool":
    pid = process.pid
    if pid is None:
        return False
    try:
        pgid = _getpgid(pid)
    except (OSError, ProcessLookupError):
        return False
    if pgid != pid:
        return False
    try:
        _killpg(pgid, sig)
    except (OSError, ProcessLookupError):
        return False
    return True


def _force_stop_process(
    process: "_ProcessLike",
    *,
    _platform: "str | None" = None,
    _getpgid: "Callable[[int], int]" = _get_process_group,
    _killpg: "Callable[[int, int], None]" = _kill_process_group,
) -> "None":
    platform = _os_name() if _platform is None else _platform
    if platform == "nt":
        try:
            _kill_windows_process_tree(process)
        finally:
            process.join(_FORCE_STOP_TIMEOUT)
        return
    if platform == "posix" and _verified_kill_process_group(
        process, signal.SIGTERM, _getpgid=_getpgid, _killpg=_killpg
    ):
        process.join(_FORCE_STOP_TIMEOUT)
        if not process.is_alive():
            return
        if _verified_kill_process_group(process, _POSIX_SIGKILL, _getpgid=_getpgid, _killpg=_killpg):
            process.join(_FORCE_STOP_TIMEOUT)
            return
        process.kill()
        process.join(_FORCE_STOP_TIMEOUT)
        return
    process.terminate()
    process.join(_FORCE_STOP_TIMEOUT)
    if process.is_alive():
        process.kill()
        process.join(_FORCE_STOP_TIMEOUT)


class ServerWorkerSupervisor:
    """Own one fresh queue-worker child for a Litestar server invocation."""

    __slots__ = (
        "_config",
        "_connection",
        "_context",
        "_expected_stop",
        "_launch_spec",
        "_lock",
        "_process",
        "_request_parent_shutdown",
        "_send_connection",
        "_shutdown_requested",
        "_start_attempted",
        "_stop_event",
        "_thread_factory",
        "_wait",
        "_watchdog",
    )

    def __init__(
        self,
        config: "QueueConfig",
        *,
        launch_spec: "_WorkerLaunchSpec | None" = None,
        _context: "BaseContext | None" = None,
        _request_parent_shutdown: "Callable[[], None]" = _request_server_shutdown,
        _wait: "Callable[[Sequence[object], float | None], list[object]]" = _connection_wait,
        _thread_factory: "Callable[..., threading.Thread]" = threading.Thread,
    ) -> "None":
        self._config = config
        self._launch_spec = launch_spec
        self._context = _context
        self._request_parent_shutdown = _request_parent_shutdown
        self._wait = _wait
        self._thread_factory = _thread_factory
        self._expected_stop = threading.Event()
        self._shutdown_requested = threading.Event()
        self._lock = threading.Lock()
        self._start_attempted = False
        self._process: "_ProcessLike | None" = None
        self._connection: "Connection | None" = None
        self._send_connection: "Connection | None" = None
        self._stop_event: "_EventLike | None" = None
        self._watchdog: "threading.Thread | None" = None

    @classmethod
    def from_plugin(cls, plugin: "QueuePlugin") -> "ServerWorkerSupervisor":
        return cls(plugin.config)

    def _raise_start_failure(self, stage: "str", exception_type: "str") -> "NoReturn":
        """Fail startup with stage and exception type only.

        Raises:
            QueueConfigurationError: Always.
        """
        msg = f"Server queue worker failed during {stage} ({exception_type})."
        raise QueueConfigurationError(msg)

    def _receive_startup(self, process: "_ProcessLike", connection: "Connection") -> "None":
        ready = self._wait((connection, process.sentinel), self._config.worker.startup_timeout)
        if connection in ready:
            try:
                message = connection.recv()
            except EOFError:
                self._raise_start_failure("bootstrap", "EOFError")
            if (
                isinstance(message, tuple)
                and len(message) == _READY_MESSAGE_LENGTH
                and message[0] == "ready"
                and isinstance(message[1], int)
                and message[1] == process.pid
            ):
                return
            if (
                isinstance(message, tuple)
                and len(message) == _ERROR_MESSAGE_LENGTH
                and message[0] == "error"
                and isinstance(message[1], str)
                and message[1] in _SAFE_STAGES
                and _is_safe_exception_type(message[2])
            ):
                self._raise_start_failure(message[1], cast("str", message[2]))
            self._raise_start_failure("bootstrap", "ProtocolError")
        if process.sentinel in ready or process.exitcode is not None:
            self._raise_start_failure("bootstrap", "ChildProcessError")
        self._raise_start_failure("bootstrap", "TimeoutError")

    def _watch_child(self) -> "None":
        process = self._process
        if process is None:
            return
        try:
            with contextlib.suppress(BaseException):
                process.join()
            if self._expected_stop.is_set() or self._shutdown_requested.is_set():
                return
            self._shutdown_requested.set()
            exit_code = process.exitcode if isinstance(process.exitcode, int) else -1
            logger.error("Server queue worker exited unexpectedly with exit code %s.", exit_code)
            with contextlib.suppress(BaseException):
                self._request_parent_shutdown()
        finally:
            with self._lock:
                self._close_handles()

    def start(self) -> "None":
        with self._lock:
            if self._start_attempted:
                msg = "Server queue worker supervisor has already started."
                raise QueueConfigurationError(msg)
            self._start_attempted = True
            context = self._context or _select_process_context()
            spec = self._launch_spec or _build_launch_spec()
            receive_connection, send_connection = context.Pipe(duplex=False)
            self._connection = receive_connection
            self._send_connection = send_connection
            try:
                stop_event = context.Event()
                process = cast("Any", context).Process(
                    target=_worker_process_main,
                    args=(spec, stop_event, send_connection),
                    name="litestar-queues-server-worker",
                )
            except BaseException:
                self._close_handles()
                raise
            self._process = cast("_ProcessLike", process)
            self._stop_event = cast("_EventLike", stop_event)
            try:
                process.start()
            except BaseException:
                self._cleanup_failed_start()
                raise
            send_close_error = self._close_handle("_send_connection")
            if send_close_error is not None:
                self._cleanup_failed_start()
                raise send_close_error
            try:
                self._receive_startup(cast("_ProcessLike", process), receive_connection)
            except BaseException:
                self._cleanup_failed_start()
                raise
            try:
                watchdog = self._thread_factory(
                    target=self._watch_child, name="litestar-queues-child-watch", daemon=True
                )
                self._watchdog = watchdog
                watchdog.start()
            except BaseException:
                self._cleanup_failed_start()
                raise

    def _cleanup_failed_start(self) -> "None":
        self._expected_stop.set()
        stop_event = self._stop_event
        if stop_event is not None:
            with contextlib.suppress(BaseException):
                stop_event.set()
        process = self._process
        if process is not None:
            try:
                alive = process.is_alive()
            except BaseException:  # noqa: BLE001 - failed status inspection must not skip handle cleanup.
                alive = True
            if alive:
                with contextlib.suppress(BaseException):
                    _force_stop_process(process)
        self._close_handles()

    def _close_handle(self, attribute: "str") -> "BaseException | None":
        handle = cast("Any", getattr(self, attribute))
        if handle is None:
            return None
        try:
            handle.close()
        except BaseException as exc:  # noqa: BLE001 - failed handles remain available for a later retry.
            return exc
        setattr(self, attribute, None)
        return None

    def _close_handles(self) -> "BaseException | None":
        first_error: "BaseException | None" = None
        for attribute in ("_connection", "_send_connection", "_process"):
            error = self._close_handle(attribute)
            if first_error is None and error is not None:
                first_error = error
        if self._connection is None and self._send_connection is None and self._process is None:
            self._stop_event = None
        return first_error

    def close(self) -> "None":
        with self._lock:
            process = self._process
            stop_event = self._stop_event
            if process is None and self._connection is None and self._send_connection is None:
                return
            self._expected_stop.set()
            primary_error: "BaseException | None" = None
            if stop_event is not None:
                try:
                    stop_event.set()
                except BaseException as exc:  # noqa: BLE001 - all remaining cleanup must still be attempted.
                    primary_error = exc
            if process is not None:
                try:
                    process.join(
                        self._config.worker.graceful_shutdown_timeout + self._config.worker.final_cancel_timeout + 1.0
                    )
                except BaseException as exc:  # noqa: BLE001 - force and handle cleanup must still run.
                    if primary_error is None:
                        primary_error = exc
                try:
                    if process.is_alive():
                        _force_stop_process(process)
                except BaseException as exc:  # noqa: BLE001 - handle cleanup must still run.
                    if primary_error is None:
                        primary_error = exc
            handle_error = self._close_handles()
            if primary_error is not None:
                raise primary_error
            if handle_error is not None:
                raise handle_error

import json
import os
import signal
import socket
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path
from typing import Any, cast
from urllib.error import URLError
from urllib.request import Request, urlopen
from uuid import uuid4

import psutil  # type: ignore[import-untyped]
import pytest
from typing_extensions import Self

pytestmark = pytest.mark.topology

ROOT = Path(__file__).resolve().parents[3]
APP_PATH = "tests.helpers.support.server_worker_app:create_app"
WINDOWS_CREATE_NEW_PROCESS_GROUP = cast("int", getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
WINDOWS_CTRL_BREAK_EVENT = cast("int", getattr(signal, "CTRL_BREAK_EVENT", signal.SIGINT))


def _free_port() -> "int":
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _json_request(url: str, *, method: str = "GET") -> "dict[str, Any]":
    request = Request(url, method=method)
    with urlopen(request, timeout=1) as response:
        return cast("dict[str, Any]", json.loads(response.read()))


def _wait_for_json(url: str, *, timeout: float = 20) -> "dict[str, Any]":
    deadline = time.monotonic() + timeout
    last_error: "BaseException | None" = None
    while time.monotonic() < deadline:
        try:
            return _json_request(url)
        except (OSError, URLError, TimeoutError) as exc:  # noqa: PERF203
            last_error = exc
            time.sleep(0.02)
    message = f"server did not become ready: {type(last_error).__name__}"
    raise AssertionError(message)


def _wait_for(predicate: "Any", *, timeout: float = 20, message: str) -> "None":
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(message)


class ServerProcess:
    def __init__(self, tmp_path: "Path", *, server: str, workers: int = 1) -> "None":
        self.marker_dir = tmp_path / "markers"
        self.marker_dir.mkdir()
        self.output_path = tmp_path / "server.log"
        self.port = _free_port()
        self.server = server
        self.workers = workers
        self.process: "subprocess.Popen[bytes] | None" = None
        self.forced_shutdown = False
        self._output: "Any" = None
        self._descendants: "dict[int, psutil.Process]" = {}

    def log_tail(self, limit: int = 2048) -> "str":
        return self.output_path.read_text(encoding="utf-8", errors="replace")[-limit:]

    @property
    def base_url(self) -> "str":
        return f"http://127.0.0.1:{self.port}"

    def __enter__(self) -> "Self":
        environment = os.environ.copy()
        python_path = str(ROOT / "src")
        if environment.get("PYTHONPATH"):
            python_path = f"{python_path}{os.pathsep}{environment['PYTHONPATH']}"
        environment.update({
            "LITESTAR_APP": APP_PATH,
            "LITESTAR_QUEUES_TEST_INVOCATION": uuid4().hex,
            "LITESTAR_QUEUES_TEST_MARKERS": str(self.marker_dir),
            "LITESTAR_QUEUES_TEST_SERVER": self.server,
            "PYTHONPATH": python_path,
        })
        command = [
            sys.executable,
            "-m",
            "litestar",
            "--app",
            APP_PATH,
            "run",
            "--host",
            "127.0.0.1",
            "--port",
            str(self.port),
        ]
        if self.server == "granian":
            command.extend(("--workers", str(self.workers)))
        self._output = self.output_path.open("wb")
        creationflags = WINDOWS_CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        self.process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=environment,
            stdout=self._output,
            stderr=subprocess.STDOUT,
            creationflags=creationflags,
            start_new_session=os.name != "nt",
        )
        try:
            _wait_for_json(f"{self.base_url}/health")
            self._remember_descendants()
        except BaseException:  # noqa: BLE001 - always reap a failed subprocess launch.
            self.close(assert_clean=False)
            tail = self.output_path.read_text(encoding="utf-8", errors="replace")[-2048:]
            message = f"server failed to start\n{tail}"
            raise AssertionError(message) from None
        return self

    def markers(self, role: str) -> "list[dict[str, Any]]":
        return [
            cast("dict[str, Any]", json.loads(path.read_text(encoding="utf-8")))
            for path in sorted(self.marker_dir.glob(f"{role}-*.json"))
        ]

    def _remember_descendants(self) -> "None":
        process = self.process
        if process is None:
            return
        try:
            descendants = psutil.Process(process.pid).children(recursive=True)
        except psutil.Error:
            return
        self._descendants.update({descendant.pid: descendant for descendant in descendants})

    @staticmethod
    def _live(processes: "list[psutil.Process]") -> "list[psutil.Process]":
        live: "list[psutil.Process]" = []
        for process in processes:
            with suppress(psutil.Error):
                if process.is_running() and process.status() != psutil.STATUS_ZOMBIE:
                    live.append(process)
        return live

    def _reap_descendants(self, *, assert_clean: bool) -> "None":
        descendants = list(self._descendants.values())
        _, live = psutil.wait_procs(descendants, timeout=5)
        leaked = self._live(live)
        for process in leaked:
            with suppress(psutil.Error):
                process.terminate()
        _, live = psutil.wait_procs(leaked, timeout=2)
        for process in self._live(live):
            with suppress(psutil.Error):
                process.kill()
        psutil.wait_procs(live, timeout=2)
        remaining = self._live(descendants)
        if assert_clean and leaked:
            message = f"server descendants survived normal shutdown: {[process.pid for process in leaked]}"
            raise AssertionError(message)
        if remaining:
            message = f"could not reap server descendants: {[process.pid for process in remaining]}"
            raise AssertionError(message)

    def close(self, *, assert_clean: bool = True) -> "None":
        process = self.process
        if process is None:
            return
        self._remember_descendants()
        try:
            if process.poll() is None:
                process.send_signal(WINDOWS_CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGINT)
                try:
                    # Windows runners unwind a server lifespan far slower than the
                    # POSIX ones, and a premature kill skips the storage teardown
                    # this suite exists to prove.
                    process.wait(timeout=45 if os.name == "nt" else 15)
                except subprocess.TimeoutExpired:
                    self.forced_shutdown = True
                    process.kill()
                    process.wait(timeout=5)
            self._reap_descendants(assert_clean=assert_clean)
            with pytest.raises(psutil.NoSuchProcess):
                psutil.Process(process.pid)
        finally:
            self.process = None
            if self._output is not None:
                self._output.close()
                self._output = None

    def __exit__(self, exc_type: object, _exc: object, _traceback: object) -> "None":
        self.close(assert_clean=exc_type is None)


@pytest.mark.parametrize(("server", "workers"), [("uvicorn", 1), ("granian", 3)])
def test_server_placement_owns_exactly_one_fresh_queue_child(tmp_path: "Path", server: str, workers: int) -> "None":
    if server == "granian" and os.name == "nt" and workers > 1:
        # Granian serves requests here (the health check passes) but only ever
        # registers one web process, so the multi-worker placement proof runs on
        # the POSIX runners. Gated rather than weakened so the assertion keeps
        # its meaning where multiple workers do start.
        pytest.skip("Granian does not start multiple web workers on Windows runners")

    with ServerProcess(tmp_path, server=server, workers=workers) as running:
        _wait_for(
            lambda: len(running.markers("web")) == workers,
            message=(
                f"expected {workers} web process markers, saw {len(running.markers('web'))}\n{running.log_tail()}"
            ),
        )
        _wait_for(lambda: len(running.markers("queue")) == 1, message="expected one queue child marker")
        queue_pid = int(running.markers("queue")[0]["pid"])
        web_pids = {int(marker["pid"]) for marker in running.markers("web")}

        assert queue_pid not in web_pids
        assert len(web_pids) == workers

        token = uuid4().hex
        task_id = str(_json_request(f"{running.base_url}/enqueue/{token}", method="POST")["task_id"])
        terminal: "dict[str, Any]" = {}

        def completed() -> "bool":
            nonlocal terminal
            terminal = _json_request(f"{running.base_url}/tasks/{task_id}")
            return cast("str | None", terminal.get("status")) == "completed"

        _wait_for(completed, message="enqueued task did not complete")
        assert terminal["result"] == queue_pid

        paths = {Path(str(marker["ephemeral_path"])) for marker in running.markers("web")}
        assert len(paths) == 1
        database_path = paths.pop()
        assert database_path.is_file()
        database_directory = database_path.parent

    # The server removes its private database on the way out. A survivor means
    # either the lifespan never unwound (forced kill) or a handle outlived the
    # retry budget, so report which one rather than just that a file exists.
    assert not running.forced_shutdown, f"server did not shut down on signal\n{running.log_tail()}"
    assert not database_path.exists(), f"ephemeral database survived shutdown\n{running.log_tail()}"
    assert not database_directory.exists(), f"ephemeral directory survived shutdown\n{running.log_tail()}"

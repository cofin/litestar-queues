"""Process lifecycle for the shipped HTMX realtime examples."""

import os
import signal
import socket
import subprocess
import time
from collections import deque
from collections.abc import Mapping
from pathlib import Path
from threading import Lock, Thread
from typing import Literal

import httpx

from .assertions import assert_asset_urls_on_origin, assert_production_assets
from .health_check import wait_for_paths

REPO_ROOT = Path(__file__).resolve().parents[3]
ExampleMode = Literal["dev", "production"]

EXAMPLE_APPS: dict[str, str] = {
    "sse": "examples.htmx_realtime_sse.app:app",
    "websocket": "examples.htmx_realtime_websocket.app:app",
    "sse_redis": "examples.htmx_realtime_sse_redis.app:app",
    "websocket_redis": "examples.htmx_realtime_websocket_redis.app:app",
    "sse_valkey": "examples.htmx_realtime_sse_valkey.app:app",
    "websocket_valkey": "examples.htmx_realtime_websocket_valkey.app:app",
}


def find_free_port() -> int:
    """Ask the operating system for an available local TCP port.

    Returns:
        An available local TCP port.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class ExampleServer:
    """Start one realtime example through the Litestar CLI."""

    def __init__(self, example_name: str, *, mode: ExampleMode, environment: Mapping[str, str] | None = None) -> None:
        if example_name not in EXAMPLE_APPS:
            message = f"Unsupported browser example: {example_name!r}"
            raise ValueError(message)
        if mode not in {"dev", "production"}:
            message = f"Unsupported example mode: {mode!r}"
            raise ValueError(message)

        self.example_name = example_name
        self.mode = mode
        self.port: int | None = None
        self._process: subprocess.Popen[str] | None = None
        self._log_lines: deque[str] = deque(maxlen=40)
        self._log_lock = Lock()
        self._capture_thread: Thread | None = None
        self._extra_environment = dict(environment or {})

    @property
    def base_url(self) -> str:
        """Return the Litestar origin once the server has started."""
        if self.port is None:
            message = "Example server has not started"
            raise RuntimeError(message)
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        """Install/build assets as needed and start the Litestar process."""
        if self._process is not None:
            message = "Example server is already started"
            raise RuntimeError(message)

        env = self._environment()
        if self.mode == "production":
            self._run_cli(["assets", "install"], env=env)
            self._run_cli(["assets", "build"], env=env)

        self.port = find_free_port()
        command = ["uv", "run", "litestar", "run", "--host", "127.0.0.1", "--port", str(self.port)]
        self._process = subprocess.Popen(
            command,
            cwd=REPO_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        self._capture_thread = Thread(target=self._capture_output, daemon=True)
        self._capture_thread.start()

    def wait_until_ready(self, timeout: float) -> None:
        """Wait for the app, status route, and required asset mode to respond."""
        try:
            self._check_process_alive()
            wait_for_paths(self.base_url, ("/", "/demo/status"), timeout=timeout)

            if self.mode == "dev":
                wait_for_paths(self.base_url, ("/static/@vite/client",), timeout=timeout)

            with httpx.Client(base_url=self.base_url, follow_redirects=True) as client:
                page = client.get("/", timeout=5.0)
                page.raise_for_status()
                assert_asset_urls_on_origin(page.text, self.base_url)
                if self.mode == "production":
                    assert_production_assets(page.text)

            self._check_process_alive()
        except (TimeoutError, RuntimeError, httpx.HTTPError) as exc:
            message = f"{exc}\nRecent server output:\n{self._recent_logs()}"
            raise RuntimeError(message) from exc

    def stop(self) -> None:
        """Terminate the Litestar process and its process group."""
        process = self._process
        self._process = None
        if process is None:
            return

        if process.poll() is None:
            if os.name == "nt":
                process.terminate()
            else:
                os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                if os.name == "nt":
                    process.kill()
                else:
                    os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5.0)

        if self._capture_thread is not None:
            self._capture_thread.join(timeout=2.0)
            self._capture_thread = None

    def _environment(self) -> dict[str, str]:
        env = os.environ.copy()
        env.update({
            "LITESTAR_APP": EXAMPLE_APPS[self.example_name],
            "LITESTAR_QUEUES_EXAMPLE_STEPS": "2",
            "LITESTAR_QUEUES_EXAMPLE_STEP_DELAY": "0.05",
            "LITESTAR_QUEUES_EXAMPLE_VITE_DEV": "1" if self.mode == "dev" else "0",
            "PYTHONUNBUFFERED": "1",
        })
        env.update(self._extra_environment)
        return env

    def _run_cli(self, arguments: list[str], *, env: dict[str, str]) -> None:
        result = subprocess.run(
            ["uv", "run", "litestar", *arguments],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
            timeout=180.0,
        )
        if result.returncode:
            output = "\n".join((result.stdout + result.stderr).splitlines()[-40:])
            message = f"Litestar CLI command failed ({arguments!r}) with code {result.returncode}:\n{output}"
            raise RuntimeError(message)

    def _capture_output(self) -> None:
        process = self._process
        if process is None or process.stdout is None:
            return
        for line in process.stdout:
            with self._log_lock:
                self._log_lines.append(line.rstrip())

    def _recent_logs(self) -> str:
        with self._log_lock:
            return "\n".join(self._log_lines)

    def _check_process_alive(self) -> None:
        process = self._process
        if process is None:
            message = "Example server has not started"
            raise RuntimeError(message)
        if process.poll() is None:
            return
        with self._log_lock:
            output = "\n".join(self._log_lines)
        message = f"Example server exited with code {process.returncode}:\n{output}"
        raise RuntimeError(message)


class QueueWorker:
    """Run a queue worker process against an example app."""

    def __init__(self, example_name: str, *, environment: Mapping[str, str]) -> None:
        if example_name not in EXAMPLE_APPS:
            message = f"Unsupported browser example: {example_name!r}"
            raise ValueError(message)

        self.command = ["uv", "run", "litestar", "queues", "run", "--queue", "demo", "--drain-timeout", "30"]
        self.environment = os.environ.copy()
        self.environment.update(environment)
        self.environment.update({"LITESTAR_APP": EXAMPLE_APPS[example_name], "PYTHONUNBUFFERED": "1"})
        self._process: subprocess.Popen[str] | None = None
        self._log_lines: deque[str] = deque(maxlen=40)
        self._log_lock = Lock()
        self._capture_thread: Thread | None = None

    def start(self) -> None:
        """Start the standalone Litestar queue worker."""
        if self._process is not None:
            message = "Queue worker is already started"
            raise RuntimeError(message)
        self._process = subprocess.Popen(
            self.command,
            cwd=REPO_ROOT,
            env=self.environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        self._capture_thread = Thread(target=self._capture_output, daemon=True)
        self._capture_thread.start()

    def wait_until_running(self, timeout: float = 10.0) -> None:
        """Wait until the worker process remains alive for one polling interval."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self._check_process_alive()
            time.sleep(0.1)
            self._check_process_alive()
            return
        message = f"Queue worker did not remain alive within {timeout}s:\n{self._recent_logs()}"
        raise TimeoutError(message)

    def stop(self) -> None:
        """Terminate the worker process and its process group."""
        process = self._process
        self._process = None
        if process is None:
            return
        if process.poll() is None:
            if os.name == "nt":
                process.terminate()
            else:
                os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                if os.name == "nt":
                    process.kill()
                else:
                    os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5.0)
        if self._capture_thread is not None:
            self._capture_thread.join(timeout=2.0)
            self._capture_thread = None

    def _capture_output(self) -> None:
        process = self._process
        if process is None or process.stdout is None:
            return
        for line in process.stdout:
            with self._log_lock:
                self._log_lines.append(line.rstrip())

    def _recent_logs(self) -> str:
        with self._log_lock:
            return "\n".join(self._log_lines)

    def _check_process_alive(self) -> None:
        process = self._process
        if process is None:
            message = "Queue worker has not started"
            raise RuntimeError(message)
        if process.poll() is None:
            return
        message = f"Queue worker exited with code {process.returncode}:\n{self._recent_logs()}"
        raise RuntimeError(message)

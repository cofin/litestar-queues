"""Subprocess-driven drain test for ``litestar queues run``.

``CliRunner`` cannot deliver real signals, so this test spawns the real
CLI in a subprocess, sends SIGTERM, and asserts the process exits cleanly
within the drain window.

Skipped on Windows because SIGTERM is not meaningfully delivered there.
"""

import asyncio
import os
import select
import signal
import subprocess
import sys
import time

import pytest

pytestmark = [
    pytest.mark.anyio,
    pytest.mark.timeout(15),
    pytest.mark.skipif(sys.platform.startswith("win"), reason="SIGTERM unavailable on Windows"),
]


async def test_run_worker_returns_2_when_single_signal_drain_timeout_cancels(
    monkeypatch: "pytest.MonkeyPatch",
) -> "None":
    from litestar_queues import QueueConfig, QueuePlugin, task
    from litestar_queues._cli import _run_worker
    from litestar_queues.backends import InMemoryQueueBackend

    started = asyncio.Event()

    @task("cli.stuck")
    async def stuck() -> "None":
        started.set()
        await asyncio.Event().wait()

    backend = InMemoryQueueBackend()
    await backend.enqueue("cli.stuck")
    plugin = QueuePlugin(
        QueueConfig(
            execution_backend="local", in_app_worker=False, worker_poll_interval=0.01, worker_final_cancel_timeout=0.1
        )
    )
    plugin._queue_backend = backend
    handlers: "dict[signal.Signals, object]" = {}

    def add_signal_handler(sig: "signal.Signals", callback: "object") -> "None":
        handlers[sig] = callback

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", add_signal_handler)
    run_task = asyncio.create_task(_run_worker(plugin, 1, 0.01, ()))

    await asyncio.wait_for(started.wait(), timeout=1)
    handler = handlers[signal.SIGTERM]
    assert callable(handler)
    handler()

    assert await asyncio.wait_for(run_task, timeout=2) == 2


async def test_run_worker_returns_1_when_worker_loop_crashes(monkeypatch: "pytest.MonkeyPatch") -> "None":
    from litestar_queues import QueueConfig, QueuePlugin
    from litestar_queues import _cli as cli_module

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda *_args: None)
    monkeypatch.setattr(cli_module, "Worker", _FailingStartWorker)
    plugin = QueuePlugin(QueueConfig(execution_backend="local", in_app_worker=False))

    assert await cli_module._run_worker(plugin, 1, 0.01, ()) == 1


async def test_run_worker_uses_configured_heartbeat_miss_threshold(monkeypatch: "pytest.MonkeyPatch") -> "None":
    from litestar_queues import QueueConfig, QueuePlugin
    from litestar_queues import _cli as cli_module

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda *_args: None)
    monkeypatch.setattr(cli_module, "Worker", _CapturingStartWorker)
    _CapturingStartWorker.instances.clear()
    plugin = QueuePlugin(QueueConfig(execution_backend="local", in_app_worker=False, worker_heartbeat_miss_threshold=7))

    assert await cli_module._run_worker(plugin, 1, 0.01, ()) == 0
    assert _CapturingStartWorker.instances[0].kwargs["heartbeat_miss_threshold"] == 7


async def test_run_worker_uses_configured_poll_backoff_settings(monkeypatch: "pytest.MonkeyPatch") -> "None":
    from litestar_queues import QueueConfig, QueuePlugin
    from litestar_queues import _cli as cli_module

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda *_args: None)
    monkeypatch.setattr(cli_module, "Worker", _CapturingStartWorker)
    _CapturingStartWorker.instances.clear()
    plugin = QueuePlugin(
        QueueConfig(
            execution_backend="local",
            in_app_worker=False,
            worker_poll_backoff_max=2.0,
            worker_poll_backoff_multiplier=1.5,
            worker_poll_jitter=0.1,
        )
    )

    assert await cli_module._run_worker(plugin, 1, 0.01, ()) == 0
    kwargs = _CapturingStartWorker.instances[0].kwargs
    assert kwargs["poll_backoff_max"] == 2.0
    assert kwargs["poll_backoff_multiplier"] == 1.5
    assert kwargs["poll_jitter"] == 0.1


def test_run_subcommand_drains_on_sigterm() -> "None":
    env = os.environ.copy()
    env["LITESTAR_APP"] = "tests.support.cli_app:app"
    env["PYTHONPATH"] = "src" + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")

    proc = subprocess.Popen(
        [sys.executable, "-m", "litestar", "queues", "run", "--drain-timeout", "2"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        _wait_for_worker_started(proc)
        assert proc.poll() is None, "worker exited before SIGTERM was sent"
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            pytest.fail("worker did not drain within 8s after SIGTERM")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)

    assert proc.returncode == 0, (
        f"expected clean drain (exit 0), got {proc.returncode}; "
        f"stderr={proc.stderr.read().decode()[-500:] if proc.stderr else ''!r}"
    )


def _wait_for_worker_started(proc: "subprocess.Popen[bytes]", *, timeout: "float" = 8.0) -> "None":
    """Wait until the worker command has installed signal handlers.

    Returns:
        None.
    """
    assert proc.stderr is not None
    deadline = time.monotonic() + timeout
    stderr = []
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            break
        ready, _, _ = select.select([proc.stderr], [], [], 0.1)
        if not ready:
            continue
        line = proc.stderr.readline().decode()
        stderr.append(line)
        if "litestar queues worker started" in line:
            return
    pytest.fail(f"worker did not report startup before SIGTERM; stderr={''.join(stderr)[-500:]!r}")


class _FailingStartWorker:
    __slots__ = ()

    def __init__(self, *_args: "object", **_kwargs: "object") -> "None":
        pass

    async def start(self) -> "None":
        msg = "worker crashed"
        raise RuntimeError(msg)

    async def stop(self, *, force: "bool" = False) -> "bool":
        return False


class _CapturingStartWorker:
    __slots__ = ("kwargs",)

    instances: "list[_CapturingStartWorker]" = []

    def __init__(self, *_args: "object", **kwargs: "object") -> "None":
        self.kwargs = kwargs
        self.instances.append(self)

    async def start(self) -> "None":
        return None

    async def stop(self, *, force: "bool" = False) -> "bool":
        return False

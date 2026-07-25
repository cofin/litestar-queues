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
from typing import TYPE_CHECKING

import pytest

from litestar_queues import WorkerConfig

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from pathlib import Path

    from litestar_queues import QueueConfig, QueuePlugin
    from litestar_queues.service import QueueService
    from litestar_queues.worker.runtime import WorkerRunResult

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = [
    pytest.mark.anyio,
    pytest.mark.timeout(15),
    pytest.mark.skipif(sys.platform.startswith("win"), reason="SIGTERM unavailable on Windows"),
]


def _plugin(**worker_kwargs: object) -> "QueuePlugin":
    from litestar_queues import QueueConfig, QueuePlugin

    return QueuePlugin(
        QueueConfig(
            queue_backend="memory",
            execution_backend="local",
            worker=WorkerConfig(placement="external", **worker_kwargs),  # type: ignore[arg-type]
        )
    )


def _capture_runner(
    monkeypatch: "pytest.MonkeyPatch", behaviour: "Callable[..., Awaitable[object]]"
) -> "list[dict[str, object]]":
    """Replace the shared runner and record what the CLI hands it."""
    from litestar_queues.worker import runtime

    calls: "list[dict[str, object]]" = []

    async def fake_run_worker(
        service: "QueueService",
        config: "QueueConfig",
        *,
        graceful_stop: "asyncio.Event",
        force_stop: "asyncio.Event",
        ready: "Callable[[], None] | None" = None,
        **_: object,
    ) -> "object":
        calls.append({"service": service, "config": config, "graceful_stop": graceful_stop, "force_stop": force_stop})
        return await behaviour(graceful_stop=graceful_stop, force_stop=force_stop, ready=ready)

    monkeypatch.setattr(runtime, "run_worker", fake_run_worker)
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda *_args: None)
    return calls


async def test_run_worker_delegates_to_the_shared_runner(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """The CLI is a thin adapter; the runner owns tasks, schedules, and the service."""
    from litestar_queues.worker.runtime import WorkerRunResult

    async def clean(**_kwargs: object) -> "WorkerRunResult":
        return WorkerRunResult.CLEAN

    calls = _capture_runner(monkeypatch, clean)
    from litestar_queues._cli import _run_worker

    plugin = _plugin(heartbeat_miss_threshold=7, poll_backoff_max=2.0, poll_backoff_multiplier=1.5, poll_jitter=0.1)

    assert await _run_worker(plugin, 4, 0.5, ("reports",)) == 0
    assert len(calls) == 1
    worker_config = calls[0]["config"].worker  # type: ignore[union-attr]
    assert worker_config.heartbeat_miss_threshold == 7
    assert worker_config.poll_backoff_max == 2.0
    assert worker_config.poll_backoff_multiplier == 1.5
    assert worker_config.poll_jitter == 0.1
    assert worker_config.max_concurrency == 4
    assert worker_config.graceful_shutdown_timeout == 0.5
    assert worker_config.queues == ("reports",)


async def test_run_worker_uses_a_worker_owned_service(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """A standalone worker owns its event resources; it never shares the app service."""
    from litestar_queues.worker.runtime import WorkerRunResult

    async def clean(**_kwargs: object) -> "WorkerRunResult":
        return WorkerRunResult.CLEAN

    calls = _capture_runner(monkeypatch, clean)
    from litestar_queues._cli import _run_worker

    plugin = _plugin()

    assert await _run_worker(plugin, 1, 0.01, ()) == 0
    service = calls[0]["service"]
    assert service is not plugin.get_service()
    assert service.get_queue_backend() is not plugin.get_service().get_queue_backend()


async def test_run_worker_does_not_duplicate_task_module_loading(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """Task modules and schedules are initialized once, by the runner."""
    import inspect

    from litestar_queues import _cli as cli_module

    source = inspect.getsource(cli_module.run_command.callback)
    assert "load_task_modules" not in source
    assert "initialize_schedules" not in source


async def test_first_signal_requests_a_graceful_stop(monkeypatch: "pytest.MonkeyPatch") -> "None":
    from litestar_queues.worker.runtime import WorkerRunResult

    handlers: "dict[object, object]" = {}
    observed: "dict[str, bool]" = {}

    async def drain(*, graceful_stop, force_stop, ready=None, **_kwargs: object) -> "WorkerRunResult":
        if ready is not None:
            ready()
        await graceful_stop.wait()
        observed["forced_during_drain"] = force_stop.is_set()
        return WorkerRunResult.ESCALATED

    from litestar_queues.worker import runtime

    async def fake_run_worker(
        service: "QueueService",
        config: "QueueConfig",
        *,
        graceful_stop: "asyncio.Event",
        force_stop: "asyncio.Event",
        ready: "Callable[[], None] | None" = None,
        **_: object,
    ) -> "WorkerRunResult":
        del service, config
        return await drain(graceful_stop=graceful_stop, force_stop=force_stop, ready=ready)

    monkeypatch.setattr(runtime, "run_worker", fake_run_worker)
    loop = asyncio.get_running_loop()

    def record_handler(sig: "object", callback: "object") -> "None":
        handlers[sig] = callback

    monkeypatch.setattr(loop, "add_signal_handler", record_handler)
    from litestar_queues._cli import _run_worker

    run = asyncio.create_task(_run_worker(_plugin(), 1, 0.01, ()))
    await asyncio.sleep(0)
    handlers[signal.SIGTERM]()  # type: ignore[operator]

    assert await asyncio.wait_for(run, timeout=2) == 2
    assert observed["forced_during_drain"] is False


async def test_second_signal_forces_while_the_first_drain_is_pending(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """The force request must not queue behind the graceful drain."""
    from litestar_queues.worker import runtime
    from litestar_queues.worker.runtime import WorkerRunResult

    handlers: "dict[object, object]" = {}
    draining = asyncio.Event()

    async def fake_run_worker(
        service: "QueueService",
        config: "QueueConfig",
        *,
        graceful_stop: "asyncio.Event",
        force_stop: "asyncio.Event",
        ready: "Callable[[], None] | None" = None,
        **_: object,
    ) -> "WorkerRunResult":
        del service, config, ready
        await graceful_stop.wait()
        draining.set()
        await asyncio.wait_for(force_stop.wait(), timeout=2)
        return WorkerRunResult.ESCALATED

    monkeypatch.setattr(runtime, "run_worker", fake_run_worker)
    loop = asyncio.get_running_loop()

    def record_handler(sig: "object", callback: "object") -> "None":
        handlers[sig] = callback

    monkeypatch.setattr(loop, "add_signal_handler", record_handler)
    from litestar_queues._cli import _run_worker

    run = asyncio.create_task(_run_worker(_plugin(), 1, 0.01, ()))
    await asyncio.sleep(0)
    handlers[signal.SIGTERM]()  # type: ignore[operator]
    await asyncio.wait_for(draining.wait(), timeout=2)
    handlers[signal.SIGINT]()  # type: ignore[operator]

    assert await asyncio.wait_for(run, timeout=3) == 2


async def test_runner_failure_maps_to_the_crash_exit_code(monkeypatch: "pytest.MonkeyPatch") -> "None":
    async def crash(**_kwargs: object) -> "None":
        msg = "worker loop crashed"
        raise RuntimeError(msg)

    _capture_runner(monkeypatch, crash)
    from litestar_queues._cli import _run_worker

    assert await _run_worker(_plugin(), 1, 0.01, ()) == 1


async def test_run_command_rejects_process_local_and_managed_configurations() -> "None":
    """Storage and ownership are validated before any service is opened."""
    import click

    from litestar_queues import QueueConfig, QueuePlugin
    from litestar_queues._cli import _reject_ephemeral_storage, _reject_managed_placement

    with pytest.raises(click.ClickException, match="litestar run"):
        _reject_ephemeral_storage(QueuePlugin(QueueConfig()), "run")

    for placement in ("server", "asgi"):
        plugin = QueuePlugin(
            QueueConfig(queue_backend="redis", worker=WorkerConfig(placement=placement))  # type: ignore[arg-type]
        )
        with pytest.raises(click.ClickException, match="placement='external'"):
            _reject_managed_placement(plugin)

    inline = QueuePlugin(
        QueueConfig(queue_backend="memory", execution_backend="immediate", worker=WorkerConfig(placement="external"))
    )
    with pytest.raises(click.ClickException, match="immediate"):
        _reject_managed_placement(inline)


# This spawns a real interpreter (cold imports: litestar, click, litestar_queues,
# and their transitive deps) and waits for it to reach the CLI's startup log
# line before signaling it. That subprocess-startup latency -- not the drain
# itself -- is what varies under load on shared/constrained CI runners, so the
# module-level timeout(15) is not generous enough here; override it per-test
# rather than loosening the budget for the other, in-process tests above.
@pytest.mark.timeout(45)
def test_run_subcommand_drains_on_sigterm(tmp_path: "Path") -> "None":
    env = os.environ.copy()
    env["LITESTAR_APP"] = "tests.support.cli_worker_app:app"
    env["LITESTAR_QUEUES_TEST_DB"] = str(tmp_path / "queue.db")
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
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            pytest.fail("worker did not drain within 15s after SIGTERM")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)

    assert proc.returncode == 0, (
        f"expected clean drain (exit 0), got {proc.returncode}; "
        f"stderr={proc.stderr.read().decode()[-500:] if proc.stderr else ''!r}"
    )


def _wait_for_worker_started(proc: "subprocess.Popen[bytes]", *, timeout: "float" = 25.0) -> "None":
    """Wait until the worker command has installed signal handlers.

    Polls stderr for the startup marker instead of sleeping a fixed duration,
    so this only waits as long as the subprocess actually needs to import and
    reach that line -- generous enough to absorb cold-import latency on a
    loaded CI runner without masking a genuinely broken startup path.

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
    __slots__ = ("config",)

    instances: "list[_CapturingStartWorker]" = []

    def __init__(self, _service: "object", config: "WorkerConfig") -> "None":
        self.config = config
        self.instances.append(self)

    async def start(self) -> "None":
        return None

    async def stop(self, *, force: "bool" = False) -> "bool":
        return False

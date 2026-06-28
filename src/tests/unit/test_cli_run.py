"""Subprocess-driven drain test for ``litestar queues run``.

``CliRunner`` cannot deliver real signals, so this test spawns the real
CLI in a subprocess, sends SIGTERM, and asserts the process exits cleanly
within the drain window.

Skipped on Windows because SIGTERM is not meaningfully delivered there.
"""

import os
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
        time.sleep(1.5)
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

from pathlib import Path

TESTS = Path("src/tests")


def test_end_to_end_harness_resolves_the_real_repository_root() -> None:
    """The harness launches example apps with the repo root as its cwd.

    ``REPO_ROOT`` is counted in parent hops, so moving the harness between test
    tiers silently retargets it. Pointing one level short leaves the subprocess
    in ``src/``, where ``examples`` does not exist, and every example server
    fails to import rather than reporting a bad path.
    """
    from tests.integration.e2e.server_manager import REPO_ROOT

    assert (REPO_ROOT / "pyproject.toml").is_file()
    assert (REPO_ROOT / "examples" / "htmx_realtime_websocket" / "app.py").is_file()


def test_shared_test_helpers_use_the_single_helpers_tree() -> None:
    assert not (TESTS / "support").exists()
    assert not (TESTS / "_factories").exists()
    assert not (TESTS / "topology").exists()
    assert not (TESTS / "e2e").exists()
    assert (TESTS / "helpers" / "_timing.py").is_file()
    assert (TESTS / "helpers" / "queue_tasks.py").is_file()
    assert (TESTS / "helpers" / "support" / "server_worker_app.py").is_file()


def test_the_standard_test_job_leaves_the_browser_suites_to_their_own_workflow() -> None:
    """The end-to-end suites need an installer the standard test job never runs.

    They live under ``src/tests/integration`` now, which that job sweeps wholesale.
    Collecting them there starts example servers against an environment with no
    Chromium and no frontend assets, and those servers run ``uv`` against the very
    virtualenv the session is importing from.
    """
    workflow = Path(".github/workflows/test.yml").read_text(encoding="utf-8")

    commands = [line.strip() for line in workflow.splitlines() if "uv run pytest" in line]

    assert commands
    assert all('-m "not e2e"' in command for command in commands)

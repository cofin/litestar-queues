"""Source gates that keep worker ownership explicit."""

import ast
import inspect
from pathlib import Path

from litestar.plugins import CLIPlugin

from litestar_queues import QueuePlugin

PACKAGE_ROOT = Path("src/litestar_queues")


def _python_sources(*roots: "Path") -> "list[Path]":
    return [path for root in roots for path in sorted(root.rglob("*.py"))]


def test_queue_plugin_inherits_the_concrete_cli_plugin() -> "None":
    """Litestar registers server lifespans only for concrete ``CLIPlugin`` subclasses."""
    assert issubclass(QueuePlugin, CLIPlugin)
    assert "server_lifespan" in vars(QueuePlugin)


def test_the_server_branch_of_the_asgi_lifespan_starts_nothing() -> "None":
    """The ASGI lifespan validates server placement; it never owns the worker."""
    source = inspect.getsource(QueuePlugin._lifespan)

    assert "ServerWorkerSupervisor" not in source
    assert "supervisor" not in source
    assert "Process" not in source
    # The one Worker construction is guarded by explicit ASGI placement.
    assert source.count("Worker(") == 1
    assert 'placement == "asgi"' in source


def test_server_lifespan_validates_before_creating_anything() -> "None":
    """Validation runs before the invocation marker, the database, and the child."""
    source = inspect.getsource(QueuePlugin.server_lifespan)

    assert source.index("_validate_server_placement") < source.index("server_context")
    assert source.index("server_context") < source.index("ServerWorkerSupervisor")


def test_the_package_never_imports_a_specific_asgi_server() -> "None":
    """``CLIPlugin.server_lifespan`` is the whole integration contract."""
    forbidden = ("litestar_granian", "granian", "uvicorn", "hypercorn", "daphne")
    offenders: "list[str]" = []
    for path in _python_sources(PACKAGE_ROOT):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                modules = [node.module or ""]
            else:
                continue
            offenders.extend(
                f"{path}:{node.lineno}: {module}" for module in modules if module.split(".")[0] in forbidden
            )

    assert offenders == []


def test_the_worker_entry_points_stay_click_free() -> "None":
    """``click`` belongs to the CLI surface, not to the worker runtime."""
    offenders = [
        str(path)
        for path in (
            PACKAGE_ROOT / "worker" / "supervisor.py",
            PACKAGE_ROOT / "worker" / "runtime.py",
            PACKAGE_ROOT / "worker" / "invocation.py",
        )
        if "click" in path.read_text(encoding="utf-8")
    ]

    assert offenders == []

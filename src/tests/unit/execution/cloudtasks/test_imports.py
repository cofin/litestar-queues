"""Registration, export surface, and the optional-dependency import boundary.

``google-cloud-tasks`` is an extra. Selecting, validating, or even importing the
Cloud Tasks surface must therefore work on an installation that never asked for
it -- the same lazy boundary the Cloud Run backend already holds.
"""

import subprocess
import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

_TIMEOUT = 20


def _run(code: "str") -> "None":
    """Execute a boundary assertion in a fresh interpreter.

    In-process imports from earlier tests leak into ``sys.modules``, so the
    absence of a module is only meaningful in a subprocess.
    """
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, check=False, timeout=_TIMEOUT)
    assert result.returncode == 0, result.stderr.decode()


# --------------------------------------------------------------------------- packaging


def test_cloud_tasks_is_an_optional_extra() -> "None":
    pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

    optional_dependencies = pyproject["project"]["optional-dependencies"]

    assert "cloud-tasks" in optional_dependencies
    assert any(entry.startswith("google-cloud-tasks") for entry in optional_dependencies["cloud-tasks"])
    assert not any(entry.startswith("google-cloud-tasks") for entry in pyproject["project"].get("dependencies", []))


# --------------------------------------------------------------------------- registry


def test_cloudtasks_is_a_builtin_execution_backend() -> "None":
    from litestar_queues.execution import get_execution_backend_class, list_execution_backends
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend

    assert get_execution_backend_class("cloudtasks") is CloudTasksExecutionBackend
    assert "cloudtasks" in list_execution_backends()


def test_execution_package_exports_the_typed_cloud_tasks_surface() -> "None":
    from litestar_queues import execution

    assert {"CloudTasksExecutionBackend", "CloudTasksExecutionConfig"}.issubset(set(execution.__all__))
    assert execution.CloudTasksExecutionConfig is not None
    assert execution.CloudTasksExecutionBackend is not None


def test_package_root_exports_the_typed_cloud_tasks_surface() -> "None":
    import litestar_queues
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend, CloudTasksExecutionConfig

    assert {"CloudTasksExecutionBackend", "CloudTasksExecutionConfig"}.issubset(set(litestar_queues.__all__))
    assert litestar_queues.CloudTasksExecutionConfig is CloudTasksExecutionConfig
    assert litestar_queues.CloudTasksExecutionBackend is CloudTasksExecutionBackend


# --------------------------------------------------------------------------- import boundary


def test_importing_the_package_does_not_import_google_cloud_tasks() -> "None":
    _run(
        "import sys, litestar_queues\n"
        "from litestar_queues import QueueConfig, QueuePlugin\n"
        "assert 'google.cloud.tasks_v2' not in sys.modules, "
        "sorted(m for m in sys.modules if 'tasks_v2' in m)\n"
    )


def test_importing_the_typed_config_does_not_import_google_cloud_tasks() -> "None":
    """Configuration is validated without a client, so an extra-less install can hold it."""
    _run(
        "import sys\n"
        "from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig\n"
        "assert 'google.cloud.tasks_v2' not in sys.modules\n"
    )


def test_selecting_the_backend_class_does_not_import_google_cloud_tasks() -> "None":
    """Resolution through the registry must not construct or import a client."""
    _run(
        "import sys\n"
        "from litestar_queues.execution import get_execution_backend_class\n"
        "get_execution_backend_class('cloudtasks')\n"
        "assert 'google.cloud.tasks_v2' not in sys.modules\n"
    )


def test_the_cloud_tasks_surface_does_not_pull_in_click() -> "None":
    """``click`` belongs to the CLI entry point, not to an execution backend."""
    _run(
        "import sys\n"
        "import litestar_queues.execution.cloudtasks\n"
        "assert 'click' not in sys.modules, sorted(m for m in sys.modules if 'click' in m)\n"
    )

"""Registration, export surface, and the optional-dependency import boundary.

``google-cloud-tasks`` is an extra, which makes two separate promises:

* when it is installed, nothing imports it until a client is actually built, and
* when it is absent, importing, selecting, configuring, and validating the
  backend all still work.

The development environment syncs ``--all-extras``, so the package is present
here and the first promise is what the ``sys.modules`` assertions test. The
second is only observable with the module unavailable, so
:func:`_run_without_cloud_tasks` blocks it in a fresh interpreter rather than
relying on it happening not to be installed.
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


_BLOCK_CLOUD_TASKS = """
import sys


class _Blocked:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "google.cloud.tasks_v2" or fullname.startswith("google.cloud.tasks_v2."):
            msg = "simulated missing extra"
            raise ImportError(msg)
        return None


sys.meta_path.insert(0, _Blocked())
assert "google.cloud.tasks_v2" not in sys.modules
"""


def _run_without_cloud_tasks(code: "str") -> "None":
    """Execute an assertion in an interpreter where the extra cannot be imported.

    Simulated rather than assumed: the development environment installs every
    extra, so an installation without this one is not otherwise reachable here.
    """
    _run(_BLOCK_CLOUD_TASKS + code)


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


# --------------------------------------------------------------------------- extra absent


def test_the_backend_is_importable_and_configurable_without_the_extra() -> "None":
    """An application that never asked for Cloud Tasks still has to import cleanly."""
    _run_without_cloud_tasks(
        "from litestar_queues.execution import get_execution_backend_class\n"
        "from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend, CloudTasksExecutionConfig\n"
        "assert get_execution_backend_class('cloudtasks') is CloudTasksExecutionBackend\n"
        "config = CloudTasksExecutionConfig(\n"
        "    project_id='example-project',\n"
        "    location='us-central1',\n"
        "    queue_id='queue-consumer',\n"
        "    service_url='https://consumer.example.run.app',\n"
        "    service_account_email='queues@example-project.iam.gserviceaccount.com',\n"
        "    trust_platform_auth=True,\n"
        ")\n"
        "assert config.target_url == 'https://consumer.example.run.app/_litestar-queues/cloud-tasks'\n"
        "assert CloudTasksExecutionBackend(execution_config=config).is_external is True\n"
    )


def test_building_a_client_without_the_extra_names_the_install_target() -> "None":
    """The only operation that needs the package says which extra to install."""
    _run_without_cloud_tasks(
        "import asyncio\n"
        "from litestar_queues.exceptions import MissingDependencyError\n"
        "from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend, CloudTasksExecutionConfig\n"
        "backend = CloudTasksExecutionBackend(\n"
        "    execution_config=CloudTasksExecutionConfig(\n"
        "        project_id='example-project',\n"
        "        location='us-central1',\n"
        "        queue_id='queue-consumer',\n"
        "        service_url='https://consumer.example.run.app',\n"
        "        service_account_email='queues@example-project.iam.gserviceaccount.com',\n"
        "        trust_platform_auth=True,\n"
        "    )\n"
        ")\n"
        "try:\n"
        "    asyncio.run(backend._get_client())\n"
        "except MissingDependencyError as exc:\n"
        "    assert 'cloud-tasks' in str(exc), str(exc)\n"
        "    assert 'google-cloud-tasks' in str(exc), str(exc)\n"
        "else:\n"
        "    raise AssertionError('expected MissingDependencyError')\n"
    )


def test_the_delivery_route_does_not_pull_in_google_or_click() -> "None":
    """The consumer imports the route on every request path; the producer's client is not its business."""
    _run(
        "import sys\n"
        "import litestar_queues.execution.cloudtasks.routes\n"
        "assert 'google.cloud.tasks_v2' not in sys.modules\n"
        "assert 'click' not in sys.modules, sorted(m for m in sys.modules if 'click' in m)\n"
    )


def test_a_plugin_without_cloud_tasks_never_loads_the_delivery_route() -> "None":
    """An ordinary application pays nothing for a route it will never serve."""
    _run(
        "import sys\n"
        "from litestar import Litestar\n"
        "from litestar_queues import QueueConfig, QueuePlugin, WorkerConfig\n"
        "Litestar(plugins=[QueuePlugin(QueueConfig(\n"
        "    queue_backend='memory', execution_backend='local', worker=WorkerConfig(placement='external')\n"
        "))])\n"
        "assert 'litestar_queues.execution.cloudtasks.routes' not in sys.modules\n"
    )


def test_the_delivery_route_is_servable_without_the_extra() -> "None":
    """Only building a producer client needs Google; receiving a delivery does not.

    A consumer-only deployment is a legitimate shape -- it reads records and
    runs them -- so the route has to stand up without the extra installed.
    """
    _run_without_cloud_tasks(
        "from litestar import Litestar\n"
        "from litestar_queues import QueueConfig, QueuePlugin, WorkerConfig\n"
        "from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig\n"
        "app = Litestar(plugins=[QueuePlugin(QueueConfig(\n"
        "    queue_backend='sqlspec',\n"
        "    execution_backend=CloudTasksExecutionConfig(\n"
        "        project_id='example-project',\n"
        "        location='us-central1',\n"
        "        queue_id='queue-consumer',\n"
        "        service_url='https://consumer.example.run.app',\n"
        "        service_account_email='queues@example-project.iam.gserviceaccount.com',\n"
        "        trust_platform_auth=True,\n"
        "    ),\n"
        "    worker=WorkerConfig(placement='external'),\n"
        "))])\n"
        "assert any(r.path == '/_litestar-queues/cloud-tasks' for r in app.routes)\n"
    )

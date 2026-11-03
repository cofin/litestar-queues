"""Runtime verification tests for standalone documentation code examples."""

from importlib import import_module
from inspect import iscoroutinefunction
from pathlib import Path

import pytest
from docs.examples.getting_started.quickstart_app import app as quickstart_app
from docs.examples.getting_started.quickstart_app import get_report_status, render_report, start_report
from litestar import Litestar
from litestar.testing import AsyncTestClient

from litestar_queues import QueueConfig, QueuePlugin, WorkerConfig, get_task_registry

ROOT = Path(__file__).resolve().parents[3]
EXAMPLES_DIR = ROOT / "docs" / "examples"

EXAMPLE_MODULES: tuple[str, ...] = tuple(
    ".".join(path.relative_to(ROOT).with_suffix("").parts)
    for path in sorted(EXAMPLES_DIR.rglob("*.py"))
    if path.name not in {"__init__.py", "quickstart_app.py"}
)


@pytest.mark.anyio
@pytest.mark.parametrize("module_name", EXAMPLE_MODULES)
async def test_docs_example_module_runs(module_name: str) -> None:
    """Every standalone documentation example module imports and executes its main() verification routine."""
    module = import_module(module_name)
    entrypoint = getattr(module, "main", None)
    assert callable(entrypoint), f"{module_name} must define a callable main() verification function"
    if iscoroutinefunction(entrypoint):
        await entrypoint()
    else:
        entrypoint()


@pytest.mark.anyio
async def test_quickstart_app_enqueues_and_inspects_task() -> None:
    """The quickstart application routes enqueue a report job and return its completed status."""
    assert any(isinstance(plugin, QueuePlugin) for plugin in quickstart_app.plugins)
    get_task_registry()[render_report.name] = render_report

    test_app = Litestar(
        route_handlers=[start_report, get_report_status],
        plugins=[
            QueuePlugin(
                config=QueueConfig(
                    queue_backend="memory", execution_backend="immediate", worker=WorkerConfig(placement="external")
                )
            )
        ],
    )
    async with AsyncTestClient(app=test_app) as client:
        create_response = await client.post("/reports/q4-summary")
        assert create_response.status_code == 201
        payload = create_response.json()
        task_id = payload["task_id"]

        status_response = await client.get(f"/tasks/{task_id}")
        assert status_response.status_code == 200
        status_payload = status_response.json()
        assert status_payload["task_id"] == task_id
        assert status_payload["status"] == "completed"
        assert status_payload["is_terminal"] is True
        assert status_payload["result"] == {
            "report_id": "q4-summary",
            "requested_by": "team@example.com",
            "status": "ready",
        }

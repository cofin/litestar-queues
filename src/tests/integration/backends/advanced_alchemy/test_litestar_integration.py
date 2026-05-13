from pathlib import Path

import pytest
from litestar import Litestar, post
from litestar.testing import TestClient

pytest.importorskip("advanced_alchemy")
pytest.importorskip("aiosqlite")
pytest.importorskip("sqlalchemy")

from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig, SQLAlchemyPlugin

from litestar_queues import QueueConfig, QueuePlugin, QueueService, task
from litestar_queues.task import clear_task_registry


@pytest.fixture(autouse=True)
def clean_task_registry() -> None:
    clear_task_registry()


def _sqlite_config(path: Path) -> SQLAlchemyAsyncConfig:
    return SQLAlchemyAsyncConfig(connection_string=f"sqlite+aiosqlite:///{path}")


def test_advanced_alchemy_litestar_integration_uses_app_owned_sqlalchemy_plugin(tmp_path: Path) -> None:
    @task("tasks.litestar_aa")
    async def litestar_aa_task() -> str:
        return "ok"

    alchemy_config = _sqlite_config(tmp_path / "litestar.db")
    queue_plugin = QueuePlugin(
        QueueConfig(
            queue_backend="advanced-alchemy",
            queue_backend_config={
                "sqlalchemy_config": alchemy_config,
                "create_schema": True,
            },
            initialize_schedules=False,
        )
    )

    @post("/enqueue")
    async def enqueue(queue_service: QueueService) -> dict[str, str]:
        result = await queue_service.enqueue("tasks.litestar_aa", execution_backend="local")
        return {"task_id": str(result.id), "status": str(result.status)}

    app = Litestar(route_handlers=[enqueue], plugins=[SQLAlchemyPlugin(config=alchemy_config), queue_plugin])

    with TestClient(app) as client:
        response = client.post("/enqueue")

    assert response.status_code == 201
    assert response.json()["status"] == "pending"
    assert queue_plugin.config.queue_backend_config["sqlalchemy_config"] is alchemy_config


def test_advanced_alchemy_backend_does_not_create_sqlalchemy_litestar_plugin() -> None:
    from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyQueueBackend

    with pytest.raises(TypeError):
        AdvancedAlchemyQueueBackend(register_plugin=True)  # type: ignore[call-arg]

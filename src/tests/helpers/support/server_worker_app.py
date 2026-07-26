"""Real-process Litestar app used by server-worker topology tests."""

import json
import os
from pathlib import Path
from typing import Any
from uuid import UUID  # noqa: TC003 - Litestar resolves handler annotations at runtime.

from litestar import Litestar, get, post
from litestar.di import NamedDependency  # noqa: TC002 - Litestar resolves handler annotations at runtime.

from litestar_queues import QueueConfig, QueuePlugin, QueueService, WorkerConfig, task

MARKERS_ENV_VAR = "LITESTAR_QUEUES_TEST_MARKERS"
SERVER_ENV_VAR = "LITESTAR_QUEUES_TEST_SERVER"
INVOCATION_ENV_VAR = "LITESTAR_QUEUES_TEST_INVOCATION"
PROCESS_ROLE_ENV_VAR = "LITESTAR_QUEUES_PROCESS_ROLE"
EPHEMERAL_PATH_ENV_VAR = "LITESTAR_QUEUES_EPHEMERAL_PATH"


def _marker_directory() -> "Path":
    return Path(os.environ[MARKERS_ENV_VAR])


def _write_marker(role: str, **payload: "object") -> "None":
    document = {
        "invocation": os.environ[INVOCATION_ENV_VAR],
        "pid": os.getpid(),
        "ppid": os.getppid(),
        "role": role,
        **payload,
    }
    path = _marker_directory() / f"{role}-{os.getpid()}.json"
    path.write_text(json.dumps(document, sort_keys=True), encoding="utf-8")


@task("topology.record_process")
async def record_process(token: str) -> "int":
    pid = os.getpid()
    _write_marker(f"task-{token}", execution_pid=pid)
    return pid


async def mark_web_process() -> "None":
    _write_marker("web", ephemeral_path=os.environ.get(EPHEMERAL_PATH_ENV_VAR))


@get("/health")
async def health() -> "dict[str, int]":
    return {"web_pid": os.getpid()}


@post("/enqueue/{token:str}")
async def enqueue(token: str, queue_service: "NamedDependency[QueueService]") -> "dict[str, str]":
    result = await queue_service.enqueue(record_process, token)
    return {"task_id": str(result.id)}


@get("/tasks/{task_id:uuid}")
async def task_status(task_id: "UUID", queue_service: "NamedDependency[QueueService]") -> "dict[str, Any]":
    record = await queue_service.get_task(task_id)
    if record is None:
        return {"status": None, "result": None}
    return {"status": record.status, "result": record.result}


def create_app() -> "Litestar":
    if os.environ.get(PROCESS_ROLE_ENV_VAR) == "server-worker":
        _write_marker("queue")
    plugins: "list[Any]" = [
        QueuePlugin(
            QueueConfig(
                worker=WorkerConfig(
                    startup_timeout=10, graceful_shutdown_timeout=2, final_cancel_timeout=1, poll_interval=0.02
                )
            )
        )
    ]
    if os.environ.get(SERVER_ENV_VAR) == "granian":
        from litestar_granian import GranianPlugin

        plugins.append(GranianPlugin())
    return Litestar(route_handlers=[health, enqueue, task_status], on_startup=[mark_web_process], plugins=plugins)

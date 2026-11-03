"""Minimal runnable Litestar Queues quickstart application."""

from uuid import UUID

from litestar import Litestar, get, post
from litestar.di import NamedDependency
from litestar.exceptions import NotFoundException

from litestar_queues import QueueConfig, QueuePlugin, QueueService, task


@task(queue="reports")
async def render_report(report_id: str, requested_by: str) -> dict[str, str]:
    """Render an analytics report in the background."""
    return {"report_id": report_id, "requested_by": requested_by, "status": "ready"}


@post("/reports/{report_id:str}")
async def start_report(
    report_id: str, queue_service: NamedDependency[QueueService], requested_by: str = "team@example.com"
) -> dict[str, str]:
    """Enqueue a background report job and return its tracking ID."""
    handle = await queue_service.enqueue(render_report, report_id, requested_by=requested_by)
    return {"task_id": str(handle.id), "status": handle.status or "pending"}


@get("/tasks/{task_id:uuid}")
async def get_report_status(task_id: UUID, queue_service: NamedDependency[QueueService]) -> dict[str, object]:
    """Inspect the status and return value of a queued task."""
    record = await queue_service.get_task(task_id)
    if record is None:
        raise NotFoundException(detail=f"Task {task_id} not found")
    return {
        "task_id": str(record.id),
        "status": record.status,
        "is_terminal": record.is_terminal,
        "result": record.result,
    }


app = Litestar(route_handlers=[start_report, get_report_status], plugins=[QueuePlugin(config=QueueConfig())])

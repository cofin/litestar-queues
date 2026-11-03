"""Enqueuing tasks after an HTTP response completes using QueuedBackgroundTask."""

from litestar import Litestar, Response, post
from litestar.status_codes import HTTP_201_CREATED
from litestar.testing import AsyncTestClient

from litestar_queues import QueueConfig, QueuedBackgroundTask, QueuePlugin, WorkerConfig, task


@task("audit.record_login", queue="audit")
async def record_login(username: str) -> str:
    """Persist an audit trail entry for a user login."""
    return f"logged:{username}"


@post("/login/{username:str}")
async def login_user(username: str) -> Response[dict[str, str]]:
    """Return the HTTP response immediately and enqueue the audit task afterward."""
    return Response(
        content={"username": username, "status": "authenticated"},
        background=QueuedBackgroundTask(record_login, username),
    )


async def main() -> None:
    """Verify QueuedBackgroundTask enqueues and executes after the HTTP response."""
    app = Litestar(
        route_handlers=[login_user],
        plugins=[
            QueuePlugin(
                config=QueueConfig(
                    queue_backend="memory", execution_backend="immediate", worker=WorkerConfig(placement="external")
                )
            )
        ],
    )
    async with AsyncTestClient(app=app) as client:
        response = await client.post("/login/cody")
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"username": "cody", "status": "authenticated"}

# Post-Response Background Tasks

When an HTTP route handler triggers background work, you can either enqueue the task inside the handler before returning the response or attach a [`QueuedBackgroundTask`](../reference/plugin-and-config.md#litestar_queues.background.QueuedBackgroundTask) to the Litestar `Response`.

## Runnable Example

```python title="docs/examples/learn/background_responses.py"
--8<-- "docs/examples/learn/background_responses.py"
```

## Choosing Between In-Handler Enqueue and QueuedBackgroundTask

| Dimension | In-Handler `await queue_service.enqueue(...)` | Post-Response `QueuedBackgroundTask(...)` |
| :--- | :--- | :--- |
| **When Enqueue Runs** | Inside the route handler before the HTTP response is sent | In the ASGI post-response hook after the response body is flushed to the client |
| **Task ID in HTTP Response** | Yes — `handle.id` can be returned in the response payload (`202 Accepted`) | No — the response has already been sent before the record is persisted |
| **Enqueue Failure Handling** | Exceptions propagate to Litestar exception handlers before the client receives `2xx` | Client already received the response; enqueue errors are logged by the ASGI server |
| **Queue Persistence** | Full `QueuedTaskRecord` durability, retries, uniqueness, and worker routing | Full `QueuedTaskRecord` durability, retries, uniqueness, and worker routing |
| **Best For** | Critical workflows (payments, exports, provisioning) where the caller tracks `task_id` | Non-blocking side effects (audit logs, analytics, cache warming) off the response latency path |

## Passing Enqueue Options to QueuedBackgroundTask

`QueuedBackgroundTask` accepts the same scheduling and routing keyword options as `QueueService.enqueue`:

```python
from datetime import timedelta
from litestar import Response, post
from litestar_queues import QueuedBackgroundTask, task


@task("notifications.send_welcome", queue="notifications")
async def send_welcome(email: str) -> None:
    pass


@post("/signup")
async def signup(email: str) -> Response[dict[str, str]]:
    return Response(
        content={"email": email, "status": "registered"},
        background=QueuedBackgroundTask(
            send_welcome,
            email,
            queue="notifications",
            priority=5,
            run_after=timedelta(seconds=30),
            key=f"welcome:{email}",
        ),
    )
```

If multiple `QueuePlugin` instances are mounted with custom [`QueueNamespace`](runtime-namespace.md) configurations, pass `service=queue_service` to `QueuedBackgroundTask` to target a specific `QueueService` instance.

import asyncio
from pathlib import Path

from litestar import Litestar, get, post
from litestar.channels import ChannelsPlugin
from litestar.channels.backends.memory import MemoryChannelsBackend
from litestar.di import NamedDependency
from litestar.plugins.htmx import HTMXPlugin, HTMXTemplate
from litestar.plugins.jinja import JinjaTemplateEngine
from litestar.response import Template
from litestar.template.config import TemplateConfig
from litestar_vite import PathConfig, ViteConfig, VitePlugin

from litestar_queues import QueueConfig, QueuePlugin, QueueService, task
from litestar_queues.events import (
    EventBufferConfig,
    EventConfig,
    EventStreamConfig,
    TaskExecutionContext,
    publish_task_log,
)

__all__ = ("index", "restart_demo", "run_crawl", "status_json")


EXAMPLE_ROOT = Path(__file__).parent
BACKEND_NAME = "memory"
DEMO_STEPS = 6
DEMO_STEP_DELAY = 1
DEMO_KEY = "demo:current"

CRAWL_LINES = (
    "The task started.",
    "The task sent an event from the backend.",
    "The task is sleeping before the next event.",
    "The task sent another event.",
    "The page received the event.",
    "The task finished.",
)


# -- docs-task-start --
@task("examples.htmx_realtime_websocket.crawl", queue="demo", retries=0, timeout=90)
async def run_crawl(*, _task_context: TaskExecutionContext) -> dict[str, str]:
    ctx = _task_context
    await publish_task_log("The task started", payload={"task_id": ctx.task_id}, immediate=True)

    for step in range(1, DEMO_STEPS + 1):
        line = CRAWL_LINES[(step - 1) % len(CRAWL_LINES)]
        message = f"{step}/{DEMO_STEPS} - {line}"
        ctx.beat(message)
        await ctx.progress(current=step, total=DEMO_STEPS, message=message, payload={"line": message})
        await ctx.event(
            "task.event",
            message=message,
            payload={"task_id": ctx.task_id, "line": message, "step": step},
            immediate=step == DEMO_STEPS,
        )
        await asyncio.sleep(DEMO_STEP_DELAY)

    await publish_task_log("The task finished", payload={"task_id": ctx.task_id}, immediate=True)
    return {"task_id": ctx.task_id, "status": "complete"}


# -- docs-task-end --


# -- docs-routes-start --
@get("/")
async def index() -> Template:
    return Template("index.html", context={"backend_name": BACKEND_NAME})


@get("/demo/status")
async def status_json() -> dict[str, str]:
    return {"backend": BACKEND_NAME}


@post("/demo/restart")
async def restart_demo(queue_service: NamedDependency[QueueService]) -> Template:
    existing = await queue_service.get_queue_backend().get_task_by_key(DEMO_KEY)
    result = await queue_service.enqueue(run_crawl, key=DEMO_KEY, description="Run the HTMX realtime queue event demo")
    task_id = str(result.id)
    reused = existing is not None and not existing.is_terminal and existing.id == result.id
    return HTMXTemplate(
        template_name="partials/stream_mount.html",
        context={"task_id": task_id, "task_ws_url": f"/queues/events/tasks/{result.id}"},
        push_url=False,
        re_target="#stream-mount",
        trigger_event="queue-demo:started",
        params={"taskId": task_id, "reused": reused},
        after="swap",
    )


# -- docs-routes-end --


# -- docs-app-config-start --
channels = ChannelsPlugin(
    backend=MemoryChannelsBackend(history=200),
    arbitrary_channels_allowed=True,
    subscriber_max_backlog=1000,
    subscriber_backlog_strategy="dropleft",
)

queue_config = QueueConfig(
    # Demo apps exit fast on Ctrl+C instead of draining the minute-long job.
    worker_graceful_shutdown_timeout=5,
    event=EventConfig(channels_backend=channels, buffer=EventBufferConfig(buffer_size=8, flush_interval=0.2)),
    # The demo registers only its own transport so a stale tab from another
    # example cannot silently reconnect to this app's routes.
    event_stream=EventStreamConfig(scopes={"task"}, sse=False, history=25, heartbeat_interval=15),
)

vite_config = ViteConfig(mode="htmx", paths=PathConfig(root=EXAMPLE_ROOT, resource_dir="resources"))

app = Litestar(
    route_handlers=[index, status_json, restart_demo],
    template_config=TemplateConfig(directory=EXAMPLE_ROOT / "templates", engine=JinjaTemplateEngine),
    signature_namespace={"NamedDependency": NamedDependency},
    plugins=[HTMXPlugin(), channels, QueuePlugin(queue_config), VitePlugin(config=vite_config)],
)
# -- docs-app-config-end --

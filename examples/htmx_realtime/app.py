import asyncio
import os
from pathlib import Path
from uuid import uuid4

from litestar import Litestar, get, post
from litestar.channels import ChannelsPlugin
from litestar.channels.backends.memory import MemoryChannelsBackend
from litestar.di import NamedDependency
from litestar.plugins.htmx import HTMXPlugin, HTMXRequest, HTMXTemplate
from litestar.plugins.jinja import JinjaTemplateEngine
from litestar.response import Template
from litestar.template.config import TemplateConfig
from litestar_vite import PathConfig, ViteConfig, VitePlugin

from litestar_queues import QueueConfig, QueuePlugin, QueueService, task
from litestar_queues.events import (
    EventBufferConfig,
    EventConfig,
    EventStreamConfig,
    QueueEventProducer,
    TaskExecutionContext,
    publish_task_log,
)

__all__ = ("allow_demo_channel", "index", "publish_mission_control", "restart_demo", "run_crawl", "status_json")


EXAMPLE_ROOT = Path(__file__).parent
MISSION_CONTROL_SCOPE = "demo:mission-control"
BACKEND_NAME = "memory"
DEMO_STEPS = int(os.getenv("LITESTAR_QUEUES_EXAMPLE_STEPS", "12"))
DEMO_STEP_DELAY = float(os.getenv("LITESTAR_QUEUES_EXAMPLE_STEP_DELAY", "5"))
VITE_DEV_MODE = os.getenv("LITESTAR_QUEUES_EXAMPLE_VITE_DEV", "0") == "1"

CRAWL_LINES = (
    "A quiet signal leaves the Litestar launch bay.",
    "The queue accepts the mission and assigns a worker.",
    "Telemetry starts flowing through task-scoped events.",
    "The browser keeps one stream open while the work continues.",
    "Mission control publishes a custom channel message.",
    "The worker heartbeat keeps the task claim fresh.",
    "Buffered progress events arrive in order.",
    "The crawl waits for the terminal completion event.",
    "A final payload confirms the route is clear.",
    "The example is ready for another run.",
)


def allow_demo_channel(*_: object) -> bool:
    """Allow every stream channel in this local demo app.

    Returns:
        Always ``True`` for the demo-only stream authorizer.
    """
    return True


# -- docs-task-start --
@task("examples.htmx_realtime.crawl", queue="demo", retries=0, timeout=90)
async def run_crawl(job_id: str, *, _task_context: TaskExecutionContext) -> dict[str, str]:
    ctx = _task_context
    await publish_task_log("Launch sequence accepted", payload={"job_id": job_id}, immediate=True)

    for step in range(1, DEMO_STEPS + 1):
        line = CRAWL_LINES[(step - 1) % len(CRAWL_LINES)]
        message = f"Transmission {step}/{DEMO_STEPS}: {line}"
        ctx.beat(message)
        await ctx.progress(current=step, total=DEMO_STEPS, message=message, payload={"line": message})
        await ctx.event(
            "task.event",
            message=message,
            payload={"job_id": job_id, "line": message, "step": step},
            immediate=step == DEMO_STEPS,
        )
        await asyncio.sleep(DEMO_STEP_DELAY)

    await publish_task_log("Mission complete", payload={"job_id": job_id}, immediate=True)
    return {"job_id": job_id, "status": "complete"}


# -- docs-task-end --


# -- docs-routes-start --
@get("/")
async def index() -> Template:
    return Template(
        "index.html",
        context={
            "backend_name": BACKEND_NAME,
            "mission_scope": MISSION_CONTROL_SCOPE,
            "mission_ws_url": f"/queues/events/custom/{MISSION_CONTROL_SCOPE}",
            "mission_sse_url": f"/queues/events/sse/custom/{MISSION_CONTROL_SCOPE}",
        },
    )


@get("/demo/status")
async def status_json() -> dict[str, str]:
    return {"backend": BACKEND_NAME, "scope": MISSION_CONTROL_SCOPE}


@post("/demo/restart")
async def restart_demo(
    request: HTMXRequest,
    queue_service: NamedDependency[QueueService],
    queue_events: NamedDependency[QueueEventProducer],
) -> Template:
    job_id = uuid4().hex[:8]
    result = await queue_service.enqueue(
        run_crawl, job_id, key=f"demo:{job_id}", description="Run the HTMX realtime queue event demo"
    )
    await queue_events.channel("demo:mission-control").publish(
        "mission.control",
        message=f"Mission {job_id} queued",
        payload={
            "job_id": job_id,
            "task_id": str(result.id),
            "status": "queued",
            "trigger": request.htmx.trigger if request.htmx else None,
        },
        immediate=True,
    )
    return HTMXTemplate(
        template_name="partials/job_status.html",
        context={
            "job_id": job_id,
            "task_id": str(result.id),
            "task_ws_url": f"/queues/events/tasks/{result.id}",
            "task_sse_url": f"/queues/events/sse/tasks/{result.id}",
            "mission_scope": MISSION_CONTROL_SCOPE,
            "mission_ws_url": f"/queues/events/custom/{MISSION_CONTROL_SCOPE}",
            "mission_sse_url": f"/queues/events/sse/custom/{MISSION_CONTROL_SCOPE}",
        },
        push_url=False,
        re_target="#job-status",
        trigger_event="queue-demo:started",
        params={"jobId": job_id, "taskId": str(result.id)},
        after="swap",
    )


# -- docs-routes-end --


# -- docs-mission-control-start --
@post("/demo/mission-control")
async def publish_mission_control(request: HTMXRequest, queue_events: NamedDependency[QueueEventProducer]) -> Template:
    form = await request.form()
    message = str(form.get("message") or "Manual mission-control ping")
    await queue_events.channel("demo:mission-control").publish(
        "mission.control", message=message, payload={"source": "mission-control", "message": message}, immediate=True
    )
    return HTMXTemplate(
        template_name="partials/mission_note.html",
        context={"message": message},
        push_url=False,
        re_target="#mission-ack",
        trigger_event="mission-control:published",
        params={"scope": MISSION_CONTROL_SCOPE},
        after="swap",
    )


# -- docs-mission-control-end --


# -- docs-app-config-start --
channels = ChannelsPlugin(
    backend=MemoryChannelsBackend(history=200),
    arbitrary_channels_allowed=True,
    subscriber_max_backlog=1000,
    subscriber_backlog_strategy="dropleft",
)

queue_config = QueueConfig(
    event=EventConfig(
        enabled=True,
        channels_backend=channels,
        buffer=EventBufferConfig(buffer_size=8, flush_interval=0.2, overflow="drop_oldest"),
    ),
    event_stream=EventStreamConfig(
        scopes={"task", "custom"}, history=25, heartbeat_interval=15, channel_authorizer=allow_demo_channel
    ),
)

vite_config = ViteConfig(
    mode="htmx", dev_mode=VITE_DEV_MODE, paths=PathConfig(root=EXAMPLE_ROOT, resource_dir="resources")
)

app = Litestar(
    route_handlers=[index, status_json, restart_demo, publish_mission_control],
    template_config=TemplateConfig(directory=EXAMPLE_ROOT / "templates", engine=JinjaTemplateEngine),
    signature_namespace={"NamedDependency": NamedDependency},
    plugins=[HTMXPlugin(), channels, QueuePlugin(queue_config), VitePlugin(config=vite_config)],
)
# -- docs-app-config-end --

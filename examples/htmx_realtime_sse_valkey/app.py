import asyncio
import os
from pathlib import Path
from uuid import uuid4

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
from litestar_queues.backends.valkey import ValkeyBackendConfig
from litestar_queues.events import (
    EventBufferConfig,
    EventConfig,
    EventStreamConfig,
    TaskExecutionContext,
    publish_task_log,
)

__all__ = ("allow_demo_channel", "index", "restart_demo", "run_crawl", "status_json")


EXAMPLE_ROOT = Path(__file__).parent
BACKEND_NAME = "valkey"
DEMO_STEPS = int(os.getenv("LITESTAR_QUEUES_EXAMPLE_STEPS", "12"))
DEMO_STEP_DELAY = float(os.getenv("LITESTAR_QUEUES_EXAMPLE_STEP_DELAY", "5"))
VITE_DEV_MODE = os.getenv("LITESTAR_QUEUES_EXAMPLE_VITE_DEV", "0") == "1"
VALKEY_URL = os.getenv("LITESTAR_QUEUES_EXAMPLE_VALKEY_URL", "redis://localhost:6379/0")

CRAWL_LINES = (
    "This line was published from inside the running job.",
    "A worker claimed the job your restart click enqueued.",
    "ctx.progress() reports each step over the task stream.",
    "ctx.event() carries custom payloads like this line.",
    "ctx.beat() folds detail into the worker heartbeat.",
    "The event buffer flushes batches to the Channels backend.",
    "A plugin-owned stream endpoint relays events to this page.",
    "htmx swapped in the stream element when you pressed restart.",
    "The page marks the job complete only on task.completed.",
    "The job publishes every line - the page only listens.",
)


def allow_demo_channel(*_: object) -> bool:
    """Allow every stream channel in this local demo app.

    Returns:
        Always ``True`` for the demo-only stream authorizer.
    """
    return True


# -- docs-task-start --
@task("examples.htmx_realtime_sse_valkey.crawl", queue="demo", retries=0, timeout=90)
async def run_crawl(job_id: str, *, _task_context: TaskExecutionContext) -> dict[str, str]:
    ctx = _task_context
    await publish_task_log("Job started: the task itself publishes everything below", payload={"job_id": job_id}, immediate=True)

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

    await publish_task_log("Job finished: task.completed ends this stream", payload={"job_id": job_id}, immediate=True)
    return {"job_id": job_id, "status": "complete"}


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
    job_id = uuid4().hex[:8]
    result = await queue_service.enqueue(
        run_crawl, job_id, key=f"demo:{job_id}", description="Run the HTMX realtime queue event demo"
    )
    return HTMXTemplate(
        template_name="partials/stream_mount.html",
        context={"job_id": job_id, "task_id": str(result.id), "task_sse_url": f"/queues/events/sse/tasks/{result.id}"},
        push_url=False,
        re_target="#stream-mount",
        trigger_event="queue-demo:started",
        params={"jobId": job_id, "taskId": str(result.id)},
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
    queue_backend=ValkeyBackendConfig(url=VALKEY_URL, key_prefix="litestar_queues:examples:htmx_realtime_sse_valkey"),
    event=EventConfig(
        enabled=True,
        channels_backend=channels,
        buffer=EventBufferConfig(buffer_size=8, flush_interval=0.2, overflow="drop_oldest"),
    ),
    # A demo-only allow-all authorizer: it suppresses the missing-auth warning
    # for this local single-process example. Real deployments must authorize.
    event_stream=EventStreamConfig(
        scopes={"task"}, history=25, heartbeat_interval=15, channel_authorizer=allow_demo_channel
    ),
)

vite_config = ViteConfig(
    mode="htmx", dev_mode=VITE_DEV_MODE, paths=PathConfig(root=EXAMPLE_ROOT, resource_dir="resources")
)

app = Litestar(
    route_handlers=[index, status_json, restart_demo],
    template_config=TemplateConfig(directory=EXAMPLE_ROOT / "templates", engine=JinjaTemplateEngine),
    signature_namespace={"NamedDependency": NamedDependency},
    plugins=[HTMXPlugin(), channels, QueuePlugin(queue_config), VitePlugin(config=vite_config)],
)
# -- docs-app-config-end --

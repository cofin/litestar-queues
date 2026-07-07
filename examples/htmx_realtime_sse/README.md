# HTMX Realtime Queue Events - SSE (Memory Backend)

This standalone app runs a Litestar page with the default memory queue backend, the default local worker behavior, and memory Channels delivery.
The first viewport is an animated crawl fed by task events. The second
viewport publishes and subscribes to the custom `demo:mission-control` channel.

This copy is SSE-only. It uses the plugin-owned `/queues/events/sse/tasks/{task_id}` and `/queues/events/sse/custom/{scope_key}`
endpoints so the transport requirements stay visible in the code.

The example leaves queue defaults alone. Its `QueueConfig` only enables buffered
events and task/custom event streams.

It runs in one process and needs no external service.

## Run It

From the repository root:

```bash
uv sync --extra examples --group dev
LITESTAR_APP=examples.htmx_realtime_sse.app:app \
uv run litestar assets install
LITESTAR_APP=examples.htmx_realtime_sse.app:app \
LITESTAR_QUEUES_EXAMPLE_VITE_DEV=1 \
uv run litestar run --reload
```

For a faster local run:

```bash
LITESTAR_APP=examples.htmx_realtime_sse.app:app \
LITESTAR_QUEUES_EXAMPLE_VITE_DEV=1 \
LITESTAR_QUEUES_EXAMPLE_STEPS=4 \
LITESTAR_QUEUES_EXAMPLE_STEP_DELAY=0.5 \
uv run litestar run --reload
```

Open `http://127.0.0.1:8000` and press Restart.

## What It Demonstrates

- `HTMXPlugin()` with `litestar-vite` `mode="htmx"`.
- The Litestar Vite HTMX helper registered with `registerHtmxExtension()`.
- HTMX 2 indicators, disabled elements, sync replacement, transition swaps,
  and `HX-Trigger-After-Swap` events through `HTMXTemplate`.
- `hx-swap="json"` with the Litestar `ls-*` JSON-template extension.
- `EventStreamConfig(scopes={"task", "custom"})` for task and application
  channel streams.
- Task progress, task logs, `ctx.beat(...)`, and a terminal completion event.

## External Publisher

`scripts/external_publisher.py` shows the shape for non-Litestar publishers via
`create_event_producer`. The script deliberately raises until you replace the
placeholder config with a shared Redis or SQLSpec Channels backend. The memory
Channels backend cannot bridge two separate processes.

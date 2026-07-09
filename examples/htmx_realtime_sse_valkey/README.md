# HTMX Realtime Queue Events - SSE (Valkey Backend)

A cinematic, full-screen "space opera" page: an animated opening crawl of text
receding into an animated galaxy starfield, fed live by queue task events over
Server-Sent Events. There is one control, a planet-styled **Restart** button
that (re)enqueues the roughly one-minute demo job and restarts the crawl.

This copy is SSE-only. It uses the plugin-owned
`/queues/events/sse/tasks/{task_id}` endpoint so the transport stays visible in
the code. Queue persistence runs through Valkey, and delivery uses memory
Channels in the same process, so only Valkey is needed as an external service.

The default remains one process: Valkey stores queue records and can provide
worker wakeups, but it does not make the live Channels stream shared. A
separate `litestar queues run` worker needs an explicit broker-backed Channels
configuration; selecting `ValkeyBackendConfig` alone is not enough.

Set `LITESTAR_QUEUES_EXAMPLE_VALKEY_URL` when Valkey is not available at
`redis://localhost:6379/0`. See the repository's local infra helpers for
spinning up a Valkey container.

## Shared Web and Worker Mode

The opt-in shared topology uses Litestar's Redis Channels Streams backend with
the Valkey client. It keeps the configured event history (`history=25`) and is
intentionally separate from Valkey queue wakeup pub/sub:

```bash
export LITESTAR_QUEUES_EXAMPLE_VALKEY_URL=redis://127.0.0.1:16380/0
export LITESTAR_QUEUES_EXAMPLE_SHARED_CHANNELS=1
export LITESTAR_QUEUES_EXAMPLE_IN_APP_WORKER=0
export LITESTAR_QUEUES_EXAMPLE_VALKEY_KEY_PREFIX=litestar_queues:demo:valkey:queue
export LITESTAR_QUEUES_EXAMPLE_CHANNELS_KEY_PREFIX=litestar_queues:demo:valkey:channels

LITESTAR_APP=examples.htmx_realtime_sse_valkey.app:app uv run litestar run
```

In a second terminal, run the worker with the same environment and app:

```bash
LITESTAR_APP=examples.htmx_realtime_sse_valkey.app:app \
uv run litestar queues run --queue demo --drain-timeout 30
```

Do not switch this branch to `RedisChannelsPubSubBackend`: it has no history
support. The Valkey queue backend's notifications wake workers; Redis Channels
Streams carry the browser events. The Valkey branch never creates a Redis
client directly.

## Run It (dev server, hot reload)

From the repository root:

```bash
uv sync --extra examples --group dev
LITESTAR_APP=examples.htmx_realtime_sse_valkey.app:app \
uv run litestar assets install
LITESTAR_APP=examples.htmx_realtime_sse_valkey.app:app \
LITESTAR_QUEUES_EXAMPLE_VITE_DEV=1 \
LITESTAR_PORT=8000 \
uv run litestar run --reload
```

For a faster local run:

```bash
LITESTAR_APP=examples.htmx_realtime_sse_valkey.app:app \
LITESTAR_QUEUES_EXAMPLE_VITE_DEV=1 \
LITESTAR_QUEUES_EXAMPLE_STEPS=4 \
LITESTAR_QUEUES_EXAMPLE_STEP_DELAY=0.5 \
LITESTAR_PORT=8000 \
uv run litestar run --reload
```

Open `http://127.0.0.1:8000` (the `LITESTAR_PORT` app variable) and press
Restart.

## Run It (without the dev server)

`LITESTAR_QUEUES_EXAMPLE_VITE_DEV` defaults to off. Without it, the page loads
built assets from the manifest, so `GET /` returns 500 until you build them
once. Build, then run:

```bash
LITESTAR_APP=examples.htmx_realtime_sse_valkey.app:app \
uv run litestar assets install
LITESTAR_APP=examples.htmx_realtime_sse_valkey.app:app \
uv run litestar assets build
LITESTAR_APP=examples.htmx_realtime_sse_valkey.app:app \
LITESTAR_PORT=8000 \
uv run litestar run
```

## How It Works

- **One button, no forms.** `hx-post="/demo/restart"` lives directly on the
  planet `<button>`. Clicking it enqueues the demo job and swaps the returned
  `partials/stream_mount.html` into `#stream-mount`.
- **The htmx SSE extension owns the connection.** The swapped-in
  `#stream-mount` element carries `hx-ext="sse"` and
  `sse-connect="/queues/events/sse/tasks/{task_id}"`, so htmx opens the
  `EventSource` declaratively. Because each restart replaces the element, the
  previous `EventSource` closes automatically: the connection lifecycle equals
  the swap lifecycle, so a restart is a reconnect. This is the idiomatic
  pattern and replaces roughly ninety lines of hand-rolled `EventSource` code.
- **One small JS adapter.** Queue events are JSON, so the extension cannot swap
  them as HTML. `resources/main.ts` listens for `htmx:sseBeforeMessage`, parses
  the frame, ignores `{"type":"ping"}` heartbeats, appends a line to the crawl,
  flips the readout to gold on `task.completed`, and calls `preventDefault()` so
  htmx never attempts an HTML swap. On the `queue-demo:started` trigger event
  (fired by `HTMXTemplate` after the swap) it clears the crawl for the new run.
- **htmx wiring.** `resources/main.ts` imports htmx, publishes it as
  `window.htmx`, calls `registerHtmxExtension()`, then dynamically imports
  `htmx-ext-sse` so the extension sees the global. htmx 2's ESM build does not
  attach itself to `window`, so this ordering is required.
- **Litestar Vite JSON template.** A muted corner caption uses
  `hx-ext="litestar"` with `hx-swap="json"` and `<template ls-if>` to render the
  backend name from `GET /demo/status`.

The `QueueConfig` enables buffered events and the task event stream only
(`EventStreamConfig(scopes={"task"})`). Its `channel_authorizer` allows every
channel; this is a demo-only shortcut that suppresses the missing-auth warning
for a local single-process app. Real deployments must authorize stream access.

## External Publisher

`scripts/external_publisher.py` shows the shape for non-Litestar publishers via
`create_event_producer`. The script deliberately raises until you replace the
placeholder config with a shared Redis or SQLSpec Channels backend. The memory
Channels backend cannot bridge two separate processes.

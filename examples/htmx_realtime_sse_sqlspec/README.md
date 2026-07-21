# HTMX Realtime Queue Events - SSE (SQLSpec Aiosqlite Backend)

This is a small app that shows realtime messages moving from a queue task to
the frontend. Each update is pushed from the backend over Server-Sent Events
(SSE) and shown on the page. The **Restart** button starts the demo task again.

This copy is SSE-only. It uses the plugin-owned
`/queues/events/sse/tasks/{task_id}` endpoint so the transport stays visible in
the code. Queue persistence runs through SQLSpec with the Aiosqlite driver,
and delivery uses memory Channels in the same process, so no external service
is needed.

This setup uses one process. SQLite stores queue records, but it does not send
live Channels events to a worker in another process.

The queue database defaults to `queue-sqlspec.db` under the example directory.
The example registers the queue migration with SQLSpec and runs it during
startup. Production applications should register it before their normal
SQLSpec migration command and run migrations during deployment.

## Run It (dev server, hot reload)

From the repository root:

```bash
uv sync --group examples --group dev
LITESTAR_APP=examples.htmx_realtime_sse_sqlspec.app:app \
uv run litestar assets install
LITESTAR_APP=examples.htmx_realtime_sse_sqlspec.app:app \
VITE_DEV_MODE=1 \
LITESTAR_PORT=8000 \
uv run litestar run --reload
```

For a faster local run:

```bash
LITESTAR_APP=examples.htmx_realtime_sse_sqlspec.app:app \
VITE_DEV_MODE=1 \
LITESTAR_PORT=8000 \
uv run litestar run --reload
```

Open `http://127.0.0.1:8000` (the `LITESTAR_PORT` app variable) and press
Restart.

## Run It (without the dev server)

`VITE_DEV_MODE` defaults to off. Without it, the page loads
built assets from the manifest, so `GET /` returns 500 until you build them
once. Build, then run:

```bash
LITESTAR_APP=examples.htmx_realtime_sse_sqlspec.app:app \
uv run litestar assets install
LITESTAR_APP=examples.htmx_realtime_sse_sqlspec.app:app \
uv run litestar assets build
LITESTAR_APP=examples.htmx_realtime_sse_sqlspec.app:app \
LITESTAR_PORT=8000 \
uv run litestar run
```

## How It Works

### Repeated clicks

The demo enqueues the task with `key="demo:current"`. If you click
**Restart** while that task is pending or running, the queue returns the
same task instead of starting a second copy. The page shows that the task is
already running. After the task finishes, the next click creates a new task.
Remove the key, or use a different key for each job, when concurrent runs are
what your application needs.

- **One button, no forms.** `hx-post="/demo/restart"` lives directly on the
  planet `<button>`. Clicking it enqueues the demo job and swaps the returned
  `partials/stream_mount.html` into `#stream-mount`.
- **The htmx SSE extension manages the connection.** The swapped-in
  `#stream-mount` element carries `hx-ext="sse"` and
  `sse-connect="/queues/events/sse/tasks/{task_id}"`, so htmx opens the
  `EventSource`. Each restart replaces this element, so htmx closes the old
  connection and opens a new one. This replaces roughly ninety lines of custom
  `EventSource` code.
- **One small JS adapter.** Queue events are JSON, so the extension cannot swap
  them as HTML. `resources/main.ts` listens for `htmx:sseBeforeMessage`, parses
  the frame, ignores `{"type":"ping"}` heartbeats, appends the event to the page,
  turns the readout gold on `task.completed`, and calls `preventDefault()` so
  htmx does not treat the JSON as HTML. After `HTMXTemplate` swaps the element,
  its `queue-demo:started` event resets the display for the task returned by
  the backend.
- **htmx wiring.** `resources/main.ts` imports htmx, publishes it as
  `window.htmx`, calls `registerHtmxExtension()`, and then imports
  `htmx-ext-sse`. This order is required because the htmx 2 ESM build does not
  add itself to `window`.
- **Litestar Vite JSON template.** A muted corner caption uses
  `hx-ext="litestar"` with `hx-swap="json"` and `<template ls-if>` to render the
  backend name from `GET /demo/status`.

`QueueConfig` buffers events and enables only the task event stream
(`EventStreamConfig(scopes={"task"})`). This local demo does not add
authentication to its stream. Real deployments must protect the stream route
and check who may access each task.

## External Publisher

`scripts/external_publisher.py` shows how code outside Litestar can publish with
`create_event_producer`. It raises an error until you replace its placeholder
with a shared Redis or SQLSpec Channels backend. The memory Channels backend
cannot connect separate processes.

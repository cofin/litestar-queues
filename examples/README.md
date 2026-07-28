# Examples

This directory contains standalone example apps. Each example has its own app
module, templates, frontend assets, and README. You can copy or run one without
using files from the test suite.

## Available Apps

### `htmx_realtime_websocket/`

Litestar + HTMX + `litestar-vite` app for queue event streams over the
plugin-owned WebSocket endpoints. It explicitly uses the memory queue backend,
ASGI placement, and a memory Channels backend in one process.

### `htmx_realtime_sse/`

Litestar + HTMX + `litestar-vite` app for queue event streams over the
plugin-owned SSE endpoints. It explicitly uses the memory queue backend, ASGI
placement, and a memory Channels backend in one process.

## Backend Copies

Each transport has the same backend variants:

- `htmx_realtime_websocket_sqlspec/` and `htmx_realtime_sse_sqlspec/`:
  `SQLSpecBackendConfig` with `AiosqliteConfig`.
- `htmx_realtime_websocket_advanced_alchemy/` and
  `htmx_realtime_sse_advanced_alchemy/`: `SQLAlchemyBackendConfig` with
  `sqlite+aiosqlite`.
- `htmx_realtime_websocket_redis/` and `htmx_realtime_sse_redis/`:
  `RedisBackendConfig`.
- `htmx_realtime_websocket_valkey/` and `htmx_realtime_sse_valkey/`:
  `ValkeyBackendConfig`.

The backend name tells you where queue records are stored. It does not tell you
how events reach the browser. By default, every example uses
`MemoryChannelsBackend`, which works in one process only. Redis or Valkey queue
notifications can wake a worker, but they do not share the browser stream. To
run the worker in another process, configure a shared Channels backend
explicitly. Selecting a Redis or Valkey queue backend is not enough.

All demos select ASGI placement so the worker shares their process-local live
event transport. Set `LITESTAR_QUEUES_EXAMPLE_PLACEMENT` to `server`, `asgi`,
or `external` only with a queue and Channels backend appropriate for that
process topology.

## Task context contract

Every app uses the same runnable task semantics:

- Automatic heartbeats keep active jobs live; the basic loop does not call
  `ctx.beat()`. Use `ctx.beat(detail)` only to replace the short diagnostic
  detail written by the next heartbeat.
- `ctx.progress(current=..., total=..., message=..., payload=...)` publishes
  standardized progress for status and UI consumers.
- `ctx.event(name, message=..., payload=..., immediate=False)` publishes an
  application event. It does not change progress or terminal task state.
- Returning completes the task, stores the returned result, and produces the
  queue-owned `task.completed` event. Raising follows retry policy:
  `task.failed` carries `will_retry`, and another `task.started` can follow for
  the next attempt.

The demo payloads are intentionally small. Store large pages, files, or model
artifacts outside the queue and publish a stable reference.

Start with the README in the directory you want to run. Every example uses the
`examples` dev dependency group (`uv sync --group examples --group dev`) and
local frontend dependencies from its own `package.json`.

You can provision frontend assets for all shipped examples at once with:

```bash
make install
```

## Conventions

- Examples are copyable apps, not test fixtures.
- Each example should include its own README, dependency notes, and run command.
- Long documentation snippets should be imported from example files with
  `literalinclude` tags so the docs stay tied to runnable code.

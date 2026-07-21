# Examples

This directory contains standalone example apps. Each example has its own app
module, templates, frontend assets, and README. You can copy or run one without
using files from the test suite.

## Available Apps

### `htmx_realtime_websocket/`

Litestar + HTMX + `litestar-vite` app for queue event streams over the
plugin-owned WebSocket endpoints. It runs with the default memory queue backend
and a memory Channels backend in one process.

### `htmx_realtime_sse/`

Litestar + HTMX + `litestar-vite` app for queue event streams over the
plugin-owned SSE endpoints. It runs with the default memory queue backend and a
memory Channels backend in one process.

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

All demos use the `QueueConfig` default and run the worker in the web process.
The Redis and Valkey copies accept
`LITESTAR_QUEUES_EXAMPLE_IN_APP_WORKER=0` only with their documented shared
web-and-worker setup.

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

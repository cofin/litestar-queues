# Examples

Standalone example apps live here. Each example owns its app module, templates,
frontend assets, and README so it can be copied or run without reaching into the
test suite.

## Available Apps

### `htmx_realtime_websocket/`

Litestar + HTMX + `litestar-vite` app for queue event streams over the
plugin-owned WebSocket endpoints. It runs with the default memory queue backend
and a memory Channels backend in one process.

### `htmx_realtime_sse/`

Litestar + HTMX + `litestar-vite` app for queue event streams over the
plugin-owned SSE endpoints. It runs with the default memory queue backend and a
memory Channels backend in one process.

Both apps have the same UI:

- An animated task-event crawl fed by a task-scoped event stream.
- A mission-control panel that publishes and receives custom channel events on
  `demo:mission-control`.

## Backend Copies

Each transport has the same backend variants:

- `htmx_realtime_websocket_sqlspec/` and `htmx_realtime_sse_sqlspec/`:
  `SQLSpecBackendConfig` with `AiosqliteConfig`.
- `htmx_realtime_websocket_advanced_alchemy/` and
  `htmx_realtime_sse_advanced_alchemy/`: `AdvancedAlchemyBackendConfig` with
  `sqlite+aiosqlite`.
- `htmx_realtime_websocket_redis/` and `htmx_realtime_sse_redis/`:
  `RedisBackendConfig`.
- `htmx_realtime_websocket_valkey/` and `htmx_realtime_sse_valkey/`:
  `ValkeyBackendConfig`.

Start with the README in the directory you want to run. Every example uses the
optional `litestar-queues[examples]` Python extra and local frontend
dependencies from its own `package.json`.

## Conventions

- Examples are copyable apps, not test fixtures.
- Each example should include its own README, dependency notes, and run command.
- Long documentation snippets should be imported from example files with
  `literalinclude` tags so the docs stay tied to runnable code.

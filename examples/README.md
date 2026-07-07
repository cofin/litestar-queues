# Examples

Standalone example apps live here. Each example owns its app module, templates,
frontend assets, and README so it can be copied or run without reaching into the
test suite.

## Available Apps

### `htmx_realtime/`

Litestar + HTMX + `litestar-vite` app for queue event streams. It runs with the
default memory queue backend and a memory Channels backend in one process. The
UI has two views:

- An animated task-event crawl that can consume either WebSocket or SSE.
- A mission-control panel that publishes and receives custom channel events on
  `demo:mission-control`.

### Backend Copies

Each copy has the same UI and event-stream behavior, but uses a different queue
backend:

- `htmx_realtime_sqlspec/`: `SQLSpecBackendConfig` with `AiosqliteConfig`.
- `htmx_realtime_advanced_alchemy/`: `AdvancedAlchemyBackendConfig` with
  `sqlite+aiosqlite`.
- `htmx_realtime_redis/`: `RedisBackendConfig`.
- `htmx_realtime_valkey/`: `ValkeyBackendConfig`.

Start with the README in the directory you want to run. Every example uses the
optional `litestar-queues[examples]` Python extra and local frontend
dependencies from its own `package.json`.

## Conventions

- Examples are copyable apps, not test fixtures.
- Each example should include its own README, dependency notes, and run command.
- Long documentation snippets should be imported from example files with
  `literalinclude` tags so the docs stay tied to runnable code.

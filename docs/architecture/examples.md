# Runnable Example Applications

The repository ships a complete, runnable realtime dashboard under `examples/htmx_realtime/` that demonstrates `litestar-queues` paired with Litestar Channels, HTMX, Server-Sent Events (SSE), and WebSockets across every storage backend and both in-process and external worker topologies.

## Realtime HTMX Dashboard (`examples/htmx_realtime`)

The `examples/htmx_realtime` application showcases:

- **In-Task Progress & Structured Logs**: Tasks inject `TaskExecutionContext` (`ctx.progress()`, `ctx.log()`, `ctx.event()`) to stream multi-stage progress bars and live log lines to connected browsers.
- **Dual Transport Support**: Switch between **Server-Sent Events (`GET /queues/events`)** and **WebSockets (`WS /queues/events/ws`)** with automatic `Last-Event-ID` / `after_sequence` cursor replay from `QueueEventLog`.
- **Pluggable Storage & Worker Topologies**: Run the exact same application code against `memory`, `sqlspec` (SQLite/PostgreSQL), `advanced_alchemy`, `redis`, or `valkey` in either single-process (`placement="asgi"`) or external worker (`placement="external"`) mode.

## Example Application Matrix

| Entrypoint Module | Storage Backend | Realtime Stream Transport | Default Worker Topology |
| :--- | :--- | :--- | :--- |
| `examples.htmx_realtime.app_memory:app` | `InMemoryQueueBackend` | WebSocket (`/queues/events/ws`) | In-process (`placement="asgi"`) |
| `examples.htmx_realtime.app_memory_sse:app` | `InMemoryQueueBackend` | Server-Sent Events (`/queues/events`) | In-process (`placement="asgi"`) |
| `examples.htmx_realtime.app_sqlspec:app` | `SQLSpecQueueBackend` (SQLite / Postgres) | WebSocket (`/queues/events/ws`) | Configurable (`asgi`, `server`, `external`) |
| `examples.htmx_realtime.app_sqlspec_sse:app` | `SQLSpecQueueBackend` (SQLite / Postgres) | Server-Sent Events (`/queues/events`) | Configurable (`asgi`, `server`, `external`) |
| `examples.htmx_realtime.app_alchemy:app` | `SQLAlchemyBackend` (Advanced Alchemy) | WebSocket (`/queues/events/ws`) | Configurable (`asgi`, `server`, `external`) |
| `examples.htmx_realtime.app_alchemy_sse:app` | `SQLAlchemyBackend` (Advanced Alchemy) | Server-Sent Events (`/queues/events`) | Configurable (`asgi`, `server`, `external`) |
| `examples.htmx_realtime.app_redis:app` | `RedisQueueBackend` (`redis>=8.1.0`) | WebSocket (`/queues/events/ws`) | Configurable (`asgi`, `server`, `external`) |
| `examples.htmx_realtime.app_redis_sse:app` | `RedisQueueBackend` (`redis>=8.1.0`) | Server-Sent Events (`/queues/events`) | Configurable (`asgi`, `server`, `external`) |
| `examples.htmx_realtime.app_valkey:app` | `ValkeyQueueBackend` (`valkey>=6.0.0`) | WebSocket (`/queues/events/ws`) | Configurable (`asgi`, `server`, `external`) |
| `examples.htmx_realtime.app_valkey_sse:app` | `ValkeyQueueBackend` (`valkey>=6.0.0`) | Server-Sent Events (`/queues/events`) | Configurable (`asgi`, `server`, `external`) |

## Running the Examples Locally

### Zero-Dependency Memory or SQLSpec SQLite

Run the in-memory or SQLSpec SQLite dashboard directly with `uv`:

```bash
uv run litestar --app examples.htmx_realtime.app_sqlspec_sse:app run --reload
```

Open `http://127.0.0.1:8000` in your browser to enqueue tasks, watch live stage progress bars, inspect durable `QueueEventLog` history, and test cancellation or simulated failures.

### Two-Process External Worker Topology

To test cross-process queue execution and durable `QueueEventLog` replay between separate web and worker processes:

=== "Terminal 1: Web Server"

    ```bash
    LITESTAR_QUEUES_PLACEMENT=external \
      uv run litestar --app examples.htmx_realtime.app_sqlspec_sse:app run --port 8000
    ```

=== "Terminal 2: External Worker"

    ```bash
    LITESTAR_QUEUES_PLACEMENT=external \
      uv run litestar --app examples.htmx_realtime.app_sqlspec_sse:app queues run
    ```

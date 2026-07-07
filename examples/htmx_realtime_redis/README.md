# HTMX Realtime Queue Events (Redis Backend)

This copy uses `RedisBackendConfig` for task storage. It keeps queue execution
and worker defaults unchanged, and only adds the Redis queue backend to the
shared event-stream configuration.

The browser stream fan-out still uses memory Channels because this is a
one-process demo. Use a shared Channels backend when separate web replicas,
workers, or external publishers must reach the same browser stream.

## Run It

Start Redis locally, then run from the repository root:

```bash
uv sync --extra examples --extra redis --group dev
LITESTAR_APP=examples.htmx_realtime_redis.app:app \
uv run litestar assets install
LITESTAR_APP=examples.htmx_realtime_redis.app:app \
LITESTAR_QUEUES_EXAMPLE_REDIS_URL=redis://localhost:6379/0 \
LITESTAR_QUEUES_EXAMPLE_VITE_DEV=1 \
uv run litestar run --reload
```

## What It Demonstrates

- `RedisBackendConfig(url=..., key_prefix=...)`.
- `HTMXPlugin()` with `litestar-vite` `mode="htmx"`.
- `registerHtmxExtension()`, `hx-swap="json"`, and Litestar `ls-*` templates.
- HTMX 2 indicators, disabled elements, sync replacement, and transition swaps.
- Task and custom-channel queue event streams over WebSocket or SSE.

# HTMX Realtime Queue Events (Advanced Alchemy / aiosqlite Backend)

This copy uses `AdvancedAlchemyBackendConfig` with
`SQLAlchemyAsyncConfig(connection_string="sqlite+aiosqlite:///...")` for task
storage. `SQLAlchemyAsyncConfig(create_all=True)` and
`AdvancedAlchemyBackendConfig(create_schema=True)` keep the local SQLite schema
self-bootstrapping. It keeps queue execution and worker defaults unchanged.

The browser stream fan-out still uses memory Channels because this is a
one-process demo. Use a shared Channels backend when separate web replicas,
workers, or external publishers must reach the same browser stream.

## Run It

From the repository root:

```bash
uv sync --extra examples --extra advanced-alchemy --group dev
LITESTAR_APP=examples.htmx_realtime_advanced_alchemy.app:app \
uv run litestar assets install
LITESTAR_APP=examples.htmx_realtime_advanced_alchemy.app:app \
LITESTAR_QUEUES_EXAMPLE_VITE_DEV=1 \
uv run litestar run --reload
```

Set `LITESTAR_QUEUES_EXAMPLE_ADVANCED_ALCHEMY_DB=/tmp/queue.db` to choose the
SQLite file. The default is
`examples/htmx_realtime_advanced_alchemy/queue-advanced-alchemy.db`.

## What It Demonstrates

- `AdvancedAlchemyBackendConfig(sqlalchemy_config=SQLAlchemyAsyncConfig(..., create_all=True))`.
- `HTMXPlugin()` with `litestar-vite` `mode="htmx"`.
- `registerHtmxExtension()`, `hx-swap="json"`, and Litestar `ls-*` templates.
- HTMX 2 indicators, disabled elements, sync replacement, and transition swaps.
- Task and custom-channel queue event streams over WebSocket or SSE.

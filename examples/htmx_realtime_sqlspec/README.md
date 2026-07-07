# HTMX Realtime Queue Events (SQLSpec / aiosqlite Backend)

This copy uses `SQLSpecBackendConfig` with `AiosqliteConfig` for task storage.
It otherwise keeps the same simple queue-event setup as the memory example:
buffered events, task/custom streams, and no execution or worker-default
overrides. `SQLSpecBackendConfig(run_migrations=True)` applies the packaged
queue migration by calling the adapter config's `migrate_up()` path on backend
open.

The browser stream fan-out still uses memory Channels because this is a
one-process demo. Use a shared Channels backend when separate web replicas,
workers, or external publishers must reach the same browser stream.

## Run It

From the repository root:

```bash
uv sync --extra examples --extra sqlspec --group dev
LITESTAR_APP=examples.htmx_realtime_sqlspec.app:app \
uv run litestar assets install
LITESTAR_APP=examples.htmx_realtime_sqlspec.app:app \
LITESTAR_QUEUES_EXAMPLE_VITE_DEV=1 \
uv run litestar run --reload
```

Set `LITESTAR_QUEUES_EXAMPLE_SQLSPEC_DB=/tmp/queue.db` to choose the SQLite
file. The default is `examples/htmx_realtime_sqlspec/queue-sqlspec.db`.

## What It Demonstrates

- `SQLSpecBackendConfig(config=AiosqliteConfig(...), create_schema=False, run_migrations=True)`.
- `HTMXPlugin()` with `litestar-vite` `mode="htmx"`.
- `registerHtmxExtension()`, `hx-swap="json"`, and Litestar `ls-*` templates.
- HTMX 2 indicators, disabled elements, sync replacement, and transition swaps.
- Task and custom-channel queue event streams over WebSocket or SSE.

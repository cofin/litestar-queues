# Litestar Queues

[![PyPI](https://img.shields.io/pypi/v/litestar-queues)](https://pypi.org/project/litestar-queues/)
[![Python](https://img.shields.io/pypi/pyversions/litestar-queues)](https://pypi.org/project/litestar-queues/)
[![License](https://img.shields.io/pypi/l/litestar-queues)](https://github.com/cofin/litestar-queues/blob/main/LICENSE)
[![CI](https://github.com/cofin/litestar-queues/actions/workflows/ci.yml/badge.svg)](https://github.com/cofin/litestar-queues/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-cofin.github.io-blue)](https://cofin.github.io/litestar-queues/)

Litestar Queues adds background task queues to Litestar applications. Define a
task with a decorator, enqueue it from a route handler or service, and let a
worker run it now, later, after a retry, or on a schedule.

Use it when a request should return quickly while work continues elsewhere:
sending email, refreshing reports, syncing accounts, importing files, calling
slow APIs, or running operational maintenance jobs.

## What You Get

- **Simple task API**: `@task(...)` registers async or sync callables with
  defaults for queues, retries, priority, timeout, delay, and metadata.
- **Litestar plugin**: `QueuePlugin` wires a managed `QueueService` into
  Litestar dependency injection, app state, startup, shutdown, and CLI commands.
- **Workers included**: run an in-app worker for local/lightweight apps or a
  standalone worker process for production deployments.
- **Scheduling**: run recurring interval or five-field cron tasks.
- **Result tracking**: queued records move through `pending`, `scheduled`,
  `running`, `completed`, `failed`, and `cancelled`.
- **Pluggable backends**: start with the in-memory backend, then move to
  SQLSpec, Advanced Alchemy, Redis, Valkey, or Cloud Run execution when needed.
- **Realtime events**: publish lifecycle, progress, log, and custom task events
  to your own Litestar Channels setup.

## Install

```bash
pip install litestar-queues
```

The base install is intentionally small. It includes the task API, Litestar
plugin, in-memory queue backend, immediate execution, and local workers.
Persistent or remote integrations are optional extras.

## Quick Start

Create an `app.py`:

```python
from litestar import Litestar, post
from litestar.di import NamedDependency

from litestar_queues import QueueConfig, QueuePlugin, QueueService, task


@task("accounts.sync", queue="accounts", retries=3, timeout=300)
async def sync_account(account_id: str) -> dict[str, str]:
    return {"account_id": account_id, "status": "synced"}


@post("/accounts/{account_id:str}/sync")
async def create_sync_job(
    account_id: str,
    queue_service: NamedDependency[QueueService],
) -> dict[str, str]:
    result = await queue_service.enqueue(sync_account, account_id)
    return {"task_id": str(result.id), "status": result.status or "queued"}


app = Litestar(
    route_handlers=[create_sync_job],
    plugins=[QueuePlugin(config=QueueConfig())],
)
```

Run the app:

```bash
LITESTAR_APP=app:app litestar run --reload
```

Call the route:

```bash
curl -X POST http://127.0.0.1:8000/accounts/acct-123/sync
```

Trigger the same job in whichever form fits the caller:

```python
# 1. Pass the decorated task object.
await queue_service.enqueue(sync_account, account_id)

# 2. Pass the registered task name when the caller should not import the task.
await queue_service.enqueue("accounts.sync", account_id)

# 3. Use the task helper when the QueuePlugin has an active default service.
await sync_account.enqueue(account_id)
```

If you enqueue by string to avoid importing the task function, make sure the
module is loaded at startup so the decorator can register the task name:

```python
app = Litestar(
    route_handlers=[create_sync_job],
    plugins=[
        QueuePlugin(
            config=QueueConfig(task_modules=("app.accounts.tasks",)),
        ),
    ],
)
```

All three forms can still override execution for one job:

```python
await queue_service.enqueue(
    "accounts.sync",
    account_id,
    execution_backend="cloudrun",
    execution_profile="heavy",
)
```

By default, Litestar Queues uses in-memory queue storage and starts a local
worker inside the Litestar process. That is useful for learning, tests, and
small local apps. For production, use a persistent queue backend and usually run
workers separately from the web process.

## The Basic Model

Litestar Queues keeps two decisions separate:

- A **queue backend** stores task records and state.
- An **execution backend** decides where claimed work runs.

The default is `queue_backend="memory"` and `execution_backend="local"`.
`memory` stores records inside the current Python process. `local` runs claimed
tasks in the worker process. `immediate` is available for inline execution,
mostly in tests.

## Running Workers

For local development, the in-app worker is the shortest path:

```python
config = QueueConfig(in_app_worker=True)
```

For heavier deployments, turn off the in-app worker in the web app:

```python
config = QueueConfig(in_app_worker=False)
```

Then run one or more standalone workers:

```bash
LITESTAR_APP=app:app litestar queues run --drain-timeout 30
```

Workers process every queue by default. Restrict a worker to one or more queue
names with `--queue`:

```bash
LITESTAR_APP=app:app litestar queues run --queue accounts --queue emails
```

## Documentation

- [Getting Started](https://cofin.github.io/litestar-queues/getting_started/index.html)
- [Usage Guides](https://cofin.github.io/litestar-queues/usage/index.html)
- [Backends](https://cofin.github.io/litestar-queues/usage/backends.html)
- [API Reference](https://cofin.github.io/litestar-queues/reference/index.html)

<details>
<summary>Optional backend and execution choices</summary>

Install only the extras your app needs:

```bash
pip install "litestar-queues[sqlspec]"
pip install "litestar-queues[advanced-alchemy]"
pip install "litestar-queues[redis]"
pip install "litestar-queues[valkey]"
pip install "litestar-queues[cloudrun]"
```

| Name | Type | Typical use |
| --- | --- | --- |
| `memory` | Queue backend | Tests, examples, and local in-process apps |
| `sqlspec` | Queue backend | SQL-backed persistence through SQLSpec adapters |
| `advanced-alchemy` | Queue backend | SQLAlchemy/Advanced Alchemy persistence |
| `redis` | Queue backend | Redis-backed task records and worker wakeups |
| `valkey` | Queue backend | Valkey-backed task records and worker wakeups |
| `immediate` | Execution backend | Inline execution for tests and scripts |
| `local` | Execution backend | In-process worker execution |
| `cloudrun` | Execution backend | Dispatch to Google Cloud Run Jobs |

SQLSpec example:

```python
from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues import QueueConfig
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig

queue_config = QueueConfig(
    queue_backend=SQLSpecBackendConfig(
        config=AiosqliteConfig(connection_config={"database": "queue.db"}),
        run_migrations=True,
    ),
    execution_backend="local",
)
```

Redis example:

```python
from litestar_queues import QueueConfig
from litestar_queues.backends.redis import RedisBackendConfig

queue_config = QueueConfig(
    queue_backend=RedisBackendConfig(url="redis://localhost:6379/0"),
    execution_backend="local",
)
```

</details>

<details>
<summary>Task options, scheduling, events, and background responses</summary>

Task defaults can live on the decorator:

```python
@task(
    "reports.render",
    queue="reports",
    priority=10,
    retries=2,
    timeout=120,
    run_after=30,
)
async def render_report(report_id: str) -> str:
    return report_id
```

Override those defaults for one enqueue call:

```python
result = await queue_service.enqueue(
    render_report,
    "report-1",
    queue="slow-reports",
    priority=1,
    retries=5,
    timeout=600,
    metadata={"requested_by": "user-123"},
)
await result.wait(timeout=30)
```

Run recurring tasks with intervals or cron expressions:

```python
from datetime import timedelta

from litestar_queues import task


@task("reports.refresh", interval=timedelta(minutes=15), jitter=30)
async def refresh_reports() -> None:
    ...


@task("billing.close-day", cron="0 0 * * *", timezone="UTC")
async def close_billing_day() -> None:
    ...
```

Publish progress from inside a running task:

```python
from litestar_queues.events import publish_task_log, publish_task_progress


@task("imports.process")
async def process_import(path: str) -> None:
    await publish_task_log("Import started")
    await publish_task_progress(current=5, total=10, message="Halfway done")
```

Queue work after a Litestar response is sent:

```python
from litestar import Response, post
from litestar_queues import QueuedBackgroundTask


@post("/trigger")
async def trigger() -> Response[dict[str, str]]:
    return Response(
        {"status": "queued"},
        background=QueuedBackgroundTask(process_import, "/tmp/data.csv"),
    )
```

</details>

<details>
<summary>CLI commands</summary>

`QueuePlugin` adds a `queues` command group to the Litestar CLI:

```bash
# Start a standalone worker.
LITESTAR_APP=app:app litestar queues run --drain-timeout 30

# Process only selected queues.
LITESTAR_APP=app:app litestar queues run --queue accounts --max-concurrency 4

# Print queue status counts.
LITESTAR_APP=app:app litestar queues status

# Emit queue status as JSON.
LITESTAR_APP=app:app litestar queues status --json

# Check whether a scheduler canary task completed recently.
LITESTAR_APP=app:app litestar queues scheduler-health --minutes 5
```

Every command loads the application the same way the Litestar CLI does: via
`LITESTAR_APP`, `--app`, or the standard app discovery paths.

</details>

<details>
<summary>Development</summary>

```bash
# Install local development dependencies.
make install

# Run unit tests only.
make test-unit

# Run integration tests. Docker-backed services autoskip when unavailable.
make test-integration

# Build documentation.
make docs

# Run linting and type checks.
make lint
```

The source lives under `src/litestar_queues`. Tests live under
`src/tests/unit` and `src/tests/integration`.

</details>

## Links

- Docs: <https://cofin.github.io/litestar-queues/>
- Source: <https://github.com/cofin/litestar-queues>
- Issues: <https://github.com/cofin/litestar-queues/issues>
- Litestar: <https://litestar.dev/>

## License

MIT

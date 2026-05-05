# litestar-queues

Task queue support for Litestar applications. This package provides a typed
task decorator, result handles, memory-backed queue persistence, immediate
execution, local in-process workers, and Litestar plugin lifecycle wiring.

## Installation

```bash
pip install litestar-queues
```

Optional backend extras are reserved for deployments that need additional queue
or execution integrations:

```bash
# SQLSpec queue backend
pip install litestar-queues[sqlspec]

# Advanced Alchemy queue backend
pip install litestar-queues[advanced-alchemy]

# Redis queue backend
pip install litestar-queues[redis]

# Cloud Run execution backend
pip install litestar-queues[cloudrun]
```

The core install stays memory-only and does not require SQLSpec, Advanced
Alchemy, Redis, Valkey, or Cloud Run client dependencies.

## Usage

```python
from litestar import Litestar
from litestar_queues import QueueConfig, QueuePlugin, task


@task("accounts.sync", queue="accounts", retries=3, timeout=300)
async def sync_account(account_id: str) -> dict[str, str]:
    return {"account_id": account_id, "status": "synced"}

config = QueueConfig(
    queue_backend="memory",
    execution_backend="local",
    start_worker=True,
)

app = Litestar(plugins=[QueuePlugin(config=config)])
```

The plugin registers a `queue_service` dependency for route handlers:

```python
from litestar import post
from litestar_queues import QueueService


@post("/accounts/{account_id:str}/sync")
async def create_task(account_id: str, queue_service: QueueService) -> dict[str, str]:
    result = await queue_service.enqueue(sync_account, account_id)
    return {"task_id": str(result.id), "status": result.status or "queued"}
```

For scripts and tests that do not need a worker, task wrappers can enqueue with
the default immediate memory service:

```python
result = await sync_account.enqueue("acct-123")

assert result.status == "completed"
assert result.result == {"account_id": "acct-123", "status": "synced"}
```

## Standalone Usage

Use the config helper directly outside of a Litestar application:

```python
from litestar_queues import QueueConfig, task


@task("reports.refresh")
async def refresh_report(report_id: str) -> str:
    return report_id

config = QueueConfig(queue_backend="memory")

async with config.provide_service() as queue_service:
    result = await queue_service.enqueue(refresh_report, "report-123")
    await result.refresh()
```

## Available Backend Names

| Backend | Type | Use Case |
|---------|------|----------|
| `memory` | queue | Tests and local development |
| `immediate` | execution | Inline task execution |
| `local` | execution | In-process worker execution |
| `sqlspec` | queue | Optional SQLSpec-backed persistence |
| `advanced-alchemy` | queue | Optional Advanced Alchemy persistence |
| `redis` | queue | Optional Redis persistence |
| `valkey` | queue | Optional Valkey persistence |
| `cloudrun` | execution | Optional Cloud Run dispatch |

The core package registers `memory`, `immediate`, and `local`. The `sqlspec`
queue backend is available when the SQLSpec extra is installed:

```python
from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues import QueueConfig

config = QueueConfig(
    queue_backend="sqlspec",
    queue_backend_config={
        "sqlspec_config": AiosqliteConfig(
            connection_config={"database": "queue.db"},
        ),
        "run_migrations": True,
    },
    execution_backend="local",
)
```

SQLSpec persists task arguments, keyword arguments, metadata, and results with
SQLSpec's JSON serializer. Litestar applications should register SQLSpec's
first-party plugin directly and pass the same `SQLSpec`/adapter config into
`queue_backend_config` when they want SQLSpec dependency injection.

SQLSpec worker wakeups can use SQLSpec Events when configured:

```python
from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues import QueueConfig

sqlspec_config = AiosqliteConfig(
    connection_config={"database": "queue.db"},
    extension_config={
        "events": {
            "backend": "table_queue",
            "queue_table": "queue_events",
            "poll_interval": 0.1,
        }
    },
)

config = QueueConfig(
    queue_backend="sqlspec",
    queue_backend_config={
        "sqlspec_config": sqlspec_config,
        "create_schema": False,
        "run_migrations": True,
        "notifications": True,
        "notification_channel": "queue_notifications",
    },
    execution_backend="local",
)
```

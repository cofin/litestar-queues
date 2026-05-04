# litestar-queues

Task queue support for Litestar applications. This package provides a typed
task decorator, result handles, memory-backed task storage, immediate
execution, local in-process workers, and Litestar plugin lifecycle wiring.

## Installation

```bash
pip install litestar-queues
```

Optional backend extras are reserved for deployments that need additional
storage or execution integrations:

```bash
# SQLSpec storage backend
pip install litestar-queues[sqlspec]

# Advanced Alchemy storage backend
pip install litestar-queues[advanced-alchemy]

# Redis storage backend
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
    storage_backend="memory",
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

config = QueueConfig(storage_backend="memory")

async with config.provide_service() as queue_service:
    result = await queue_service.enqueue(refresh_report, "report-123")
    await result.refresh()
```

## Available Backend Names

| Backend | Type | Use Case |
|---------|------|----------|
| `memory` | storage | Tests and local development |
| `immediate` | execution | Inline task execution |
| `local` | execution | In-process worker execution |
| `sqlspec` | storage | Optional SQLSpec-backed persistence |
| `advanced-alchemy` | storage | Optional Advanced Alchemy persistence |
| `redis` | storage | Optional Redis storage |
| `valkey` | storage | Optional Valkey storage |
| `cloudrun` | execution | Optional Cloud Run dispatch |

The core package registers `memory`, `immediate`, and `local`. The `sqlspec`
storage backend is available when the SQLSpec extra is installed:

```python
from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues import QueueConfig

config = QueueConfig(
    storage_backend="sqlspec",
    storage_backend_config={
        "sqlspec_config": AiosqliteConfig(
            connection_config={"database": "queue.db"},
        ),
        "run_migrations": True,
    },
    execution_backend="local",
)
```

SQLSpec storage persists JSON-compatible task arguments, keyword arguments,
metadata, and results. Set `register_plugin=True` in
`storage_backend_config` when a Litestar app should also register SQLSpec's
first-party Litestar plugin during application initialization.

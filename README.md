# litestar-queues

Task queue support for Litestar applications. This package provides the
scaffold for defining queue configuration, registering a Litestar plugin, and
choosing storage and execution backends.

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
from litestar_queues import QueueConfig, QueuePlugin

config = QueueConfig(
    storage_backend="memory",
    execution_backend="immediate",
    start_worker=False,
)

app = Litestar(plugins=[QueuePlugin(config=config)])
```

The plugin registers a `queue_service` dependency for route handlers:

```python
from litestar import post
from litestar_queues import QueueService


@post("/tasks/{task_name:str}")
async def create_task(task_name: str, queue_service: QueueService) -> dict[str, str]:
    await queue_service.enqueue(task_name)
    return {"status": "queued"}
```

Queue runtime behavior is intentionally minimal in the first scaffold. The
storage backend contract, task decorator, result handles, and local worker
runtime are extension points for the next implementation chapter.

## Standalone Usage

Use the config helper directly outside of a Litestar application:

```python
from litestar_queues import QueueConfig

config = QueueConfig(storage_backend="memory")

async with config.provide_service() as queue_service:
    await queue_service.enqueue("sync-account", "acct-123")
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

Only the memory, immediate, and local backend placeholders are registered in
the scaffold. Optional backend implementations are added in later chapters.

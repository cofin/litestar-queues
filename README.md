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

# Valkey queue backend
pip install litestar-queues[valkey]

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

app = Litestar(plugins=[QueuePlugin(config=QueueConfig())])
```

The plugin registers a `queue_service` dependency for route handlers:

```python
from litestar import post
from litestar.di import NamedDependency
from litestar_queues import QueueService


@post("/accounts/{account_id:str}/sync")
async def create_task(account_id: str, queue_service: NamedDependency[QueueService]) -> dict[str, str]:
    result = await queue_service.enqueue(sync_account, account_id)
    return {"task_id": str(result.id), "status": result.status or "queued"}
```

Queue names are routing labels for tasks. They are separate from the queue
backend (`memory`, `redis`, and so on). Set a task's default queue with the
decorator, override it with `task.using(queue="...")`, or pass
`queue="..."` to `queue_service.enqueue()`. Tasks use the `"default"` queue
when no queue is set.

Workers process every queue unless you filter them:

```python
config = QueueConfig(worker_queues=("accounts",))
```

```console
LITESTAR_APP=app.asgi:app litestar queues run --queue accounts
LITESTAR_APP=app.asgi:app litestar queues run --queue emails
```

`litestar queues run --queue ...` applies only to that standalone worker
process and overrides `QueueConfig.worker_queues` for the run.

The default configuration runs a worker inside the Litestar application process.
For heavier deployments, use a shared backend, set `in_app_worker=False` in the
web app, and run workers separately:

```python
config = QueueConfig(in_app_worker=False)
```

```console
LITESTAR_APP=app.asgi:app litestar queues run --drain-timeout 30
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

The package registers optional backend names without importing their client
libraries. Opening an optional backend requires the matching extra or an
injected client. The `sqlspec` queue backend is available when the SQLSpec
extra is installed:

```python
from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues import QueueConfig
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig

config = QueueConfig(
    queue_backend=SQLSpecBackendConfig(
        config=AiosqliteConfig(
            connection_config={"database": "queue.db"},
        ),
        run_migrations=True,
    ),
    execution_backend="local",
)
```

SQLSpec persists task arguments, keyword arguments, metadata, and results with
SQLSpec's JSON serializer. Litestar applications should register SQLSpec's
first-party plugin directly and pass the same `SQLSpec`/adapter config into
`SQLSpecBackendConfig` when they want SQLSpec dependency injection.

The `advanced-alchemy` queue backend is available when the Advanced Alchemy
extra is installed:

```python
from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig

from litestar_queues import QueueConfig
from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyBackendConfig

alchemy_config = SQLAlchemyAsyncConfig(
    connection_string="sqlite+aiosqlite:///queue.db",
)

config = QueueConfig(
    queue_backend=AdvancedAlchemyBackendConfig(
        sqlalchemy_config=alchemy_config,
        create_schema=True,
    ),
    execution_backend="local",
)
```

Litestar applications should register Advanced Alchemy's `SQLAlchemyPlugin`
directly and pass the same `SQLAlchemyAsyncConfig` into the queue backend. The
queue backend defaults to a `litestar_queue_task` table through its built-in
model. Override `model_class` when an application needs a custom table name,
base class, or migration ownership. When the app imports its queue config at
startup, Advanced Alchemy's metadata includes that model for Alembic
autogenerate. The backend uses operation-scoped sessions from that config and
does not append database plugins itself.

The `redis` queue backend is available when the Redis extra is installed:

```python
from litestar_queues import QueueConfig
from litestar_queues.backends.redis import RedisBackendConfig

config = QueueConfig(
    queue_backend=RedisBackendConfig(
        url="redis://localhost:6379/0",
        key_prefix="litestar_queues",
        notifications=True,
    ),
    execution_backend="local",
)
```

The `valkey` queue backend uses the same queue contract with Valkey's asyncio
client:

```python
from litestar_queues import QueueConfig
from litestar_queues.backends.valkey import ValkeyBackendConfig

config = QueueConfig(
    queue_backend=ValkeyBackendConfig(
        url="redis://localhost:6379/0",
        key_prefix="litestar_queues",
        notifications=True,
    ),
    execution_backend="local",
)
```

Redis and Valkey store queue records in hashes indexed by a package key prefix,
use a sorted set for delayed scheduling, use short-lived `SET NX` locks around
claim and key-replacement mutations, and use pub/sub only for worker wakeups.
Pub/sub notifications are not durable; a worker that misses a notification
falls back to polling. Task arguments, keyword arguments, metadata, and results
must be JSON serializable for these backends.

The `cloudrun` execution backend is available when the Cloud Run extra is
installed:

```python
from litestar_queues import QueueConfig, task
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig
from litestar_queues.execution.cloudrun import CloudRunExecutionConfig


@task("reports.render", execution_backend="cloudrun", execution_profile="heavy")
async def render_report(report_id: str) -> None:
    ...

config = QueueConfig(
    queue_backend=SQLSpecBackendConfig(config=...),
    execution_backend=CloudRunExecutionConfig(
        project_id="example-project",
        region="us-central1",
        job_name="queue-worker",
        profiles={"heavy": "queue-worker-heavy"},
    ),
)
```

Cloud Run dispatch stores an execution reference on the queue record. The
package entry point `litestar-queues-cloudrun-worker` reads generic
`LITESTAR_QUEUES_*` environment variables, loads the configured task modules,
claims the persisted record, executes it with normal queue lifecycle semantics,
and publishes task events through the configured event publisher. Applications
own the queue backend configuration passed into the worker process.

SQLSpec worker wakeups can use SQLSpec Events when configured:

```python
from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues import QueueConfig
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig

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

queue_config = QueueConfig(
    queue_backend=SQLSpecBackendConfig(
        config=sqlspec_config,
        create_schema=False,
        run_migrations=True,
        notifications=True,
        notification_channel="queue_notifications",
    ),
    execution_backend="local",
)
```

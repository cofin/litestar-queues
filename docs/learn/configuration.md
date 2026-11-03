# Plugin & Service Configuration

[`QueueConfig`](../reference/plugin-and-config.md#litestar_queues.config.QueueConfig) is the root configuration dataclass passed to [`QueuePlugin`](../reference/plugin-and-config.md#litestar_queues.plugin.QueuePlugin) or [`QueueService`](../reference/plugin-and-config.md#litestar_queues.service.QueueService).

## Configuration Hierarchy

`QueueConfig` composes five specialized sub-configurations:

| Section | Field on `QueueConfig` | Configuration Type | Responsibility |
| :--- | :--- | :--- | :--- |
| **Storage Backend** | `queue_backend` | `str \| QueueBackendConfigProtocol` | Authoritative task state, claims, heartbeats, uniqueness reservations, and event history storage |
| **Execution Transport** | `execution_backend` / `execution_backends` | `str \| ExecutionBackendConfigProtocol` | How tasks are triggered (`"local"`, `"immediate"`, Cloud Tasks, Cloud Run Jobs, Pub/Sub, SQS, RabbitMQ, Kafka) |
| **Worker Runtime** | `worker` | [`WorkerConfig`](workers.md) | Placement (`"server"`, `"asgi"`, `"external"`), concurrency, adaptive polling, heartbeats, stale recovery, and terminal cleanup |
| **Maintenance** | `maintenance` | [`QueueMaintenanceConfig`](../execution/maintenance.md) | Distributed lease coordination, operation selection, and signed HTTP maintenance endpoint mounting |
| **Events & Streams** | `events` | [`QueueEventsConfig`](../events/index.md) | Live SSE/WebSocket stream routes, durable event history retention, and ring buffer tuning |
| **Observability** | `observability` | [`ObservabilityConfig`](../events/observability.md) | OpenTelemetry tracing and metrics instrumentation (`"auto"`, `"enabled"`, `"disabled"`) |

## Complete Production Configuration Example

```python
from litestar import Litestar
from litestar_queues import (
    EventHistoryConfig,
    EventStreamConfig,
    ObservabilityConfig,
    QueueConfig,
    QueueEventsConfig,
    QueueMaintenanceConfig,
    QueuePlugin,
    WorkerConfig,
)
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig

db_config = AsyncpgConfig(connection_config={"dsn": "postgresql://app:secret@db.internal:5432/app"})

queue_config = QueueConfig(
    queue_backend=SQLSpecBackendConfig(
        config=db_config,
        auto_create_tables=False,
        wakeup_transport="notify_queue",
        terminal_ttl=86_400.0,
    ),
    execution_backend="local",
    task_modules=["my_app.tasks"],
    initialize_schedules=True,
    worker=WorkerConfig(
        placement="external",
        queues=("critical", "default", "bulk"),
        max_concurrency=32,
        queue_concurrency={"critical": 20, "bulk": 4},
        stale_after=90.0,
        requeue_on_shutdown=True,
    ),
    maintenance=QueueMaintenanceConfig(
        lease_ttl=60.0,
        terminal_retention=86_400.0,
    ),
    events=QueueEventsConfig(
        enabled=True,
        history=EventHistoryConfig(retention_seconds=604_800.0),
        streams=EventStreamConfig(mount_routes=True, route_prefix="/queues/events"),
    ),
    observability=ObservabilityConfig(mode="auto"),
)

app = Litestar(plugins=[QueuePlugin(config=queue_config)])
```

## Multi-Backend Execution Routing

An application can combine local workers with one or more external execution transports by passing `execution_backends` on `QueueConfig`:

- `execution_backend` defines the default transport used when a `@task` or `enqueue()` call does not specify `execution_backend=...`.
- `execution_backends` registers additional named or typed execution backend configurations (such as `CloudTasksExecutionConfig` and `CloudRunExecutionConfig`) so individual tasks can route to serverless transports via `@task(..., execution_backend="cloudrun", execution_profile="heavy-gpu")`.

See [Execution Transports Overview](../execution/index.md) for multi-transport routing examples.

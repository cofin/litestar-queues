# Advanced Alchemy Storage Backend

The Advanced Alchemy backend ([`SQLAlchemyBackend`](../reference/backends.md#litestar_queues.backends.advanced_alchemy.SQLAlchemyBackend), configured via [`SQLAlchemyBackendConfig`](../reference/backends.md#litestar_queues.backends.advanced_alchemy.SQLAlchemyBackendConfig)) integrates task queues directly with SQLAlchemy 2.0 async sessions and Alembic migration workflows.

## Runnable Example

```python title="docs/examples/backends/advanced_alchemy_custom_models.py"
--8<-- "docs/examples/backends/advanced_alchemy_custom_models.py"
```

## Default Models vs Custom Model Mixins

Out of the box, `SQLAlchemyBackendConfig` uses four package-provided ORM models inheriting from `UUIDAuditBase`:

| Default Model Class | Default Table Name | Composable Mixin Class | Purpose |
| :--- | :--- | :--- | :--- |
| `QueueTaskModel` | `queue_task` | `QueueTaskModelMixin` | Primary task records, arguments, lease timestamps, and retry counters |
| `QueueEventHistoryModel` | `queue_task_event_history` | `QueueEventHistoryModelMixin` | Durable task and worker event history rows |
| `QueueMaintenanceModel` | `queue_maintenance` | `QueueMaintenanceModelMixin` | Distributed maintenance lease coordination rows |
| `QueueTaskReservationModel` | `queue_task_reservation` | `QueueTaskReservationModelMixin` | Permanent `unique_until="forever"` identity reservations |

When your application defines its own declarative base or schema conventions, compose the four mixins (`QueueTaskModelMixin`, `QueueEventHistoryModelMixin`, `QueueMaintenanceModelMixin`, `QueueTaskReservationModelMixin`) into your own model classes and pass them to `SQLAlchemyBackendConfig`. Setting any optional model class (`event_history_model_class`, `maintenance_model_class`, or `task_reservation_model_class`) to `None` disables that table when the corresponding feature is not used.

## PostgreSQL Push Worker Wakeups

When running on PostgreSQL with `postgresql+asyncpg` or `postgresql+psycopg`, enable `worker_wakeups=True` on `SQLAlchemyBackendConfig` to use native PostgreSQL `LISTEN` / `NOTIFY` for low-latency worker wakeups, running-task cancellation control hints, and `TaskResult.wait()` completion notifications:

```python
from advanced_alchemy.config.asyncio import SQLAlchemyAsyncConfig
from litestar_queues.backends.advanced_alchemy import SQLAlchemyBackendConfig

alchemy_config = SQLAlchemyAsyncConfig(
    connection_string="postgresql+asyncpg://app:secret@db.internal:5432/app",
)

backend_config = SQLAlchemyBackendConfig(
    sqlalchemy_config=alchemy_config,
    worker_wakeups=True,
    wakeup_channel="billing_queue_wakeups",
)
```

Non-PostgreSQL SQLAlchemy drivers automatically fall back to adaptive interval polling.

## Isolating Heartbeats with heartbeat_session_maker

To prevent bulk worker heartbeat updates from competing with application session checkout limits under burst load, supply a dedicated `async_sessionmaker[AsyncSession]` bound to a small engine pool via `SQLAlchemyBackendConfig.heartbeat_session_maker`:

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from litestar_queues.backends.advanced_alchemy import SQLAlchemyBackendConfig

heartbeat_engine = create_async_engine(
    "postgresql+asyncpg://app:secret@db.internal:5432/app",
    pool_size=2,
    max_overflow=0,
)
heartbeat_sessions = async_sessionmaker(heartbeat_engine, expire_on_commit=False)

backend_config = SQLAlchemyBackendConfig(
    sqlalchemy_config=alchemy_config,
    heartbeat_session_maker=heartbeat_sessions,
)
```

## Schema Indexes for Custom Alembic Migrations

When generating Alembic migrations for custom models composed with the Advanced Alchemy mixins, ensure your migration includes:

1. The `dispatch_checked_at` nullable timezone-aware timestamp column and the composite `ix_<table_name>_dispatch_repair` index on `(status, execution_backend, dispatch_checked_at, scheduled_at, queued_at)` on your `QueueTaskModelMixin` table for external dispatch repair scans.
2. The `queue_task_reservation` table (`QueueTaskReservationModelMixin`) with primary/unique `task_key`, `task_id`, `task_name`, and `reserved_at` columns when tasks use `unique_until="forever"`.

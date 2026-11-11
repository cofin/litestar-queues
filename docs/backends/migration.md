# Backend Migration Guide

This guide covers moving an application from local development storage (`"ephemeral"` or `"memory"`) to a production database backend, selecting SQLSpec wakeup transports, and provisioning forever-uniqueness reservation tables.

## Moving from Ephemeral or Memory to Production Storage

During initial prototyping, `QueueConfig()` defaults to `queue_backend="ephemeral"`, which stores queue tables in a temporary local SQLite file shared across processes on a single host.

Because `QueueService`, `@task`, `WorkerConfig`, and `QueueEventsConfig` use a backend-neutral storage contract (`BaseQueueBackend`), migrating to PostgreSQL, CockroachDB, Spanner, Oracle, or Redis/Valkey requires changing only `QueueConfig.queue_backend` and `WorkerConfig.placement`:

=== "Development (Ephemeral SQLite)"

    ```python
    from litestar_queues import QueueConfig, WorkerConfig

    config = QueueConfig(
        queue_backend="ephemeral",
        worker=WorkerConfig(placement="server"),
    )
    ```

=== "Production (SQLSpec PostgreSQL)"

    ```python
    from litestar_queues import QueueConfig, WorkerConfig
    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecWorkerWakeupConfig
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    config = QueueConfig(
        queue_backend=SQLSpecBackendConfig(
            sqlspec_config=AsyncpgConfig(connection_config={"dsn": "postgresql://app:secret@db:5432/app"}),
            manage_schema=False,
            terminal_ttl=86_400.0,
            worker_wakeups=SQLSpecWorkerWakeupConfig(transport="notify_queue"),
        ),
        worker=WorkerConfig(placement="external", max_concurrency=32, stale_after=60.0),
    )
    ```

=== "Production (Redis)"

    ```python
    from litestar_queues import QueueConfig, WorkerConfig
    from litestar_queues.backends.redis import RedisBackendConfig

    config = QueueConfig(
        queue_backend=RedisBackendConfig(
            url="redis://redis.internal:6379/0",
            key_prefix="prod_queues",
            worker_wakeups=True,
            terminal_ttl=86_400.0,
        ),
        worker=WorkerConfig(placement="external", max_concurrency=32, stale_after=60.0),
    )
    ```

## SQLSpec Wakeup Transports Across Environments

When transitioning from embedded SQLite or DuckDB to a network database in production, `SQLSpecWorkerWakeupConfig.transport` selects the optimal wakeup strategy for the target adapter:

- **`notify_queue`**: Default on PostgreSQL (`asyncpg`, `psycopg`, `psqlpy`); combines `LISTEN` / `NOTIFY` push delivery with the durable `sqlspec_event_queue` table.
- **`notify`**: Transient PostgreSQL `LISTEN` / `NOTIFY` push delivery without requiring the `sqlspec_event_queue` table.
- **`poll_queue`**: Durable event-queue polling (default on `duckdb`).
- **`aq` / `txeventq`**: Oracle Advanced Queuing and Transactional Event Queues.
- **`polling`**: Adaptive interval polling without an event channel.

## SQLSpec Migration Modes (reconcile vs. versioned)

When `manage_schema=True` on [`SQLSpecBackendConfig`](../reference/backends.md#litestar_queues.backends.sqlspec.SQLSpecBackendConfig), choose the schema lifecycle mode that fits your deployment pipeline:

- **`migration_mode="reconcile"` (default, `sqlspec>=0.72.0`)**: Converges `queue_task`, optional `queue_task_event_history` (including nullable `EventHistoryExtraColumn` additions), `queue_maintenance`, `queue_task_reservation`, companion indexes, CockroachDB `terminal_ttl`, and durable `sqlspec_event_queue` tables on every `upgrade("head")` without adding `ext_litestar_queues_*` rows to `ddl_migrations`. Raises `MigrationError` under `strict=True` on non-additive column drift and drops enabled queue tables in reverse order on `downgrade("base")`.
- **`migration_mode="versioned"`**: Applies packaged revision `0001_create_queue_tasks` once and tracks `ext_litestar_queues_0001` in `ddl_migrations` when immutable migration histories are required.
- **`manage_schema=False` (external DDL) & Google Cloud Spanner**: Set `manage_schema=False` when Liquibase, Flyway, Terraform, or DBA pipelines own DDL completely. On Google Cloud Spanner (`spanner`), schema reconciliation refuses `upgrade("head")` with `MigrationError`; apply DDL explicitly via `await backend.create_schema()` or external Spanner DDL tooling.

## Forever-Uniqueness Reservation Storage

Tasks configured with `@task(..., unique_until="forever")` persist a lightweight [`TaskReservation`](../reference/tasks-and-models.md#litestar_queues.models.TaskReservation) (`key`, `task_id`, `task_name`, `created_at`) in a dedicated reservation store separate from `queue_task`:

- **SQLSpec**: Stored in `queue_task_reservation` (configurable via `SQLSpecBackendConfig.task_reservation_table_name`), provisioned automatically by default `migration_mode="reconcile"`, versioned migration `0001_create_queue_tasks`, or `await backend.create_schema()`.
- **Advanced Alchemy**: Stored in `QueueTaskReservationModel` (`queue_task_reservation` table, or a custom model composed with `QueueTaskReservationModelMixin`).
- **Redis & Valkey**: Stored in the `{prefix}:task_reservations` hash.

Routine terminal cleanup (`cleanup_terminal`, `terminal_ttl`, and `WorkerConfig.terminal_retention`) and event history pruning never delete `TaskReservation` entries. To release a forever reservation so the identity key can be enqueued again, call `await queue_service.reset_task_identity(key)` with the exact effective key.

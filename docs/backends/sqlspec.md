# SQLSpec Storage Backend

The SQLSpec backend ([`SQLSpecQueueBackend`](../reference/backends.md#litestar_queues.backends.sqlspec.SQLSpecQueueBackend), configured via [`SQLSpecBackendConfig`](../reference/backends.md#litestar_queues.backends.sqlspec.SQLSpecBackendConfig)) provides high-throughput relational storage across 11 SQL dialects without requiring an ORM.

## Runnable Example

```python title="docs/examples/backends/sqlspec_sqlite.py"
--8<-- "docs/examples/backends/sqlspec_sqlite.py"
```

## Supported SQLSpec Adapters & Dialects

`litestar-queues` ships dedicated [`SQLSpecQueueStore`](../reference/backends.md#litestar_queues.backends.sqlspec.stores.base.SQLSpecQueueStore) implementations tuned for each driver's parameter binding, locking (`FOR UPDATE SKIP LOCKED`), `UPDATE ... RETURNING` batch claiming, and JSON codecs:

| Database Engine | Supported SQLSpec Adapter Names | Claim & Locking Strategy | Default Wakeup Transport | Native Row-Level TTL |
| :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL** | `asyncpg`, `psycopg` (async & sync), `psqlpy` | Single-statement `UPDATE ... RETURNING` with `FOR UPDATE SKIP LOCKED` | `notify_queue` | Worker `terminal_ttl` sweep |
| **CockroachDB** | `cockroach_asyncpg`, `cockroach_psycopg` (async & sync) | `UPDATE ... RETURNING` with `FOR UPDATE SKIP LOCKED` + conflict retry | `polling` | Native `ttl_expiration_expression` |
| **Google Cloud Spanner** | `spanner` | Optimistic read-write transaction with conflict retry | `polling` | Native `ROW DELETION POLICY` |
| **Oracle Database** | `oracledb` (async & sync) | `FOR UPDATE SKIP LOCKED` cursor claim | `polling` (`aq` / `txeventq` optional) | Worker `terminal_ttl` sweep |
| **DuckDB** | `duckdb` | Atomic statement claim | `poll_queue` | Worker `terminal_ttl` sweep |
| **SQLite** | `aiosqlite`, `sqlite`, `adbc` (`dialect="sqlite"`) | Single-statement `UPDATE ... RETURNING` | `polling` | Worker `terminal_ttl` sweep |
| **MySQL / MariaDB** | `asyncmy`, `aiomysql`, `pymysql`, `mysqlconnector` (async & sync) | `FOR UPDATE SKIP LOCKED` batch claim | `polling` | Worker `terminal_ttl` sweep |
| **Microsoft SQL Server** | `pymssql`, `mssql_python`, `arrow_odbc` | `UPDLOCK, READPAST, ROWLOCK` batch claim | `polling` | Worker `terminal_ttl` sweep |

## Schema Management & Migrations

The SQLSpec queue schema provisions four tables across unversioned schema reconciliation (`migration_mode="reconcile"`, the default in `sqlspec>=0.72.0`), packaged versioned migrations (`0001_create_queue_tasks`), and `await backend.create_schema()`:

1. **`queue_task`** (override via `queue_table_name`): Active and terminal task records, payload JSON, lease timestamps, and retry fences.
2. **`queue_task_event_history`** (override via `event_history_table_name`): Durable lifecycle and custom progress events when `QueueEventsConfig(history=EventHistoryConfig(...))` is active.
3. **`queue_maintenance`** (override via `maintenance_table_name`): Distributed maintenance lease coordination rows.
4. **`queue_task_reservation`** (override via `task_reservation_table_name`): Permanent `unique_until="forever"` identity reservations (`TaskReservation`).

### Schema Reconciliation vs. Versioned Migrations (`migration_mode`)

`SQLSpecBackendConfig.migration_mode` selects how `SQLSpecBackendConfig.configure_migrations()` registers both `configure_queue_migration_extension` (`"litestar_queues"`) and `configure_events_migration_extension` (`"events"`):

- **`migration_mode="reconcile"` (default, `sqlspec>=0.72.0`)**: Registers queue and durable event-queue stores in `commands.extension_reconcilers`. Every `upgrade("head")` idempotently creates missing enabled tables, adds missing nullable columns (such as `expires_at` on `queue_task` or declarative `EventHistoryExtraColumn` columns on `queue_task_event_history`), repairs dropped companion indexes via catalog inspection (`metadata_has_index`), and converges CockroachDB `terminal_ttl` policies without inserting `ext_litestar_queues_*` rows into `ddl_migrations`.
- **`migration_mode="versioned"`**: Registers the packaged migration directory (`0001_create_queue_tasks`) in `commands.runner.extension_migrations` and records `ext_litestar_queues_0001` in `ddl_migrations`.
- **Strict Additive Safety & Base Downgrade**: Reconcile mode runs `ensure_schema_sync` / `ensure_schema_async` with `strict=True`, raising `sqlspec.exceptions.MigrationError` before executing DDL whenever an existing table omits required defaultless `NOT NULL` or constrained columns, narrows nullability, or introduces incompatible column type drift. Running `downgrade("base")` invokes `reconcile.downgrade(context)` to drop enabled queue tables in reverse dependency order (`queue_task_reservation`, `queue_maintenance`, `queue_task_event_history`, `queue_task`).
- **Supported & Refused Reconcile Adapters**: Supported across SQLite (`aiosqlite`, `sqlite`, `adbc` with `dialect="sqlite"`), PostgreSQL (`asyncpg`, `psycopg`, `psqlpy`), CockroachDB (`cockroach_asyncpg`, `cockroach_psycopg`), MySQL and MariaDB (`asyncmy`, `aiomysql`, `pymysql`, `mysqlconnector`), Oracle Database (`oracledb`), DuckDB (`duckdb`), Microsoft SQL Server (`pymssql`, `mssql_python`, `arrow_odbc` targeting `mssql` / `tsql`), and third-party adapters declaring an explicit `queue_store_class`. Refused with `MigrationError` on Google Cloud Spanner (`spanner`, which applies administrative DDL via `await backend.create_schema()` / `Database.update_ddl`), IBM Db2 (`db2`), all non-`sqlite` `adbc` dialects (such as FlightSQL, PostgreSQL, DuckDB, or Snowflake over ADBC), and all non-`mssql` `arrow_odbc` dialects (including `"db2"`).
- **Programmatic `create_schema()` Parity**: Calling `await backend.create_schema()` in tests or bootstrap scripts reconciles enabled queue, event-history, maintenance, reservation, and durable wakeup tables when `migration_mode="reconcile"` while keeping idempotent `CREATE` DDL for `migration_mode="versioned"` and Google Cloud Spanner.
- **Adopter-Managed Schemas (`manage_schema=False`)**: Set `manage_schema=False` when external migration pipelines own all DDL. When `manage_schema=False`, `litestar-queues` removes registered queue and event-queue migrations, executes zero `CREATE`, `ALTER`, or `DROP` statements, and supports custom column mappings via `column_map` and `native_json_columns`.

## Native Row-Level Terminal TTL & Worker Sweeps

Setting `SQLSpecBackendConfig(terminal_ttl=86_400.0)` configures retention for finished records (`completed`, `failed`, `cancelled`, `expired`) in two complementary ways:

1. **Database-Native Row Expiration (CockroachDB & Google Cloud Spanner)**:
   - **CockroachDB**: `create_statements()`, versioned migration `0001_create_queue_tasks`, and `migration_mode="reconcile"` (`ALTER TABLE ... SET (ttl = 'on', ...)` and `ALTER TABLE ... RESET (ttl)` when cleared) manage a `ttl_expiration_expression` storage parameter computed from `completed_at + INTERVAL '<N> seconds'` so only terminal rows with a non-null `completed_at` expire automatically in the background.
   - **Google Cloud Spanner**: `create_statements()` attaches `ROW DELETION POLICY (OLDER_THAN(completed_at, INTERVAL <N> DAY))` (rounding `terminal_ttl` up to whole days) because Spanner only deletes rows where `completed_at IS NOT NULL`.
2. **In-Worker Periodic Cleanup (All SQL Dialects)**:
   - When `WorkerConfig.terminal_retention` is left as `None`, workers automatically inherit `SQLSpecBackendConfig.terminal_ttl` and run periodic `cleanup_terminal()` sweeps every `WorkerConfig.terminal_check_interval` (`300.0s`).

## Worker Wakeup Transports

Configure [`SQLSpecWorkerWakeupConfig`](../reference/backends.md#litestar_queues.backends.sqlspec.SQLSpecWorkerWakeupConfig) on `SQLSpecBackendConfig.worker_wakeups` to control how workers learn about newly enqueued tasks:

| Transport | Supported Dialects | Durable Queue Table Required | Behavior |
| :--- | :--- | :--- | :--- |
| `notify_queue` | PostgreSQL (`asyncpg`, `psycopg`, `psqlpy`) | Yes (`sqlspec_event_queue`) | Default on PostgreSQL. Combines instant PostgreSQL `LISTEN/NOTIFY` push wakeups with a durable event-queue table fallback across reconnects |
| `notify` | PostgreSQL (`asyncpg`, `psycopg`, `psqlpy`) | No | Transient PostgreSQL `LISTEN/NOTIFY` push wakeups without querying or provisioning `sqlspec_event_queue` (ideal when `manage_schema=False`) |
| `poll_queue` | All SQLSpec dialects (default on `duckdb`) | Yes (`sqlspec_event_queue`) | Polls the SQLSpec durable event-queue table for wakeup hints |
| `aq` / `txeventq` | Oracle (`oracledb`) | Oracle AQ / TxEventQ queues | Native Oracle Advanced Queuing / Transactional Event Queues wakeup delivery |
| `polling` | All SQLSpec dialects | No | Disables event-channel wakeups; workers rely on adaptive interval polling (`WorkerConfig.poll_interval` .. `poll_backoff_max`) |

Passing `worker_wakeups=None` on `SQLSpecBackendConfig` is equivalent to `SQLSpecWorkerWakeupConfig(transport="polling")`. When `notify_queue` or `poll_queue` is selected, `SQLSpecQueueBackend.open()` validates at startup that the event-queue table exists and raises a descriptive `QueueConfigurationError` if migrations have not been applied.

## Dedicated Heartbeat Connection Pool

Under heavy concurrency, long-running query transactions can contend with periodic worker heartbeat writes. Pass a separate `SQLSpecStoreConfig` with a small dedicated pool to `SQLSpecBackendConfig.heartbeat_pool_config` to isolate `touch_heartbeats()` traffic from normal enqueue and claim queries:

```python
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig

main_pool = AsyncpgConfig(connection_config={"dsn": "postgresql://app:secret@db:5432/app", "min_size": 4, "max_size": 20})
heartbeat_pool = AsyncpgConfig(connection_config={"dsn": "postgresql://app:secret@db:5432/app", "min_size": 1, "max_size": 2})

backend_config = SQLSpecBackendConfig(
    sqlspec_config=main_pool,
    heartbeat_pool_config=heartbeat_pool,
)
```

## Third-Party SQLSpec Adapter & Store Resolution

When using an external or custom SQLSpec adapter package outside `sqlspec.adapters.*`, `litestar-queues` resolves the adapter name and store classes through class attributes or `sqlspec.utils.adapters` helpers (`resolve_adapter_name` and `resolve_extension_store_class`) without requiring changes to internal adapter registries:

- **`adapter_name: ClassVar[str]`**: Declares the canonical adapter identifier on the custom SQLSpec config class (or resolves it from the base class MRO).
- **`queue_store_class: ClassVar[type[SQLSpecQueueStore]]`**: Points `create_queue_store()` directly to your custom `SQLSpecQueueStore` subclass.
- **`events_store_class: ClassVar[type[BaseEventQueueStore]]`**: Points SQLSpec's durable event channel to your custom event-queue store class when using `notify_queue` or `poll_queue`.

```python title="docs/examples/backends/sqlspec_third_party_adapter.py"
--8<-- "docs/examples/backends/sqlspec_third_party_adapter.py"
```

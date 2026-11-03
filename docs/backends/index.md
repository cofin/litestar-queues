# Storage Backends Overview

A **queue backend** (`BaseQueueBackend`) is the authoritative storage layer for task records (`QueuedTaskRecord`), fenced claims and heartbeats (`expected_retry_count`), permanent uniqueness reservations (`TaskReservation`), distributed maintenance leases, and durable event history (`QueueEventLog`).

--8<-- "tools/docs/includes/diagram-wakeup-transports.html"

## Backend Capability Matrix

| Storage Backend | Persistence Scope | Typical Pickup Latency | Default Wakeup Transport | Native Terminal TTL | Durable Event History | Distributed Maintenance |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Ephemeral SQLite (`"ephemeral"`)** | Cross-process temp file (default) | Polling (`~100 ms` base) | Interval polling | In-worker `terminal_ttl` sweep | Yes | Yes |
| **In-Memory (`"memory"`)** | Single-process RAM | `< 1 ms` | In-process `asyncio.Event` | In-worker `terminal_retention` sweep | Yes (in-memory) | Yes (in-process) |
| **SQLSpec — PostgreSQL (`asyncpg`, `psycopg`, `psqlpy`)** | Relational database | `5.5 ms` (`asyncpg`), `9.5 ms` (`psycopg`) | `notify_queue` (or `notify`) | In-worker `terminal_ttl` sweep | Yes | Yes |
| **SQLSpec — DuckDB (`duckdb`)** | Embedded analytical file | `~15 ms` | `poll_queue` | In-worker `terminal_ttl` sweep | Yes | Yes |
| **SQLSpec — Oracle (`oracledb`)** | Relational database | Sub-20 ms with AQ/TxEventQ | `polling` (or `aq` / `txeventq`) | In-worker `terminal_ttl` sweep | Yes | Yes |
| **SQLSpec — CockroachDB (`cockroach_asyncpg`, `cockroach_psycopg`)** | Distributed SQL database | Adaptive polling | `polling` | Native `ttl_expiration_expression` + worker sweep | Yes | Yes |
| **SQLSpec — Google Cloud Spanner (`spanner`)** | Distributed SQL database | Adaptive polling | `polling` | Native `ROW DELETION POLICY` + worker sweep | Yes | Yes |
| **SQLSpec — SQLite (`aiosqlite`, `sqlite`, `adbc`), MySQL/MariaDB (`asyncmy`, `aiomysql`, `pymysql`, `mysqlconnector`), SQL Server (`pymssql`, `mssql_python`, `arrow_odbc`)** | Relational database | Adaptive polling | `polling` | In-worker `terminal_ttl` sweep | Yes | Yes |
| **Advanced Alchemy (`SQLAlchemyBackendConfig`)** | SQLAlchemy async ORM | `~8 ms` (PostgreSQL `LISTEN/NOTIFY`) | `LISTEN/NOTIFY` on `postgresql+asyncpg` and `postgresql+psycopg` when `worker_wakeups=True` | In-worker `terminal_retention` sweep | Yes | Yes |
| **Redis (`RedisBackendConfig`)** | Redis server / cluster | `4.3 ms` | Redis Pub/Sub (`worker_wakeups=True`) | Native `PEXPIRE` + `{prefix}:terminal_meta` pruning | Yes (Redis Streams + Hashes) | Yes |
| **Valkey (`ValkeyBackendConfig`)** | Valkey server / cluster | `4.6 ms` | Valkey Pub/Sub (`worker_wakeups=True`) | Native `PEXPIRE` + `{prefix}:terminal_meta` pruning | Yes (Valkey Streams + Hashes) | Yes |

## Choosing a Storage Backend

<div class="grid cards" markdown>

-   :material-database-sync:{ .lg .middle } **SQLSpec Backend**

    ---

    Driver-first SQL storage across 11 database dialects with `UPDATE ... RETURNING` batch claiming, durable `notify_queue` wakeups, native CockroachDB/Spanner row-level TTL, and third-party adapter hooks.

    [:octicons-arrow-right-24: SQLSpec Guide](sqlspec.md)

-   :material-layers-triple-outline:{ .lg .middle } **Advanced Alchemy Backend**

    ---

    Integrate queue tables directly into an existing SQLAlchemy 2.0 declarative base and Alembic migration pipeline using composable model mixins.

    [:octicons-arrow-right-24: Advanced Alchemy Guide](advanced-alchemy.md)

-   :material-lightning-bolt:{ .lg .middle } **Redis & Valkey Backends**

    ---

    Sub-5ms task pickup using atomic Lua claim scripts, Pub/Sub worker wakeups, and native `PEXPIRE` terminal record expiration with secondary-index pruning.

    [:octicons-arrow-right-24: Redis & Valkey Guide](redis-valkey.md)

-   :material-swap-horizontal-bold:{ .lg .middle } **Backend Migration Guide**

    ---

    Move from default `"ephemeral"` or `"memory"` storage to persistent databases, adopt canonical SQLSpec wakeup transport names, and provision forever-uniqueness reservation tables.

    [:octicons-arrow-right-24: Migration Guide](migration.md)

</div>

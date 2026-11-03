# Core Mental Model

`litestar-queues` is built around a clean separation of concerns between **authoritative task storage**, **worker wakeups**, **execution dispatch**, and **realtime task events**. Understanding these primitives prevents common distributed-queue pitfalls.

--8<-- "tools/docs/includes/diagram-storage-vs-execution.html"

## Key Architectural Primitives

### Task Definition & Record

A task function decorated with `@task` becomes a `TaskDefinition`. When you call `await queue_service.enqueue(my_task, *args, **kwargs)`, `QueueService` creates a `QueuedTaskRecord` representing that execution unit (status, serialized arguments, priority, retry counters, lease timestamps, and result or error metadata).

### Queue Backend (Authoritative Source of Truth)

The **queue backend** (`BaseQueueBackend`) is the single **source of truth** for task state, arguments, uniqueness reservations (`TaskReservation`), fenced leases (`expected_retry_count`, `expected_execution_ref`), and results:

- **`EphemeralQueueBackend`** (`backend="ephemeral"`, default): Zero-config temporary WAL SQLite database shared across local web and CLI-supervised worker processes.
- **`InMemoryQueueBackend`** (`backend="memory"`): Fast, single-process memory backend for unit tests and `placement="asgi"`.
- **`SQLSpecQueueBackend`** (`SQLSpecBackendConfig`): Production SQL storage across 11 database dialects (SQLite, PostgreSQL, CockroachDB, MySQL, MariaDB, Oracle, DuckDB, Spanner) plus third-party SQLSpec adapters.
- **`SQLAlchemyBackend`** (`SQLAlchemyBackendConfig`): Production SQLAlchemy 2.0 async storage integrated with Advanced Alchemy.
- **`RedisQueueBackend` & `ValkeyQueueBackend`**: High-throughput atomic Lua-scripted storage with native key expiration (`terminal_ttl`).

### Execution Backend (ID-Only Dispatch Transport)

The **execution backend** (`BaseExecutionBackend`) controls *how* task execution is triggered once a `QueuedTaskRecord` is persisted in the queue backend:

- **`LocalExecutionBackend`** (`execution_backend="local"`, default): Workers claim and execute tasks directly from the queue backend.
- **`ImmediateExecutionBackend`** (`execution_backend="immediate"`): Runs tasks inline inside `enqueue()` for deterministic unit tests without starting a worker loop.
- **Serverless Push & Batch** (`CloudTasksExecutionConfig`, `CloudRunExecutionConfig`): Dispatches an OIDC-authenticated HTTP webhook (`/_litestar-queues/cloud-tasks`) or triggers a one-shot Cloud Run Job container (`litestar queues run-task`).
- **Broker Transports** (`PubSubExecutionConfig`, `SQSExecutionConfig`, `RabbitMQExecutionConfig`, `KafkaExecutionConfig`): Publishes an ID-only message to an external broker consumed by `litestar queues run-consumer`.

!!! important "Wire Contract: ID-Only Execution Envelopes"
    External execution transports never serialize task arguments, metadata, or retry state onto the wire. Every external transport carries only `{"version": 1, "task_id": "<uuid>"}`. The receiving worker, consumer, or webhook claims the authoritative `QueuedTaskRecord` from the queue backend using epoch-fenced compare-and-swap (`expected_retry_count`, `expected_execution_ref`).

### Worker Discovery & Placement

A `Worker` claims ready tasks from configured queues, maintains fenced heartbeats (`WorkerHeartbeatManager`), runs the task callable (with full dependency injection and `TaskExecutionContext`), and settles the record as `completed` or `failed`.

- **Automatic Worker Discovery**: When `WorkerConfig.queues` is omitted, worker discovery automatically subscribes to every queue referenced by registered tasks in `QueueConfig.tasks` (falling back to `("default",)` when no tasks are registered).
- **Placements**: `WorkerConfig(placement="server")` supervises a background worker subprocess during `litestar run`; `placement="asgi"` runs the worker inside the ASGI event loop; `placement="external"` disables in-app workers so web pods remain stateless while dedicated `litestar queues run` processes execute tasks.

### Worker Wakeup Hints vs Task Events

`litestar-queues` keeps internal **worker wakeup** signals completely separate from user-facing **task event** streams:

--8<-- "tools/docs/includes/diagram-wakeup-transports.html"

| Layer | Primitive | Responsibility |
| :--- | :--- | :--- |
| **Queue Backend** | `BaseQueueBackend` | Authoritative source of truth for `QueuedTaskRecord`, fenced claims, heartbeats, schedules, and `TaskReservation`. |
| **Worker Wakeup** | `wakeup_transport` | Low-latency control hint (`notify_queue`, `notify`, `aq`, `txeventq`, `poll_queue`, Redis/Valkey Pub/Sub) telling idle workers to poll immediately. Losing a wakeup hint never loses a task because adaptive polling backs every transport. |
| **Execution Backend** | `BaseExecutionBackend` | Dispatches ID-only `{"version": 1, "task_id": "<uuid>"}` execution triggers to local workers, Cloud Tasks, Cloud Run Jobs, or message brokers. |
| **Task Event** | `QueueEvent` / `QueueEventsConfig` | Structured lifecycle (`enqueued`, `started`, `completed`, `failed`, `retrying`, `cancelled`, `expired`) and in-task (`progress`, `log`, `event`, `beat`) payloads persisted to `QueueEventLog` and streamed over SSE/WebSockets. |

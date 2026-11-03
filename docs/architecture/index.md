# Interactive Visual Architecture

This page provides an interactive visual breakdown of the six core architectural subsystems in `litestar-queues`. Use the tab switchers inside each diagram to inspect state transitions, wire contracts, fencing invariants, and deployment topologies.

## Decoupled Storage vs ID-Only Execution Transports

`BaseQueueBackend` is the sole authority for task state (`QueuedTaskRecord`), uniqueness locks (`TaskReservation`), and results. External execution transports (`CloudTasksExecutionBackend`, `CloudRunExecutionBackend`, and message broker backends) carry only `{"version": 1, "task_id": "<uuid>"}` on the wire.

--8<-- "tools/docs/includes/diagram-storage-vs-execution.html"

| Invariant | Authoritative Component | Enforcement Mechanism |
| :--- | :--- | :--- |
| **Single Source of Truth** | `BaseQueueBackend` | All task arguments, status transitions, retry counters, and results live exclusively in storage. |
| **ID-Only Wire Payload** | `BaseExecutionBackend` (`TaskDispatchEnvelope`) | Wire payload is strictly `{"version": 1, "task_id": "<uuid>"}` with zero task arguments or secrets. |
| **Epoch & Lease Fencing** | `heartbeat_tasks()`, `complete_task()`, `fail_task()` | Every mutation checks `status == "running"`, `retry_count == expected_retry_count`, and `execution_ref == expected_execution_ref`. |

## Worker Placement Topologies

Switch between the four worker execution topologies to see how `WorkerConfig.placement` adapts from single-command local development to horizontally scaled worker fleets and serverless scale-to-zero deployments.

--8<-- "tools/docs/includes/diagram-worker-placements.html"

| Placement Mode | Configuration | Process Boundary | Best For |
| :--- | :--- | :--- | :--- |
| **Server Supervised** | `WorkerConfig(placement="server")` | Child OS process managed by `ServerWorkerSupervisor` during `litestar run` | Local development and single-node deployments. |
| **In-Process ASGI** | `WorkerConfig(placement="asgi")` | Coroutine inside the web server's `asyncio` event loop | Unit tests, `TestClient`, and lightweight single-process apps. |
| **Dedicated External** | `WorkerConfig(placement="external")` | Standalone `litestar queues run` or `run-consumer` pods | Multi-replica production web tiers with independent worker scaling. |
| **Serverless Scale-to-Zero** | `placement="external"` + Cloud Tasks / Cloud Run | On-demand OIDC webhooks or one-shot Cloud Run Job containers | Workloads that scale to zero compute cost when queues are idle. |

## Task State Machine, Clocks & Uniqueness Windows

Explore how tasks transition from `pending` or `scheduled` through fenced `running` leases into terminal states, how the three independent clocks operate without interfering, and how `unique_until` windows prevent duplicate work.

--8<-- "tools/docs/includes/diagram-task-lifecycle.html"

| Clock / Mechanism | Configured On | When It Ticks | Terminal Outcome |
| :--- | :--- | :--- | :--- |
| **Queue Expiration** (`expires_in` / `expires_at`) | `@task` or `enqueue()` | Only while `status` is `pending` or `scheduled` | Transitions unstarted task to `status="expired"`. Never interrupts `running` tasks. |
| **Execution Timeout** (`timeout`) | `@task` or `enqueue()` | Per execution attempt while `status == "running"` | Cancels attempt and either schedules a retry or transitions to `status="failed"`. |
| **Terminal Retention** (`terminal_ttl` / `terminal_retention`) | `*BackendConfig` or `QueueMaintenanceConfig` | After task reaches `completed`, `failed`, `cancelled`, or `expired` | Deletes aged terminal task records while preserving `unique_until="forever"` reservations. |

## Serverless Scale-to-Zero & Six-Phase Maintenance

In serverless topologies, `QueueConfig` can register a default push lane (`CloudTasksExecutionConfig` for interactive tasks up to 30 minutes) alongside named batch lanes (`execution_backends=(CloudRunExecutionConfig(...),)` for multi-hour Cloud Run Jobs), backed by the 6-phase `QueueMaintenanceService`.

--8<-- "tools/docs/includes/diagram-serverless-scale-to-zero.html"

## Realtime Event Pipeline & Cursor Replay

Running tasks emit structured progress, log, and custom events through `TaskExecutionContext`. When `EventHistoryConfig(enabled=True)` is active, `QueueEventPublisher` commits durable `QueueEventLogRecord` rows before broadcasting live frames to `ChannelsQueueEventSink`, guaranteeing gap-free cursor replay across reconnects and planned `max_stream_duration` rollovers.

--8<-- "tools/docs/includes/diagram-events-and-streams.html"

## Four-Layer Transport Matrix

`litestar-queues` separates storage, worker wakeup hints, execution dispatch, and browser event streaming into orthogonal layers that can be mixed and matched independently:

--8<-- "tools/docs/includes/diagram-wakeup-transports.html"

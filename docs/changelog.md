# Changelog

Notable changes to Litestar Queues are recorded here. Entries focus on user-visible behavior, public API changes, and operational fixes.

## 0.14.0 - 2026-11-03

**Added:**

- Native terminal task TTL (`RedisBackendConfig.terminal_ttl` and `ValkeyBackendConfig.terminal_ttl`) for Redis and Valkey backends, applying `PEXPIRE` atomically when tasks enter a terminal state (`completed`, `failed`, `cancelled`, or `expired`) while opportunistically pruning secondary index memberships (`:tasks`, `:status:*`, `:keys`, `:maintenance:terminal`, and `:queues`) as hashes expire ([#187](https://github.com/cofin/litestar-queues/pull/187)). See [Redis & Valkey Backends](backends/redis-valkey.md).
- Native SQL row-level terminal TTL (`SQLSpecBackendConfig.terminal_ttl`) in the packaged `0001_create_queue_tasks` migration and `create_schema()` DDL for databases that support filtered row expiration: CockroachDB `ttl_expiration_expression` over `completed_at` and terminal `status` values, and Google Cloud Spanner `ROW DELETION POLICY (OLDER_THAN(completed_at, INTERVAL <N> DAY))` ([#187](https://github.com/cofin/litestar-queues/pull/187)). See [SQLSpec Backend](backends/sqlspec.md).
- In-worker periodic terminal record cleanup via `WorkerConfig.terminal_retention` (inheriting the queue backend's `terminal_ttl` when left as `None`) and `WorkerConfig.terminal_check_interval`, running a lock-coordinated `backend.cleanup_terminal()` sweep inside the worker loop ([#187](https://github.com/cofin/litestar-queues/pull/187)). See [Workers & Placements](learn/workers.md) and [Bounded Maintenance](execution/maintenance.md).
- Third-party SQLSpec database adapter configuration support outside the `sqlspec.adapters` package, resolving adapter names and queue stores through an explicit `adapter_name: ClassVar[str]` or `queue_store_class: ClassVar[type[SQLSpecQueueStore]]` attribute before falling back to module MRO inspection ([#186](https://github.com/cofin/litestar-queues/issues/186), [#187](https://github.com/cofin/litestar-queues/pull/187)).
- Custom events queue store resolution for durable worker wakeups via an explicit `events_store_class: ClassVar[type[BaseEventQueueStore]]` attribute on SQLSpec configurations before falling back to upstream `sqlspec.utils.adapters.resolve_extension_store_class` or adapter package inspection ([#186](https://github.com/cofin/litestar-queues/issues/186), [#187](https://github.com/cofin/litestar-queues/pull/187)).

**Changed:**

- **Breaking:** Increased the minimum supported Python version from 3.10 to 3.11 (`requires-python = ">=3.11"`), added Python 3.15 support, and removed Python 3.10 compatibility shims ([#187](https://github.com/cofin/litestar-queues/pull/187)).

## 0.13.0 - 2026-11-03

**Added:**

- Multi-backend execution routing via `QueueConfig.execution_backends` and `QueueService.execution_backends`, allowing tasks to declare `@task(execution_backend="cloudrun")` or `queue_service.enqueue(..., execution_backend="cloudrun")` alongside a default execution backend ([#175](https://github.com/cofin/litestar-queues/issues/175), [#185](https://github.com/cofin/litestar-queues/pull/185)). See [Google Cloud Run Jobs](execution/cloud-run.md) and [Google Cloud Tasks](execution/cloud-tasks.md).
- On-enqueue Cloud Run Jobs execution via `CloudRunExecutionBackend` (`CloudRunExecutionConfig.schedules_on_enqueue`) and single-task execution via `litestar queues run-task --task-id <uuid>`, `QUEUES_TASK_ID`, or `LITESTAR_QUEUES_TASK_ID` ([#176](https://github.com/cofin/litestar-queues/issues/176), [#185](https://github.com/cofin/litestar-queues/pull/185)). See [Google Cloud Run Jobs](execution/cloud-run.md).
- `CloudTasksOIDCGuard`, `CloudTasksExecutionConfig.mount_route`, and `CloudTasksExecutionConfig.opt` for custom route mounting, middleware exclusion, and Google OIDC token verification on Cloud Tasks push callbacks ([#177](https://github.com/cofin/litestar-queues/issues/177), [#185](https://github.com/cofin/litestar-queues/pull/185)). See [Google Cloud Tasks](execution/cloud-tasks.md).
- `QueueMaintenanceConfig(mount_route=True, route_path=..., guards=...)` to mount a guarded HTTP maintenance endpoint (`POST /_litestar-queues/maintenance`) for Cloud Scheduler and scale-to-zero deployments, plus `"expire"` and `"schedules"` maintenance phases ([#172](https://github.com/cofin/litestar-queues/issues/172), [#173](https://github.com/cofin/litestar-queues/issues/173), [#174](https://github.com/cofin/litestar-queues/issues/174), [#185](https://github.com/cofin/litestar-queues/pull/185)). See [Bounded Maintenance](execution/maintenance.md).
- `EventStreamConfig(max_stream_duration=..., retry_ms=...)` for planned SSE and WebSocket stream rollover ahead of serverless request timeouts, closing streams cleanly before proxy deadlines and emitting SSE `retry:` frames so clients reconnect with `Last-Event-ID` ([#179](https://github.com/cofin/litestar-queues/issues/179), [#185](https://github.com/cofin/litestar-queues/pull/185)). See [Realtime Streams (SSE & WebSocket)](events/streams.md).
- Public `litestar_queues.cli` facade exporting `queues_group`, `register`, `run_command`, `run_consumer_command`, `run_task_command`, `run_maintenance_command`, `status_command`, and `scheduler_health_command` ([#185](https://github.com/cofin/litestar-queues/pull/185)). See [CLI Reference](reference/cli.md).
- `WorkerConfig.run_app_lifespan` to enter `async with app.lifespan():` inside standalone `Worker` and `litestar queues` CLI invocations (`run`, `run-consumer`, `run-task`, and `run-maintenance`) when no ASGI server manages the application lifespan ([#184](https://github.com/cofin/litestar-queues/issues/184), [#185](https://github.com/cofin/litestar-queues/pull/185)). See [Workers & Placements](learn/workers.md).
- Scale-to-zero connection pool sizing guidance for Cloud Run and serverless database deployments ([#178](https://github.com/cofin/litestar-queues/issues/178), [#185](https://github.com/cofin/litestar-queues/pull/185)). See [Google Cloud Run Jobs](execution/cloud-run.md).

**Changed:**

- **Breaking:** Calling `publish_task_event`, `publish_task_progress`, or `publish_task_log` when no `TaskExecutionContext` is bound is now a silent no-op (`None`) by default instead of raising `QueueConfigurationError`. Pass `strict=True` to require an active task context ([#185](https://github.com/cofin/litestar-queues/pull/185)). See [Task Events Overview](events/index.md).
- Task dependency resolution filters resolved dependencies against the task function's signature before invocation unless the callable accepts `**kwargs`, allowing global `QueueConfig.task_dependency_provider` scopes to yield shared resources without requiring every task signature to declare them ([#185](https://github.com/cofin/litestar-queues/pull/185)). See [Task Dependency Injection](learn/dependency-injection.md).
- `QueueConfig.initialize_schedules` controls startup schedule seeding, and when `WorkerConfig(placement="external")` is paired with an external execution backend and `maintenance=None`, startup emits a `UserWarning` explaining how to configure `QueueMaintenanceConfig` to advance schedules and recover stale tasks ([#174](https://github.com/cofin/litestar-queues/issues/174), [#185](https://github.com/cofin/litestar-queues/pull/185)).

**Fixed:**

- Cloud Tasks push callback redelivery recovers stale `running` claims whose heartbeat age exceeds `stale_after` before attempting `claim_task()`, and returns HTTP `503 Service Unavailable` when a previous attempt is still actively heartbeating so Cloud Tasks retries according to queue backoff ([#170](https://github.com/cofin/litestar-queues/issues/170), [#185](https://github.com/cofin/litestar-queues/pull/185)). See [Google Cloud Tasks](execution/cloud-tasks.md).
- When a task executed by an on-enqueue push backend (`CloudTasksExecutionBackend` or `CloudRunExecutionBackend` with `schedules_on_enqueue=True`) fails and transitions to `pending` or `scheduled` for retry, the execution backend immediately re-dispatches the follow-up delivery with `scheduled_at=retry_at` and persists the new delivery reference ([#171](https://github.com/cofin/litestar-queues/issues/171), [#185](https://github.com/cofin/litestar-queues/pull/185)).
- SSE and WebSocket event stream unsubscribe cleanup shields asynchronous backend teardown with `anyio.move_on_after(5.0, shield=True)` so client disconnects on Python 3.12+ / Granian do not abort pub/sub channel cleanup ([#180](https://github.com/cofin/litestar-queues/issues/180), [#185](https://github.com/cofin/litestar-queues/pull/185)).

## 0.12.0 - 2026-09-14

**Breaking:**

- The Redis backend now requires `redis>=8.1.0`.
- The SQLSpec backend requires `sqlspec>=0.63.0`.
- The SQLSpec backend consolidates packaged migrations into a single revision (`0001_create_queue_tasks`). Setting `manage_schema=False` registers no migration directory or revisions. See [SQLSpec Backend](backends/sqlspec.md).

**Fixed:**

- SQLSpec duplicate handling preserves uniqueness conflicts while propagating foreign-key, check-constraint, and other unrelated database errors.
- SQLSpec claims use locking capabilities reported by the connected database version. Unsupported servers retain conditional task claims; reopening refreshes capabilities.
- Application-defined SQLSpec config subclasses retain the correct sync or async queue store. PostgreSQL subclass wakeups retain the upstream durable polling fallback; default configs retain native notifications.
- Toggling SQLSpec schema management updates migration registration without losing unrelated extension settings or existing migration records.
- SQLSpec mutation outcomes use the configured driver's rowcount reliability while preserving task ownership checks.
- Synchronous Psycopg mutations that return rows persist before the session closes, including enqueue, batch claims, completion, and retries.
- SQLSpec SQL Server (`pymssql`) stores use native transaction management without skipping explicit transaction blocks.
- SQLSpec PostgreSQL migrations use distinct index names within the server's 63-byte limit for long queue table names.
- SQLSpec wakeup transports validate the presence of the events queue table prior to first use, raising a descriptive `QueueConfigurationError` instead of raw database missing-relation errors.

## 0.11.0 - 2026-09-08

**Changed:**

- **Breaking:** Custom `QueueEventLog` providers must implement `publish_event_after_commit()` and `aclose()`. History strictness is configured via `EventHistoryConfig`. See [Durable Event History](events/history.md).
- Maintenance external phase bounds records examined. Time budget exhaustion or reaching the external scan limit reports partial execution (CLI exit code `2`). See [Bounded Maintenance](execution/maintenance.md).
- Recurring tasks inherit configured retries on startup and succession; persisted occurrences retain their budget and backoff until completion. See [Recurring Schedules](learn/schedules.md).

**Fixed:**

- Cloud Tasks delivery repair selects unexpired pending and scheduled records (including records without delivery references), preserves task identity, and uses bounded fair selection with persisted check timestamps. See [Google Cloud Tasks](execution/cloud-tasks.md).
- DuckDB scheduled-reference reservation handles native CAS transaction conflicts gracefully.
- Sparse event history flushes on timer deadlines, enforces commit-before-live delivery ordering, and bounds failure buffering (`EventHistoryConfig.max_pending`). See [Durable Event History](events/history.md) and [Task Events Overview](events/index.md).
- SQLSpec history pagination totals count all matching records before pagination, including empty pages.
- SQLSpec Arrow ODBC history uses transactional bound inserts.
- External event producers unwind partial startup and attempt all cleanup steps, preserving primary exceptions and cancellation. See [Standalone Events](events/standalone.md).
- Added runnable event history example and aligned documentation with supported public query APIs. See [Durable Event History](events/history.md).

## 0.10.0 - 2026-08-30

**Added:**

- Universal actor attachment: `@task(actor=...)` accepts a `QueueEventActor` or zero-argument resolver, `TaskExecutionContext.actor` provides attempt-scoped defaults, and per-call `actor=` overrides are supported across event publishing helpers. Worker lifecycle events inherit the context actor.
- Backend-neutral event history `extra=` query surface across all backends. **Breaking:** `EventHistoryExtraColumn` and `validate_event_history_extra_columns` moved to `litestar_queues.events`, and extra columns are declared on `EventHistoryConfig` via `extra_columns`. Querying undeclared extra keys raises `QueueConfigurationError`.
- Attempt-scoped task dependency injection via `QueueConfig.task_dependency_provider`, providing an async context manager scope per attempt across all execution outcomes. See [Task Dependency Injection](learn/dependency-injection.md).
- Remote execution cancellation with provider-first ordering: active provider execution (Cloud Run Jobs / Cloud Tasks) is cancelled before writing durable cancellation state. See [Failures, Retries & Cancellation](learn/failures-and-cancellation.md).
- Cross-backend event query protocol (`QueueEventQuery`) supporting multi-field filtering, configurable ordering, and offset pagination.
- Task lifecycle stage summaries (`QueueEventStageSummary` / `summarize_stages`) reporting event counts, duration, and terminal statuses per stage.
- Ordered event retention engine (`QueueEventRetentionRule`) with prioritized rules under a shared deletion budget. See [Durable Event History](events/history.md) and [Bounded Maintenance](execution/maintenance.md).

**Changed:**

- **Breaking:** The SQLSpec `litestar_queues` extension setting for the primary queue table is renamed from `table_name` to `queue_table_name`.

## 0.9.0 - 2026-08-14

**Added:**

- Persisted event dimensions (entity, scope) and context APIs for query and retention rules.
- Apache Kafka is available as an optional execution backend through `litestar-queues[kafka]`. Dispatch publishes only the task UUID, with the attempt fence travelling as a message header, and `litestar queues run-consumer --backend kafka` runs a manual-commit consumer group against the authoritative queue backend. See [Apache Kafka](execution/kafka.md).
- The event subpackage can be adopted on its own by a runtime that has its own task runner and never starts this package's worker. `bind_task_context` and `bind_beat_sink` are public, so module-level publish helpers resolve against a context that external code binds. See [Standalone Events](events/standalone.md).
- The SQLSpec event-history table accepts adopter-defined columns for scoping dimensions the queue does not model, such as a tenant or project ID, with matching filters on `query_events()`. See [Extending Event History](events/history-extending.md).
- Event history stores the event actor and can filter on it. `QueueEventLogRecord` carries `actor_type` and `actor_id`, and `QueueEventLog.query_events()` filters by `actor_id` and `actor_type`. See [Durable Event History](events/history.md).
- Google Cloud Pub/Sub is available as an optional execution backend through `litestar-queues[pubsub]`. Dispatch publishes only the task UUID and an attempt fence; `litestar queues run-consumer --backend pubsub` processes streaming-pull deliveries against the authoritative queue backend. See [Google Cloud Pub/Sub](execution/pubsub.md).
- Shutdown requeue: `WorkerConfig.requeue_on_shutdown` returns an in-flight attempt to `pending` once its coroutine accepts cancellation, with a per-task `@task(requeue_on_shutdown=...)` override, `QueueService.interrupt_task`, and the `task.interrupted` lifecycle event. See [Workers & Placements](learn/workers.md).
- `WorkerConfig.max_interruptions` (default `3`) bounds how many times one attempt may be requeued by shutdown.
- `WorkerConfig.hard_exit_timeout` (default `10.0` seconds, `None` to disable) bounds a forced shutdown.
- `QueueConfig.stale_requeue_priority` chooses the priority recovered work re-enters with: `"preserve"` (the default), an integer ceiling clamp, or a callable mapping the old priority to the new one. See [Workers & Placements](learn/workers.md).
- `WorkerConfig.queue_concurrency` sets per-worker caps for named queues.
- `WorkerConfig.cancellation_poll_interval`: workers reconcile durable `cancelled` state into running executions, so `cancel_task(..., include_running=True)` interrupts a cooperative task on another worker.
- Service-level cancellation, single and bulk, with task name, queue, kwargs, and metadata filters, plus cooperative in-task cancellation checkpoints. See [Failures, Retries & Cancellation](learn/failures-and-cancellation.md).

**Changed:**

- Claim ordering is fair: ready work is ordered by priority, then `queued_at`, then creation time.
- Queue-scoped statistics are enforced filters rather than advisory hints.

## 0.8.0 - 2026-08-03

**Added:**

- Amazon SQS is available as an optional execution backend (`litestar-queues[sqs]`). Configure it with `SqsExecutionConfig` and run long-polling consumers against the persistent queue backend. See [Amazon SQS](execution/sqs.md).
- `litestar queues run-consumer --backend sqs` starts a continuous broker consumer, with `--max-concurrency` and `--drain-timeout` to bound in-flight deliveries and shutdown.
- Every SQS delivery is fenced to the exact persisted retry generation and dispatch attempt through a private message attribute. `consume_one` accepts `expected_retry_count` and `expected_execution_ref` to express the same fence directly.
- SQS reports `litestar_queues.execution` metrics and supports bounded maintenance delivery repair.

**Breaking changes:**

- Queue backends gained `clear_execution_ref` and `replace_execution_ref`, and `claim_task` gained `expected_retry_count` and `expected_execution_ref`.

**Changed:**

- Reduced steady-state pickup latency on Redis, Valkey, and PostgreSQL, and prepared SQLSpec correlation state during service startup.
- Completion subscription registration and shutdown are race-safe.
- The SQLSpec extra requires `sqlspec>=0.58.0`.

## 0.7.0 - 2026-07-28

**Added:**

- Google Cloud Tasks is available as an optional execution backend (`litestar-queues[cloud-tasks]`). Only the record's ID crosses the network—arguments, metadata, and results are re-read from the queue store by the consumer route at `/_litestar-queues/cloud-tasks`. See [Google Cloud Tasks](execution/cloud-tasks.md).
- Queued work can carry a not-started deadline through `expires_in` or `expires_at`, settling in the terminal `expired` state when passed without being claimed.
- `QueueConfig(namespace=...)` isolates Litestar state keys, DI keys, default route prefixes, event channels, maintenance coordination, Redis/Valkey key prefixes, wakeups, and telemetry under a named namespace. See [Multi-Runtime Namespaces](learn/runtime-namespace.md).
- Bounded maintenance repairs deliveries lost by managed transports.
- Managed transports report `litestar_queues.execution.dispatch`, `litestar_queues.execution.delivery`, and `litestar_queues.execution.repair`.

**Breaking changes:**

- Configure streaming through `EventStreamConfig`, which owns the path, transports, guards, channel authorizer, scopes, heartbeat interval, and replay limit.
- Ephemeral storage is no longer restricted by worker placement when an embedder prepares a private database. Process-local `"memory"` storage remains rejected under `placement="server"`.

**Fixed:**

- Fenced worker fleet coordination locks (`acquire_worker_lock`) and external reconciliation interval checks.
- Lazy-loaded queue adapters so applications only import configured extras.
- Durably retired records referencing unregistered tasks and restricted `TaskExitCode.CANCELLED` to records whose durable status is `cancelled`.

## 0.6.0 - 2026-07-25

**Breaking changes:**

- `WorkerConfig.placement` configures which process owns the worker: `"server"` (default) for one worker per `litestar run` invocation, `"asgi"` for one inside each ASGI process, and `"external"` for standalone worker processes.
- `QueueConfig()` defaults to `queue_backend="ephemeral"` instead of `"memory"`.
- Incompatible storage, execution, and placement combinations are rejected at startup with a descriptive `QueueConfigurationError`.

**Added:**

- Added the ephemeral SQLite queue backend (`EphemeralQueueBackend`) built on standard-library `sqlite3` with WAL and `BEGIN IMMEDIATE` transactions.
- Added server-owned worker startup under `placement="server"`.
- Added `sync_to_thread` to `@task` so synchronous `def` tasks run on a bounded thread pool.

**Changed:**

- `litestar queues run` starts standalone workers alongside existing application configurations.
- Default synchronous-task thread-pool size follows process-usable CPU count plus four, capped at 32.
- Consolidated Prometheus counter and duration histogram naming conventions and shared collectors per registry.

## 0.5.0 - 2026-07-23

**Breaking changes:**

- Consolidated event delivery, realtime streams, and event history under `QueueEventsConfig`.
- Consolidated worker startup and runtime settings in `WorkerConfig`.
- Standardized task submission and durable uniqueness under `TaskRequest` and `TaskReservation`, positive `log_success`, and `max_argument_identity_bytes` with `TaskIdentityTooLargeError`.
- Unified worker-wakeup configuration across Redis, Valkey, Advanced Alchemy, and SQLSpec.
- Standardized default relational table names: `queue_task`, `queue_task_event_history`, `queue_task_reservation`, and `queue_maintenance`.

**Added:**

- Added `litestar queues run-task` for single-record external executor dispatch.
- Added task uniqueness policies with argument-based identities, durable forever reservations, and administrative reset support.
- Added bounded maintenance configuration and `litestar queues run-maintenance`.
- Added adaptive worker polling and backend-native worker wakeups.

## 0.4.0 - 2026-07-21

- Reduced queue hot paths, worker wakeup, and status checks to a single database round trip each.
- Added native `claim_many` batching for the memory and SQLSpec backends.
- Added persistent backend-native notification waits across worker poll intervals.

## 0.3.0 - 2026-07-10

- Overhauled queue backends and added Playwright browser end-to-end coverage for the realtime HTMX examples.

## 0.2.0 - 2026-07-08

- Added SQLSpec queue stores for CockroachDB, Spanner, SQL Server, ADBC SQLite, and PyMySQL.
- Added queue observability instrumentation, durable event history, realtime event streams, cooperative task cancellation, Python 3.14 support, and the Cloud Run deployment guide.

## 0.1.0 - 2026-06-29

- Initial release of `litestar-queues` with five queue backends (in-memory, SQLSpec, Advanced Alchemy, Redis, and Valkey) and two execution backends (local and Google Cloud Run Jobs).

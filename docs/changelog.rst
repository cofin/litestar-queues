=========
Changelog
=========

Notable changes to Litestar Queues are recorded here. Entries focus on
user-visible behavior, public API changes, and important operational fixes. The
project is pre-1.0, so minor releases may make intentional API breaks.

Unreleased
==========

**Added:**

* Universal actor attachment and lifecycle propagation: ``@task(actor=...)``
  accepts a literal ``QueueEventActor`` or zero-argument resolver,
  ``TaskExecutionContext.actor`` provides attempt-scoped defaults, and
  ``publish()``, ``progress()``, ``log()``, ``event()``, and module helpers
  accept per-call ``actor=`` overrides. Worker lifecycle events automatically inherit
  the context actor.
* Backend-neutral event history ``extra=`` query surface: ``extra=`` is promoted
  to the :class:`~litestar_queues.events.QueueEventLog` protocol and supported across
  all six backends (Memory, Ephemeral, Redis, Valkey, Advanced Alchemy, SQLSpec).
  **Breaking:** ``EventHistoryExtraColumn`` and ``validate_event_history_extra_columns``
  are moved from ``litestar_queues.backends.sqlspec`` to ``litestar_queues.events``,
  and extra columns are declared on :class:`~litestar_queues.events.EventHistoryConfig`
  via ``extra_columns``. Querying undeclared extra keys raises ``QueueConfigurationError``.
* ``QueueConfig.task_dependency_provider`` supplies task keyword arguments from
  an attempt-scoped async context manager, so an application's dependency
  container can open a child scope for each attempt and release it on every
  outcome -- success, retry, terminal failure, timeout, cancellation, claim
  loss, and shutdown interruption. Acquisition runs inside the attempt timeout.
  A provider object that exposes ``open()``/``close()`` joins the
  :class:`~litestar_queues.QueueService` lifecycle and its startup rollback.
  ``task_dependency_resolver`` is unchanged for stateless injection; setting
  both raises ``QueueConfigurationError``. See :doc:`usage/dependency-resolver`.
* Remote execution cancellation with provider-first ordering: When cancelling a task,
  Litestar Queues cancels active provider execution (via Cloud Run jobs API or Cloud Tasks deletion)
  before writing durable cancellation state. Durable cancellation fencing and retry guards prevent
  out-of-order execution recovery from resurrecting cancelled work. See
  :doc:`usage/failures-and-cancellation`.

0.9.0 - 2026-08-14
==================

**Added:**

* Persisted event dimensions (entity, scope) and context APIs for query and retention rules (gh-129).
* Apache Kafka is available as an optional execution backend through
  ``litestar-queues[kafka]``. Dispatch publishes only the task UUID, with the
  attempt fence travelling as a message header, and
  ``litestar queues run-consumer --backend kafka`` runs a manual-commit consumer
  group against the authoritative queue backend. See
  :doc:`usage/deployment/kafka`.
* The event subpackage can be adopted on its own, by a runtime that has its own
  task runner and never starts this package's worker.
  :func:`~litestar_queues.events.bind_task_context` and
  :func:`~litestar_queues.events.bind_beat_sink` are public, so the module-level
  publish helpers resolve against a context that external code binds. See
  :doc:`usage/events-standalone`.
* The SQLSpec event-history table accepts adopter-defined columns for scoping
  dimensions the queue does not model, such as a tenant or project id, with
  matching filters on ``list_events()``. See :doc:`usage/event-history-extending`.
* Event history stores the event actor and can filter on it.
  ``QueueEventLogRecord`` carries ``actor_type`` and ``actor_id``, and
  ``QueueEventLog.list_events()`` accepts ``actor_id=`` and ``actor_type=``,
  ANDed with the task filters. The actor's display ``name`` is deliberately not
  stored, because it is mutable text that would go stale against the event it
  was stamped on, so it stays on the live event envelope. **Breaking:** the
  event-history table gains ``actor_type`` and ``actor_id`` columns plus an
  ``(actor_id, occurred_at)`` index, so the schema must be recreated; adopters
  declaring their own event-history columns can no longer use those two names,
  while ``actor`` is no longer reserved. See :doc:`usage/event-history`.
* Google Cloud Pub/Sub is available as an optional execution backend through
  ``litestar-queues[pubsub]``. Dispatch publishes only the task UUID and an
  attempt fence; ``litestar queues run-consumer --backend pubsub`` processes
  streaming-pull deliveries against the authoritative queue backend. The
  official Google emulator backs the integration suite. See
  :doc:`usage/deployment/pubsub`.
* Shutdown requeue: ``WorkerConfig.requeue_on_shutdown`` returns an in-flight
  attempt to ``pending`` once its coroutine accepts cancellation, with a
  per-task ``@task(requeue_on_shutdown=...)`` override,
  :meth:`~litestar_queues.QueueService.interrupt_task`, and the
  ``task.interrupted`` lifecycle event. Every shipped backend now performs the
  requeue behind an owner and generation fence. See :doc:`usage/workers`.
* ``WorkerConfig.max_interruptions`` (default ``3``) bounds how many times one
  attempt may be requeued by shutdown. Interruptions are counted in the record's
  metadata and consume no retry attempt below the cap; at the cap the
  interruption goes through the ordinary retry policy instead, so a task that is
  restarted forever eventually fails.
* ``WorkerConfig.hard_exit_timeout`` (default ``10.0`` seconds, ``None`` to
  disable) bounds a forced shutdown. When the deadline passes, or a third
  termination signal arrives, the process exits with ``128 + signum`` instead of
  hanging. Tasks still alive after ``final_cancel_timeout`` are logged and have
  their heartbeats cleared so stale recovery can reclaim them at once.
* ``QueueConfig.stale_requeue_priority`` chooses the priority recovered work
  re-enters with: ``"preserve"`` (the default), an integer ceiling clamp, or a
  callable mapping the old priority to the new one. **Breaking:** stale-recovered
  work now keeps its original priority instead of being clamped to ``4``. Pass
  ``stale_requeue_priority=4`` to restore the clamp. See
  :ref:`worker-recovery`.
* ``WorkerConfig.queue_concurrency`` sets per-worker caps for named queues.
  These are local limits, not fleet-wide semaphores.
* ``WorkerConfig.cancellation_poll_interval``: workers reconcile durable
  ``cancelled`` state into running executions, so
  ``cancel_task(..., include_running=True)`` interrupts a cooperative task on
  another worker. Worker-control notifications make that pickup prompt on
  notification-capable backends.
* Service-level cancellation, single and bulk, with task name, queue, kwargs,
  and metadata filters, plus cooperative in-task cancellation checkpoints. See
  :doc:`usage/failures-and-cancellation`.

**Changed:**

* Claim ordering is now fair: ready work is ordered by priority, then
  ``queued_at``, then creation time. A retried, requeued, or interrupted record
  re-enters the line at its requeue time instead of jumping ahead of newer work.
  Deployments that relied on retried work being claimed first will see a
  different claim order.
* Queue-scoped statistics are enforced filters rather than advisory hints.
* The guides are reorganized around what a reader is doing. The separate worker
  wakeup and recovery pages are merged into :doc:`usage/workers`, and the
  telemetry metric catalog moves to the reference section.

0.8.0 - 2026-08-03
==================

Amazon SQS joins Google Cloud Tasks as a managed execution transport, and
steady-state pickup latency drops on every persistent backend. Like Cloud Tasks,
SQS carries nothing but a record's id, so arguments, results, and the queue's
schema stay out of the transport entirely.

**Added:**

* Amazon SQS is available as an optional execution backend, installed with
  ``pip install "litestar-queues[sqs]"``. Configure it with
  :class:`SqsExecutionConfig` and run one dispatcher alongside any number of
  long-polling consumers against the same persistent queue backend. SQS is an
  execution transport and never owns queue state: only the task's id is
  published, while arguments, task names, results, retries, schedules, and
  leases stay in queue storage. Standard queues are the default and ``fifo=True``
  selects a FIFO queue. See :doc:`usage/deployment/sqs`.
* ``litestar queues run-consumer --backend sqs`` starts a continuous broker
  consumer, with ``--max-concurrency`` and ``--drain-timeout`` to bound in-flight
  deliveries and shutdown.
* Every SQS delivery is fenced to the exact persisted retry generation and
  dispatch attempt through a private message attribute, so a delivery that
  outlives its record cannot execute a later attempt. ``consume_one`` accepts
  ``expected_retry_count`` and ``expected_execution_ref`` to express the same
  fence directly.
* SQS joins the managed-transport observability and repair surface introduced in
  0.7.0, reporting the same ``litestar_queues.execution`` metrics with the same
  fixed outcome vocabulary, and having its lost deliveries repaired by bounded
  maintenance.

**Breaking changes:**

* Queue backends gained ``clear_execution_ref`` and ``replace_execution_ref``,
  and ``claim_task`` gained ``expected_retry_count`` and
  ``expected_execution_ref``. The shipped backends implement all of them; a
  custom backend only needs them to serve an external transport, and raises
  rather than silently mis-settling a record if it does not.

**Changed:**

* Steady-state pickup is materially faster on every persistent backend. Redis
  and Valkey no longer make extra enqueue and index round trips, PostgreSQL
  performs expiry and ordered claim in a single transactional statement, and
  optional SQLSpec correlation state is prepared during service startup instead
  of on the first task. Fencing, expiration, heartbeat, maintenance, event, and
  persisted-record guarantees are unchanged.
* Completion subscription registration and shutdown are race-safe, so a
  persistent completion reader no longer contends with task execution.
* The SQLSpec extra now requires ``sqlspec>=0.58.0`` to pick up its native
  psycopg hot-path behavior.

0.7.0 - 2026-07-28
==================

Google Cloud Tasks arrives as an execution backend that needs no worker process
anywhere, queued work gains not-started deadlines, and every runtime name the
package owns can be moved under a namespace of your choosing.

**Added:**

* Google Cloud Tasks is available as an optional execution backend, installed
  with ``pip install "litestar-queues[cloud-tasks]"``. A queue configured for it
  keeps no worker process anywhere: Google holds each record's delivery and
  calls a private consumer route when it is due, so every process can scale to
  zero between deliveries. Only the record's id crosses the network — arguments,
  metadata, and results are re-read from the queue store by the consumer.
  See :doc:`usage/deployment/cloud-tasks`.
* The delivery route is registered for you at ``/_litestar-queues/cloud-tasks``
  when the execution backend is Cloud Tasks. It requires either Cloud Run's own
  IAM asserted explicitly or your guards, and never treats a delivery header as
  authentication.
* Queued work can carry a not-started deadline through ``expires_in`` or
  ``expires_at``, and a record that passes it without being claimed settles in
  the terminal ``expired`` state. The deadline is enforced atomically by every
  backend, and ``expired`` is reported through task results, events, metrics,
  CLI status, recurring schedules, and cleanup. It is distinct from user
  cancellation and from a runtime failure.
* ``QueueConfig(namespace=...)`` names the runtime identity the package owns.
  It derives Litestar state, dependency and route registrations, default stream
  paths, event channels, maintenance coordination, Redis and Valkey keys,
  backend wakeups, Cloud Tasks delivery resources, telemetry, and logger
  hierarchies, so two independent queue runtimes can share a process without
  colliding. Explicit component settings stay authoritative, and SQL table
  names, ORM model classes, task names, and queue names are untouched. The
  default namespace preserves every existing identifier.
* Bounded maintenance now repairs deliveries a managed transport has lost. On a
  queue nobody polls, a delivery that disappears would otherwise leave its record
  waiting forever with no error raised. Repair shares the existing external
  phase's budget, so the number of records one pass touches is unchanged.
* Managed transports report ``litestar_queues.execution.dispatch``,
  ``litestar_queues.execution.delivery``, and
  ``litestar_queues.execution.repair``, each with a fixed outcome vocabulary.
  Task ids, delivery names, and API error text never reach a metric, span, or
  event.

**Breaking changes:**

* ``stream_queue_events_hardened``, ``stream_queue_events_sse``, and
  ``build_stream_router`` are now private, with no aliases. They were rendered
  into the API reference but were never re-exported, documented, or shown in an
  example, and the plugin is their only caller. Configure streaming through
  :class:`EventStreamConfig`, which owns the path, transports, guards, channel
  authorizer, scopes, heartbeat interval, and replay limit. ``StreamMetrics``
  stays public, since it describes the stream observability surface.
* Ephemeral storage is no longer restricted by worker placement. Configuring
  ``queue_backend="ephemeral"`` with ``placement="asgi"`` or
  ``placement="external"``, or with ``execution_backend="immediate"``, was
  rejected at startup; the backend already refuses to open when no private
  database has been prepared, so an embedder that creates one itself may now use
  any placement. Process-local ``"memory"`` storage is still rejected under
  ``placement="server"``.

**Fixed:**

* Worker fleet coordination is now actually fenced. ``acquire_worker_lock``
  returned true unconditionally, so stale recovery, expiry sweeps, and external
  reconciliation ran on every worker at once instead of one at a time.
* External reconciliation checks its interval before taking the fleet lock. It
  previously wrote a coordination record on every worker loop iteration and
  discarded it at the interval check.
* Applications no longer import every installed queue adapter at startup. The
  Litestar signature namespace carried the whole public API, which defeated the
  package's lazy imports and charged applications for extras they never selected.
* Queue backends that fail to implement a required method now raise instead of
  silently succeeding. Two of those defaults returned an unpersisted record, so a
  gap dropped execution references rather than reporting itself.
* A record naming a task the running process does not have registered is now
  retired durably. The failure was written while the record was still pending,
  which every persistent backend rejects, so the record stayed pending forever —
  and on a queue with no worker, nothing was coming back for it.
* A consumer whose caller is cancelled no longer reports the record as
  cancelled. It said the record was settled while the task was still running,
  which an HTTP delivery would have acknowledged. ``TaskExitCode.CANCELLED`` now
  means only a record whose durable status is ``cancelled``.

0.6.0 - 2026-07-25
==================

Queue workers now run in their own process, started by the Litestar CLI, instead
of sharing the event loop with request handlers. ``QueueConfig()`` with no
arguments gives you working background execution with nothing to install or
configure.

**Breaking changes:**

* ``WorkerConfig.run_in_app`` is removed with no alias. ``WorkerConfig.placement``
  replaces it and names which process owns the worker: ``"server"`` (the new
  default) for one worker per ``litestar run`` invocation, ``"asgi"`` for one
  inside each ASGI process, and ``"external"`` for nothing automatic. Replace
  ``run_in_app=True`` with ``placement="server"`` or ``placement="asgi"``, and
  ``run_in_app=False`` with ``placement="external"``. There is no silent fallback
  between placements.
* ``QueueConfig()`` now defaults to ``queue_backend="ephemeral"`` instead of
  ``"memory"``. Code that relied on process-local memory storage must ask for it:
  ``QueueConfig(queue_backend="memory", worker=WorkerConfig(placement="asgi"))``,
  or ``execution_backend="immediate"`` with ``placement="external"`` for inline
  execution.
* Storage, execution, and placement combinations that cannot work are rejected at
  startup rather than at first claim. Process-local storage with a separate worker
  process, inline execution under a managed placement, and ephemeral storage
  outside its owning server all raise with a message that names the fix. Starting
  through a raw ASGI server under server placement fails before serving traffic.

**Added:**

* Added an ephemeral SQLite queue backend built on the standard library
  ``sqlite3`` module, using WAL, ``BEGIN IMMEDIATE`` for read-modify-write, and a
  versioned JSON codec with no pickle. The server creates one private temporary
  database per invocation, every process in that invocation shares it, and it is
  removed on shutdown. No broker, no port, no extra dependency.
* Added server-owned worker startup. Under ``placement="server"`` the Litestar CLI
  server lifespan starts exactly one fresh worker process per ``litestar run``. It
  loads the application itself rather than receiving pickled objects, so it needs
  ``--app`` or ``LITESTAR_APP``.
* Added ``sync_to_thread`` to the task decorator. A ``def`` task runs on a bounded
  thread pool instead of blocking the event loop, and ``sync_to_thread`` makes the
  choice explicit.

**Changed:**

* ``litestar queues run`` now runs alongside whatever the application already
  starts, so adding worker processes does not require changing application
  configuration. It refuses only storage it genuinely cannot reach.
* The default synchronous-task thread-pool size now follows the process-usable
  CPU count plus four, capped at 32. Linux cgroup quotas and CPU affinity
  constrain the default; an explicit ``sync_thread_pool_size`` remains exact.
* Selecting one queue backend no longer imports another backend's package, so an
  application with only ``asyncpg`` installed starts cleanly.
* Worker modules are grouped into one ``litestar_queues.worker`` package split by
  responsibility, the SQLSpec store adapters are one flat module each, and the
  ephemeral database lifecycle lives with its backend.

**Fixed:**

* Prometheus counters no longer export with a doubled suffix. The ``.count``
  segment is dropped from the instrument name because ``prometheus_client``
  already appends ``_total`` on export, so metrics no longer appear as
  ``..._count_total``.
* Duration histograms now carry the conventional ``_seconds`` suffix and bucket
  out to 30 minutes. The ``prometheus_client`` default tops out at ten seconds,
  which sent every real task duration into the ``+Inf`` bucket.
* Prometheus collectors are shared per registry, so two observability runtimes
  using the same registry no longer raise ``Duplicated timeseries in
  CollectorRegistry``.

0.5.0 - 2026-07-23
==================

Consolidates the public configuration API. This release does not retain aliases,
deprecations, or compatibility shims for the replaced pre-release surface.

**Breaking changes:**

* Event delivery, realtime streams, and event history now share one
  ``QueueEventsConfig`` group. Capability objects enable their respective
  features, Channels resolution has explicit precedence, and custom delivery sinks
  are additive.
* Worker startup and runtime settings now live in ``WorkerConfig``, which is also
  passed directly to ``Worker``. CLI options override a copied configuration.
* Task submission and durable uniqueness use the clearer ``TaskRequest`` and
  ``TaskReservation`` vocabulary. Successful-task logging uses the positive
  ``log_success`` option, and argument identity limits use
  ``max_argument_identity_bytes`` with ``TaskIdentityTooLargeError``.
* Redis, Valkey, Advanced Alchemy, and SQLSpec worker-wakeup settings now use a
  consistent vocabulary. SQLSpec has one explicit transport path and supports
  disabling wakeups with ``worker_wakeups=None``.
* Dead Cloud Run, scheduling, observability, state-key, and event configuration
  fields were removed. The supported Advanced Alchemy names remain
  ``SQLAlchemyBackend`` and ``SQLAlchemyBackendConfig``.
* The default relational tables are ``queue_task``, ``queue_task_event_history``,
  ``queue_task_reservation``, and ``queue_maintenance``. SQLSpec creates the
  complete schema through migration ``0001``.

**Added:**

* Added ``litestar queues run-task`` for one-record external executor dispatch.
* Added task uniqueness policies with argument-based identities, durable forever
  reservations, and explicit administrative reset support.
* Added bounded maintenance configuration and ``litestar queues run-maintenance``
  for external reconciliation, stale-task recovery, terminal retention, and
  event-history retention.
* Added adaptive worker polling, richer heartbeat and progress lifecycle handling,
  and backend-native worker wakeups where supported.
* Added Litestar signature-namespace coverage for the consolidated public queue,
  worker, event, observability, and backend types.

**Changed:**

* Raised the SQLSpec requirement to 0.56.0, adopted its authoritative DML row
  counts, removed obsolete adapter workarounds, and added ``mssql-python``
  coverage.
* Added Advanced Alchemy psycopg notification wakeups, and stopped package
  observability from duplicating SQLSpec queue-domain telemetry while retaining
  SQL statement telemetry.
* Updated configuration, worker, backend, event, observability, scheduling,
  maintenance, and example documentation for the consolidated API.
* Updated every realtime demo to enable Vite explicitly and opt into allowed
  unauthenticated access, keeping discovery and asset-status output quiet.

**Fixed:**

* Preserved standalone Redis and Valkey example worker settings while allowing the
  CI topology runner to select external placement.
* Trusted the self-signed SQL Server certificate in the ``mssql-python`` test
  adapter, and handled wrapped Advanced Alchemy integrity errors during concurrent
  task reservation.
* Ensured SQLSpec-native queue telemetry is suppressed only when the package
  observability runtime is actually enabled.

0.4.0 - 2026-07-21
==================

**Changed:**

* Reduced queue hot paths, worker wakeup, and status checks to a single database
  round trip each, and removed unreachable code paths across the backends.
* Cut the SQLSpec backend over to the SQLSpec event transport contract.
* Raised the SQLSpec requirement to 0.55.0.

**Added:**

* Added native ``claim_many`` batching for the memory and SQLSpec backends.
* Added backend-native notification waits that persist across worker poll
  intervals instead of being torn down and re-established each cycle.

0.3.0 - 2026-07-10
==================

**Changed:**

* Overhauled the queue backends and the realtime examples.

**Added:**

* Added browser end-to-end coverage for the realtime examples.

0.2.0 - 2026-07-08
==================

**Added:**

* Added SQLSpec queue stores for CockroachDB, Spanner, SQL Server (both the native
  driver and Arrow ODBC), ADBC SQLite, and PyMySQL.
* Added queue observability instrumentation.
* Added backend event history and realtime event streams.
* Added cooperative queue cancellation.
* Added Python 3.14 support.
* Added the Cloud Run deployment guide.

**Changed:**

* Raised the SQLSpec base requirement to 0.52.

**Fixed:**

* Fenced SQL queue lifecycle updates and hardened the worker claim loop.
* Hardened Cloud Run dispatch and the Cloud Run entrypoint.
* Made the Redis and Valkey clients lazy-loaded.
* Registered SQLSpec queue migrations without mutating the caller's configuration.
* Quoted schema-qualified SQLSpec queue tables.

0.1.0 - 2026-06-29
==================

Initial release of the Litestar Queues worker abstraction.

**Added:**

* Added five queue backends: in-memory, SQLSpec, Advanced Alchemy, Redis, and
  Valkey.
* Added two execution backends: local execution and the optional Google Cloud Run
  executor.

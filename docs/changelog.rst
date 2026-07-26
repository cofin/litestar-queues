=========
Changelog
=========

Notable changes to Litestar Queues are recorded here. Entries focus on
user-visible behavior, public API changes, and important operational fixes. The
project is pre-1.0, so minor releases may make intentional API breaks.

Unreleased
==========

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

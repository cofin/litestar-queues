=================
Queue maintenance
=================

``litestar queues run-maintenance`` performs a small, predictable amount of
repair and retention work and then exits. It is designed for an **infrequent
external schedule** — a six-hour or daily cron — and is deliberately finite:
it never starts a worker, never executes queued work, and never loops to drain
a backlog. The package does not create cron records, enqueue a hidden
maintenance task, or persist a due-state.

Maintenance is not a worker or a scheduler
==========================================

* A :doc:`worker <workers>` claims and executes due tasks continuously.
* The :doc:`scheduler <schedules>` promotes recurring task definitions into due
  records.
* **Maintenance** repairs and prunes existing records once and exits. It does
  not dispatch ordinary queued work.

Phases
======

Every invocation runs the configured phases once, in this fixed order:

#. **external** — reconcile externally dispatched records against their
   execution backend. Enabled only for external execution backends (skipped for
   immediate/local execution).
#. **stale** — recover running tasks whose heartbeats are stale. Enabled when
   ``stale_after`` is set.
#. **terminal** — delete terminal (completed/failed/cancelled) records older
   than ``terminal_retention``. Enabled when ``terminal_retention`` is set.
#. **events** — delete durable event-history rows older than
   ``event_retention``. Enabled when ``event_retention`` is set *and* a durable
   event log is configured; otherwise the phase is skipped, not an error.

Each phase performs **at most one bounded batch** per invocation. Retention
cutoffs are computed once, at the start of the run, so every phase in a single
invocation uses a stable boundary.

If a phase fails, its result contains a package-owned error code and exception
type, later phases still run while time remains, and the whole invocation exits
``1``. Exception messages, connection strings, credentials, and task payloads
are not included in the summary.

Configuration
=============

Maintenance thresholds live on :attr:`QueueConfig.maintenance`. There are **no
destructive defaults**: stale recovery and both retention phases stay disabled
until you supply their thresholds.

.. code-block:: python

   from litestar_queues import QueueConfig, QueueMaintenanceConfig

   config = QueueConfig(
       queue_backend=...,  # a persistent backend
       maintenance=QueueMaintenanceConfig(
           time_budget=300.0,          # seconds; bounds one whole invocation
           coordination_timeout=360.0, # seconds; must exceed time_budget
           external_limit=100,         # max external records reconciled per run
           stale_after=900.0,          # recover heartbeats older than 15 minutes
           stale_limit=100,            # max stale records recovered per run
           terminal_retention=None,    # None disables terminal cleanup
           terminal_limit=1000,        # max terminal records deleted per run
           event_retention=None,       # None disables event-history cleanup
           event_limit=1000,           # max event rows deleted per run
       ),
   )

Durations and retention windows are seconds; every limit and duration must be
positive. ``coordination_timeout`` must be greater than ``time_budget`` because
ownership is not renewed during a run. Leaving
``stale_after``, ``terminal_retention``, or ``event_retention`` as ``None``
disables that phase.

Bounded batches and the time budget
===================================

Each phase mutates at most its configured limit of rows (``external_limit``,
``stale_limit``, ``terminal_limit``, ``event_limit``) ordered oldest-first, and
the service checks the wall-clock ``time_budget`` between phases. When the budget
is exhausted, the remaining enabled phases are reported ``partial`` and no
further backend operation starts. The budget does not interrupt a phase already
in progress. Keep the schedule frequent enough that new work does not outpace
one batch, or raise the limits.

Distributed coordination
========================

Before running, the service acquires token-fenced ownership of the
``queue-maintenance`` operation on the queue backend. If another process already
owns it, this invocation is a **successful no-op** (outcome ``already_running``,
exit ``0``) so an overlapping scheduled run does not retry into the active one.
Ownership is released in a ``finally`` block. Token checking prevents a stale
holder from removing a successor's ownership record.

A backend call that hangs past ``coordination_timeout`` is outside the guarantee, but every
bounded mutation is idempotent or conditionally updates current persisted state.
The next scheduled invocation can therefore resume any remaining work. Keep
``coordination_timeout`` comfortably above the expected runtime as well as above the
configured budget.

Command and exit codes
======================

.. code-block:: console

   $ litestar queues run-maintenance
   outcome: completed
   acquired: True
   duration_ms: 41.2
   Phase     Status        Changed  Duration(ms)  Error
   ---------------------------------------------------
   external  skipped             0           0.0  -
   stale     completed           4           9.1  -
   terminal  completed          18          21.4  -
   events    skipped             0           0.0  -

``--phase [external|stale|terminal|events]`` (repeatable) narrows the run;
filtering only narrows configuration and never enables a disabled retention
threshold. ``--json`` emits one compact object matching
:class:`~litestar_queues.QueueMaintenanceSummary`. The output includes the
outcome, ownership state, duration, and one result per selected phase, and never
includes task payloads.

============  ==================================================================
Code          Meaning
============  ==================================================================
``0``         Completed, a clean no-op, or maintenance was already running.
``1``         Configuration error, lifecycle failure, or a phase failed.
``2``         The time budget was exhausted and later phases were skipped.
============  ==================================================================

Backend support and schema ownership
====================================

.. list-table::
   :header-rows: 1
   :widths: 18 20 24 38

   * - Queue backend
     - Separate CLI process
     - Embedded service
     - Coordination storage and setup
   * - In-memory
     - Rejected (exit ``1``)
     - Supported in the same process
     - Process-local memory; no schema
   * - Redis / Valkey
     - Supported
     - Supported
     - Namespaced ``SET NX PX`` maintenance key; populated prefixes need
       the one-time :ref:`maintenance index rebuild <redis-maintenance-index-upgrade>`
   * - SQLSpec (shared database)
     - Supported
     - Supported
     - ``queue_maintenance`` table from the packaged queue migration
   * - Advanced Alchemy (shared database)
     - Supported
     - Supported
     - Application-owned maintenance model and migration

A separate CLI process cannot see in-memory records.
:class:`~litestar_queues.QueueMaintenanceService` still supports memory for
tests and same-process applications.

Provision SQL maintenance tables before scheduling the command:

* **SQLSpec** — run the application's normal migrations, including the packaged
  ``0001_create_queue_tasks`` migration. Override the table name with
  ``SQLSpecBackendConfig.maintenance_table_name``.
* **Advanced Alchemy** — include ``QueueMaintenanceModel`` in application
  metadata, or compose
  :class:`~litestar_queues.backends.advanced_alchemy.QueueMaintenanceModelMixin`
  into an application model and pass it as
  ``SQLAlchemyBackendConfig.maintenance_model_class``. Create the table
  with the same Alembic or ``create_all`` workflow that owns the queue model;
  the queue backend never creates it.

A missing maintenance table fails closed instead of falling back to a process-local
lock.

Scheduling recipe
=================

Run maintenance from **one** external scheduler on an infrequent cadence.
The same finite command runs from Cloud Run, a Kubernetes ``CronJob``, a
``systemd`` timer, or a shell. Do **not** create four per-phase schedules or run
maintenance at minute-level cadence.

Recommended: one six-hour cron (``0 */6 * * *``, roughly 120 invocations over 30
days). A low-volume alternative is once daily (``0 3 * * *``).

Kubernetes cron job
-------------------

.. code-block:: yaml

   apiVersion: batch/v1
   kind: CronJob
   metadata:
     name: queue-maintenance
   spec:
     schedule: "0 */6 * * *"  # every six hours
     concurrencyPolicy: Forbid
     jobTemplate:
       spec:
         template:
           spec:
             restartPolicy: Never
             containers:
               - name: maintain
                 image: your-app-image
                 command: ["litestar", "queues", "run-maintenance"]
                 env:
                   - name: LITESTAR_APP
                     value: app.asgi:app

systemd timer
-------------

.. code-block:: ini

   # queue-maintenance.timer
   [Timer]
   OnCalendar=*-*-* 00/6:00:00
   Persistent=true

   # queue-maintenance.service
   [Service]
   Type=oneshot
   Environment=LITESTAR_APP=app.asgi:app
   ExecStart=/usr/local/bin/litestar queues run-maintenance

Cloud Run job invoked by Cloud Scheduler
----------------------------------------

Deploy the command as a Cloud Run **Job** and trigger it from Cloud Scheduler on
the same six-hour cron. Cloud Run is not part of the core design — it is one
launch surface among many. A Job launched this way runs exactly the same finite
repair and retention pass as any other host, and still does not dispatch
ordinary queued work.

.. code-block:: console

   $ gcloud run jobs create queue-maintenance \
       --image your-app-image \
       --region CLOUD_RUN_REGION \
       --command litestar --args queues,run-maintenance \
       --set-env-vars LITESTAR_APP=app.asgi:app

   $ gcloud scheduler jobs create http queue-maintenance \
       --location SCHEDULER_REGION \
       --schedule "0 */6 * * *" \
       --uri "https://run.googleapis.com/v2/projects/PROJECT_ID/locations/CLOUD_RUN_REGION/jobs/queue-maintenance:run" \
       --http-method POST \
       --oauth-service-account-email SCHEDULER_SERVICE_ACCOUNT

Grant the scheduler service account the Cloud Run Invoker role on the job.
Google's `scheduled jobs guide <https://docs.cloud.google.com/run/docs/execute/jobs-on-schedule>`_
lists the required roles and current command shape.

=================
Queue maintenance
=================

``litestar queues maintain`` performs a small, predictable amount of repair and
retention work and then exits. It is designed for an **infrequent external
schedule** — a six-hour or daily cron — and is deliberately finite: it never
starts a worker, never executes queued work, and never loops to drain a
backlog. One external scheduler owns cadence; the package does not create cron
records, enqueue a hidden maintenance task, or persist a separate due-state.

Maintenance is not a worker or a scheduler
==========================================

* A :doc:`worker <workers>` claims and executes due tasks continuously.
* The :doc:`scheduler <schedules>` promotes recurring task definitions into due
  records.
* **Maintenance** repairs and prunes existing records once per invocation and
  exits. It does not dispatch ordinary queued work.

Run maintenance on its own schedule, separate from your worker fleet.

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
           time_budget=300.0,       # seconds; bounds one whole invocation
           lease_ttl=360.0,         # seconds; must exceed time_budget
           external_limit=100,      # max external records reconciled per run
           stale_after=900.0,       # recover heartbeats older than 15 minutes
           stale_limit=100,         # max stale records recovered per run
           terminal_retention=None, # None disables terminal cleanup
           terminal_limit=1000,     # max terminal records deleted per run
           event_retention=None,    # None disables event-history cleanup
           event_limit=1000,        # max event rows deleted per run
       ),
   )

Every limit and duration must be positive, and ``lease_ttl`` must be greater
than ``time_budget`` so the lease outlives the whole budget (there is no
lease-renewal path). Durations and retention windows are seconds. Leaving
``stale_after``, ``terminal_retention``, or ``event_retention`` as ``None``
disables that phase.

Bounded batches and the time budget
===================================

Each phase mutates at most its configured limit of rows (``external_limit``,
``stale_limit``, ``terminal_limit``, ``event_limit``) ordered oldest-first, and
the service checks the wall-clock ``time_budget`` between phases. When the budget
is exhausted, the remaining enabled phases are reported ``partial`` and no
further backend operation is started. Because each invocation only drains one
bounded batch per phase, keep your schedule frequent enough that new work does
not outpace a single run — or raise the per-phase limits.

Distributed lease
=================

Before running, the service acquires a token-fenced distributed lease named
``queue-maintenance`` on the queue backend. If another process already holds the
lease, this invocation is a **successful no-op** (outcome ``lease_held``, exit
``0``) so an overlapping scheduled run does not retry into the active one. The
lease is released in a ``finally`` block. A stale holder that has lost the lease
cannot mutate or release a successor's lease, because both release and the
bounded mutations are token-fenced or compare-and-set.

A backend call that hangs past ``lease_ttl`` is outside the guarantee, but every
mutation is idempotent/CAS-fenced, so the next scheduled invocation resumes
safely from persisted queue state.

Command and exit codes
======================

.. code-block:: console

   $ litestar queues maintain
   outcome: completed
   lease_acquired: True
   duration_ms: 41.2
   Phase     Status      Changed  Duration(ms)
   --------------------------------------------
   external  skipped           0           0.0
   stale     completed         4           9.1
   terminal  completed        18          21.4
   events    skipped           0           0.0

``--phase [external|stale|terminal|events]`` (repeatable) narrows the run;
filtering only narrows configuration and never enables a disabled retention
threshold. ``--json`` emits one compact object matching
:class:`~litestar_queues.QueueMaintenanceSummary`. The output includes the
outcome, lease state, duration, and one result per selected phase, and never
includes task payloads.

============  ==================================================================
Code          Meaning
============  ==================================================================
``0``         Completed, a clean no-op, or the lease was held elsewhere.
``1``         Configuration error, lifecycle failure, or a phase failed.
``2``         The time budget was exhausted and later phases were skipped.
============  ==================================================================

Persistent backends only
========================

Maintenance is meant for persistent backends. A separately launched
``litestar queues maintain`` process **rejects the in-memory backend** with a
configuration error (exit ``1``), because in-memory state is process-local and
is not shared with the CLI process. The underlying
:class:`~litestar_queues.QueueMaintenanceService` remains usable with the memory
backend inside the same process for tests and embedded applications.

Migrations
==========

The distributed lease needs a small table on SQL backends. Provision it once,
before scheduling maintenance:

* **SQLSpec** — the packaged migration
  ``0002_create_queue_maintenance_lease`` creates the lease table (derived from
  the queue table name with a ``_maintenance_lease`` suffix). It is registered
  automatically with your SQLSpec migration runner alongside
  ``0001_create_queue_tasks``; run your SQLSpec migrations to apply it.
* **Advanced Alchemy** — you own the models and migrations. Include the
  ``QueueMaintenanceLeaseModel`` table (or compose
  :class:`~litestar_queues.backends.advanced_alchemy.QueueMaintenanceLeaseModelMixin`
  into your own base) in your metadata and Alembic migrations. The backend never
  calls ``metadata.create_all``.
* **Redis / Valkey** — no migration is required; the lease is a namespaced
  ``SET NX PX`` key.

An install that is missing the lease table fails closed with a migration error
rather than falling back to an unsafe process-local lock.

Scheduling recipe
================

Run maintenance from **one** external scheduler on an infrequent cadence.
Cloud Run is only one way to launch the finite command — the same repair and
retention behavior applies to a Kubernetes ``CronJob``, a ``systemd`` timer, or
a plain shell. Do **not** create four per-phase schedules and do **not** run
maintenance at minute-level cadence.

Recommended: one six-hour cron (``0 */6 * * *``, roughly 120 invocations over 30
days). A low-volume alternative is once daily (``0 3 * * *``).

Kubernetes CronJob
------------------

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
                 command: ["litestar", "queues", "maintain"]
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
   ExecStart=/usr/local/bin/litestar queues maintain

Cloud Run Job invoked by Cloud Scheduler
----------------------------------------

Deploy the command as a Cloud Run **Job** and trigger it from Cloud Scheduler on
the same six-hour cron. Cloud Run is not part of the core design — it is one
launch surface among many. A Job launched this way runs exactly the same finite
repair and retention pass as any other host, and still does not dispatch
ordinary queued work.

.. code-block:: console

   $ gcloud run jobs create queue-maintenance \
       --image your-app-image \
       --command litestar --args queues,maintain \
       --set-env-vars LITESTAR_APP=app.asgi:app

   $ gcloud scheduler jobs create http queue-maintenance \
       --schedule "0 */6 * * *" \
       --uri "https://<region>-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/<project>/jobs/queue-maintenance:run"

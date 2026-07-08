Workers
=======

The local worker claims due records from the configured queue backend and
executes them with the configured execution backend.

Plugin Worker
=============

Set ``in_app_worker=True`` to run an in-app worker with the Litestar
application:

.. code-block:: python

   from litestar_queues.backends.sqlspec import SQLSpecBackendConfig

   config = QueueConfig(
       queue_backend=SQLSpecBackendConfig(config=...),
       execution_backend="local",
       in_app_worker=True,
       worker_batch_size=20,
       worker_max_concurrency=4,
   )

This is useful for tests, local development, and lightweight deployments. For
heavier production workloads, consider running workers as separate processes so
web and background capacity can be scaled independently.

Manual Worker
=============

Use ``Worker`` directly for scripts, process managers, or custom entry points:

.. code-block:: python

   from litestar_queues import QueueConfig, QueueService, Worker


   async with QueueService(QueueConfig(queue_backend="memory", execution_backend="local")) as service:
       worker = Worker(service, batch_size=10, max_concurrency=2)
       await worker.start()

``run_once()`` processes one batch and is useful in tests:

.. code-block:: python

   processed = await worker.run_once()

Heartbeats and Stale Records
============================

Workers update heartbeats while a local task is running and clear heartbeat
values after execution finishes. Backends that support stale recovery can
requeue running records whose heartbeat is older than ``worker_stale_after``.
The worker re-checks stale records every ``worker_stale_check_interval``
seconds (default ``60``) from inside the poll loop, so a worker that survives
a peer crash will rescue orphaned records without operator intervention. Set
``worker_stale_after`` to ``None`` (the default) to disable stale recovery
entirely; the periodic check is skipped in that case.

When a stale running task is retried, its priority is demoted to at most ``4``
and any previous error message is preserved. If no previous error exists, the
backend stores ``"Task heartbeat stale"``. When stale recovery marks a task
terminal, it emits ``task.stale_failed`` and invokes the task's
``on_stale_failure`` callback when one is registered:

.. code-block:: python

   from litestar_queues import QueuedTaskRecord, task


   async def notify_operator(record: QueuedTaskRecord) -> None:
       ...


   @task("reports.refresh", requeue_on_stale=False, on_stale_failure=notify_operator)
   async def refresh_report(report_id: str) -> None:
       ...

Workers ask the backend for an ``acquire_worker_lock()`` coordination lock
before running stale-recovery and external-reconciliation sweeps. Backends that
override that hook can make those sweeps single-owner across a fleet for each
interval. Backends without lock support return ``True`` and keep the local
timer behavior.

SQL-backed backends can optionally route heartbeat writes through a dedicated
connection so they do not contend with task fetch and lifecycle UPDATEs under
high concurrency. See :ref:`Heartbeat Pool Isolation
<heartbeat-pool-isolation>` (SQLSpec) and
:ref:`Heartbeat Session Maker Isolation <aa-heartbeat-session-maker>`
(Advanced Alchemy).

External Execution
==================

External execution backends dispatch work outside the local process and store an
execution reference on the queue record. The worker periodically calls
``reconcile_external()`` so external backends can move records into terminal
queue states after the remote execution completes.

Cloud Run uses this flow: local workers dispatch records to Cloud Run Jobs, and
the Cloud Run worker entry point claims the persisted record inside the remote
container.

Worker Wakeups
==============

Queue backends can implement notifications to wake sleeping workers. These
notifications are only hints. Workers always fall back to polling via
``worker_poll_interval`` when notifications are unavailable or missed.

Worker Identity
===============

Each :class:`~litestar_queues.Worker` carries a string ``worker_id`` used to
tag published ``QueueEvent`` envelopes (the ``workerId`` field on the wire).
The default is ``"worker-{os.getpid()}"``; operators that run multiple
workers per host, or that need stable identities across hosts where PIDs
may collide, should pass an explicit ``worker_id`` to ``Worker(...)``:

.. code-block:: python

   worker = Worker(service, worker_id="orders-worker-3")

The :class:`~litestar_queues.QueuePlugin` startup path uses the PID-based
default. Standalone worker entry points should pass an explicit ``worker_id``
per process.

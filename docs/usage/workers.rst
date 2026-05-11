=======
Workers
=======

The local worker claims due records from the configured queue backend and
executes them with the configured execution backend.

Plugin Worker
=============

Set ``start_worker=True`` to run an in-process worker with the Litestar
application:

.. code-block:: python

   config = QueueConfig(
       queue_backend="sqlspec",
       queue_backend_config={...},
       execution_backend="local",
       start_worker=True,
       worker_batch_size=20,
       worker_max_concurrency=4,
   )

This is useful for small deployments and tests. Larger deployments usually run
workers as separate processes so web and background capacity can be scaled
independently.

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

The SQLSpec backend can optionally route heartbeat writes through a dedicated
connection pool so they do not contend with task fetch and lifecycle UPDATEs
under high concurrency. See :ref:`Heartbeat Pool Isolation
<heartbeat-pool-isolation>` in the SQLSpec backend docs.

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

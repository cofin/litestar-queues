===========
Run workers
===========

``WorkerConfig.placement`` names the process that owns the worker. There are
three choices and no fallback between them.

Server placement (the default)
==============================

.. code-block:: python

   from litestar_queues import QueueConfig

   queue_config = QueueConfig()

``QueueConfig()`` uses ``placement="server"``, so one ``litestar run``
invocation starts exactly one worker in its own fresh process, alongside a
private temporary SQLite database. Nothing to install, nothing to configure,
and the worker never competes with your web workers for the event loop:

.. code-block:: bash

   litestar --app app:app run

Server placement needs an explicit application path, either ``--app`` or
``LITESTAR_APP``, because the worker process loads the application itself.
It also needs Litestar's CLI: starting a raw ASGI server or a ``TestClient``
fails immediately rather than serving traffic against a queue nothing drains.

The default database is deliberately ephemeral. It is created when the server
starts and removed when it stops, so queued work does not survive a restart.
It has no listener or configurable path, cannot be attached by standalone CLI
commands, and is unsupported on network storage. A hard parent crash may leave
one random private directory that no later invocation reuses. Choose a backend
from :doc:`backends` as soon as you need durability.

One worker per ASGI process
===========================

.. code-block:: python

   queue_config = QueueConfig(
       queue_backend="memory", worker=WorkerConfig(placement="asgi")
   )

Every ASGI process runs its own worker inside the application lifespan. This
multiplies with your web-worker count, so four Uvicorn workers means four
queue workers. It is the right choice for a single-process development server
using ``queue_backend="memory"``, and a deliberate one everywhere else.

Standalone workers
==================

Choose a shared, persistent queue backend, declare that nothing starts
automatically, and run the same Litestar application as a worker service:

.. code-block:: python

   queue_config = QueueConfig(
       queue_backend="redis", worker=WorkerConfig(placement="external")
   )

.. code-block:: bash

   LITESTAR_APP=app:app litestar queues run --drain-timeout 30

Process only selected queues or override concurrency:

.. code-block:: bash

   LITESTAR_APP=app:app litestar queues run \
     --queue reports --queue email --max-concurrency 4

This command refuses process-local storage. Memory lives inside the process
that created it, and the ephemeral database belongs to the ``litestar run``
invocation that created it, so neither is visible to a separate worker.

Scaling out
===========

``placement`` names the worker your application starts for itself, not a limit
on how many workers may exist. Once you are on a persistent backend you can add
standalone workers to any placement:

.. code-block:: bash

   # one worker from the server invocation, plus three more elsewhere
   litestar --app app:app run
   LITESTAR_APP=app:app litestar queues run   # x3, on other hosts or containers

They all claim from the same backend, so the total worker count is the built-in
one plus however many you start.

Heartbeat timing
================

``WorkerConfig.heartbeat_interval`` sets the base interval between running-task
heartbeat writes, while ``heartbeat_miss_threshold`` controls how many
consecutive misses are tolerated. ``heartbeat_jitter_fraction`` adds up to that
fraction of positive random delay to each interval so a fleet does not write in
lockstep. It defaults to ``0.1``; set it to ``0.0`` for an exact fixed interval.

Choosing a placement
====================

.. list-table::
   :header-rows: 1

   * - Placement
     - Who starts the worker
     - How many it starts
     - Storage
   * - ``server``
     - the Litestar CLI server lifespan
     - one per ``litestar run``
     - ephemeral or persistent
   * - ``asgi``
     - the application lifespan
     - one per ASGI process
     - memory or persistent
   * - ``external``
     - nobody; you do
     - none
     - persistent for ``litestar queues run``

The count is what the application starts for itself. Any placement on a
persistent backend can have standalone workers added alongside it.

Managed placements (``server`` and ``asgi``) reject
``execution_backend="immediate"``, because inline execution leaves a worker
with nothing to claim.

What one worker loop does
=========================

A worker makes due scheduled tasks ready, claims as many tasks as its
concurrency limit allows, and starts local execution. It also sends heartbeats
for running records and checks external work. ``Worker.run_once()`` returns
after it schedules claimed tasks; it does not wait for them to finish. Use
:doc:`results` when a caller must observe the final state.

``batch_size`` is the maximum requested claim size, not a promise that every
backend performs one storage operation. Memory, capable SQLSpec stores, and
PostgreSQL Advanced Alchemy have native bounded batch paths. Other stores use
the safe ``claim_next`` loop and may return a shorter batch as soon as no
eligible record remains. Both paths preserve exclusive ownership and the same
queue/execution filters.

Shutdown
========

The first termination signal stops new claims and gives running tasks time to
finish. A second signal cancels them. Server placement must become ready within
``startup_timeout``. Shutdown then allows ``graceful_shutdown_timeout``, uses
``final_cancel_timeout`` for cancellation, and finally escalates to bounded
process-tree termination if the child cannot exit. The standalone CLI returns
``0`` for a clean shutdown, ``1`` for a worker error, and ``2`` when the
graceful timeout ends and cancellation begins. Windows uses console Ctrl+C or
Ctrl+Break semantics. Focused topology smoke tests run on macOS and Windows in
native CI jobs.

See :doc:`worker-wakeups` for idle waiting and :doc:`worker-recovery` for
heartbeats and stale work.

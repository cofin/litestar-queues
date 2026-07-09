===========
Run workers
===========

The default configuration starts a local worker inside the Litestar process:

.. code-block:: python

   from litestar_queues import QueueConfig

   queue_config = QueueConfig(in_app_worker=True)

This is the shortest path for development, tests, and small single-process
deployments.

Run standalone workers
======================

Use a shared persistent queue backend, turn off the web process worker, and run
the same Litestar application as a worker service:

.. code-block:: python

   queue_config = QueueConfig(in_app_worker=False)

.. code-block:: bash

   LITESTAR_APP=app:app litestar queues run --drain-timeout 30

Process only selected queues or override concurrency:

.. code-block:: bash

   LITESTAR_APP=app:app litestar queues run \
     --queue reports --queue email --max-concurrency 4

Memory persistence cannot coordinate separate processes. Choose a backend in
:doc:`backends` before separating the worker.

What one worker loop does
=========================

A worker promotes due work, claims up to its available concurrency, schedules
local executions, heartbeats running records, and reconciles external work.
``Worker.run_once()`` returns after claims are scheduled; it is not a
completion barrier. Use :doc:`results` when a caller must observe terminal
state.

Shutdown
========

The first termination signal stops new claims and drains running tasks up to
the configured graceful timeout. A second signal forces cancellation. The CLI
returns ``0`` for clean shutdown, ``1`` for a worker error, and ``2`` when
draining escalates to cancellation.

See :doc:`worker-wakeups` for idle waiting and :doc:`worker-recovery` for
heartbeats and stale work.

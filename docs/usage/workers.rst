===========
Run workers
===========

The default configuration starts a local worker inside the Litestar process:

.. code-block:: python

   from litestar_queues import QueueConfig, WorkerConfig

   queue_config = QueueConfig(worker=WorkerConfig(run_in_app=True))

This is the shortest path for development, tests, and small single-process
deployments.

Run standalone workers
======================

Choose a shared, persistent queue backend. Then turn off the worker in the web
process and run the same Litestar application as a worker service:

.. code-block:: python

   queue_config = QueueConfig(worker=WorkerConfig(run_in_app=False))

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

A worker makes due scheduled tasks ready, claims as many tasks as its
concurrency limit allows, and starts local execution. It also sends heartbeats
for running records and checks external work. ``Worker.run_once()`` returns
after it schedules claimed tasks; it does not wait for them to finish. Use
:doc:`results` when a caller must observe the final state.

Shutdown
========

The first termination signal stops new claims and gives running tasks time to
finish. A second signal cancels them. The CLI returns ``0`` for a clean
shutdown, ``1`` for a worker error, and ``2`` when the graceful timeout ends
and cancellation begins.

See :doc:`worker-wakeups` for idle waiting and :doc:`worker-recovery` for
heartbeats and stale work.

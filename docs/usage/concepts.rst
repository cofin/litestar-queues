========
Concepts
========

Litestar Queues separates the durable description of work from the place that
runs it. This keeps the task API stable while deployment choices change.

The task flow
=============

.. code-block:: text

   enqueue -> queued task record -> worker wakeup or polling -> claim -> execute
   -> retry/terminal state -> optional task event delivery

Task and record
===============

A function decorated with :func:`~litestar_queues.task` is a registered
``Task``. Calling ``QueueService.enqueue()`` creates a queued task record with
the task name, arguments, queue, state, retry count, and execution choice.
The record—not the Python coroutine—is what a worker claims.

Queue backend
=============

The **queue backend** stores task records and their states. Memory is the
default and is process-local. SQLSpec, Advanced Alchemy, Redis, and Valkey can
share records between web and worker processes. Persisted task state remains
the source of truth.

Execution backend
=================

The **execution backend** controls where claimed work runs. ``local`` runs it
in the worker process, ``immediate`` runs it inline, and Cloud Run dispatches
it to a Cloud Run Job. Execution placement does not replace queue persistence.

Worker lifecycle
================

A worker finds due records, claims them, runs or dispatches them, records the
outcome, and retries eligible failures. The plugin can start a worker in the
web process; production deployments commonly run workers separately.

Wakeups and reconciliation
==========================

A backend may notify workers when work arrives. A **worker wakeup is a hint**:
notifications can be delayed or dropped, so workers also poll and reconcile
against persisted state. A wakeup is never durable task storage.

Task events
===========

Tasks can publish lifecycle, progress, log, and custom events for application
or operator consumers. Live task event delivery uses event sinks and Litestar
Channels. It is separate from queue-backend wakeups and is not worker discovery.
Durable event history is another queue-backend capability and is also separate
from live browser fan-out.

Choose the next guide
=====================

* :doc:`tasks` defines and enqueues work.
* :doc:`results` observes the persisted record.
* :doc:`workers` chooses in-app or standalone worker placement.
* :doc:`backends` chooses persistence and execution independently.
* :doc:`events` publishes user-facing progress.

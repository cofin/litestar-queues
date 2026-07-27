========
Concepts
========

Litestar Queues stores a description of the work separately from the process
that runs it. You can change the deployment without changing the task API.

The task flow
=============

.. code-block:: text

   enqueue -> pending/scheduled record -> worker wakeup or polling
                                  |              |
                                  | overdue      | claim
                                  v              v
                              expired         running -> retry or terminal state
                                  \______________|______________/
                                                 v
                                      optional task event delivery

``expired`` means the record passed its not-started deadline before a worker
claimed it. It is distinct from user cancellation and from a runtime failure.
See :doc:`task-options` for the deadline clock and its relationship to runtime
timeouts and terminal retention.

Task and record
===============

A function decorated with :func:`~litestar_queues.task` is a registered
``Task``. Calling ``QueueService.enqueue()`` creates a queued task record with
the task name, arguments, queue, state, retry count, and execution choice.
The record—not the Python coroutine—is what a worker claims.

Queue backend
=============

The **queue backend** stores task records and their states. The default
ephemeral SQLite backend shares one private temporary file between the server
and its worker child; it is not durable across server restarts. The explicit
memory backend stores live Python objects in one process. SQLSpec, Advanced
Alchemy, Redis, and Valkey can share durable records between web and worker
processes. The saved task record is always the source of truth.

Execution backend
=================

The **execution backend** controls where claimed work runs. ``local`` runs it
in the worker process, ``immediate`` runs it inline, and Cloud Run dispatches
it to a Cloud Run Job. Execution placement does not replace queue persistence.

Worker lifecycle
================

A worker finds tasks that are ready, claims them, and runs or dispatches them.
It then saves the result and retries failures when allowed. By default the
Litestar CLI server lifespan owns one fresh worker child. ``asgi`` placement
deliberately starts one inside each web process, while ``external`` placement
leaves worker startup to the operator.

Wakeups and reconciliation
==========================

A backend may notify workers when work arrives. A **worker wakeup** is only a
hint to check the queue. Because notifications may be late or lost, workers
also poll the saved task state. A wakeup never stores a task.

Task events
===========

Tasks can publish lifecycle, progress, log, and custom events for applications
and operators. Event sinks and Litestar Channels deliver these events live.
This live delivery is not worker discovery. It does not wake workers or help
them find tasks. Event history is stored by the queue backend and is also
separate from live browser delivery.

Choose the next guide
=====================

* :doc:`tasks` defines and enqueues work.
* :doc:`results` observes the persisted record.
* :doc:`workers` chooses in-app or standalone worker placement.
* :doc:`backends` chooses persistence and execution independently.
* :doc:`events` publishes user-facing progress.

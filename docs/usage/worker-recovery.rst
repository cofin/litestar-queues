===============
Worker recovery
===============

Workers heartbeat running tasks and can recover records whose worker stopped:

.. code-block:: python

   from litestar_queues import QueueConfig, WorkerConfig

   queue_config = QueueConfig(
       worker=WorkerConfig(
           heartbeat_interval=15,
           stale_after=120,
           stale_check_interval=30,
       ),
   )

These settings are seconds. Leaving ``WorkerConfig.stale_after=None`` disables
automatic stale recovery.

Heartbeats and stale records
============================

The heartbeat manager updates active records. A shared lock lets only one
worker at a time check for stale records. A stale task returns to the queue if
it has retries left. Otherwise, it ends with a stale failure. A task may turn
off stale requeueing or register ``on_stale_failure`` for cleanup.

Recovered priority
------------------

``QueueConfig.stale_requeue_priority`` decides the priority recovered work
re-enters the queue with:

.. code-block:: python

   QueueConfig(stale_requeue_priority="preserve")   # keep the original priority
   QueueConfig(stale_requeue_priority=4)            # ceiling clamp (the default)
   QueueConfig(stale_requeue_priority=lambda p: p - 1)  # map old priority to new

The default clamps to ``4``, which is the historical behavior. That demotion
protects a queue from a record that crashes its worker repeatedly, but it also
inverts priority: work enqueued at priority ``9`` re-enters at ``4`` and can be
starved indefinitely by ordinary priority-``5`` inflow. Prefer ``"preserve"``
together with bounded shutdown interruptions and a real ``max_retries`` budget
when the priority actually matters. A callable that returns anything other than
an integer fails the sweep loudly rather than silently clamping.

Heartbeat timestamps are automatic for every running task. Calling
``beat(detail)`` is optional: it replaces the latest short diagnostic detail
without changing heartbeat cadence. Progress and custom task events are
observability signals, not liveness writes.

Completion and recovery ordering
================================

On success, the task body returns first. The service then persists the
completed record and result, publishes ``task.completed``, flushes buffered
and live delivery, schedules the next recurring run when applicable, and
finally clears heartbeat ownership. A terminal event therefore means the
record transition was already stored, but consumers should still call
:meth:`~litestar_queues.TaskResult.refresh` before reading cached result data.

A retryable attempt publishes ``task.failed`` with ``will_retry=True`` and a
later attempt publishes ``task.started`` again for the same task ID. A final
exception uses ``will_retry=False``. Cancellation, claim loss, and stale
failure stop the current execution through their own fenced paths; claim loss
must not let the old worker overwrite the record now owned by another worker.

Heartbeat pool isolation
========================

SQLSpec can send heartbeat-only writes through ``heartbeat_pool_config``.
Advanced Alchemy can use an app-owned ``heartbeat_session_maker``. Both must
point to the same database as normal queue operations. Other task-state writes
continue to use the main transaction path.

Worker identity
===============

Workers default to ``worker-{pid}``, where ``pid`` is the process ID. Set an
explicit ``WorkerConfig.id`` when process IDs may repeat across hosts or preforked
processes. The ID appears in logs, metrics, and task events. It does not stop
another worker from running.

Diagnosis
=========

If work remains ``running`` after a crash, check heartbeat timestamps, stale
thresholds, backend connectivity, and whether at least one worker has stale
recovery enabled. Use ``litestar queues status`` for counts and inspect task
errors after :meth:`~litestar_queues.TaskResult.refresh`.

Worker recovery checks continuously while a worker runs. For an infrequent,
finite recovery pass that also applies retention, use
:doc:`maintenance` instead.

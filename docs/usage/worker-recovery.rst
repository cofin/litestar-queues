===============
Worker recovery
===============

Workers heartbeat running tasks and can recover records whose worker stopped:

.. code-block:: python

   from datetime import timedelta
   from litestar_queues import QueueConfig

   queue_config = QueueConfig(
       worker_heartbeat_interval=15,
       worker_stale_after=timedelta(minutes=2),
       worker_stale_check_interval=30,
   )

Leaving ``worker_stale_after=None`` disables automatic stale recovery.

Heartbeats and stale records
============================

The heartbeat manager updates active records. A shared lock lets only one
worker at a time check for stale records. A stale task returns to the queue if
it has retries left. Otherwise, it ends with a stale failure. A task may turn
off stale requeueing or register ``on_stale_failure`` for cleanup.

Heartbeat pool isolation
========================

SQLSpec can send heartbeat-only writes through ``heartbeat_pool_config``.
Advanced Alchemy can use an app-owned ``heartbeat_session_maker``. Both must
point to the same database as normal queue operations. Other task-state writes
continue to use the main transaction path.

Worker identity
===============

Workers default to ``worker-{pid}``, where ``pid`` is the process ID. Set an
explicit ``worker_id`` when process IDs may repeat across hosts or preforked
processes. The ID appears in logs, metrics, and task events. It does not stop
another worker from running.

Diagnosis
=========

If work remains ``running`` after a crash, check heartbeat timestamps, stale
thresholds, backend connectivity, and whether at least one worker has stale
recovery enabled. Use ``litestar queues status`` for counts and inspect task
errors after :meth:`~litestar_queues.TaskResult.refresh`.

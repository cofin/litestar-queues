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

The worker heartbeat manager updates active records. A distributed worker lock
allows one worker at a time to run a stale sweep. Eligible stale work is
requeued while retries remain; otherwise it becomes a terminal stale failure.
Tasks may disable stale requeue or register ``on_stale_failure`` for cleanup.

Heartbeat pool isolation
========================

SQLSpec can route heartbeat-only writes through ``heartbeat_pool_config``.
Advanced Alchemy can use an adopter-owned ``heartbeat_session_maker``. Both
must point at the same database as ordinary queue operations. Lifecycle writes
remain on the main transaction path.

Worker identity
===============

Workers default to ``worker-{pid}``. Provide an explicit ``worker_id`` when
process IDs are ambiguous across hosts or prefork processes. The ID appears in
logs, metrics, and task events; it does not provide mutual exclusion.

Diagnosis
=========

If work remains ``running`` after a crash, check heartbeat timestamps, stale
thresholds, backend connectivity, and whether at least one worker has stale
recovery enabled. Use ``litestar queues status`` for counts and inspect task
errors after :meth:`~litestar_queues.TaskResult.refresh`.

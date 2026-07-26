=============
Configuration
=============

Pass one :class:`~litestar_queues.QueueConfig` to the plugin:

.. code-block:: python

   from litestar_queues import QueueConfig, QueuePlugin, WorkerConfig

   queue_plugin = QueuePlugin(
       config=QueueConfig(
           queue_backend="memory",
           execution_backend="local",
           worker=WorkerConfig(placement="asgi"),
       )
   )

Use this page as a map; each linked guide owns the detailed behavior.

.. list-table::
   :header-rows: 1

   * - Concern
     - Settings
     - Guide
   * - Persistence
     - ``queue_backend``
     - :doc:`backends`
   * - Execution placement
     - ``execution_backend``
     - :doc:`backends`
   * - Worker placement
     - ``worker.placement``
     - :doc:`workers`
   * - Claiming and concurrency
     - ``worker.batch_size``, ``worker.max_concurrency``
     - :doc:`workers`
   * - Idle waiting
     - ``worker.poll_interval``
     - :doc:`worker-wakeups`
   * - Heartbeats and recovery
     - ``worker.heartbeat_interval``, ``worker.heartbeat_miss_threshold``,
       ``worker.stale_after``, ``worker.stale_check_interval``
     - :doc:`worker-recovery`
   * - Queued task expiration
     - ``worker.expiry_check_interval``, task ``expires_in``, enqueue
       ``expires_in`` / ``expires_at``
     - :doc:`task-options`
   * - Shutdown
     - ``worker.graceful_shutdown_timeout``, ``worker.final_cancel_timeout``
     - :doc:`workers`
   * - Task discovery
     - ``task_modules``
     - :doc:`tasks`
   * - Argument identity size guard
     - ``max_argument_identity_bytes``
     - :doc:`task-options`
   * - Schedules
     - ``initialize_schedules``, ``scheduler_canary_task``
     - :doc:`schedules`
   * - Bounded maintenance
     - ``maintenance``
     - :doc:`maintenance`
   * - Events
     - ``events.delivery``, ``events.history``, ``events.stream``
     - :doc:`events`, :doc:`event-history`, :doc:`event-streams`
   * - Observability
     - ``observability``
     - :doc:`observability`
   * - External dependencies
     - ``task_dependency_resolver``
     - :doc:`dependency-resolver`

Defaults favor a one-process development app: memory persistence, local
execution, and an in-app worker. Make production choices explicit.

=============
Configuration
=============

Pass one :class:`~litestar_queues.QueueConfig` to the plugin:

.. code-block:: python

   from litestar_queues import QueueConfig, QueuePlugin

   queue_plugin = QueuePlugin(
       config=QueueConfig(
           queue_backend="memory",
           execution_backend="local",
           in_app_worker=True,
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
     - ``in_app_worker``
     - :doc:`workers`
   * - Claiming and concurrency
     - ``worker_batch_size``, ``worker_max_concurrency``
     - :doc:`workers`
   * - Idle waiting
     - ``worker_poll_interval``
     - :doc:`worker-wakeups`
   * - Heartbeats and recovery
     - ``worker_heartbeat_interval``, ``worker_heartbeat_miss_threshold``,
       ``worker_stale_after``, ``worker_stale_check_interval``
     - :doc:`worker-recovery`
   * - Shutdown
     - ``worker_graceful_shutdown_timeout``, ``worker_final_cancel_timeout``
     - :doc:`workers`
   * - Task discovery
     - ``task_modules``
     - :doc:`tasks`
   * - Schedules
     - ``initialize_schedules``, ``scheduler_canary_task``
     - :doc:`schedules`
   * - Events
     - ``event``, ``event_log``, ``event_stream``
     - :doc:`events`, :doc:`event-history`, :doc:`event-streams`
   * - Observability
     - ``observability``
     - :doc:`observability`
   * - External dependencies
     - ``task_dependency_resolver``
     - :doc:`dependency-resolver`

Defaults favor a one-process development app: memory persistence, local
execution, and an in-app worker. Make production choices explicit.

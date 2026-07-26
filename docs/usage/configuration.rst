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
     - ``worker.poll_interval``, ``worker.poll_backoff_max``,
       ``worker.poll_backoff_multiplier``, ``worker.poll_jitter``
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

Adaptive polling
================

Polling-only workers reduce idle backend traffic by increasing their wait
after empty cycles. The default grows from ``0.1`` seconds to at most ``30``
seconds, applies bounded jitter, and resets immediately when work or a native
notification arrives:

.. code-block:: python

   worker = WorkerConfig(
       poll_interval=0.1,
       poll_backoff_max=10.0,
       poll_backoff_multiplier=2.0,
       poll_jitter=0.15,
   )

For polling-only backends, ``poll_backoff_max`` is the worst-case discovery
latency for newly inserted work. Native notifications still wake immediately,
and known scheduled or retry work clamps the wait to its due time. Set
``poll_backoff_max=None`` to retain fixed-interval polling when latency matters
more than idle load.

Defaults favor zero-configuration background execution: ephemeral SQLite,
local execution, and one server-owned worker started by ``litestar run``.
Choose persistent storage and placement explicitly for durable deployments.

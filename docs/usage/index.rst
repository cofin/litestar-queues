=============
How-to guides
=============

After the :doc:`../getting_started/quickstart`, follow this three-step route:

1. :doc:`tasks` — define and enqueue work.
2. :doc:`workers` and :doc:`results` — run and observe it.
3. :doc:`backends` and :doc:`events` — choose production persistence,
   execution, and delivery options.

Define and control tasks
========================

.. grid:: 1 1 2 3
   :gutter: 2
   :padding: 0

   .. grid-item-card:: Define and enqueue
      :link: tasks
      :link-type: doc

      Register task functions and enqueue them through ``QueueService``.

   .. grid-item-card:: Configure task options
      :link: task-options
      :link-type: doc

      Set retries, timeouts, priority, delay, metadata, and deduplication keys.

   .. grid-item-card:: Inspect results
      :link: results
      :link-type: doc

      Refresh records, wait for terminal state, and inspect results or errors.

   .. grid-item-card:: Background responses
      :link: background-tasks
      :link-type: doc

      Choose whether enqueueing happens before or after the response starts.

   .. grid-item-card:: Failures and cancellation
      :link: failures-and-cancellation
      :link-type: doc

      Control retries and cooperative cancellation.

   .. grid-item-card:: Schedules
      :link: schedules
      :link-type: doc

      Register interval and cron tasks.

Run and operate workers
=======================

.. grid:: 1 1 2 3
   :gutter: 2
   :padding: 0

   .. grid-item-card:: Run workers
      :link: workers
      :link-type: doc

      Pick in-app or standalone placement and start worker processes.

   .. grid-item-card:: Understand wakeups
      :link: worker-wakeups
      :link-type: doc

      Combine notification hints with polling and shutdown interruption.

   .. grid-item-card:: Recover stale work
      :link: worker-recovery
      :link-type: doc

      Configure heartbeats, recovery, identity, and diagnosis.

   .. grid-item-card:: Observe queues
      :link: observability
      :link-type: doc

      Add traces, metrics, logs, and operational status checks.

Choose production options
=========================

.. grid:: 1 1 2 3
   :gutter: 2
   :padding: 0

   .. grid-item-card:: Choose backends
      :link: backends
      :link-type: doc

      Select queue persistence, execution placement, and wakeups independently.

   .. grid-item-card:: Publish task events
      :link: events
      :link-type: doc

      Publish progress and connect live SSE or WebSocket consumers.

   .. grid-item-card:: Deploy Cloud Run execution
      :link: deployment/cloud-run
      :link-type: doc

      Persist, dispatch, and execute work across Cloud Run services and Jobs.

.. toctree::
   :hidden:
   :maxdepth: 2

   concepts
   tasks
   task-options
   results
   background-tasks
   failures-and-cancellation
   schedules
   workers
   worker-wakeups
   worker-recovery
   configuration
   cli
   testing
   backends
   backends/sqlspec
   backends/advanced-alchemy
   backends/redis-valkey
   events
   event-streams
   event-history
   event-testing
   observability
   dependency-resolver
   deployment/cloud-run
   migration

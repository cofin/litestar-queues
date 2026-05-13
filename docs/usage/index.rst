Usage
=====

This section contains focused guides for runtime configuration and production
queue behavior.

Choose a Topic
==============

.. grid:: 1 1 2 3
   :gutter: 2
   :padding: 0

   .. grid-item-card:: Configuration
      :link: configuration
      :link-type: doc

      Configure the plugin, service, workers, dependency keys, and runtime
      settings.

   .. grid-item-card:: Tasks
      :link: tasks
      :link-type: doc

      Register tasks, enqueue work, set retries, deduplicate records, and use
      task context helpers.

   .. grid-item-card:: Schedules
      :link: schedules
      :link-type: doc

      Run recurring interval and cron tasks with startup synchronization.

   .. grid-item-card:: Workers
      :link: workers
      :link-type: doc

      Run local workers, process batches, send heartbeats, and reconcile
      external execution.

   .. grid-item-card:: Events
      :link: events
      :link-type: doc

      Publish lifecycle, progress, log, and custom task events to application
      realtime infrastructure.

   .. grid-item-card:: Backends
      :link: backends
      :link-type: doc

      Select queue and execution backends while keeping optional drivers
      optional.

   .. grid-item-card:: Dependency Resolver
      :link: dependency-resolver
      :link-type: doc

      Inject services into task callables through an external DI container
      with the optional ``task_dependency_resolver`` hook.

   .. grid-item-card:: CLI
      :link: cli
      :link-type: doc

      Run worker fleets, inspect queue status, and check scheduler health
      via the ``litestar queues`` subcommand group.

Recommended Path
----------------

1. Start with :doc:`../getting_started/quickstart` to wire the plugin and
   enqueue a task.
2. Configure deployment defaults in :doc:`configuration`.
3. Add task-level behavior with :doc:`tasks` and recurring work with
   :doc:`schedules`.
4. Choose queue persistence and execution integrations in :doc:`backends`.
5. Add progress streaming or external subscribers with :doc:`events`.

.. toctree::
   :hidden:
   :maxdepth: 1

   configuration
   tasks
   schedules
   workers
   events
   quickstart
   backends
   cli
   dependency-resolver
   testing
   migration

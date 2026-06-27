=============
Configuration
=============

``QueueConfig`` configures the queue backend, execution backend, Litestar plugin
lifecycle, workers, scheduled tasks, and realtime event publishing.

Litestar Plugin
===============

Use :class:`litestar_queues.QueuePlugin` when a Litestar app should own the
queue service lifecycle:

.. code-block:: python

   from litestar import Litestar
   from litestar_queues import QueueConfig, QueuePlugin


   queue_config = QueueConfig(
       queue_backend="memory",
       execution_backend="local",
       start_worker=True,
       task_modules=("app.tasks",),
   )

   app = Litestar(plugins=[QueuePlugin(config=queue_config)])

The plugin registers a ``QueueService`` dependency, stores the opened service on
application state, loads configured task modules during startup, initializes
registered schedules, and starts a local worker when ``start_worker=True``.
Route handlers should request the dependency explicitly with
``queue_service: NamedDependency[QueueService]``.

Standalone Service
==================

Use ``QueueService`` directly in scripts, tests, or external worker entry
points:

.. code-block:: python

   from litestar_queues import QueueConfig, QueueService


   async with QueueService(QueueConfig(queue_backend="memory")) as queue_service:
       await queue_service.initialize_schedules()

Core Settings
=============

.. list-table::
   :header-rows: 1

   * - Setting
     - Default
     - Purpose
   * - ``queue_backend``
     - ``"memory"``
     - Queue persistence backend name or typed backend config object.
   * - ``execution_backend``
     - ``"immediate"``
     - Default execution backend name or typed execution config object.
   * - ``task_modules``
     - ``()``
     - Modules imported on startup so task decorators register tasks.
   * - ``initialize_schedules``
     - ``True``
     - Create or refresh pending records for scheduled tasks on startup.
   * - ``start_worker``
     - ``False``
     - Start an in-process worker with the Litestar app.

Dependency and State Keys
=========================

The default Litestar dependency key is ``queue_service``. The plugin also stores
the service, worker, event publisher, and optional Channels backend on app
state. Override these keys when an application needs multiple queue services or
custom naming:

.. code-block:: python

   config = QueueConfig(
       queue_service_dependency_key="jobs",
       queue_service_state_key="jobs_service",
       queue_worker_state_key="jobs_worker",
   )

Worker Settings
===============

Worker settings are used when ``start_worker=True`` or when constructing a
``Worker`` manually:

.. list-table::
   :header-rows: 1

   * - Setting
     - Default
     - Purpose
   * - ``worker_batch_size``
     - ``10``
     - Maximum due records fetched per worker iteration.
   * - ``worker_poll_interval``
     - ``0.1``
     - Fallback sleep when no work is available.
   * - ``worker_max_concurrency``
     - ``1``
     - Concurrent local task executions.
   * - ``worker_heartbeat_interval``
     - ``30``
     - Seconds between heartbeat updates for running records.
   * - ``worker_reconcile_interval``
     - ``30``
     - Seconds between external execution reconciliation passes.
   * - ``worker_stale_after``
     - ``None``
     - Seconds after which stale running records can be requeued on startup.

Event Settings
==============

``event_config`` controls application-facing queue event delivery. Events are
disabled by default and can be enabled with a custom sink or an app-owned
Litestar Channels backend:

.. code-block:: python

   from litestar_queues.events import QueueEventConfig


   config = QueueConfig(
       event_config=QueueEventConfig(enabled=True, channels_backend=channels),
   )

See :doc:`events` for the event envelope, channels, helper APIs, and streaming
patterns.

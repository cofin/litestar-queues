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
       in_app_worker=True,
       task_modules=("app.tasks",),
   )

   app = Litestar(plugins=[QueuePlugin(config=queue_config)])

The plugin registers a ``QueueService`` dependency, stores the opened service on
application state, loads configured task modules during startup, initializes
registered schedules, and starts a local worker when ``in_app_worker=True``.
The default in-app worker is a low-friction setup for tests, local development,
and lightweight deployments; heavier production workloads can run standalone
workers when web and background capacity should scale independently.
Route handlers should request the dependency explicitly with
``queue_service: NamedDependency[QueueService]``.

Worker Placement
================

By default, ``QueuePlugin`` starts a worker inside the Litestar application
process. To split web and background capacity, use a shared queue backend,
disable the in-app worker in the web process, and run a separate worker process:

.. code-block:: python

   queue_config = QueueConfig(in_app_worker=False)

.. code-block:: console

   $ LITESTAR_APP=app.asgi:app litestar queues run --drain-timeout 30

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
     - ``"local"``
     - Default execution backend name or typed execution config object.
   * - ``task_modules``
     - ``()``
     - Modules imported on startup so task decorators register tasks.
   * - ``initialize_schedules``
     - ``True``
     - Create or refresh pending records for scheduled tasks on startup.
   * - ``quiet_success``
     - ``True``
     - Suppress the operator log line for successful task completion by
       default.
   * - ``in_app_worker``
     - ``True``
     - Start an in-process worker with the Litestar app.

``quiet_success`` is a logging default only. It does not suppress
``task.completed`` lifecycle events, task progress/log events, stream delivery,
or backend-managed event history. Use ``QueueConfig(quiet_success=False)`` to
emit successful completion logs by default, then override individual tasks or
enqueue calls with ``quiet_success=True`` when they are too noisy. Queue-level
quieting is not part of the public configuration.

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

Worker settings are used when ``in_app_worker=True`` or when constructing a
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
   * - ``worker_stale_check_interval``
     - ``60``
     - Seconds between stale-running recovery sweeps.
   * - ``error_sanitizer``
     - ``None``
     - Optional callable that converts task exceptions into persisted error
       messages and failed-event messages.

Event Settings
==============

``event`` controls application-facing queue event delivery. Events are disabled
when ``QueueConfig.event`` is ``None``. Providing ``EventConfig`` enables them
by default unless ``enabled=False`` is explicit:

.. code-block:: python

   from litestar_queues.events import EventConfig


   config = QueueConfig(
       event=EventConfig(channels_backend=channels),
   )

See :doc:`events` for the event envelope, channels, helper APIs, and streaming
patterns.

Migration Guide
===============

This guide covers moving an application-specific worker package onto
``litestar_queues`` while keeping the application-owned task code, database
configuration, and deployment choices.

Port Tasks
----------

Keep task callables in application modules and decorate them with
``litestar_queues.task``:

.. code-block:: python

   from litestar_queues import task
   from litestar_queues.events import publish_task_log, publish_task_progress

   @task(
       "files.process",
       queue="files",
       retries=3,
       timeout=600,
       execution_backend="cloudrun",
       execution_profile="worker-heavy",
       description="Process an uploaded file.",
       log_level="info",
   )
   async def process_file(file_id: str, *, _job_id: str) -> dict[str, str]:
       await publish_task_progress(current=1, total=3, message="started")
       await publish_task_log("file processing started", payload={"file_id": file_id})
       return {"task_id": str(_job_id), "file_id": file_id}

Tasks that accept ``_job_id`` receive the queue record ID. Tasks can also
accept ``_task_context`` to publish events directly through the bound
``TaskExecutionContext``.

Use ``Task.using()`` or ``QueueService.enqueue()`` keyword arguments for
per-call overrides:

.. code-block:: python

   result = await queue_service.enqueue(
       process_file.using(key=f"file:{file_id}", priority=10),
       file_id,
       quiet_success=True,
   )

Configure Backends
------------------

Choose a queue backend independently from where tasks execute. Optional drivers
are imported only by their own public subpackages:

.. code-block:: bash

   pip install litestar-queues[sqlspec]
   pip install litestar-queues[advanced-alchemy]
   pip install litestar-queues[redis]
   pip install litestar-queues[valkey]
   pip install litestar-queues[cloudrun]

Applications that already configure SQLSpec or Advanced Alchemy should keep
owning those framework plugins and pass the configured objects to
the typed queue backend config object. ``litestar_queues`` does not register
SQLSpec or Advanced Alchemy plugins on behalf of the application.

Schedules
---------

Recurring jobs use the same decorator. Interval and cron schedules preserve
task metadata on the queued record, including description, log level, quiet
success, execution backend, execution profile, and schedule metadata:

.. code-block:: python

   @task(
       "maintenance.cleanup",
       cron="0 2 * * *",
       timezone="UTC",
       description="Remove expired queue records.",
       log_level="debug",
   )
   async def cleanup() -> None:
       ...

   @task(
       "integrations.sync",
       interval=300,
       initial_delay=60,
       jitter=30,
       max_instances=1,
   )
   async def sync_external_data() -> None:
       ...

When ``QueueConfig.initialize_schedules`` is enabled, startup creates one
deduplicated scheduled record per registered schedule key. If a pending
scheduled record exists but the schedule metadata has changed, startup cancels
the old record and creates a new one with the updated definition.

Cron schedules use five fields: minute, hour, day-of-month, month, and
day-of-week. Litestar Queues supports wildcards, lists, ranges, named months and
weekdays, positive steps, Sunday as ``0`` or ``7``, and ``?`` in the two day
fields. It does not support Quartz-style extensions such as seconds or year
fields, ``@reboot``, ``L``, ``W``, or ``#`` modifiers. Rewrite those schedules as
multiple supported cron expressions or move the advanced calendar rule into
application code.

Realtime Updates
----------------

Queue persistence notifications are for waking workers only. Application
progress, logs, lifecycle events, and custom events should use the queue event
contract:

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.backends.redis import RedisBackendConfig
   from litestar_queues.events import QueueEventConfig

   config = QueueConfig(
       queue_backend=RedisBackendConfig(url="redis://localhost:6379/0"),
       execution_backend="local",
       event_config=QueueEventConfig(
           enabled=True,
           channels_backend=app_channels_backend,
           publish_global_lifecycle=True,
       ),
   )

``QueueEventConfig.sink`` can point at an in-process test sink, a Litestar
Channels sink, or a custom sink that forwards events outside the application.
Event publishing is best effort by default; set ``strict=True`` when event
delivery failures should fail task execution.

Cloud Run Workers
-----------------

External execution uses the same queue record contract. Configure Cloud Run as
an execution backend and run the packaged entry point in the job container:

.. code-block:: bash

   litestar-queues-cloudrun-worker

The entry point loads configured task modules, reconstructs the queue service,
claims the persisted record, executes the task, publishes lifecycle and task
events through the configured event sink, and exits with a deterministic status
code.

Migration Checklist
-------------------

1. Move generic worker behavior to ``litestar_queues`` configuration.
2. Keep application-specific task modules, settings, and service dependencies
   in the application.
3. Replace private queue storage code with an optional backend subpackage that
   matches the deployed adapter.
4. Replace private progress or log publishers with ``litestar_queues.events``.
5. Add tests that enqueue representative immediate, scheduled, retrying,
   external, and event-publishing tasks against the selected backend.

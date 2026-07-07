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
Scheduled records use ``QueueConfig.quiet_success`` when the task does not set
``quiet_success`` itself, and task-level values continue to win over the config
default.

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
   from litestar_queues.events import EventConfig

   config = QueueConfig(
       queue_backend=RedisBackendConfig(url="redis://localhost:6379/0"),
       execution_backend="local",
       event=EventConfig(
           channels_backend=app_channels_backend,
           publish_global_lifecycle=True,
       ),
   )

``EventConfig.sink`` can point at an in-process test sink, a Litestar
Channels sink, or a custom sink that forwards events outside the application.
Event publishing is best effort by default; set ``strict=True`` when event
delivery failures should fail task execution.

If existing WebSocket routes call ``stream_queue_events(socket, ...)``, replace
them with plugin-owned stream routes configured through ``EventStreamConfig``.
Task-scoped streams are available at ``/queues/events/tasks/{task_id}`` for
WebSocket clients and ``/queues/events/sse/tasks/{task_id}`` for SSE clients by
default. Queue, worker, global, and custom scopes follow the same route pattern.
The old helper is no longer part of the public ``litestar_queues.events`` API.

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

``CloudRunExecutionConfig.fallback_execution_backend`` defaults to ``None``.
This is intentional: dispatch failures should surface instead of silently
rerouting to a backend that may not have a worker. If you want local rerouting,
set ``fallback_execution_backend="local"`` explicitly and run a local worker
that can consume those records.

Stateless Data Migration Runbook
--------------------------------

Use this checklist when moving an existing job table to ``litestar_queues``.
Run it with workers drained unless you have separately verified that old and new
workers can safely share the table during the cutover.

1. Back up the job table and any job-log table before changing schema.
2. Point SQLSpec at the existing table with canonical-to-existing column names:

   .. code-block:: python

      SQLSpecBackendConfig(
          table_name="job",
          column_map={
              "task_name": "function",
              "kwargs_json": "data",
              "task_key": "key",
              "result_json": "result",
              "metadata_json": "metadata",
              "execution_backend": "execution_target",
          },
      )

3. Add the columns required by ``litestar_queues`` when they are missing:
   ``args_json`` (backfill ``[]``), ``queue`` (backfill ``"default"``),
   ``execution_profile``, and ``execution_ref``. Backfill
   ``execution_profile`` and ``execution_ref`` from existing metadata keys such
   as ``profile``, ``execution_ref``, or ``cloudrun_execution`` before adding
   ``NOT NULL`` constraints where your schema requires them.
4. Keep the six lifecycle states unchanged: ``pending``, ``scheduled``,
   ``running``, ``completed``, ``failed``, and ``cancelled``. Drain or
   explicitly account for ``running`` rows before switching workers.
5. If existing result values are wrapped as ``{"result": value}``, unwrap them
   into the raw ``result_json`` value expected by ``litestar_queues``.
6. Rewrite schedule rows:

   * keys from ``scheduled-<name>`` to ``scheduled:<name>``;
   * schedule config from task data into ``metadata_json["schedule"]``;
   * ``max_retries`` to ``0`` unless a task explicitly opts into another value;
   * remove legacy schedule-only columns after validation.

7. Audit task declarations and callers:

   * ``execution_target`` becomes ``execution_backend``;
   * ``profile`` becomes ``execution_profile``;
   * ``requeue`` becomes ``requeue_on_stale``;
   * ``quiet_when_healthy`` becomes ``quiet_success``;
   * retry defaults are ``0`` unless ``retries=`` is explicit;
   * task priority defaults to ``0``; scheduled rows preserve explicit task
     priority;
   * ``TaskResult.wait(max_wait=...)`` becomes ``wait(timeout=...)``; and
   * private progress/log calls become ``publish_task_progress()`` and
     ``publish_task_log()``.

8. Audit cron schedules. Unsupported seconds fields, year fields, ``@reboot``,
   ``L``, ``W``, and ``#`` are rejected at decoration time. Replace those with
   multiple supported schedules or application code.
9. Configure backend-managed event history if the application reads durable job
   logs. Live event sinks are delivery transports only; queryable history should
   be written by the durable queue backend so task state and task events share
   the same database boundary. SQLSpec writes history to the backend-managed
   event-log table, or to the table named by
   ``SQLSpecBackendConfig.event_log_table_name`` when a compatibility view or
   adopter-owned name is required.
10. Configure Cloud Run workers with the new environment contract:
    ``CONFIG_FACTORY`` is required by the packaged worker entry point, and
    queue-specific environment variables should use the ``LITESTAR_QUEUES_*``
    prefix. The Cloud Run worker exits with deterministic status codes, so use
    Cloud Run Job execution status plus queue record status during validation.
11. Configure ``QueueConfig.error_sanitizer`` if persisted errors must not carry
    secrets, tenant data, or provider payloads. The sanitizer controls both the
    stored error and the ``task.failed`` message.
12. Validate by enqueuing representative immediate, scheduled, retrying,
    stale-recovered, cancelled, Cloud Run, and event-publishing tasks against
    the migrated table before opening user traffic.

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

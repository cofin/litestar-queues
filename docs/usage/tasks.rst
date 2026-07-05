=====
Tasks
=====

Decorate a callable with :func:`litestar_queues.task` to register it in the
process task registry:

.. code-block:: python

   from litestar_queues import task


   @task("reports.render", queue="reports", priority=10, retries=2, timeout=120)
   async def render_report(report_id: str) -> str:
       return report_id

The decorated object can still be called directly:

.. code-block:: python

   value = await render_report("report-1")

Enqueueing
==========

Use ``QueueService.enqueue()`` from a route handler, service layer, script, or
worker entry point:

.. code-block:: python

   result = await queue_service.enqueue(render_report, "report-1")
   await result.wait(timeout=30)

``TaskResult`` caches the last known record state. Call ``refresh()`` to reload
the record or ``wait()`` to poll until it reaches a terminal status.

Per-Enqueue Overrides
=====================

Task decorator defaults can be overridden for one enqueue call:

.. code-block:: python

   result = await queue_service.enqueue(
       render_report,
       "report-1",
       queue="slow-reports",
       priority=1,
       retries=5,
       timeout=600,
       run_after=30,
       execution_backend="cloudrun",
       execution_profile="heavy",
       metadata={"requested_by": "user-123"},
   )

Use ``Task.using()`` when a configured copy is easier to pass around:

.. code-block:: python

   heavy_render = render_report.using(execution_backend="cloudrun", execution_profile="heavy")
   await queue_service.enqueue(heavy_render, "report-1")

Deduplication Keys
==================

Pass ``key=`` to deduplicate active work. Queue backends should reuse or replace
records according to the backend contract instead of creating duplicate active
records for the same key:

.. code-block:: python

   await queue_service.enqueue(render_report, "report-1", key="report:report-1")

Task Context
============

When a task accepts ``_job_id`` or ``_task_context``, the worker injects those
values during queued execution:

.. code-block:: python

   from litestar_queues.events import TaskExecutionContext


   @task("imports.run")
   async def run_import(path: str, _job_id: object, _task_context: TaskExecutionContext) -> None:
       await _task_context.progress(current=1, total=10, message=f"Started {_job_id}")

You can also publish through the helper functions while a task context is bound:

.. code-block:: python

   from litestar_queues.events import publish_task_log, publish_task_progress


   @task("imports.process")
   async def process_import(path: str) -> None:
       await publish_task_log("Import started")
       await publish_task_progress(current=5, total=10)

Retries
=======

Unhandled exceptions retry until ``max_retries`` is exhausted. Raise
``NonRetryableError`` or use the ``non_retryable`` helper when a failure should
move directly to the terminal ``failed`` state.

Cancellation
============

Use ``job_cancelled("message")`` from inside a task when the task detects a
domain cancellation and should stop without retrying. The worker marks the
record ``cancelled`` and emits ``task.cancelled``.

Backends also expose ``cancel_tasks(...)`` for exact top-level predicate
cancellation across pending and scheduled records. Pass ``include_running=True``
when the caller is intentionally cancelling running records that cooperate with
``job_cancelled``.

Background Tasks
================

Litestar can run a small function after it sends the HTTP response. Use this
when the route should return first and enqueue the queue job second.

This only saves the job in the queue. Your queue settings decide when the task
runs.

If the job must be saved before the response is sent, enqueue inside the route
instead:

.. code-block:: python

   result = await queue_service.enqueue(background_process, 42)
   return {"task_id": str(result.id)}

Native BackgroundTask
---------------------

Pass the task's ``enqueue`` method to Litestar's ``BackgroundTask``:

.. code-block:: python

   from litestar import post, Response
   from litestar.background_tasks import BackgroundTask
   from litestar_queues import task

   @task("tasks.background_process")
   async def background_process(value: int) -> None:
       pass

   @post("/trigger")
   async def trigger() -> Response[dict[str, str]]:
       return Response(
           {"status": "queued"},
           background=BackgroundTask(background_process.enqueue, 42)
       )

QueuedBackgroundTask Helper
---------------------------

Use ``QueuedBackgroundTask`` when you want to pass the queue task itself. It
finds the app's active ``QueueService`` for you.

``QueuePlugin`` must be registered on the app before you use this helper.

.. code-block:: python

   from litestar import post, Response
   from litestar_queues import QueuedBackgroundTask, task

   @task("tasks.background_process")
   async def background_process(value: int) -> None:
       pass

   @post("/trigger")
   async def trigger() -> Response[dict[str, str]]:
       return Response(
           {"status": "queued"},
           background=QueuedBackgroundTask(background_process, 42)
       )

If the route already receives ``QueueService`` from Litestar, pass it to the
helper:

.. code-block:: python

   from litestar.di import NamedDependency


   @post("/trigger")
   async def trigger(queue_service: NamedDependency[QueueService]) -> Response[dict[str, str]]:
       return Response(
           {"status": "queued"},
           background=QueuedBackgroundTask(background_process, 42, service=queue_service)
       )

Which Form To Use
-----------------

Use ``await queue_service.enqueue(...)`` when the job must be saved before the
route returns.

Use ``BackgroundTask(background_process.enqueue, ...)`` when you want Litestar's
standard background task behavior.

Use ``QueuedBackgroundTask(background_process, ...)`` when you want a short
helper that finds ``QueueService`` for you.

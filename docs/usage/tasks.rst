==================
Define and enqueue
==================

Start with a named task and enqueue it through the injected
:class:`~litestar_queues.QueueService`:

.. code-block:: python

   from litestar import post
   from litestar.di import NamedDependency
   from litestar_queues import QueueService, task


   @task("reports.render", queue="reports", timeout=120)
   async def render_report(report_id: str) -> str:
       return report_id


   @post("/reports/{report_id:str}")
   async def start_report(
       report_id: str,
       queue_service: NamedDependency[QueueService],
   ) -> dict[str, str]:
       result = await queue_service.enqueue(render_report, report_id)
       return {"task_id": str(result.id)}

The decorator registers the function as ``reports.render``. Enqueueing saves
its arguments and returns a :class:`~litestar_queues.TaskResult`. It does not
wait for the task function to finish.

Enqueue by name
===============

String names let a caller avoid importing the task function:

.. code-block:: python

   result = await queue_service.enqueue("reports.render", "report-123")

The task module must still be imported before execution. Set
``QueueConfig(task_modules=("myapp.tasks",))`` or call
:func:`~litestar_queues.discover_tasks` during startup.

Choose where defaults live
==========================

Put stable defaults on ``@task`` and request-specific values on
``QueueService.enqueue()``. See :doc:`task-options` for retries, priority,
delay, keys, and metadata. See :doc:`results` when a caller must observe
completion.

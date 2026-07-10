============
Task options
============

Set behavior that applies to every enqueue on the decorator:

.. code-block:: python

   from litestar_queues import task


   @task(
       "reports.render",
       queue="reports",
       retries=2,
       timeout=120,
       priority=5,
       run_after=10,
       key="daily-report",
   )
   async def render_report(report_id: str) -> str:
       return report_id

``retries`` is the number of retries after the first attempt. ``timeout`` is
the maximum run time in seconds. Workers claim lower numeric priority values
first. ``run_after`` delays when the task can run. ``key`` names the logical
job and prevents more than one active record for that key.

Override one enqueue
====================

.. code-block:: python

   result = await queue_service.enqueue(
       render_report,
       "report-123",
       retries=4,
       timeout=600,
       priority=1,
       run_after=30,
       key="report:report-123",
       metadata={"requested_by": "user-42"},
       description="Render the monthly report",
   )

Queue records store JSON-like arguments and metadata. A persistent backend can
store only values supported by its serializer.

Execution overrides
===================

``execution_backend`` selects inline, local-worker, or external execution for
one task. ``execution_profile`` lets an external backend select a configured
profile. These options change placement, not queue persistence.

Use a key when duplicate requests should share one active record. If an active
record already has that key, enqueueing returns that record. A task in a final
state does not reserve the key forever, so the next enqueue can create a new
record. Omit the key, or generate a different one, when concurrent
invocations are wanted.

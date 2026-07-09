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
the execution ceiling in seconds. Lower numeric priority values are claimed
first. ``run_after`` delays eligibility. ``key`` deduplicates non-terminal
records for the same logical job.

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

Queue records store JSON-like arguments and metadata. Persistent backends
require values their serializer can encode.

Execution overrides
===================

``execution_backend`` selects inline, local-worker, or external execution for
one task. ``execution_profile`` lets an external backend select a configured
profile. These options change placement, not queue persistence.

Use a key only when duplicate requests should share one active record. A
terminal record does not permanently reserve the key.

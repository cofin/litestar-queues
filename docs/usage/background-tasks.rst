====================
Background responses
====================

Litestar background tasks and queue tasks begin at different points in the
response lifecycle.

Persist before responding
=========================

Enqueue inside the handler when the task record must exist before Litestar
sends the response:

.. code-block:: python

   @post("/imports")
   async def start_import(
       queue_service: NamedDependency[QueueService],
   ) -> dict[str, str]:
       result = await queue_service.enqueue(process_import, "/tmp/data.csv")
       return {"task_id": str(result.id)}

If enqueueing fails, the handler fails and no success response is sent.

Enqueue after the response starts
=================================

:class:`~litestar_queues.QueuedBackgroundTask` uses Litestar's response
background phase:

.. code-block:: python

   from litestar import Response, post
   from litestar_queues import QueuedBackgroundTask


   @post("/imports/deferred")
   async def defer_import() -> Response[dict[str, str]]:
       return Response(
           {"status": "accepted"},
           background=QueuedBackgroundTask(process_import, "/tmp/data.csv"),
       )

Here the response lifecycle begins first and the queue record is persisted
afterward. The plugin keeps its application-scoped service alive for this
phase. This shape cannot return a queue task ID in the initial body unless the
application creates and manages one separately.

A plain Litestar ``BackgroundTask`` executes its callable in the web process;
it does not persist a queue record. ``QueuedBackgroundTask`` uses the same
response hook but enqueues through the active ``QueuePlugin`` service.

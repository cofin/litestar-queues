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

Here Litestar starts the response before it saves the queue record. The plugin
keeps the app's queue service open until that step finishes. The initial
response cannot include a queue task ID unless the application creates and
manages an ID separately.

A plain Litestar ``BackgroundTask`` runs its function in the web process. It
does not save a queue record. ``QueuedBackgroundTask`` uses the same response
hook, but adds the task through the active ``QueuePlugin`` service.

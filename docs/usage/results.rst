-------
Results
-------

Every enqueue returns a :class:`~litestar_queues.TaskResult` linked to its
``QueueService``:

.. code-block:: python

   result = await queue_service.enqueue(render_report, "report-123")
   await result.wait(timeout=30)

   if result.status == "completed":
       report_path = result.result
   elif result.status == "failed":
       error = result.error

``wait()`` checks the task until its state is ``completed``, ``failed``, or
``cancelled``. It raises ``TimeoutError`` when its timeout expires. That local
timeout does not stop the queued task.

Refresh cached state
====================

``TaskResult.status``, ``result``, ``error``, and ``record`` are cached. Call:

.. code-block:: python

   await result.refresh()

before reading a later state. Persistent backends return a new record object
instead of changing the original object in place. Call ``refresh()`` so the
same code works with every backend.

Result ownership
================

The queue backend owns the source record. A ``TaskResult`` is a handle to that
record and needs its queue service for ``refresh()`` or ``wait()``. Cleanup may
eventually remove finished records. Copy business results into app-owned
storage when you must keep them indefinitely.

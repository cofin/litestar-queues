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

``wait()`` polls until ``completed``, ``failed``, or ``cancelled`` and raises
``TimeoutError`` if its timeout expires. The queue record continues running
after that local timeout.

Refresh cached state
====================

``TaskResult.status``, ``result``, ``error``, and ``record`` are cached. Call:

.. code-block:: python

   await result.refresh()

before reading later state. This is essential with persistent backends, which
return fresh record objects instead of mutating the enqueue-time object in
place. Code that works with every backend should refresh explicitly.

Result ownership
================

The queue backend owns the authoritative record. A ``TaskResult`` is only a
handle and needs its associated service for ``refresh()`` or ``wait()``.
Terminal cleanup policies can eventually remove records, so copy business
results into application-owned storage when they must be retained indefinitely.

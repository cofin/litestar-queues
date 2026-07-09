=============
Testing tasks
=============

Use immediate execution when a test should receive the terminal record before
``enqueue()`` returns:

.. code-block:: python

   from litestar_queues import QueueConfig, QueueService


   async def test_report_task() -> None:
       async with QueueService(
           QueueConfig(queue_backend="memory", execution_backend="immediate")
       ) as service:
           result = await service.enqueue(render_report, "report-123")

       assert result.status == "completed"
       assert result.result == "report-123"

Use local execution when the worker lifecycle is part of the behavior. Start a
``Worker``, enqueue work, and await ``TaskResult.wait()`` before asserting the
terminal state. Do not treat ``Worker.run_once()`` as a completion barrier.

Backend contracts
=================

Persistent backends return fresh record objects. Always call
``await result.refresh()`` before post-execution assertions so the test does
not depend on memory backend object mutation.

For event publishing and stream tests, see :doc:`event-testing`. Repository
contributors should use :doc:`../contributing/testing` for the package's unit,
integration, and browser commands.

=============
Testing tasks
=============

Use immediate execution when a test should receive the terminal record before
``enqueue()`` returns:

.. code-block:: python

   from litestar_queues import QueueConfig, QueueService, WorkerConfig, task


   @task("reports.render")
   async def render_report(report_id: str) -> str:
       return report_id


   async def test_report_task() -> None:
       config = QueueConfig(
           queue_backend="memory",
           execution_backend="immediate",
           worker=WorkerConfig(placement="external"),
       )

       async with QueueService(config) as service:
           result = await service.enqueue(render_report, "report-123")

       assert result.status == "completed"
       assert result.result == "report-123"

``execution_backend="immediate"`` runs the task inline at enqueue time, so it
needs ``WorkerConfig(placement="external")``: the default server placement would
start a worker with nothing to claim, and the config rejects that combination.

Use local execution when the test covers worker behavior. Start a ``Worker``,
enqueue work, and await ``TaskResult.wait()`` before checking the final state.
``Worker.run_once()`` schedules claimed work; it does not mean the work has
finished.

Backend contracts
=================

Persistent backends return fresh record objects. Always call
``await result.refresh()`` before post-execution assertions so the test does
not depend on memory backend object mutation.

For event publishing and stream tests, see :doc:`event-testing`. Repository
contributors should use :doc:`../contributing/testing` for the package's unit,
integration, and browser commands.

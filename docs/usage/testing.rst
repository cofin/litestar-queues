Testing
-------

The memory queue backend is the default backend for tests and local
development. Combine it with immediate execution when a test needs completed
results without a worker:

.. code-block:: python

   from litestar_queues import QueueConfig, QueueService, task

   @task("math.double")
   async def double(value: int) -> int:
       return value * 2

   config = QueueConfig(queue_backend="memory", execution_backend="immediate")

   async with QueueService(config) as queue_service:
       result = await queue_service.enqueue(double, 21)

   assert result.status == "completed"
   assert result.result == 42

Use ``QueueConfig.provide_service()`` when a test needs the same lifecycle shape
as Litestar dependency injection:

.. code-block:: python

   async with config.provide_service() as queue_service:
       assert queue_service.config is config

Use ``execution_backend="local"`` with ``Worker.run_once()`` when a test needs
to assert queued state before processing.

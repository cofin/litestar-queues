Quickstart
==========

Install the package:

.. code-block:: bash

   pip install litestar-queues

Register the plugin with a Litestar application:

.. code-block:: python

   from litestar import Litestar
   from litestar_queues import QueueConfig, QueuePlugin

   config = QueueConfig(
       storage_backend="memory",
       execution_backend="immediate",
       start_worker=False,
   )

   app = Litestar(plugins=[QueuePlugin(config=config)])

The plugin registers a ``queue_service`` dependency:

.. code-block:: python

   from litestar import post
   from litestar_queues import QueueService

   @post("/tasks/{task_name:str}")
   async def create_task(task_name: str, queue_service: QueueService) -> dict[str, str]:
       await queue_service.enqueue(task_name)
       return {"status": "queued"}

The first scaffold focuses on configuration, plugin registration, and backend
extension points. Task persistence, result handles, and worker execution are
added in later chapters.

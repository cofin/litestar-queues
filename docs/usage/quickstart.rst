Quickstart
==========

Install the package:

.. code-block:: bash

   pip install litestar-queues

Register the plugin with a Litestar application:

.. code-block:: python

   from litestar import Litestar
   from litestar_queues import QueueConfig, QueuePlugin, task

   @task("accounts.sync", queue="accounts", retries=3, timeout=300)
   async def sync_account(account_id: str) -> dict[str, str]:
       return {"account_id": account_id, "status": "synced"}

   config = QueueConfig(
       queue_backend="memory",
       execution_backend="local",
       start_worker=True,
   )

   app = Litestar(plugins=[QueuePlugin(config=config)])

The plugin registers a ``queue_service`` dependency:

.. code-block:: python

   from litestar import post
   from litestar_queues import QueueService

   @post("/accounts/{account_id:str}/sync")
   async def create_task(account_id: str, queue_service: QueueService) -> dict[str, str]:
       result = await queue_service.enqueue(sync_account, account_id)
       return {"task_id": str(result.id), "status": result.status or "queued"}

Tasks can also execute immediately without a Litestar app:

.. code-block:: python

   result = await sync_account.enqueue("acct-123")

   assert result.status == "completed"
   assert result.result == {"account_id": "acct-123", "status": "synced"}

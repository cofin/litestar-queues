===================
Test task events
===================

Use :class:`~litestar_queues.events.InMemoryQueueEventSink` for focused
publisher and task tests:

.. code-block:: python

   from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
   from litestar_queues.events import (
       EventDeliveryConfig,
       InMemoryQueueEventSink,
       QueueEventsConfig,
       publish_task_progress,
   )


   @task("catalog.import")
   async def process_import(path: str) -> int:
       await publish_task_progress(current=1, total=2, message=f"reading {path}")
       return 2


   async def test_import_publishes_progress() -> None:
       sink = InMemoryQueueEventSink()
       config = QueueConfig(
           queue_backend="memory",
           execution_backend="immediate",
           worker=WorkerConfig(placement="external"),
           events=QueueEventsConfig(delivery=EventDeliveryConfig(sinks=(sink,), buffer=None)),
       )

       async with QueueService(config) as service:
           await service.enqueue(process_import, "/tmp/data.csv")

       assert any(event.type == "task.progress" for event in sink.events)

``execution_backend="immediate"`` runs the task inline at enqueue time, so it
needs ``WorkerConfig(placement="external")``: the default server placement would
start a worker with nothing to claim, and the config rejects that combination.

``EventDeliveryConfig(buffer=None)`` delivers every event immediately. Leave the
default buffer in place only when the assertion looks at events published after
the task reaches its final state, or flush the buffer first.

Inspect ``sink.published`` when channel names matter. Use
``events_for(channel)`` for one task or queue channel.

Stream tests should cover the package's SSE/WebSocket routes, authorization,
content type, keepalives, and event envelopes. Use the Playwright suite in
``src/tests/integration/e2e`` for browser behavior. Curl cannot prove that HTMX starts,
updates the page, reconnects, or closes sockets.

Use unique Redis/Valkey queue and Channels prefixes in topology tests. Never
flush a shared service globally.

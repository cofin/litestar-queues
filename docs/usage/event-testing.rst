===================
Test task events
===================

Use :class:`~litestar_queues.events.InMemoryQueueEventSink` for focused
publisher and task tests:

.. code-block:: python

   from litestar_queues import QueueConfig, QueueService
   from litestar_queues.events import EventConfig, InMemoryQueueEventSink


   async def test_import_publishes_progress() -> None:
       sink = InMemoryQueueEventSink()
       config = QueueConfig(
           execution_backend="immediate",
           event=EventConfig(sink=sink),
       )

       async with QueueService(config) as service:
           await service.enqueue(process_import, "/tmp/data.csv")

       assert any(event.type == "task.progress" for event in sink.events)

Inspect ``sink.published`` when channel names matter. Use
``events_for(channel)`` for one task or queue channel. Turn off buffering, or
flush it, before checking events that occur before the task's final state.

Stream tests should cover the package's SSE/WebSocket routes, authorization,
content type, keepalives, and event envelopes. Use the Playwright suite in
``src/tests/e2e`` for browser behavior. Curl cannot prove that HTMX starts,
updates the page, reconnects, or closes sockets.

Use unique Redis/Valkey queue and Channels prefixes in topology tests. Never
flush a shared service globally.

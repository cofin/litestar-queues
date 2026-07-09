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

Inspect ``sink.published`` when the channel names matter, or
``events_for(channel)`` for one canonical task/queue channel. Disable buffering
or flush it before assertions that require intermediate non-terminal events.

Stream tests should focus on package-owned SSE/WebSocket route behavior,
authorization, content type, keepalives, and envelope delivery. Browser-facing
claims belong to the Playwright suite in ``src/tests/e2e`` because curl cannot
prove HTMX boot, DOM updates, reconnection, or socket cleanup.

Use unique Redis/Valkey queue and Channels prefixes in topology tests. Never
flush a shared service globally.

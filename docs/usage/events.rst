======
Events
======

Queue events deliver task lifecycle, progress, log, and custom updates to
application-owned realtime infrastructure. They are distinct from queue backend
wakeup notifications.

Enable Events
=============

Events are disabled by default. Enable them with a sink or an app-owned Litestar
Channels backend:

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.events import QueueEventConfig


   config = QueueConfig(
       event_config=QueueEventConfig(
           enabled=True,
           channels_backend=channels,
           publish_global_lifecycle=True,
       ),
   )

Use ``strict=True`` when publish failures should fail the task execution path.
The default is best-effort publishing with warnings on sink failures.

Event Envelope
==============

``QueueEvent`` is a stable JSON-compatible envelope. It preserves null-valued
fields so clients can rely on a consistent shape for lifecycle, progress, log,
and custom events. Core fields include:

* ``id`` and ``schema_version``,
* ``type`` and ``scope``,
* ``task_id``, ``task_name``, ``queue``, ``worker_id``,
* execution backend and profile,
* attempt and sequence,
* progress, level, message, actor, entity, and payload fields,
* ``occurred_at`` as an ISO timestamp.

Publishing From Tasks
=====================

Tasks can accept ``_task_context`` or use the helper functions that publish
through the current task context:

.. code-block:: python

   from litestar_queues import task
   from litestar_queues.events import publish_task_event, publish_task_log, publish_task_progress


   @task("videos.transcode")
   async def transcode_video(video_id: str) -> None:
       await publish_task_log("Transcode started", payload={"video_id": video_id})
       await publish_task_progress(current=1, total=4, message="Fetched source")
       await publish_task_event("task.event", message="Preview ready", payload={"video_id": video_id})

Channels
========

``QueueEventPublisher`` resolves canonical channels from the event scope:

* task channels: ``litestar_queues:task:<task-id>:events``,
* queue channels: ``litestar_queues:queue:<queue>:events``,
* worker channels: ``litestar_queues:worker:<worker-id>:events``,
* global channel: ``litestar_queues:global:events``,
* custom channels: ``litestar_queues:custom:<scope-key>:events``.

Explicit channels passed by task helpers are appended and duplicates are
removed.

Streaming With Litestar Channels
================================

``ChannelsQueueEventSink`` publishes event JSON to an app-owned Channels backend
or plugin. Use ``stream_queue_events()`` from a WebSocket route when clients need
live updates:

.. code-block:: python

   from litestar import websocket
   from litestar.connection import WebSocket
   from litestar_queues.events import QueueChannels, stream_queue_events


   @websocket("/jobs/{task_id:str}/events")
   async def task_events(socket: WebSocket, task_id: str) -> None:
       await stream_queue_events(socket, [QueueChannels.task(task_id)])

The application owns route paths, authorization, tenant filtering, and the
Channels backend configuration.

Testing Events
==============

Use ``InMemoryQueueEventSink`` for tests:

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.events import InMemoryQueueEventSink, QueueEventConfig


   sink = InMemoryQueueEventSink()
   config = QueueConfig(event_config=QueueEventConfig(enabled=True, sink=sink))

   # After task execution:
   assert [event.type for event in sink.events] == ["task.started", "task.completed"]

=============
Observability
=============

Litestar Queues can report traces, metrics, and logs for enqueueing, task
execution, worker claims and errors, idle waits, stale recovery, heartbeats,
Cloud Run dispatch, and Cloud Run status checks. These signals are called
telemetry.

Install Extras
==============

OpenTelemetry and Prometheus are optional:

.. code-block:: bash

   pip install litestar-queues[otel]
   pip install litestar-queues[prometheus]
   pip install "litestar-queues[otel,prometheus]"

Configure a Litestar App
========================

Set ``enable_otel=True`` or ``enable_prometheus=True`` on
``ObservabilityConfig``. You may enable both. The queue plugin starts telemetry
with the Litestar app. In-app workers, request handlers, and plugin-owned event
streams then use the same settings.

.. code-block:: python

   from litestar import Litestar
   from litestar_queues import QueueConfig, QueuePlugin
   from litestar_queues.observability import ObservabilityConfig

   app = Litestar(
       route_handlers=[...],
       plugins=[
           QueuePlugin(
               QueueConfig(
                   observability=ObservabilityConfig(
                       enable_otel=True,
                       enable_prometheus=True,
                   )
               )
           ),
       ],
   )

Standalone Services and CLI Workers
===================================

Use the same settings when constructing a standalone service:

.. code-block:: python

   from litestar_queues import QueueConfig, QueueService
   from litestar_queues.observability import ObservabilityConfig

   queue_config = QueueConfig(
       observability=ObservabilityConfig(
           enable_otel=True,
           enable_prometheus=True,
       )
   )

   async with QueueService(queue_config) as queue_service:
       ...

CLI workers should load a config factory that returns the same settings:

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.observability import ObservabilityConfig


   def create_queue_config() -> QueueConfig:
       return QueueConfig(
           observability=ObservabilityConfig(
               enable_otel=True,
               enable_prometheus=True,
           ),
           in_app_worker=False,
       )

.. code-block:: bash

   LITESTAR_QUEUES_CONFIG_FACTORY=app.queue:create_queue_config litestar queues run

Trace Context
=============

Litestar Queues creates a producer span around ``QueueService.enqueue()``. A
span is one timed operation in a distributed trace. When OpenTelemetry is
enabled, Litestar Queues stores the current W3C trace context in the queue
record's reserved ``_otel_context`` metadata key.

It creates a consumer span around ``QueueService.execute_record()``. Local,
immediate, and Cloud Run execution all call this method. Before running task
code, the method restores the parent trace context from the queue record.

Do not write application metadata under ``_otel_context``. That key is reserved
for trace propagation.

Bounded Attributes
==================

Queue telemetry limits attributes and metric labels to the following known
values. This prevents unbounded label counts:

- ``messaging.system``
- ``messaging.operation.name``
- ``messaging.destination.name``
- ``messaging.message.id`` on spans only
- ``queue.task.name``
- ``queue.task.status``
- ``queue.task.attempt``
- ``queue.backend``
- ``queue.execution.backend``
- ``queue.execution.profile``
- ``queue.worker.id`` on spans only when a worker id already exists
- ``scope`` on plugin-owned stream metrics
- ``reason`` on stream authorization-denial metrics only

The package never uses task arguments, results, arbitrary metadata, tenant IDs,
user IDs, job IDs, exception messages, or Cloud Run execution references as
metric labels.

Stream Metrics
==============

When ``QueueConfig.event_stream`` and observability are enabled, plugin-owned
WebSocket and SSE streams report metrics through the same runtime. Stream
labels use only ``scope`` and, for denied access, ``reason``. They never include
task IDs, queue names, tenant IDs, user IDs, exception messages, or payload
fields.

.. list-table::
   :header-rows: 1

   * - Metric
     - Type
     - Labels
     - Meaning
   * - ``litestar_queues.stream.connections``
     - Counter
     - ``scope``
     - Stream connections accepted by scope.
   * - ``litestar_queues.stream.active``
     - Gauge / OTel UpDownCounter
     - ``scope``
     - Active stream connections, incremented on connect and decremented on
       disconnect.
   * - ``litestar_queues.stream.events_sent``
     - Counter
     - ``scope``
     - Queue events sent to stream clients.
   * - ``litestar_queues.stream.dedup_drops``
     - Counter
     - ``scope``
     - Duplicate events dropped within one connection by ``eventKey`` or ``id``.
   * - ``litestar_queues.stream.heartbeats``
     - Counter
     - ``scope``
     - WebSocket ping frames or SSE keepalive comments sent.
   * - ``litestar_queues.stream.auth_denials``
     - Counter
     - ``scope``, ``reason``
     - Stream subscription denials. ``reason`` is a small category such as
       ``authz``.
   * - ``litestar_queues.stream.connection.duration``
     - Histogram
     - ``scope``
     - Stream connection lifetime in seconds.

Event Buffer Signals
====================

Live event buffering keeps the number of distinct metric labels small. Buffer
overflow handling does not add task IDs or payload data to labels.

When ``EventBufferConfig.overflow`` drops events, each buffer emits this warning
once:

.. code-block:: text

   Queue event buffer full; dropping event

The log record includes the event scope and type, but not task IDs, payloads,
or arbitrary metadata. ``drop_oldest`` removes a pending event before it adds
the new event. ``drop_newest`` rejects the incoming event. ``block`` waits for
a flush. ``error`` raises ``QueueEventBufferFull``.

Flush and publish failures are also logged without payload data:

.. code-block:: text

   Queue event buffer flush failed
   Queue event batch publish failed
   Queue event publish failed

By default, these failures do not fail the task. Set
``EventConfig(strict=True)`` when the caller must receive the exception.

SQLSpec Coexistence
===================

SQLSpec continues to control its statement spans, query spans, statement
observers, and lifecycle hooks. Litestar Queues controls queue-specific
telemetry when ``QueueConfig`` receives an ``ObservabilityConfig`` through
``observability=...``.

By default, Litestar Queues disables SQLSpec's custom queue counters and spans
for the SQLSpec backend. This prevents duplicate ``enqueue``, ``claim``,
``complete``, ``fail``, and stale-recovery signals.

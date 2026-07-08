=============
Observability
=============

Litestar Queues can emit package-level queue telemetry for enqueue, task
execution, worker claims, worker loop errors, idle waits, stale recovery,
heartbeats, Cloud Run dispatch, and Cloud Run reconciliation.

Install Extras
==============

OpenTelemetry and Prometheus are optional:

.. code-block:: bash

   pip install litestar-queues[otel]
   pip install litestar-queues[prometheus]
   pip install "litestar-queues[otel,prometheus]"

Configure a Litestar App
========================

Set ``enable_otel=True`` and/or ``enable_prometheus=True`` on
``ObservabilityConfig``. The queue plugin creates the observability runtime
during Litestar startup, so in-app workers, request handlers, and plugin-owned
event streams share the same queue telemetry settings.

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

Producer spans are created around ``QueueService.enqueue()``. When
OpenTelemetry is enabled, the current W3C context is injected into queue record
metadata under the reserved ``_otel_context`` key.

Consumer spans are created around ``QueueService.execute_record()``. Local,
immediate, and Cloud Run entrypoint execution all call that method, so task
execution extracts the parent context from the queued record metadata before
running task code.

Do not write application metadata under ``_otel_context``. That key is reserved
for trace propagation.

Bounded Attributes
==================

Queue telemetry uses bounded attributes and metric labels:

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

The package does not use task args, kwargs, result payloads, arbitrary metadata,
tenant ids, user ids, job ids, exception messages, or Cloud Run execution refs
as metric labels.

Stream Metrics
==============

When ``QueueConfig.event_stream`` is enabled and package observability is
configured, plugin-owned WebSocket and SSE streams record bounded metrics
through the same runtime. Stream labels are limited to ``scope`` and, for
authorization denials, ``reason``. They never include task ids, queue names,
tenant ids, user ids, exception messages, or payload fields.

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

Live event buffering keeps observability low-cardinality. Buffer overflow
handling does not add task IDs or payload data to metric labels.

When ``EventBufferConfig.overflow`` drops events, the buffer emits one bounded
warning per buffer instance:

.. code-block:: text

   Queue event buffer full; dropping event

The log record includes the queue event scope and event type. It does not
include task IDs, payloads, or arbitrary metadata. ``drop_oldest`` drops a
pending event before accepting the new event; ``drop_newest`` drops the incoming
event. ``block`` waits for a flush, and ``error`` raises
``QueueEventBufferFull``.

Flush and publish failures are also logged without payload data:

.. code-block:: text

   Queue event buffer flush failed
   Queue event batch publish failed
   Queue event publish failed

By default these failures are best effort and do not fail the task execution
path. Set ``EventConfig(strict=True)`` when the caller should receive the
exception.

SQLSpec Coexistence
===================

SQLSpec statement spans, query spans, statement observers, and lifecycle hooks
remain controlled by SQLSpec. Package-level queue observability owns
queue-domain telemetry when ``QueueConfig`` is supplied with a
``ObservabilityConfig`` through ``observability=...``.

By default, package-level queue observability disables SQLSpec's custom
queue-domain counters and spans for the SQLSpec queue backend. This avoids
double counting ``enqueue``, ``claim``, ``complete``, ``fail``, and
stale-recovery events.

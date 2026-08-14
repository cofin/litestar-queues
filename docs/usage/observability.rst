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

Both switches accept ``True``, ``False``, or ``None``. ``None`` is the default and
follows the app: queue tracing turns on when the app registers Litestar's
``OpenTelemetryPlugin``, and queue metrics turn on when the app registers
Litestar's Prometheus middleware from ``PrometheusConfig``. An explicit ``True``
or ``False`` always wins, and ``True`` raises when the matching extra is missing.

.. code-block:: python

   from litestar import Litestar
   from litestar.plugins.prometheus import PrometheusConfig, PrometheusController
   from litestar_queues import QueueConfig, QueuePlugin
   from litestar_queues.observability import ObservabilityConfig

   prometheus = PrometheusConfig()

   app = Litestar(
       route_handlers=[PrometheusController],
       middleware=[prometheus.middleware],
       plugins=[QueuePlugin(QueueConfig(observability=ObservabilityConfig()))],
   )

Queue metrics now appear on the same ``/metrics`` endpoint as Litestar's request
metrics, because both register with the default ``prometheus_client`` registry.
Set ``prometheus_registry`` to override that.

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

   from litestar_queues import QueueConfig, QueueService, task
   from litestar_queues.observability import ObservabilityConfig


   @task("reports.render")
   async def render_report(report_id: str) -> str:
       return report_id


   queue_config = QueueConfig(
       observability=ObservabilityConfig(
           enable_otel=True,
           enable_prometheus=True,
       )
   )

   async with QueueService(queue_config) as queue_service:
       result = await queue_service.enqueue(render_report, "report-123")

CLI workers should load a config factory that returns the same settings:

.. code-block:: python

   from litestar_queues import QueueConfig, WorkerConfig
   from litestar_queues.observability import ObservabilityConfig


   def create_queue_config() -> QueueConfig:
       return QueueConfig(
           observability=ObservabilityConfig(
               enable_otel=True,
               enable_prometheus=True,
           ),
           worker=WorkerConfig(placement="external"),
       )

.. code-block:: bash

   QUEUES_CONFIG_FACTORY=app.queue:create_queue_config litestar queues run

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

Queue spans are made current while they are open, so the publish span is the one
that gets propagated and any instrumentation running inside a task -- database
drivers, HTTP clients, log correlation -- nests underneath the consumer span.
Failed tasks and recorded exceptions set the span status to ``ERROR``.

Correlation IDs
===============

When SQLSpec is installed, ``QueueService.enqueue()`` also captures the active
SQLSpec correlation ID into the reserved ``_correlation_id`` metadata key, and the
worker rebinds it for the duration of task execution. SQLSpec's framework
middleware derives that ID from request headers, so a task enqueued during a
request keeps the request's identity in worker logs and SQL long after the
request finished.

Nothing is written when no correlation ID is active. Do not write application
metadata under ``_correlation_id``.

SQLCommenter
============

``enable_sqlcommenter`` follows resolved telemetry by default. When it is on and
the queue uses the SQLSpec backend, queue statements carry Google SQLCommenter
attributes -- including ``traceparent`` and ``correlation_id`` -- so a slow query
in the database can be traced back to the task and request that issued it:

.. code-block:: sql

   SELECT id FROM queue_tasks
   /* correlation_id='request-42',db_driver='postgres',framework='litestar-queues',
      traceparent='00-4bf92f...-00f067aa0ba902b7-01' */

Set ``enable_sqlcommenter=False`` to keep statements unannotated, or ``True`` to
annotate them even when queue tracing and metrics are off.

What Gets Emitted
=================

Every metric name, its exported Prometheus name, and the complete bounded
vocabulary of attributes and label values are listed in
:doc:`../reference/observability`. Queue telemetry never uses task arguments,
results, arbitrary metadata, tenant IDs, user IDs, job IDs, exception messages,
or execution references as metric labels, so dashboards built on these series
cannot blow up their cardinality.

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
``EventDeliveryConfig(strict=True)`` when the caller must receive the exception.

SQLSpec Coexistence
===================

SQLSpec continues to control its statement spans, query spans, statement
observers, and lifecycle hooks. Litestar Queues controls queue-specific
telemetry when ``QueueConfig`` receives an ``ObservabilityConfig`` through
``observability=...``.

By default, Litestar Queues disables SQLSpec's custom queue counters and spans
for the SQLSpec backend. This prevents duplicate ``enqueue``, ``claim``,
``complete``, ``fail``, and stale-recovery signals.

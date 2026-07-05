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

Use ``QueueObservabilityConfig(enable_otel=None)`` when a Litestar app already
uses ``OpenTelemetryPlugin``. The queue plugin checks the app during startup and
only enables OpenTelemetry when that plugin is present.

.. code-block:: python

   from litestar import Litestar
   from litestar_queues import QueueConfig, QueuePlugin
   from litestar_queues.observability import QueueObservabilityConfig

   app = Litestar(
       route_handlers=[...],
       plugins=[
           QueuePlugin(
               QueueConfig(
                   observability=QueueObservabilityConfig(enable_otel=None),
               )
           ),
       ],
   )

Standalone Services and CLI Workers
===================================

Standalone ``QueueService`` instances and ``litestar queues run`` workers do
not have a Litestar app object for auto-detection. Enable OpenTelemetry
explicitly for those paths:

.. code-block:: python

   from litestar_queues import QueueConfig, QueueService
   from litestar_queues.observability import QueueObservabilityConfig

   queue_config = QueueConfig(
       observability=QueueObservabilityConfig(enable_otel=True),
   )

   async with QueueService(queue_config) as queue_service:
       ...

Prometheus metrics require explicit opt-in:

.. code-block:: python

   QueueConfig(
       observability=QueueObservabilityConfig(
           enable_prometheus=True,
       )
   )

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

The package does not use task args, kwargs, result payloads, arbitrary metadata,
tenant ids, user ids, job ids, exception messages, or Cloud Run execution refs
as metric labels.

SQLSpec Coexistence
===================

SQLSpec statement spans, query spans, statement observers, and lifecycle hooks
remain controlled by SQLSpec. Package-level queue observability owns
queue-domain telemetry when ``QueueConfig.observability`` is supplied.

By default, ``QueueObservabilityConfig`` disables SQLSpec's custom queue-domain
counters and spans for the SQLSpec queue backend. This avoids double counting
``enqueue``, ``claim``, ``complete``, ``fail``, and stale-recovery events.

Set ``disable_sqlspec_queue_observability=False`` only when you intentionally
want both package-level queue metrics and SQLSpec queue-domain metrics.

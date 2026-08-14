=========================
Telemetry metric catalog
=========================

Every metric name, attribute, and label value queue telemetry can emit. Turn
telemetry on first with :doc:`../usage/observability`.

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
- ``queue.operation`` on batch-size histograms, currently ``enqueue_many`` or
  ``claim_many``
- ``queue.transport`` on wakeup, listener, and event-delivery metrics
- ``queue.outcome`` on listener and event-delivery metrics
- ``queue.execution.backend``
- ``queue.execution.profile``
- ``queue.execution.status`` on dispatch metrics, one of ``dispatched``,
  ``fallback``, ``error``, ``skipped``, ``cancelled``, ``ownership_lost``,
  ``scheduled``, or ``already_exists``
- ``queue.delivery.outcome`` on the delivery metric, one of ``acknowledged``,
  ``duplicate``, ``retry_scheduled``, or ``transient_error``
- ``queue.repair.outcome`` on the repair metric, one of ``present``,
  ``recreated``, or ``error``
- ``queue.stale.outcome`` on stale-recovery metrics, one of ``requeued``,
  ``failed``, ``skipped``, or ``handler_needed``
- ``queue.expiry.outcome`` on the expiry metric, currently ``expired``
- ``worker.error.type``
- ``worker.wait.kind`` on worker delay and wait metrics, either ``native`` or
  ``polling``
- ``queue.worker.id`` on spans only, when a worker id already exists
- ``scope`` on plugin-owned stream metrics
- ``reason`` on stream authorization-denial metrics only

Counts never appear as label values. Stale recovery reports each outcome as its
own sample rather than encoding the tallies into labels. The
``litestar_queues.expiry`` counter records the number of records transitioned
by a worker sweep as its sample value with the bounded ``expired`` outcome.

Each metric name has exactly one emitter. Dispatch, reconcile, and repair
counters belong to the execution backend, and the heartbeat failure counter
belongs to the heartbeat manager, so a single metric never arrives with two
different label sets.

Queue transport metrics
-----------------------

Transport instrumentation is emitted at logical ownership boundaries rather
than once per record. Batch sizes are actual batch sizes, wakeup counters belong
to the backend notification method, wait timing belongs to the worker, and
event flush timing belongs to the event publisher:

.. list-table::
   :header-rows: 1

   * - Metric
     - Labels
     - Meaning
   * - ``litestar_queues.enqueue.batch.size``
     - ``queue.backend``, ``queue.operation``
     - Records accepted by one ``enqueue_many`` call.
   * - ``litestar_queues.wakeup.emitted``
     - ``queue.backend``, ``queue.transport``
     - Wakeup hints actually sent by a backend.
   * - ``litestar_queues.wakeup.coalesced``
     - ``queue.backend``, ``queue.transport``
     - Per-record hints avoided by a coalesced notification.
   * - ``litestar_queues.worker.poll.empty``
     - ``queue.backend``
     - Worker cycles that found no local or externally claimed work.
   * - ``litestar_queues.worker.poll.delay``
     - ``queue.backend``, ``worker.wait.kind``
     - Configured delay before the next polling or native wait cycle.
   * - ``litestar_queues.worker.wait.duration``
     - ``queue.backend``, ``worker.wait.kind``
     - Time actually spent waiting for work.
   * - ``litestar_queues.worker.wakeup_to_claim.duration``
     - ``queue.backend``, ``queue.transport``
     - Time from a native notification to the following claim attempt.
   * - ``litestar_queues.listener.reconnect``
     - ``queue.backend``, ``queue.transport``
     - Native listener reconnection attempts after a read failure.
   * - ``litestar_queues.listener.error``
     - ``queue.backend``, ``queue.transport``, ``queue.outcome``
     - Native listener failures; the current outcome is ``read_failed``.
   * - ``litestar_queues.claim.batch.size``
     - ``queue.backend``, ``queue.operation``
     - Actual records returned by one ``claim_many`` call.
   * - ``litestar_queues.event.flush.size``
     - ``queue.transport``, ``queue.outcome``
     - Events in one successful or failed live-delivery attempt.
   * - ``litestar_queues.event.flush.duration``
     - ``queue.transport``, ``queue.outcome``
     - Time spent in one live-delivery attempt.
   * - ``litestar_queues.event.dropped``
     - ``queue.transport``, ``queue.outcome``
     - Buffered events dropped with the bounded ``overflow`` outcome.

Event flush outcomes are ``success`` or ``failed``. Transport and backend names
come from the configured built-in vocabulary; arbitrary channel names, payload
fields, task arguments, record identifiers, and exception messages never become
metric labels.

That constraint is why re-checking lost deliveries reports into
``litestar_queues.execution.repair`` rather than joining
``litestar_queues.execution.reconcile``. Both describe bringing a record back in
line with its executor, but they answer different questions and so carry
different label keys, and a collector fixes its label names when the metric is
first registered. Two vocabularies on one name would make whichever backend
recorded second raise instead of counting.

Managed transports
------------------

A backend that hands records to a transport instead of a worker emits three
families:

.. list-table::
   :header-rows: 1

   * - Metric
     - Outcome label
     - What it tells you
   * - ``litestar_queues.execution.dispatch``
     - ``queue.execution.status``
     - Whether a delivery was created for a record that became due.
   * - ``litestar_queues.execution.delivery``
     - ``queue.delivery.outcome``
     - What each arriving delivery did. A queue whose deliveries are mostly
       ``retry_scheduled`` is paying twice for every unit of work; a rising
       ``duplicate`` rate is redelivery, which is expected but worth watching.
   * - ``litestar_queues.execution.repair``
     - ``queue.repair.outcome``
     - How often maintenance finds a delivery the transport lost. On a queue
       nobody polls, a non-zero ``recreated`` rate is the only warning that
       records would otherwise have waited forever.

The delivery metric carries only the execution backend and its outcome. The
route holds a record id, not a record, and labelling it further would mean
reading storage again on the one path that otherwise needs nothing.

Unset Attributes
================

Spans omit an attribute that has no value. A task with no execution profile
carries no ``queue.execution.profile`` attribute at all.

Metrics cannot do that. Prometheus binds label names when a collector is first
constructed, and a later sample with a different key set is rejected, so every
sample of a metric must carry every label. Unset labels therefore carry an empty
value, which is exactly how Prometheus encodes "not set": a label with an empty
value is equivalent to the label being absent, and
``queue_execution_profile=""`` matches series that never had the label.

Prometheus Names
================

Prometheus collectors register with the configured registry, or the default
``prometheus_client`` registry when none is given. Counter instruments carry no
``.count`` suffix -- the instrument type already conveys it -- and names follow
Prometheus convention rather than mirroring the OpenTelemetry instrument names:

.. list-table::
   :header-rows: 1

   * - Instrument
     - Exported Prometheus name
   * - ``litestar_queues.enqueue``
     - ``litestar_queues_enqueue_total``
   * - ``litestar_queues.expiry``
     - ``litestar_queues_expiry_total``
   * - ``litestar_queues.task.execution.duration``
     - ``litestar_queues_task_execution_duration_seconds``
   * - ``litestar_queues.heartbeat.active``
     - ``litestar_queues_heartbeat_active``

Duration histograms use buckets that span sub-millisecond enqueues through
half-hour task executions. Override them with ``duration_buckets`` when your
workload needs a different resolution.

The package never uses task arguments, results, arbitrary metadata, tenant IDs,
user IDs, job IDs, exception messages, or Cloud Run execution references as
metric labels.

Stream Metrics
==============

When ``QueueConfig.events.stream`` and observability are enabled, plugin-owned
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

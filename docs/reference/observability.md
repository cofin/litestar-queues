# Observability & Metric Catalog

API reference for `ObservabilityConfig` and the complete catalog of OpenTelemetry instruments, Prometheus metric names, and bounded label vocabularies emitted by `litestar-queues`.

See [Observability](../events/observability.md) for configuration patterns, W3C trace context propagation, and SQLSpec correlation setup.

## Configuration

::: litestar_queues.observability.ObservabilityConfig
    options:
      inherited_members: false

## Bounded Attributes

Queue telemetry restricts span attributes and metric labels to a fixed, bounded vocabulary to prevent high-cardinality time series explosion:

- `messaging.system`
- `messaging.operation.name`
- `messaging.destination.name`
- `messaging.message.id` (spans only)
- `queue.task.name`
- `queue.task.status`
- `queue.task.attempt`
- `queue.backend`
- `queue.operation` (on batch-size histograms: `enqueue_many` or `claim_many`)
- `queue.transport` (on wakeup, listener, and event-delivery metrics)
- `queue.outcome` (on listener and event-delivery metrics)
- `queue.execution.backend`
- `queue.execution.profile`
- `queue.execution.status` (on dispatch metrics: `dispatched`, `fallback`, `error`, `skipped`, `cancelled`, `ownership_lost`, `scheduled`, or `already_exists`)
- `queue.delivery.outcome` (on delivery metrics: `acknowledged`, `duplicate`, `retry_scheduled`, or `transient_error`)
- `queue.repair.outcome` (on repair metrics: `present`, `recreated`, or `error`)
- `queue.stale.outcome` (on stale-recovery metrics: `requeued`, `failed`, `skipped`, or `handler_needed`)
- `queue.expiry.outcome` (on expiry metrics: `expired`)
- `worker.error.type`
- `worker.wait.kind` (on worker delay and wait metrics: `native` or `polling`)
- `queue.worker.id` (spans only, when a worker ID exists)
- `scope` (on plugin-owned stream metrics)
- `reason` (on stream authorization-denial metrics only)

Counts never appear as label values. The package never uses task arguments, results, arbitrary metadata, tenant IDs, user IDs, job IDs, exception messages, or Cloud Run execution references as metric labels.

## Queue Transport & Worker Metrics

| Metric | Labels | Description |
| :--- | :--- | :--- |
| `litestar_queues.enqueue` | `queue.backend`, `queue.task.name`, `messaging.destination.name`, `queue.execution.backend`, `queue.execution.profile` | Total tasks enqueued. |
| `litestar_queues.enqueue.duration` | `queue.backend`, `queue.task.name`, `messaging.destination.name`, `queue.execution.backend`, `queue.execution.profile` | Enqueue latency in seconds. |
| `litestar_queues.enqueue.batch.size` | `queue.backend`, `queue.operation` | Records accepted by one `enqueue_many` call. |
| `litestar_queues.claim.batch.size` | `queue.backend`, `queue.operation` | Records returned by one `claim_many` call. |
| `litestar_queues.task.execution` | `queue.backend`, `queue.task.name`, `messaging.destination.name`, `queue.task.status`, `queue.execution.backend`, `queue.execution.profile` | Total task executions by terminal or retry status. |
| `litestar_queues.task.execution.duration` | `queue.backend`, `queue.task.name`, `messaging.destination.name`, `queue.task.status`, `queue.execution.backend`, `queue.execution.profile` | Task execution wall-clock duration in seconds. |
| `litestar_queues.wakeup.emitted` | `queue.backend`, `queue.transport` | Wakeup notifications sent by a backend. |
| `litestar_queues.wakeup.coalesced` | `queue.backend`, `queue.transport` | Per-record hints coalesced into a batch notification. |
| `litestar_queues.worker.poll.empty` | `queue.backend` | Worker poll cycles that found no ready work. |
| `litestar_queues.worker.poll.delay` | `queue.backend`, `worker.wait.kind` | Configured delay before the next polling or native wait cycle. |
| `litestar_queues.worker.wait.duration` | `queue.backend`, `worker.wait.kind` | Time spent waiting for work. |
| `litestar_queues.worker.wakeup_to_claim.duration` | `queue.backend`, `queue.transport` | Latency from a native wakeup notification to the following claim attempt. |
| `litestar_queues.listener.reconnect` | `queue.backend`, `queue.transport` | Native wakeup listener reconnection attempts after a read failure. |
| `litestar_queues.listener.error` | `queue.backend`, `queue.transport`, `queue.outcome` | Native wakeup listener failures (`read_failed`). |
| `litestar_queues.event.flush.size` | `queue.transport`, `queue.outcome` | Events in one live-delivery flush (`success` or `failed`). |
| `litestar_queues.event.flush.duration` | `queue.transport`, `queue.outcome` | Duration of one live-delivery flush attempt. |
| `litestar_queues.event.dropped` | `queue.transport`, `queue.outcome` | Buffered events dropped due to `overflow`. |

## Managed Execution Transport Metrics

| Metric | Outcome Label | Description |
| :--- | :--- | :--- |
| `litestar_queues.execution.dispatch` | `queue.execution.status` | Dispatch outcomes when handing a due record to an external transport. |
| `litestar_queues.execution.delivery` | `queue.delivery.outcome` | Delivery outcomes (`acknowledged`, `duplicate`, `retry_scheduled`, `transient_error`). |
| `litestar_queues.execution.repair` | `queue.repair.outcome` | Maintenance delivery repair checks (`present`, `recreated`, `error`). |
| `litestar_queues.execution.reconcile` | `queue.stale.outcome` | External execution state reconciliation outcomes. |

## Prometheus Naming Conventions

Prometheus collectors register with `ObservabilityConfig.prometheus_registry` (or the default `prometheus_client` registry). Instrument names are translated to standard Prometheus conventions:

| OpenTelemetry Instrument | Exported Prometheus Metric |
| :--- | :--- |
| `litestar_queues.enqueue` | `litestar_queues_enqueue_total` |
| `litestar_queues.expiry` | `litestar_queues_expiry_total` |
| `litestar_queues.task.execution.duration` | `litestar_queues_task_execution_duration_seconds` |
| `litestar_queues.heartbeat.active` | `litestar_queues_heartbeat_active` |

## Realtime Stream Metrics

When `QueueConfig.events.stream` and observability are enabled, plugin-owned SSE and WebSocket streams report metrics using only `scope` (and `reason` for authorization denials):

| Metric | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `litestar_queues.stream.connections` | Counter | `scope` | Stream connections accepted by scope. |
| `litestar_queues.stream.active` | Gauge / UpDownCounter | `scope` | Currently active stream connections. |
| `litestar_queues.stream.events_sent` | Counter | `scope` | Queue events delivered to stream clients. |
| `litestar_queues.stream.dedup_drops` | Counter | `scope` | Duplicate events dropped within a connection by `eventKey` or `id`. |
| `litestar_queues.stream.heartbeats` | Counter | `scope` | WebSocket ping frames or SSE keepalive comments sent. |
| `litestar_queues.stream.auth_denials` | Counter | `scope`, `reason` | Stream subscription denials (such as `authz`). |
| `litestar_queues.stream.connection.duration` | Histogram | `scope` | Stream connection lifetime in seconds. |

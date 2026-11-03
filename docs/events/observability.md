# Observability (OpenTelemetry & Prometheus)

[`ObservabilityConfig`](../reference/observability.md#litestar_queues.observability.ObservabilityConfig) integrates `litestar-queues` with OpenTelemetry distributed tracing and metrics, Prometheus collectors, SQLSpec correlation IDs, and Google SQLCommenter query attribution.

## Runnable Configuration Example

```python title="docs/examples/events/observability_config.py"
--8<-- "docs/examples/events/observability_config.py"
```

## Automatic Litestar Plugin Detection

By default, `enable_otel`, `enable_prometheus`, and `enable_sqlcommenter` are set to `None` (auto-detect):

| Setting | Default (`None`) Behavior | Explicit `True` | Explicit `False` |
| :--- | :--- | :--- | :--- |
| `enable_otel` | Enables OpenTelemetry tracing and metrics when `opentelemetry` is installed and Litestar's `OpenTelemetryPlugin` is registered on the app | Forces OpenTelemetry instrumentation (raises `MissingDependencyError` if `litestar-queues[otel]` is not installed) | Disables OpenTelemetry spans and metrics |
| `enable_prometheus` | Enables Prometheus collectors when `prometheus_client` is installed and Litestar's `PrometheusConfig` middleware is active on the app | Forces Prometheus metric collection (raises `MissingDependencyError` if `litestar-queues[prometheus]` is not installed) | Disables Prometheus collectors |
| `enable_sqlcommenter` | Enabled automatically whenever `should_enable_otel(app)` or `should_enable_prometheus(app)` resolves to `True` | Appends SQLCommenter tags to SQLSpec queue queries | Disables SQLCommenter query tags |

## W3C Trace Context & Correlation ID Propagation

When `QueueService.enqueue()` or `enqueue_many()` persists a `QueuedTaskRecord`:

1. **OpenTelemetry Context (`_otel_context`)**: `QueueObservabilityRuntime.inject_trace_context()` serializes the active W3C `traceparent` / `tracestate` headers into `QueuedTaskRecord.metadata["_otel_context"]` and creates a `PRODUCER` span (`litestar_queues.enqueue`).
2. **Consumer Span Linking**: When a worker, Cloud Tasks route, Cloud Run Job, or broker consumer executes the record, `extract_trace_context()` restores the parent context and starts a linked `CONSUMER` span (`litestar_queues.process`) carrying `messaging.system="litestar_queues"`, `messaging.destination.name`, `queue.task.name`, `queue.execution.backend`, `queue.task.attempt`, and `queue.task.status`.
3. **SQLSpec Correlation ID (`_correlation_id`)**: If SQLSpec's `CorrelationContext` is active during `enqueue()`, the correlation ID is captured into `QueuedTaskRecord.metadata["_correlation_id"]` and automatically rebound around the task body during execution.

## Google SQLCommenter Query Attribution

When `enable_sqlcommenter` resolves to `True` on the SQLSpec storage backend, `SQLSpecQueueBackend` enables SQLCommenter annotations on its managed adapter configuration so database slow-query logs and `pg_stat_statements` attribute queue SQL statements to the active task and trace context. If your application supplies a shared `SQLSpec` instance with its own custom observability configuration, `litestar-queues` coexists cleanly without overwriting adopter-configured telemetry hooks.

## Exported Metrics & Bounded Cardinality

All OpenTelemetry and Prometheus metrics strictly bound label cardinality—high-cardinality identifiers (`task_id`, `worker_id`, raw error strings, and arguments) are recorded only on trace spans and structured logs, never as metric labels.

| Metric Name | Kind | Unit | Labels | Description |
| :--- | :--- | :--- | :--- | :--- |
| `litestar_queues.task.enqueue` | Counter | `1` | `messaging.destination.name`, `queue.task.name`, `queue.execution.backend`, `queue.execution.profile` | Count of enqueued task records |
| `litestar_queues.task.execution` | Counter | `1` | `messaging.destination.name`, `queue.task.name`, `queue.execution.backend`, `queue.execution.profile`, `queue.task.status` | Count of task execution attempts by outcome status |
| `litestar_queues.task.execution.duration` | Histogram | `s` | `messaging.destination.name`, `queue.task.name`, `queue.execution.backend`, `queue.execution.profile`, `queue.task.status` | Task execution attempt duration in seconds (`DEFAULT_DURATION_BUCKETS`: `5ms` to `1800s`) |
| `litestar_queues.enqueue.batch.size` | Histogram | `records` | `queue.backend`, `queue.operation` | Batch size distribution for `enqueue_many` calls |
| `litestar_queues.claim.batch.size` | Histogram | `records` | `queue.backend`, `queue.operation` | Number of due records claimed per worker batch |
| `litestar_queues.wakeup.emitted` / `coalesced` | Counter | `hints` | `queue.backend`, `queue.transport` | Worker wakeup notifications emitted and coalesced |
| `litestar_queues.worker.wakeup_to_claim.duration` | Histogram | `s` | `queue.backend`, `queue.transport` | End-to-end latency from wakeup signal to task claim |
| `litestar_queues.event.flush.size` / `duration` | Histogram | `events` / `s` | `queue.transport`, `queue.outcome` | Live event buffer flush sizes and latencies |
| `litestar_queues.event.dropped` | Counter | `events` | `queue.transport`, `queue.outcome` | Events dropped when the live ring buffer overflows `EventBufferConfig.max_size` |

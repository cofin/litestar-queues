# Runtime Namespace Customization

By default, `QueuePlugin` registers dependency injection providers and application state keys under the default [`QueueNamespace`](../reference/plugin-and-config.md#litestar_queues.namespace.QueueNamespace) (`DEFAULT_NAMESPACE`). Customizing `QueueConfig.names` isolates state keys, dependency injection targets, and telemetry identifiers.

## Default Namespace Keys

| Field on `QueueNamespace` | Default Value | Purpose |
| :--- | :--- | :--- |
| `service` | `"queue_service"` | Litestar DI dependency key and `app.state` key for `QueueService` |
| `worker` | `"queue_worker"` | `app.state` key for the in-app `Worker` instance when `placement="asgi"` |
| `event_publisher` | `"queue_event_publisher"` | Litestar DI dependency key and `app.state` key for `QueueEventPublisher` |
| `event_producer` | `"queue_event_producer"` | Litestar DI dependency key for `QueueEventProducer` |
| `event_channels` | `"queue_event_channels"` | `app.state` key for the auto-created `ChannelsPlugin` bridge |
| `event_channel` | `"litestar_queues.events"` | Channel topic name used by `ChannelsQueueEventSink` |
| `observability_runtime` | `"queue_observability_runtime"` | `app.state` key for the active `QueueObservabilityRuntime` |
| `lifespan_task_name` | `"litestar-queues-worker"` | `asyncio.Task` name assigned to the in-app worker loop |
| `logger_name` | `"litestar_queues"` | Python `logging.Logger` name used by `QueueService` and `Worker` |
| `instrumentation_name` | `"litestar_queues"` | OpenTelemetry tracer and meter instrumentation scope name |

## Deriving a Prefixed Namespace

Use `QueueNamespace.prefixed(prefix)` to generate a collision-free namespace when integrating alongside custom application state keys:

```python
from litestar_queues import QueueConfig, QueueNamespace

billing_names = QueueNamespace.prefixed("billing")
config = QueueConfig(
    queue_backend="memory",
    names=billing_names,
)
```

`QueueNamespace.prefixed("billing")` produces:

- `service="billing_queue_service"`
- `worker="billing_queue_worker"`
- `event_publisher="billing_queue_event_publisher"`
- `event_producer="billing_queue_event_producer"`
- `event_channel="billing.queues.events"`
- `logger_name="billing.queues"`
- `instrumentation_name="billing.queues"`

`QueueNamespace` validates that all state and DI keys (`service`, `worker`, `event_publisher`, `event_producer`, `event_channels`, `observability_runtime`) are valid Python identifiers and pairwise distinct, raising `QueueConfigurationError` on collisions.

## Subprocess Environment Contract Stability

While `QueueNamespace` customizes in-process Litestar DI keys, `app.state` slots, and logger/tracer names, subprocess bootstrap environment variables remain fixed across all namespaces:

- **`QUEUES_CONFIG_FACTORY`**: Dotted `module:callable` path used by standalone CLI commands (`litestar queues ...`) and Cloud Run Jobs (`python -m litestar_queues.consumer`) when starting without a Litestar `--app` target.
- **`QUEUES_TASK_ID`**: Default environment variable read by `litestar queues run-task` (`python -m litestar_queues.consumer`) and injected by `CloudRunExecutionBackend` for one-shot task execution.

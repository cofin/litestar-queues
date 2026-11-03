# RabbitMQ 4.3+ Execution

[`RabbitMQExecutionBackend`](../reference/execution-and-workers.md#litestar_queues.execution.rabbitmq.RabbitMQExecutionBackend) (configured via [`RabbitMQExecutionConfig`](../reference/execution-and-workers.md#litestar_queues.execution.rabbitmq.RabbitMQExecutionConfig)) dispatches ID-only task messages to RabbitMQ 4.3+ quorum queues with native delayed redelivery support.

## Installation

=== "uv"

    ```bash
    uv add "litestar-queues[rabbitmq]"
    ```

=== "pip"

    ```bash
    pip install "litestar-queues[rabbitmq]"
    ```

=== "pdm"

    ```bash
    pdm add "litestar-queues[rabbitmq]"
    ```

=== "poetry"

    ```bash
    poetry add "litestar-queues[rabbitmq]"
    ```

## Configuration

```python
from litestar_queues import QueueConfig, WorkerConfig
from litestar_queues.backends.redis import RedisBackendConfig
from litestar_queues.execution.rabbitmq import RabbitMQExecutionConfig

config = QueueConfig(
    queue_backend=RedisBackendConfig(url="redis://redis.internal:6379/0"),
    execution_backend=RabbitMQExecutionConfig(
        amqp_url="amqps://app:secret@rabbitmq.internal:5671/prod",
        queue_name="litestar-queues.tasks",
        declare_queue=True,
        delayed_retry_type="returned",
        delayed_retry_min_ms=1_000,
        delayed_retry_max_ms=30_000,
        dispatch_stale_after=60,
    ),
    worker=WorkerConfig(placement="external", max_concurrency=16),
)
```

### Configuration Fields

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `amqp_url` | `str` | Required | `amqp://` or `amqps://` connection URL (redacted from `repr()` and telemetry attributes) |
| `queue_name` | `str \| None` | `None` | Broker queue name (derived from `QueueConfig.names` when omitted) |
| `declare_queue` | `bool` | `True` | Declare the durable quorum queue (`x-queue-type="quorum"`) on startup; when `False`, verifies existence passively |
| `dispatch_stale_after` | `int` | `60` | Seconds after which an unacknowledged dispatch reservation can be rotated and republished |
| `api_timeout` | `float` | `30.0` | Timeout in seconds for AMQP connection and publish operations |
| `delayed_retry_type` | `Literal["disabled", "all", "failed", "returned"]` | `"returned"` | RabbitMQ 4.3+ quorum queue `x-delayed-retry-type` category |
| `delayed_retry_min_ms` | `int` | `1000` | Minimum broker-managed redelivery delay (`x-delayed-retry-min`) in milliseconds |
| `delayed_retry_max_ms` | `int` | `30000` | Maximum broker-managed redelivery delay (`x-delayed-retry-max`) in milliseconds |
| `consumer_timeout_ms` | `int \| None` | `None` | Optional per-queue `x-consumer-timeout` in milliseconds (must exceed maximum task runtime) |

## Running the Dispatcher & Quorum Queue Consumers

Start the dispatcher (`litestar queues run`) and one or more RabbitMQ consumer processes (`litestar queues run-consumer --backend rabbitmq`):

```bash
litestar --app my_app.main:app queues run
litestar --app my_app.main:app queues run-consumer --backend rabbitmq --max-concurrency 16
```

Consumers set AMQP `basic_qos(prefetch_count=concurrency)` to bound in-flight deliveries per process, verify the attempt generation against the storage backend, and send `basic_ack` once the attempt completes or `basic_nack(requeue=True)` if the attempt crashed before reaching a durable status transition.

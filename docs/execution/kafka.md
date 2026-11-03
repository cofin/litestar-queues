# Apache Kafka Execution

[`KafkaExecutionBackend`](../reference/execution-and-workers.md#litestar_queues.execution.kafka.KafkaExecutionBackend) (configured via [`KafkaExecutionConfig`](../reference/execution-and-workers.md#litestar_queues.execution.kafka.KafkaExecutionConfig)) dispatches ID-only task records to Apache Kafka (or Redpanda) topics and executes them across coordinated consumer groups.

## Installation

=== "uv"

    ```bash
    uv add "litestar-queues[kafka]"
    ```

=== "pip"

    ```bash
    pip install "litestar-queues[kafka]"
    ```

=== "pdm"

    ```bash
    pdm add "litestar-queues[kafka]"
    ```

=== "poetry"

    ```bash
    poetry add "litestar-queues[kafka]"
    ```

## Configuration

```python
from litestar_queues import QueueConfig, WorkerConfig
from litestar_queues.backends.redis import RedisBackendConfig
from litestar_queues.execution.kafka import KafkaExecutionConfig

config = QueueConfig(
    queue_backend=RedisBackendConfig(url="redis://redis.internal:6379/0"),
    execution_backend=KafkaExecutionConfig(
        bootstrap_servers="kafka-1.internal:9092,kafka-2.internal:9092",
        topic="litestar-queues-tasks",
        consumer_group="litestar-queues-consumers",
        dispatch_stale_after=60,
        producer_options={"security_protocol": "PLAINTEXT"},
        consumer_options={"security_protocol": "PLAINTEXT"},
    ),
    worker=WorkerConfig(placement="external", max_concurrency=16),
)
```

### Configuration Fields

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `bootstrap_servers` | `str` | Required | Comma-separated Kafka or Redpanda bootstrap broker addresses (redacted from `repr()`) |
| `topic` | `str` | `"litestar-queues"` | Kafka topic receiving ID-only task dispatch records |
| `consumer_group` | `str` | `"litestar-queues"` | Consumer group identifier used by `litestar queues run-consumer --backend kafka` |
| `dispatch_stale_after` | `int` | `60` | Seconds after which an unacknowledged dispatch reservation can be rotated and republished |
| `api_timeout` | `float` | `30.0` | Timeout in seconds for producer `send_and_wait` acknowledgements |
| `producer_options` | `dict[str, Any]` | `{}` | Additional `AIOKafkaProducer` keyword options (such as TLS or SASL credentials) |
| `consumer_options` | `dict[str, Any]` | `{}` | Additional `AIOKafkaConsumer` keyword options (such as TLS, SASL, or `max_poll_interval_ms`) |

!!! note "Backend-Owned Kafka Client Settings"
    `KafkaExecutionBackend` enforces `acks="all"` on the producer and `enable_auto_commit=False` on the consumer to guarantee at-least-once delivery. Passing `acks`, `bootstrap_servers`, `auto_offset_reset`, `enable_auto_commit`, or `group_id` inside `producer_options` or `consumer_options` raises `QueueConfigurationError`.

## Running the Dispatcher & Consumer Group

Start the dispatcher (`litestar queues run`) and one or more Kafka consumer group members (`litestar queues run-consumer --backend kafka`):

```bash
litestar --app my_app.main:app queues run
litestar --app my_app.main:app queues run-consumer --backend kafka --concurrency 16
```

Each consumer commits the partition consumer offset manually after the task attempt reaches a durable transition in the storage backend. Partition consumer offset tracking is strictly a transport cursor; authoritative task state and retry fencing reside in the storage backend.

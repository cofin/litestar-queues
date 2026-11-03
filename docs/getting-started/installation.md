# Installation & Extras

`litestar-queues` supports **Python 3.11 through 3.15** and requires `litestar>=2.24.0`.

## Core Package

The base installation includes the core task runtime, `InMemoryQueueBackend`, `EphemeralQueueBackend` (built on Python's standard-library `sqlite3`), `LocalExecutionBackend`, `ImmediateExecutionBackend`, cron/interval scheduling, and realtime SSE/WebSocket event streams.

=== "uv"

    ```bash
    uv add litestar-queues
    ```

=== "pip"

    ```bash
    pip install litestar-queues
    ```

=== "pdm"

    ```bash
    pdm add litestar-queues
    ```

=== "poetry"

    ```bash
    poetry add litestar-queues
    ```

## Storage Backend Extras

Install the extra corresponding to your production database or key-value store:

### SQLSpec (SQLite, PostgreSQL, CockroachDB, MySQL, MariaDB, Oracle, DuckDB, Spanner)

=== "uv"

    ```bash
    uv add "litestar-queues[sqlspec]"
    ```

=== "pip"

    ```bash
    pip install "litestar-queues[sqlspec]"
    ```

=== "pdm"

    ```bash
    pdm add "litestar-queues[sqlspec]"
    ```

=== "poetry"

    ```bash
    poetry add "litestar-queues[sqlspec]"
    ```

### Advanced Alchemy (SQLAlchemy 2.0 Async)

=== "uv"

    ```bash
    uv add "litestar-queues[advanced-alchemy]"
    ```

=== "pip"

    ```bash
    pip install "litestar-queues[advanced-alchemy]"
    ```

=== "pdm"

    ```bash
    pdm add "litestar-queues[advanced-alchemy]"
    ```

=== "poetry"

    ```bash
    poetry add "litestar-queues[advanced-alchemy]"
    ```

### Redis (`redis>=8.1.0`) or Valkey (`valkey>=6.0.0`)

=== "uv"

    ```bash
    uv add "litestar-queues[redis]"
    uv add "litestar-queues[valkey]"
    ```

=== "pip"

    ```bash
    pip install "litestar-queues[redis]"
    pip install "litestar-queues[valkey]"
    ```

=== "pdm"

    ```bash
    pdm add "litestar-queues[redis]"
    pdm add "litestar-queues[valkey]"
    ```

=== "poetry"

    ```bash
    poetry add "litestar-queues[redis]"
    poetry add "litestar-queues[valkey]"
    ```

## Execution Transport & Observability Extras

| Extra | Package Dependency | Purpose |
| :--- | :--- | :--- |
| `cloud-tasks` | `google-cloud-tasks>=2.23.0` | Serverless HTTP push dispatch via `CloudTasksExecutionConfig` and `/_litestar-queues/cloud-tasks`. |
| `cloudrun` | `google-cloud-run>=0.10.0` | Serverless batch container execution via `CloudRunExecutionConfig` and `litestar queues run-task`. |
| `pubsub` | `google-cloud-pubsub>=2.39.0` | Google Cloud Pub/Sub ID-only execution dispatch via `PubSubExecutionConfig`. |
| `sqs` | `aiobotocore>=3.8.0` | AWS SQS standard and FIFO execution dispatch via `SQSExecutionConfig`. |
| `rabbitmq` | `aio-pika>=10.0.1` | RabbitMQ AMQP 0-9-1 execution dispatch via `RabbitMQExecutionConfig`. |
| `kafka` | `aiokafka>=0.14.0` | Apache Kafka execution dispatch via `KafkaExecutionConfig`. |
| `otel` | `opentelemetry-api`, `opentelemetry-sdk` | OpenTelemetry distributed tracing spans and metrics via `ObservabilityConfig(enable_otel=True)`. |
| `prometheus` | `prometheus-client` | Prometheus queue depth, latency, and outcome counters via `ObservabilityConfig(enable_prometheus=True)`. |

Combine multiple extras in a single install command:

=== "uv"

    ```bash
    uv add "litestar-queues[sqlspec,cloud-tasks,cloudrun,otel]"
    ```

=== "pip"

    ```bash
    pip install "litestar-queues[sqlspec,cloud-tasks,cloudrun,otel]"
    ```

=== "pdm"

    ```bash
    pdm add "litestar-queues[sqlspec,cloud-tasks,cloudrun,otel]"
    ```

=== "poetry"

    ```bash
    poetry add "litestar-queues[sqlspec,cloud-tasks,cloudrun,otel]"
    ```

# Google Cloud Pub/Sub Execution

[`PubSubExecutionBackend`](../reference/execution-and-workers.md#litestar_queues.execution.pubsub.PubSubExecutionBackend) (configured via [`PubSubExecutionConfig`](../reference/execution-and-workers.md#litestar_queues.execution.pubsub.PubSubExecutionConfig)) dispatches ID-only task notifications over Google Cloud Pub/Sub topics and executes them across pull consumer fleets.

## Installation

=== "uv"

    ```bash
    uv add "litestar-queues[pubsub]"
    ```

=== "pip"

    ```bash
    pip install "litestar-queues[pubsub]"
    ```

=== "pdm"

    ```bash
    pdm add "litestar-queues[pubsub]"
    ```

=== "poetry"

    ```bash
    poetry add "litestar-queues[pubsub]"
    ```

## Configuration

```python
from litestar_queues import QueueConfig, WorkerConfig
from litestar_queues.backends.redis import RedisBackendConfig
from litestar_queues.execution.pubsub import PubSubExecutionConfig

config = QueueConfig(
    queue_backend=RedisBackendConfig(url="redis://redis.internal:6379/0"),
    execution_backend=PubSubExecutionConfig(
        project_id="my-gcp-project",
        topic_id="litestar-queue-tasks",
        subscription_id="litestar-queue-tasks-sub",
        ack_deadline=60,
        ack_extension_interval=30,
        dispatch_stale_after=60,
    ),
    worker=WorkerConfig(placement="external", max_concurrency=16),
)
```

### Configuration Fields

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `project_id` | `str` | Required | Google Cloud project owning the Pub/Sub topic and subscription |
| `topic_id` | `str` | Required | Pub/Sub topic receiving ID-only task dispatch messages |
| `subscription_id` | `str` | Required | Pull subscription read by `litestar queues run-consumer --backend pubsub` |
| `ack_deadline` | `int` | `60` | Initial Pub/Sub message ack deadline in seconds (`10`–`600`) |
| `ack_extension_interval` | `int` | `30` | Interval in seconds at which the consumer extends the Pub/Sub message ack deadline while the task executes (`0 < ack_extension_interval < ack_deadline`) |
| `dispatch_stale_after` | `int` | `60` | Seconds after which an unacknowledged dispatch reservation can be rotated and republished |
| `api_timeout` | `float` | `30.0` | Timeout in seconds for unary Pub/Sub gRPC API calls |
| `api_endpoint` | `str \| None` | `None` | Optional custom endpoint or local Pub/Sub emulator host (`localhost:8085`) |
| `api_insecure` | `bool` | `False` | Use plaintext gRPC when connecting to a local emulator (`requires api_endpoint`) |

## Running the Dispatcher & Pull Consumers

Start the dispatcher (`litestar queues run`) and one or more Pub/Sub consumer processes (`litestar queues run-consumer --backend pubsub`):

```bash
litestar --app my_app.main:app queues run
litestar --app my_app.main:app queues run-consumer --backend pubsub --concurrency 16
```

While a task is executing, the consumer automatically sends `modify_ack_deadline` requests every `ack_extension_interval` seconds so long-running tasks are not redelivered by Pub/Sub prematurely, while the storage backend remains the sole authority for fenced task state transitions.

## Required IAM Permissions & Local Emulator Testing

- **Dispatcher Principal**: `roles/pubsub.publisher` (`pubsub.topics.publish`) on `projects/<project_id>/topics/<topic_id>`.
- **Consumer Principal**: `roles/pubsub.subscriber` (`pubsub.subscriptions.consume`) on `projects/<project_id>/subscriptions/<subscription_id>`.
- **Local Emulator**: Point `api_endpoint="localhost:8085"` and `api_insecure=True` on `PubSubExecutionConfig` to test against the Google Cloud Pub/Sub emulator without Google Cloud credentials.

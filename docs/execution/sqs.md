# Amazon SQS Execution

[`SqsExecutionBackend`](../reference/execution-and-workers.md#litestar_queues.execution.sqs.SqsExecutionBackend) (configured via [`SqsExecutionConfig`](../reference/execution-and-workers.md#litestar_queues.execution.sqs.SqsExecutionConfig)) dispatches ID-only task messages to standard or FIFO Amazon SQS queues and executes them across long-polling consumer fleets.

## Installation

=== "uv"

    ```bash
    uv add "litestar-queues[sqs]"
    ```

=== "pip"

    ```bash
    pip install "litestar-queues[sqs]"
    ```

=== "pdm"

    ```bash
    pdm add "litestar-queues[sqs]"
    ```

=== "poetry"

    ```bash
    poetry add "litestar-queues[sqs]"
    ```

## Configuration

```python
from litestar_queues import QueueConfig, WorkerConfig
from litestar_queues.backends.redis import RedisBackendConfig
from litestar_queues.execution.sqs import SqsExecutionConfig

config = QueueConfig(
    queue_backend=RedisBackendConfig(url="redis://redis.internal:6379/0"),
    execution_backend=SqsExecutionConfig(
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/litestar-queues",
        region_name="us-east-1",
        wait_time_seconds=20,
        receive_batch_size=10,
        visibility_timeout=60,
        visibility_extension_interval=30,
        dispatch_stale_after=60,
    ),
    worker=WorkerConfig(placement="external", max_concurrency=16),
)
```

### Configuration Fields

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `queue_url` | `str` | Required | Absolute URL of the target standard or FIFO SQS queue |
| `region_name` | `str \| None` | `None` | Optional AWS region override (defaults to standard AWS SDK resolution) |
| `endpoint_url` | `str \| None` | `None` | Optional custom endpoint override for LocalStack (`http://localhost:4566`) |
| `fifo` | `bool` | `False` | Emit `MessageGroupId` and `MessageDeduplicationId` attributes for `.fifo` queues |
| `message_group_id` | `str \| None` | `None` | Explicit FIFO group identifier (`requires fifo=True`; derived from logical queue name when omitted) |
| `wait_time_seconds` | `int` | `20` | SQS `ReceiveMessage` long-poll duration in seconds (`0`–`20`) |
| `receive_batch_size` | `int` | `10` | Maximum messages requested per `ReceiveMessage` call (`1`–`10`) |
| `visibility_timeout` | `int` | `60` | Initial SQS message visibility timeout in seconds (`0`–`43200`) |
| `visibility_extension_interval` | `int` | `30` | Interval in seconds at which the consumer calls `ChangeMessageVisibility` during execution (`0 < visibility_extension_interval < visibility_timeout`) |
| `dispatch_stale_after` | `int` | `60` | Seconds after which an unacknowledged dispatch reservation can be rotated and republished |
| `api_timeout` | `float` | `30.0` | Timeout in seconds for individual SQS API calls |

## Running the Dispatcher & Long-Poll Consumers

Start the dispatcher (`litestar queues run`) and one or more SQS consumer processes (`litestar queues run-consumer --backend sqs`):

```bash
litestar --app my_app.main:app queues run
litestar --app my_app.main:app queues run-consumer --backend sqs --concurrency 16
```

While a task executes, the consumer extends the SQS message visibility timeout every `visibility_extension_interval` seconds and calls `DeleteMessage` once the attempt reaches a durable storage status transition. Authoritative task ownership and retry fencing are enforced in the storage backend (`expected_retry_count`).

## Required IAM Permissions & LocalStack Testing

- **Dispatcher Principal**: `sqs:SendMessage` and `sqs:GetQueueAttributes` on the target queue ARN.
- **Consumer Principal**: `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility`, and `sqs:GetQueueAttributes` on the target queue ARN.
- **LocalStack Testing**: Set `endpoint_url="http://localhost:4566"` and `region_name="us-east-1"` to run integration tests against LocalStack.

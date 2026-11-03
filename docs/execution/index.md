# Execution Transports Overview

An **execution backend** (`BaseExecutionBackend`) determines *how* a persisted [`QueuedTaskRecord`](../reference/tasks-and-models.md#litestar_queues.models.QueuedTaskRecord) is triggered and where the task callable runs. The storage backend (`BaseQueueBackend`) remains the sole authority for task arguments, status transitions, retry budgets, and fenced heartbeats across all execution transports.

--8<-- "tools/docs/includes/diagram-serverless-scale-to-zero.html"

## Execution Transport Comparison Matrix

| Execution Backend | Config / Selector | Dispatch Mechanism | Scale-to-Zero | Typical Task Duration | Execution Consumer Surface | Remote Provider Cancellation |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Local Worker (`"local"`)** | `"local"` (default) | Worker claims due records from storage | No (long-lived worker) | Seconds to hours | `Worker` (`server`, `asgi`, or `litestar queues run`) | Worker-control wakeup + durability poll |
| **Immediate (`"immediate"`)** | `"immediate"` | Executed inline inside `QueueService.enqueue()` | N/A (in-process) | Milliseconds (testing) | Inline caller coroutine | Coroutine cancellation |
| **Google Cloud Tasks (`"cloudtasks"`)** | [`CloudTasksExecutionConfig`](cloud-tasks.md) | `schedules_on_enqueue=True` (HTTP push delivery) | Yes (`--min-instances 0`) | Up to 30 minutes (`1800s` max deadline) | `POST /_litestar-queues/cloud-tasks` | Yes (`delete_task`) |
| **Google Cloud Run Jobs (`"cloudrun"`)** | [`CloudRunExecutionConfig`](cloud-run.md) | `schedules_on_enqueue=True` (default) or worker dispatcher | Yes (ephemeral job container) | Minutes to 24 hours | `litestar queues run-task` (`litestar-queues-consume`) | Yes (`cancel_execution`) |
| **Google Cloud Pub/Sub (`"pubsub"`)** | [`PubSubExecutionConfig`](pubsub.md) | Worker dispatcher publishes ID-only message | Consumer pool autoscaling | Up to 10m ack window (extended automatically) | `litestar queues run-consumer --backend pubsub` | Storage-fenced claim check + control wakeup |
| **Amazon SQS (`"sqs"`)** | [`SqsExecutionConfig`](sqs.md) | Worker dispatcher publishes ID-only message | Consumer pool autoscaling | Up to 12h visibility window (extended automatically) | `litestar queues run-consumer --backend sqs` | Storage-fenced claim check + control wakeup |
| **RabbitMQ 4.3+ (`"rabbitmq"`)** | [`RabbitMQExecutionConfig`](rabbitmq.md) | Worker dispatcher publishes ID-only message to quorum queue | Consumer pool autoscaling | Configurable consumer timeout | `litestar queues run-consumer --backend rabbitmq` | Storage-fenced claim check + control wakeup |
| **Apache Kafka (`"kafka"`)** | [`KafkaExecutionConfig`](kafka.md) | Worker dispatcher publishes ID-only message to topic | Consumer group scaling | Unbounded (manual offset commit after attempt) | `litestar queues run-consumer --backend kafka` | Storage-fenced claim check + control wakeup |

## Multi-Backend Execution Routing

A single `QueueConfig` can register multiple execution backends via `execution_backends` alongside the default `execution_backend`. Individual tasks select their transport via `@task(..., execution_backend=..., execution_profile=...)`, or callers override it per invocation at `enqueue(..., execution_backend=..., execution_profile=...)`.

```python title="docs/examples/execution/multi_backend_routing.py"
--8<-- "docs/examples/execution/multi_backend_routing.py"
```

## Guides in This Section

<div class="grid cards" markdown>

-   :material-cloud-Sync:{ .lg .middle } **Google Cloud Tasks**

    ---

    Scale-to-zero HTTP push execution with OIDC token verification, 30-day native scheduling horizons, and inline stale recovery.

    [:octicons-arrow-right-24: Cloud Tasks Guide](cloud-tasks.md)

-   :material-docker:{ .lg .middle } **Google Cloud Run Jobs**

    ---

    Isolated one-shot container executions for heavy batch and ML tasks with named hardware profiles and direct enqueue dispatch.

    [:octicons-arrow-right-24: Cloud Run Jobs Guide](cloud-run.md)

-   :material-swap-horizontal:{ .lg .middle } **Broker Transports**

    ---

    Shared ID-only dispatch architecture and consumer lifecycle for Google Cloud Pub/Sub, Amazon SQS, RabbitMQ 4.3+, and Apache Kafka.

    [:octicons-arrow-right-24: Broker Transports Guide](broker-transports.md)

-   :material-wrench-clock:{ .lg .middle } **Bounded Queue Maintenance**

    ---

    Run finite 6-phase repair and retention passes via CLI, Kubernetes CronJobs, or authenticated HTTP endpoints.

    [:octicons-arrow-right-24: Maintenance Guide](maintenance.md)

-   :material-console:{ .lg .middle } **CLI Operations**

    ---

    Operational runbook for `litestar queues run`, `run-consumer`, `run-task`, `run-maintenance`, `status`, and `scheduler-health`.

    [:octicons-arrow-right-24: CLI Operations Guide](cli.md)

</div>

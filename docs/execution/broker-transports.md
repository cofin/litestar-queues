# Broker Execution Transports

In addition to local workers and serverless Google Cloud transports, `litestar-queues` provides four message broker execution transports:

- [Google Cloud Pub/Sub](pubsub.md) (`PubSubExecutionConfig`, extra `litestar-queues[pubsub]`)
- [Amazon SQS](sqs.md) (`SqsExecutionConfig`, extra `litestar-queues[sqs]`)
- [RabbitMQ 4.3+](rabbitmq.md) (`RabbitMQExecutionConfig`, extra `litestar-queues[rabbitmq]`)
- [Apache Kafka](kafka.md) (`KafkaExecutionConfig`, extra `litestar-queues[kafka]`)

## Runnable Configuration Example

```python title="docs/examples/execution/broker_configs.py"
--8<-- "docs/examples/execution/broker_configs.py"
```

## Shared Broker Architecture: Dispatcher + Consumer Fleet

Unlike Cloud Tasks (`schedules_on_enqueue=True`), general-purpose message brokers do not natively store 30-day delayed schedules, uniqueness reservations, or fenced retry state. `litestar-queues` therefore pairs your authoritative storage backend with a two-role broker topology:

1. **Worker Dispatcher (`litestar queues run`)**: Claims due `pending` and `scheduled` records from the storage backend, reserves a monotonic attempt generation (`litestar_queues_attempt`), and publishes an ID-only message (`{"version": 1, "task_id": "<uuid>"}`) to the broker topic or queue.
2. **Broker Consumer Fleet (`litestar queues run-consumer --backend <name>`)**: Pulls ID-only messages from the broker subscription, verifies the attempt generation fence against the storage backend, executes the task body while sending periodic worker heartbeats to the storage backend, and acknowledges the broker message once the attempt reaches a durable storage transition.

## ID-Only Wire Envelope & Generation Fencing

Every broker message payload contains only `{"version": 1, "task_id": "<uuid>"}` plus transport metadata headers (`litestar_queues_attempt`, `litestar_queues_queue`, `litestar_queues_task_name`). Task arguments, keyword arguments, results, and metadata never travel across the broker wire.

Because message brokers provide at-least-once delivery, duplicate or delayed broker redeliveries are fenced against the storage record's `retry_count` and `execution_ref`:

- If a duplicate broker message arrives while the attempt is already terminal or has rotated to a newer `retry_count`, the consumer acknowledges and drops the stale message without re-executing the task.
- If a consumer crashes mid-attempt, `QueueMaintenanceService` (or the dispatcher's reconciliation pass) rotates the stale attempt after `dispatch_stale_after` seconds and republishes a fresh generation.
- Broker-level redelivery timers (such as SQS message visibility, Pub/Sub ack deadlines, or RabbitMQ consumer timeouts) govern only transport message redelivery; authoritative task ownership is always fenced in the storage backend via `expected_retry_count`.

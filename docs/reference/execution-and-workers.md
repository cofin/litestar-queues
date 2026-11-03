# Execution, Workers & Maintenance

API reference for execution backend contracts, serverless and broker execution configurations, worker supervisors, bounded maintenance, and single-record task consumers.

## Execution Backend Registry & Base Contract

::: litestar_queues.execution.get_execution_backend

::: litestar_queues.execution.get_execution_backend_class

::: litestar_queues.execution.list_execution_backends

::: litestar_queues.execution.execution_backend

::: litestar_queues.execution.base.BaseExecutionBackend
    options:
      inherited_members: false
      show_source: false

::: litestar_queues.execution.base.BaseConsumerExecutionBackend
    options:
      inherited_members: false
      show_source: false

::: litestar_queues.execution.base.DispatchRepairResult
    options:
      inherited_members: false

::: litestar_queues.execution.base.ExternalReconciliationResult
    options:
      inherited_members: false

::: litestar_queues.execution.base.ExecutionCancelResult
    options:
      inherited_members: false

## Local & Immediate Execution

::: litestar_queues.execution.local.LocalExecutionBackend
    options:
      inherited_members: false

::: litestar_queues.execution.immediate.ImmediateExecutionBackend
    options:
      inherited_members: false

## Google Cloud Tasks & Cloud Run Jobs

::: litestar_queues.execution.cloudtasks.CloudTasksExecutionConfig
    options:
      inherited_members: false

::: litestar_queues.execution.cloudtasks.CloudTasksExecutionBackend
    options:
      inherited_members: false
      show_source: false
      members:
        - __init__
        - dispatch
        - cancel_execution
        - reconcile
        - repair
        - close

::: litestar_queues.execution.cloudtasks.CloudTasksOIDCGuard
    options:
      inherited_members: false

::: litestar_queues.execution.cloudrun.CloudRunExecutionConfig
    options:
      inherited_members: false

::: litestar_queues.execution.cloudrun.CloudRunExecutionBackend
    options:
      inherited_members: false
      show_source: false
      members:
        - __init__
        - dispatch
        - cancel_execution
        - reconcile
        - repair
        - close

## Broker Transports (Pub/Sub, SQS, RabbitMQ, Kafka)

::: litestar_queues.execution.pubsub.PubSubExecutionConfig
    options:
      inherited_members: false

::: litestar_queues.execution.pubsub.PubSubExecutionBackend
    options:
      inherited_members: false
      show_source: false
      members:
        - __init__
        - dispatch
        - run_consumer
        - close

::: litestar_queues.execution.sqs.SqsExecutionConfig
    options:
      inherited_members: false

::: litestar_queues.execution.sqs.SqsExecutionBackend
    options:
      inherited_members: false
      show_source: false
      members:
        - __init__
        - dispatch
        - run_consumer
        - repair
        - close

::: litestar_queues.execution.rabbitmq.RabbitMQExecutionConfig
    options:
      inherited_members: false

::: litestar_queues.execution.rabbitmq.RabbitMQExecutionBackend
    options:
      inherited_members: false
      show_source: false
      members:
        - __init__
        - dispatch
        - run_consumer
        - close

::: litestar_queues.execution.kafka.KafkaExecutionConfig
    options:
      inherited_members: false

::: litestar_queues.execution.kafka.KafkaExecutionBackend
    options:
      inherited_members: false
      show_source: false
      members:
        - __init__
        - dispatch
        - run_consumer
        - close

## Worker, Consumer & Maintenance

::: litestar_queues.worker.Worker
    options:
      inherited_members: false
      show_source: false

::: litestar_queues.config.WorkerPlacement

::: litestar_queues.consumer.consume_one

::: litestar_queues.consumer.run_task

::: litestar_queues.consumer.TaskExitCode
    options:
      inherited_members: false

::: litestar_queues.maintenance.QueueMaintenanceConfig
    options:
      inherited_members: false

::: litestar_queues.maintenance.QueueMaintenanceService
    options:
      inherited_members: false
      show_source: false

::: litestar_queues.maintenance.QueueMaintenanceSummary
    options:
      inherited_members: false

::: litestar_queues.maintenance.QueueMaintenancePhaseResult
    options:
      inherited_members: false

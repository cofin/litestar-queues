# API & CLI Reference

Curated API reference, CLI subcommand documentation, OpenTelemetry and Prometheus metric catalog, and contributor verification guides for `litestar-queues`.

<div class="grid cards" markdown>

-   :material-cog-outline: **[Plugin, Config & Service](plugin-and-config.md)**

    ---

    `QueuePlugin`, `QueueConfig`, `WorkerConfig`, `QueueMaintenanceConfig`, `QueueService`, `QueueNamespace`, and `QueuedBackgroundTask`.

-   :material-checkbox-marked-circle-outline: **[Tasks, Models & Exceptions](tasks-and-models.md)**

    ---

    `@task`, `Task`, `ScheduleConfig`, `RetryBackoff`, `QueuedTaskRecord`, `TaskRequest`, `TaskReservation`, `TaskResult`, `TaskStatus`, dependency hooks, and exception hierarchy.

-   :material-database-outline: **[Storage Backends](backends.md)**

    ---

    `BaseQueueBackend`, `SQLSpecBackendConfig`, `SQLSpecQueueBackend`, `SQLAlchemyBackendConfig`, `SQLAlchemyBackend`, ORM model mixins, `RedisBackendConfig`, `ValkeyBackendConfig`, `EphemeralQueueBackend`, and `InMemoryQueueBackend`.

-   :material-rocket-launch-outline: **[Execution, Workers & Maintenance](execution-and-workers.md)**

    ---

    `BaseExecutionBackend`, Cloud Tasks, Cloud Run Jobs, Pub/Sub, SQS, RabbitMQ, and Kafka execution configs and backends, `Worker`, `QueueMaintenanceService`, and `consume_one` / `run_task`.

-   :material-broadcast: **[Events, Streams & History](events.md)**

    ---

    `QueueEventsConfig`, `EventDeliveryConfig`, `EventBufferConfig`, `EventStreamConfig`, `EventHistoryConfig`, `EventHistoryExtraColumn`, `TaskExecutionContext`, `QueueEvent`, `QueueEventLog`, `QueueEventQuery`, and sinks.

-   :material-chart-bell-curve-cumulative: **[Observability & Metric Catalog](observability.md)**

    ---

    `ObservabilityConfig` and the complete OpenTelemetry and Prometheus metric, histogram, gauge, and bounded attribute catalog.

-   :material-console: **[CLI Reference](cli.md)**

    ---

    Generated reference for `litestar queues` commands (`run`, `run-consumer`, `run-task`, `run-maintenance`, `status`, `scheduler-health`) and programmatic `litestar_queues.cli` integration.

-   :material-source-pull: **[Contributing & Testing](contributing.md)**

    ---

    Unit, integration, and browser E2E testing tiers, backend registry design, and the documentation verification workflow.

</div>

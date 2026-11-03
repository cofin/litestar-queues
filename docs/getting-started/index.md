# Getting Started

Welcome to `litestar-queues`. Follow this learning path to install the library, run your first background task, and understand the core primitives before moving to production storage and execution topologies.

<div class="grid cards" markdown>

-   :lucide-download: **Installation & Extras**

    ---

    Install `litestar-queues` and optional extras for SQLSpec, Advanced Alchemy, Redis, Valkey, Google Cloud Tasks, Cloud Run Jobs, message brokers, and OpenTelemetry/Prometheus.

    [:lucide-arrow-right: Installation Guide](installation.md)

-   :lucide-zap: **Quickstart**

    ---

    Build and run a complete Litestar application that enqueues background tasks and queries task results with zero external infrastructure required.

    [:lucide-arrow-right: Run the Quickstart](quickstart.md)

-   :lucide-compass: **Core Mental Model**

    ---

    Learn how tasks, authoritative storage backends, ID-only execution transports, worker wakeups, and realtime task events fit together.

    [:lucide-arrow-right: Core Mental Model](concepts.md)

-   :lucide-book-open: **Core Concepts & Guides**

    ---

    Dive deeper into task options, retry backoff, uniqueness reservations, recurring schedules, dependency injection, and testing.

    [:lucide-arrow-right: Core Concepts](../learn/index.md)

</div>

## Progression From Local Dev to Production

| Stage | Storage Backend (`backend`) | Worker Placement (`placement`) | Execution Transport (`execution_backend`) | When to Use |
| :--- | :--- | :--- | :--- | :--- |
| **Local Development** | `"ephemeral"` (default) | `"server"` (default) | `"local"` (default) | Zero-setup `litestar run` with cross-process WAL SQLite shared between web and supervised worker processes. |
| **Unit & Integration Tests** | `"memory"` | `"asgi"` or manual `Worker` | `"local"` or `"immediate"` | Fast, isolated in-process tests without external services. |
| **Dedicated Worker Fleet** | `SQLSpecBackendConfig`, `SQLAlchemyBackendConfig`, `RedisBackendConfig`, or `ValkeyBackendConfig` | `"external"` | `"local"` or Broker (`PubSub`, `SQS`, `RabbitMQ`, `Kafka`) | Production Kubernetes, ECS, or VM deployments with independent web and worker scaling. |
| **Serverless Scale-to-Zero** | `SQLSpecBackendConfig`, `SQLAlchemyBackendConfig`, `RedisBackendConfig`, or `ValkeyBackendConfig` | `"external"` (zero idle workers) | `CloudTasksExecutionConfig` and/or `CloudRunExecutionConfig` | Cloud Run services that scale to zero when idle and dispatch HTTP push webhooks or one-shot Job containers on demand. |

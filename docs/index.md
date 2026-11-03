---
title: Litestar Queues
hide:
  - navigation
  - toc
---

# Litestar Queues

<div class="lq-hero lq-home-hero">
  <img class="lq-hero-banner lq-hero-banner--light" src="assets/banner-inline-light.svg" alt="Litestar Queues" />
  <img class="lq-hero-banner lq-hero-banner--dark" src="assets/banner-inline-dark.svg" alt="Litestar Queues" />
  <p class="lq-hero-lead">
    Async task queues, supervised workers, cron schedules, realtime SSE &amp; WebSocket event streams, and decoupled storage &amp; execution backends for Litestar.
  </p>
  <div class="lq-hero-actions">
    <a class="lq-cta lq-cta--primary" href="getting-started/quickstart/">Get Started</a>
    <a class="lq-cta" href="architecture/">Visual Architecture</a>
    <a class="lq-cta" href="reference/">API Reference</a>
  </div>
</div>

## Decoupled Storage & Execution Architecture

`litestar-queues` separates **where task state lives** (`BaseQueueBackend`) from **how execution is triggered** (`BaseExecutionBackend`). External transports and cloud dispatchers carry only an ID-only `{"version": 1, "task_id": "<uuid>"}` envelope—never serializing task arguments, retry counters, or results onto the wire.

--8<-- "tools/docs/includes/diagram-storage-vs-execution.html"

## Platform Capabilities

<div class="grid cards" markdown>

-   :lucide-layers: **Decoupled Task Storage**

    ---

    Store authoritative task records in SQLSpec (11 database dialects), Advanced Alchemy (SQLAlchemy 2.0), Redis 8.1+, Valkey 6+, cross-process Ephemeral SQLite, or in-memory storage.

    [:lucide-arrow-right: Explore Storage Backends](backends/index.md)

-   :lucide-cloud-lightning: **Serverless & Broker Transports**

    ---

    Route interactive tasks to Google Cloud Tasks OIDC webhooks, heavy batch jobs to Cloud Run Jobs, or distribute work through Pub/Sub, AWS SQS, RabbitMQ, and Apache Kafka.

    [:lucide-arrow-right: Execution & Operations](execution/index.md)

-   :lucide-cpu: **Flexible Worker Placements**

    ---

    Start with zero-config CLI-supervised workers (`placement="server"`), run in-process for tests (`placement="asgi"`), or scale dedicated external worker fleets (`placement="external"`).

    [:lucide-arrow-right: Workers & Placements](learn/workers.md)

-   :lucide-radio: **Realtime SSE & WebSocket Streams**

    ---

    Emit structured progress percentages, stage updates, and logs from inside running tasks to durable `QueueEventLog` history and live `/queues/events` streams.

    [:lucide-arrow-right: Events & Observability](events/index.md)

-   :lucide-calendar-clock: **Cron & Interval Schedules**

    ---

    Declare recurring tasks with 5-field cron expressions or fixed intervals, timezone awareness, catch-up policies, and fenced leader claims.

    [:lucide-arrow-right: Recurring Schedules](learn/schedules.md)

-   :lucide-shield-check: **Fenced Leases & Uniqueness**

    ---

    Prevent duplicate execution and zombie worker overwrites with `(expected_retry_count, expected_execution_ref)` CAS fencing and `unique_until` deduplication windows.

    [:lucide-arrow-right: Task Options & Uniqueness](learn/task-options.md)

</div>

## Install

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

## Quickstart Preview

Define a background task with `@task`, register `QueuePlugin`, and inject `QueueService` into any Litestar route handler:

```python title="app.py"
--8<-- "docs/examples/getting_started/quickstart_app.py"
```

Run your application with the Litestar CLI—`QueuePlugin` automatically supervises a background worker alongside your web server:

```bash
litestar --app app:app run --reload
```

# Core Concepts Overview

This section covers the runtime primitives for defining, scheduling, executing, and testing background tasks in `litestar-queues`.

<div class="grid cards" markdown>

-   :material-function-variant:{ .lg .middle } **Defining & Enqueuing Tasks**

    ---

    Register async and sync task functions with `@task`, discover task modules, and enqueue single or batch workloads through `QueueService`.

    [:octicons-arrow-right-24: Defining & Enqueuing Tasks](tasks.md)

-   :material-tune-variant:{ .lg .middle } **Task Options & Uniqueness**

    ---

    Configure priority, delayed starts, expiration deadlines, runtime timeouts, sync thread-pool offloading, and terminal or forever deduplication keys.

    [:octicons-arrow-right-24: Task Options & Uniqueness](task-options.md)

-   :material-clipboard-check-outline:{ .lg .middle } **Task Results & Waiting**

    ---

    Inspect live task state with `TaskResult` handles, refresh records from storage, and await terminal completion across push or polling backends.

    [:octicons-arrow-right-24: Task Results & Waiting](results.md)

-   :material-refresh-auto:{ .lg .middle } **Failures, Retries & Cancellation**

    ---

    Configure exponential `RetryBackoff`, raise terminal `non_retryable()` or `job_cancelled()` exits, and cancel queued or running tasks cooperatively.

    [:octicons-arrow-right-24: Failures, Retries & Cancellation](failures-and-cancellation.md)

-   :material-calendar-clock:{ .lg .middle } **Recurring Schedules**

    ---

    Declare fixed-interval and five-field cron schedules directly on `@task`, synchronize schedule records at startup, and preserve retry budgets across restarts.

    [:octicons-arrow-right-24: Recurring Schedules](schedules.md)

-   :material-cogs:{ .lg .middle } **Workers & Execution Lifecycle**

    ---

    Choose between `server`, `asgi`, and `external` worker placements, tune per-queue concurrency and adaptive polling, and configure heartbeats and stale recovery.

    [:octicons-arrow-right-24: Workers & Execution Lifecycle](workers.md)

-   :material-needle:{ .lg .middle } **Task Dependency Injection**

    ---

    Inject attempt-scoped async context managers via `task_dependency_provider` or stateless keyword mappings via `task_dependency_resolver`.

    [:octicons-arrow-right-24: Task Dependency Injection](dependency-injection.md)

-   :material-send-clock-outline:{ .lg .middle } **Post-Response Background Tasks**

    ---

    Compare in-handler `await queue_service.enqueue(...)` durability with post-response `QueuedBackgroundTask` execution.

    [:octicons-arrow-right-24: Post-Response Background Tasks](background-responses.md)

-   :material-Sliders:{ .lg .middle } **Plugin & Service Configuration**

    ---

    Wire `QueueConfig`, `WorkerConfig`, `QueueMaintenanceConfig`, `QueueEventsConfig`, and `ObservabilityConfig` into `QueuePlugin`.

    [:octicons-arrow-right-24: Plugin & Service Configuration](configuration.md)

-   :material-tag-multiple-outline:{ .lg .middle } **Runtime Namespace Customization**

    ---

    Customize DI state keys, ASGI lifespan names, and OpenTelemetry logger/tracer names with `QueueNamespace`.

    [:octicons-arrow-right-24: Runtime Namespace Customization](runtime-namespace.md)

-   :material-test-tube:{ .lg .middle } **Testing Tasks & Workers**

    ---

    Test task callables directly, run full enqueue lifecycles inline with `execution_backend="immediate"`, and test single-claim execution with `consume_one`.

    [:octicons-arrow-right-24: Testing Tasks & Workers](testing.md)

</div>

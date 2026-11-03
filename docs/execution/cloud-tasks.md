# Google Cloud Tasks Execution

[`CloudTasksExecutionBackend`](../reference/execution-and-workers.md#litestar_queues.execution.cloudtasks.CloudTasksExecutionBackend) (configured via [`CloudTasksExecutionConfig`](../reference/execution-and-workers.md#litestar_queues.execution.cloudtasks.CloudTasksExecutionConfig)) provides scale-to-zero HTTP push execution on Google Cloud Run without running an always-on polling worker process.

## ID-Only Wire Contract

Every Cloud Tasks HTTP delivery body carries only the protocol version and the record's id:

```json
{
  "version": 1,
  "task_id": "2c8d8f72-6a4b-4c5c-9c82-5e3f21a09b41"
}
```

Task arguments, keyword arguments, result payloads, retry state, and metadata stay in your storage backend and never cross the network inside the Cloud Tasks message. When Cloud Tasks posts the delivery payload to your consumer service, the route handler delegates to [`consume_one(service, task_id)`](../reference/execution-and-workers.md#litestar_queues.consumer.consume_one), which claims the authoritative record from storage by ID and executes the attempt.

## Runnable Configuration Example

```python title="docs/examples/execution/cloud_tasks_config.py"
--8<-- "docs/examples/execution/cloud_tasks_config.py"
```

## Provisioning Cloud Tasks & Cloud Run

### Create the Cloud Tasks Queue

```bash
gcloud tasks queues create primary-tasks \
  --location=us-central1 \
  --max-dispatches-per-second=50 \
  --max-concurrent-dispatches=100 \
  --max-attempts=5 \
  --min-backoff=10s \
  --max-backoff=300s
```

### Deploy the Cloud Run Consumer Service

Because Cloud Tasks pushes each task as an active HTTP request, the Cloud Run service can scale to zero when idle and keep standard request-scoped CPU allocation (`--cpu-throttling`):

```bash
gcloud run deploy worker-service \
  --image=us-central1-docker.pkg.dev/my-gcp-project/apps/worker:latest \
  --region=us-central1 \
  --no-allow-unauthenticated \
  --cpu-throttling \
  --concurrency=8 \
  --min-instances=0 \
  --timeout=900
```

### Required IAM Roles

| Principal | IAM Role | Target Resource | Purpose |
| :--- | :--- | :--- | :--- |
| Cloud Tasks delivery service account (`service_account_email`) | `roles/run.invoker` | Cloud Run consumer service | Allows Cloud Tasks to invoke the private `--no-allow-unauthenticated` Cloud Run service |
| Application runtime service account (producer & consumer) | `roles/cloudtasks.enqueuer` | Cloud Tasks queue | Allows `QueueService.enqueue()` and remote cancellation to create and delete Cloud Tasks |
| Application runtime service account (producer & consumer) | `roles/iam.serviceAccountUser` | Cloud Tasks delivery service account | Allows the producer to attach `oidc_token.service_account_email` when creating Cloud Tasks |

## Route Mounting & OIDC Authentication

When `CloudTasksExecutionConfig.mount_route=True` (the default), `QueuePlugin` registers the delivery endpoint at `/_litestar-queues/cloud-tasks` (or `/_<namespace>/cloud-tasks` when a custom `QueueNamespace` is configured) with `opt={"exclude_from_auth": True, "exclude_from_csrf": True}` so application-wide session or CSRF middleware does not intercept Cloud Tasks deliveries.

To prevent unauthenticated delivery spoofing, `CloudTasksExecutionConfig` requires at least one route protection layer when `mount_route=True`:

1. **In-Process OIDC Verification (`verify_oidc=True`)**: Attaches [`CloudTasksOIDCGuard`](../reference/execution-and-workers.md#litestar_queues.execution.cloudtasks.CloudTasksOIDCGuard), which verifies the `Authorization: Bearer <token>` JWT signature against Google's certs, checks that the token `aud` matches `audience` (defaulting to the `service_url` origin), and verifies that `email` matches `service_account_email`.
2. **Platform IAM (`trust_platform_auth=True`)**: Accepts Cloud Run's `--no-allow-unauthenticated` IAM perimeter (Combine both `trust_platform_auth=True` and `verify_oidc=True` for defense in depth).
3. **Custom Litestar Guards (`guards=(...)`)**: Attaches application-supplied Litestar `Guard` callables to the route.

For public web tiers that only enqueue tasks and never execute them, set `mount_route=False`.

## Delivery HTTP Response Semantics

The `/_litestar-queues/cloud-tasks` endpoint translates `consume_one()` outcomes into deterministic HTTP status codes understood by Cloud Tasks:

| HTTP Status | Condition | Cloud Tasks Behavior |
| :--- | :--- | :--- |
| `204 No Content` | Task completed, permanently failed, was already terminal/cancelled, or was rescheduled for a future retry | Marks the Cloud Tasks delivery succeeded (no transport redelivery needed because `QueueService` already scheduled any retry in storage) |
| `503 Service Unavailable` | Task is currently claimed by another live attempt or crashed mid-attempt before reaching a durable status transition | Retries the HTTP delivery according to the Cloud Tasks queue backoff policy |
| `400 Bad Request` / `422 Unprocessable Entity` | Malformed JSON body, unknown field, or unsupported protocol `version` | Permanent delivery failure |
| `401 Unauthorized` / `403 Forbidden` | Missing or invalid OIDC Bearer token rejected by `CloudTasksOIDCGuard` | Rejects unauthorized caller |

## Two Retry Layers & Inline Stale Recovery

1. **Application Task Retries (`@task(retries=..., retry_backoff=...)`)**: When a task raises a Python exception, `QueueService` catches it, increments `retry_count` in storage, schedules a new Cloud Task at `scheduled_at = now + backoff`, and returns `204 No Content` so the completed HTTP delivery is acknowledged cleanly.
2. **Transport Redelivery & Inline Stale Recovery (`WorkerConfig.stale_after`)**: If a Cloud Run container crashes or hits an OOM kill while a task is `running`, Cloud Tasks receives a `5xx` or connection reset and redelivers the same `{version, task_id}` payload. When `WorkerConfig.stale_after` is configured on the consumer service, the delivery route automatically checks whether the existing `running` claim's `heartbeat_at` has exceeded `stale_after` and recovers the stale claim inline before executing the redelivered attempt.

## Deadlines & the 30-Day Scheduling Horizon

- **Timeout Budget Equation**: Keep task timeouts, response margins, Cloud Tasks dispatch deadlines (`15s`–`1800s`), and Cloud Run request timeouts ordered as:

$$\text{task.timeout} + \text{response\_margin} \le \text{dispatch\_deadline} < \text{Cloud Run } \texttt{--timeout}$$

- **30-Day Horizon (`CLOUD_TASKS_MAX_SCHEDULE_HORIZON`)**: Google Cloud Tasks accepts `schedule_time` up to 30 days in the future. Enqueuing a task or recurring schedule whose `scheduled_at` exceeds 30 days on `"cloudtasks"` raises `QueueConfigurationError` before mutating active schedule records.

## Hybrid Routing with Cloud Run Jobs

Because Cloud Tasks caps `dispatch_deadline` at 30 minutes (`1800s`), combine `CloudTasksExecutionConfig` and `CloudRunExecutionConfig` in `QueueConfig.execution_backends` to run fast tasks over HTTP push and multi-hour jobs on Cloud Run Jobs:

```python
config = QueueConfig(
    queue_backend=backend_config,
    execution_backend=cloud_tasks_config,
    execution_backends=(cloud_tasks_config, cloud_run_config),
    worker=WorkerConfig(placement="external"),
)
```

## Delivery Repair & Remote Cancellation

- **Two-Phase Enqueue Reservation & Repair**: `QueueService.enqueue()` writes an `external-dispatch:<uuid>` reservation sentinel on `QueuedTaskRecord.execution_ref` before calling the Cloud Tasks API, then finalizes `execution_ref` with the created Cloud Task resource path. If a producer process crashes between storage commit and the Cloud Tasks API call, `litestar queues run-maintenance --phase external` (or `QueueService.reconcile_external_tasks()`) repairs the missing Cloud Task automatically.
- **Remote Cancellation**: Calling `await queue_service.cancel_task(task_id)` deletes the scheduled Cloud Task in Google Cloud Tasks (`delete_task`) before marking the storage record `cancelled`.
- **Local Testing Without an Emulator**: Because Cloud Tasks routes through `consume_one(service, task_id)`, unit and integration tests can test the exact execution path with `execution_backend="local"` and `await consume_one(service, handle.id)` without running a Cloud Tasks emulator.

## Choosing Between Google Cloud Execution Products

| Workload Shape | Recommended Transport | Why |
| :--- | :--- | :--- |
| Short-to-medium tasks (`<= 30 min`) with scale-to-zero and sub-second HTTP push | **Google Cloud Tasks** (`CloudTasksExecutionConfig`) | Direct HTTP push to `--min-instances 0 --cpu-throttling` Cloud Run services with native `schedule_time` delays |
| Heavy batch, ETL, or GPU jobs (`> 30 min`, up to 24 hours) | **Google Cloud Run Jobs** (`CloudRunExecutionConfig`) | Dedicated one-shot job containers per task with named hardware `profiles` |
| High-fanout event streaming into always-on consumer pools | **Google Cloud Pub/Sub** (`PubSubExecutionConfig`) | High-throughput pull subscriptions consumed by `litestar queues run-consumer --backend pubsub` |

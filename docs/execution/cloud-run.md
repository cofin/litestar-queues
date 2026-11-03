# Google Cloud Run Jobs Execution

[`CloudRunExecutionBackend`](../reference/execution-and-workers.md#litestar_queues.execution.cloudrun.CloudRunExecutionBackend) (configured via [`CloudRunExecutionConfig`](../reference/execution-and-workers.md#litestar_queues.execution.cloudrun.CloudRunExecutionConfig)) executes each queued task inside an isolated, one-shot Google Cloud Run Job container that scales to zero immediately upon exit.

## Runnable Configuration Example

```python title="docs/examples/execution/cloud_run_config.py"
--8<-- "docs/examples/execution/cloud_run_config.py"
```

## Cloud Run Deployment Topologies

| Topology | `CloudRunExecutionConfig` / `WorkerConfig` | Always-On Instances | Delayed Task Support | Best For |
| :--- | :--- | :--- | :--- | :--- |
| **Direct Enqueue Dispatch (Default)** | `schedules_on_enqueue=True`, `WorkerConfig(placement="external")` | `0` (web tier scales to zero) | Immediate tasks dispatched on `enqueue()`; delayed tasks dispatched by periodic maintenance (`--phase external`) | Scale-to-zero web apps running batch or heavy tasks on demand |
| **Hybrid [Cloud Tasks](cloud-tasks.md) + Cloud Run Jobs** | `execution_backends=(cloud_tasks, cloud_run)`, `WorkerConfig(placement="external")` | `0` (full scale to zero) | Cloud Tasks handles `< 30m` & delayed tasks; Cloud Run Jobs handles `> 30m` batch tasks | Serverless applications needing both sub-second delayed push and multi-hour batch jobs |
| **External dispatcher (`schedules_on_enqueue=False`)** | `schedules_on_enqueue=False`, `litestar queues run` worker service (`--min-instances 1 --no-cpu-throttling`) | `1` lightweight dispatcher | Polls storage continuously and launches Cloud Run Jobs as delayed tasks become due | High-volume delayed Cloud Run Job workloads without external maintenance crons |
| **In-process worker (`placement="asgi"`)** | `execution_backend="local"`, `WorkerConfig(placement="asgi")` (`--min-instances 1 --no-cpu-throttling`) | `1+` always-on service instances | Full local polling and push wakeups | Traditional always-on Cloud Run services without Cloud Run Jobs |
| **Eventarc** | Not implemented | N/A | Not implemented (use [Cloud Tasks](cloud-tasks.md) or [Pub/Sub](pubsub.md) instead) | Not implemented as a `litestar-queues` execution backend |

## Job Container Command & Environment Contract

Configure your Cloud Run Job container entrypoint to invoke the one-shot task consumer CLI (`litestar queues run-task` or the standalone `litestar-queues-consume` binary):

```dockerfile
ENTRYPOINT ["litestar", "--app", "my_app.main:app", "queues", "run-task"]
```

Or without loading a Litestar HTTP application object, point `QUEUES_CONFIG_FACTORY` to a callable returning `QueueConfig`:

```dockerfile
ENV QUEUES_CONFIG_FACTORY="my_app.queues:build_queue_config"
ENV LITESTAR_QUEUES_TASK_MODULES="my_app.tasks"
ENTRYPOINT ["litestar-queues-consume"]
```

When `CloudRunExecutionBackend` triggers `run_job()`, it passes the ID-only override environment variable `QUEUES_TASK_ID=<uuid>` (plus any static `CloudRunExecutionConfig.extra_env` variables) to the container overrides. Task arguments and metadata are never passed in container environment overrides; `run-task` reads `QUEUES_TASK_ID` and claims the authoritative record from your storage backend.

## Execution Profiles & Hardware Sizing

Map logical profile names to distinct Cloud Run Job resources (for example standard CPU vs high-memory vs GPU jobs) using `CloudRunExecutionConfig.profiles`. When resolving which Cloud Run Job to launch for a `QueuedTaskRecord`, `resolve_job_name()` evaluates in strict order:

1. `profiles[record.execution_profile]` when `record.execution_profile` is set and present in `profiles`.
2. `CloudRunExecutionConfig.job_name` when configured.
3. `profiles["default"]` when present.

If none of the three resolves a job name, `QueueConfigurationError` is raised.

## Required IAM Permissions & Cloud SQL Connectivity

Grant the service account executing `QueueService.enqueue()` (and `QueueMaintenanceService`) permission to launch, inspect, and cancel Cloud Run Job executions:

- `run.jobs.run` and `run.jobs.runWithOverrides` (included in `roles/run.developer`)
- `run.executions.get` (used by `reconcile_external_tasks()` to inspect execution completion or failure)
- `run.executions.cancel` (used by `QueueService.cancel_task()` to terminate running job executions)

When connecting both the web service and Cloud Run Jobs to Cloud SQL or Memorystore Redis/Valkey, attach the Cloud SQL Auth Proxy volume (`--add-cloudsql-instances`) or Direct VPC Egress (`--network` / `--subnet`) to both the Cloud Run Service and every target Cloud Run Job definition.

## Dispatch Failure Handling & Deterministic Exit Codes

- **Fallback Backend (`fallback_execution_backend`)**: If `CloudRunExecutionConfig.fallback_execution_backend` is set (for example `"local"`), dispatch failures fall back to the named execution backend; when `None` (default), dispatch errors leave the record eligible for bounded `reconcile_external_tasks()` repair.
- **Deterministic Container Exit Codes (`TaskExitCode`)**: `litestar queues run-task` and `litestar-queues-consume` exit with distinct non-zero codes so Cloud Run Job execution logs reflect the exact outcome:

| Exit Code | `TaskExitCode` Constant | Meaning |
| :--- | :--- | :--- |
| `0` | `TaskExitCode.SUCCESS` | Task claimed, executed, and persisted as `completed` |
| `4` | `TaskExitCode.INVALID_INPUT` | Missing or invalid `QUEUES_TASK_ID` / `--task-id` UUID |
| `6` | `TaskExitCode.NOT_CLAIMABLE` | Record was not found, already terminal, or claimed by another worker |
| `7` | `TaskExitCode.FAILURE` | Task raised an exception (`failed`, `cancelled`, or rescheduled for retry) |
| `8` | `TaskExitCode.EXECUTION_ERROR` | Unhandled infrastructure or storage backend failure during claim/execution |
